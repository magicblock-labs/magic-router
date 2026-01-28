use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};
use clap::Parser;
use solana_pubkey::Pubkey;
use solana_rpc_client::rpc_client::RpcClient;
use solana_signature::Signer;
use solana_transaction::{
    instruction::{AccountMeta, Instruction},
    Transaction,
};
use std::str::FromStr;
use tracing::info;

// Keypair from solana-signature or we can use a simple wrapper
use solana_signature::Keypair;

/// Magic Domain Program ID
const PROGRAM_ID: &str = "DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1";
const ER_RECORD_SEED: &[u8] = b"er-record";

/// Validator keypair bytes
/// mAGicPQYBMvcYveUZA5F5UNNwyHvfYh5xkLS2Fr1mev
const VALIDATOR_KEYPAIR_BYTES: [u8; 64] = [
    7, 83, 184, 55, 200, 223, 238, 137, 166, 244, 107, 126, 189, 16, 194, 36, 228, 68, 43, 143, 13,
    91, 3, 81, 53, 253, 26, 36, 50, 198, 40, 159, 11, 80, 9, 208, 183, 189, 108, 200, 89, 77, 168,
    76, 233, 197, 132, 22, 21, 186, 202, 240, 105, 168, 157, 64, 233, 249, 100, 104, 210, 41, 83,
    87,
];

#[derive(Parser, Debug)]
#[command(name = "Magic Router Setup")]
#[command(about = "Register validator with Magic Domain Program", long_about = None)]
struct Args {
    /// Solana RPC URL
    #[arg(
        short,
        long,
        default_value = "http://localhost:8899",
        env = "SOLANA_RPC"
    )]
    rpc_url: String,

    /// Validator FQDN
    #[arg(
        short,
        long,
        default_value = "http://localhost:7799",
        env = "VALIDATOR_FQDN"
    )]
    fqdn: String,

    /// Block time in milliseconds
    #[arg(long, default_value = "400")]
    block_time_ms: u16,

    /// Base fee in lamports
    #[arg(long, default_value = "5000")]
    base_fee: u16,

    /// Country code (3 chars)
    #[arg(long, default_value = "USA")]
    country_code: String,
}

/// ErStatus enum
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
#[repr(u8)]
enum ErStatus {
    Active = 0,
    Inactive = 1,
}

/// CountryCode - 3 bytes
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
struct CountryCode([u8; 3]);

impl CountryCode {
    fn from_str(s: &str) -> Self {
        let mut code = [b' '; 3];
        let bytes = s.as_bytes();
        for (i, &byte) in bytes.iter().take(3).enumerate() {
            code[i] = byte;
        }
        CountryCode(code)
    }
}

/// FeaturesSet - u64 bitmap
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy)]
struct FeaturesSet(u64);

impl FeaturesSet {
    fn new(value: u64) -> Self {
        FeaturesSet(value)
    }
}

/// RecordV0 structure
#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct RecordV0 {
    /// Identity of ER node (pubkey from its keypair)
    pub identity: Pubkey,
    /// Current status of ER node
    pub status: ErStatus,
    /// Block time of given ER node in ms
    pub block_time_ms: u16,
    /// Base fee charged by ER node per transaction
    pub base_fee: u16,
    /// A bitmap of all possible combination of custom features that the ER node supports
    pub features: FeaturesSet,
    /// An average value, which acts as an indicator
    /// of how loaded the given ER node currently is
    pub load_average: u32,
    /// 3 digit country code, where ER node is deployed
    pub country_code: CountryCode,
    /// Variable length string representing FQDN
    pub addr: String,
}

/// ErRecord enum (V0 variant)
#[derive(BorshSerialize, BorshDeserialize, Debug)]
enum ErRecord {
    V0(RecordV0),
}

/// Instruction enum
#[derive(BorshSerialize, BorshDeserialize, Debug)]
enum RegisterInstruction {
    Register(ErRecord),
    Unregister(Pubkey),
    Sync(Vec<u8>), // Simplified for now
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let args = Args::parse();

    info!("🚀 Registering validator with Magic Domain Program...");
    info!("RPC URL: {}", args.rpc_url);
    info!("FQDN: {}", args.fqdn);
    info!("Block time: {}ms", args.block_time_ms);
    info!("Base fee: {} lamports", args.base_fee);

    // Initialize RPC client
    let client = RpcClient::new(&args.rpc_url);

    // Load validator keypair
    let validator_keypair = Keypair::from_bytes(&VALIDATOR_KEYPAIR_BYTES)?;
    let validator_pubkey = validator_keypair.pubkey();

    info!("Validator Public Key: {}", validator_pubkey);

    // Get program ID
    let program_id = Pubkey::from_str(PROGRAM_ID)?;
    info!("Program ID: {}", program_id);

    // Derive PDA for the ER record
    let (record_pda, bump) =
        Pubkey::find_program_address(&[ER_RECORD_SEED, validator_pubkey.as_ref()], &program_id);

    info!("Record PDA: {}", record_pda);
    info!("Bump: {}", bump);

    // Create ErRecord
    let country_code = CountryCode::from_str(&args.country_code);
    let record_v0 = RecordV0 {
        identity: validator_pubkey,
        status: ErStatus::Active,
        block_time_ms: args.block_time_ms,
        base_fee: args.base_fee,
        features: FeaturesSet::new(0),
        load_average: 0,
        country_code,
        addr: args.fqdn.clone(),
    };

    let er_record = ErRecord::V0(record_v0);

    // Serialize instruction
    let instruction_data = RegisterInstruction::Register(er_record);
    let instruction_bytes = borsh::to_vec(&instruction_data)?;

    info!("Instruction size: {} bytes", instruction_bytes.len());

    // Get recent blockhash
    let recent_blockhash = client.get_latest_blockhash()?;
    info!("Recent blockhash: {}", recent_blockhash);

    // Create instruction
    let instruction = Instruction {
        program_id,
        accounts: vec![
            AccountMeta::new(record_pda, false),
            AccountMeta::new(validator_pubkey, true),
            AccountMeta::new_readonly(Pubkey::from_str("11111111111111111111111111111111")?, false),
        ],
        data: instruction_bytes,
    };

    // Create transaction
    let mut transaction = Transaction::new_with_payer(&[instruction], Some(&validator_pubkey));
    transaction.sign(&[&validator_keypair], recent_blockhash);

    // Send transaction
    info!("Sending transaction...");
    match client.send_and_confirm_transaction(&transaction) {
        Ok(signature) => {
            info!("✓ Validator registered successfully!");
            info!("Transaction signature: {}", signature);
            info!(
                "View on explorer: https://explorer.solana.com/tx/{}",
                signature
            );
        }
        Err(e) => {
            eprintln!("✗ Failed to register validator: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}
