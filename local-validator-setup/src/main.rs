use anyhow::Result;
use borsh::{BorshDeserialize, BorshSerialize};
use clap::Parser;
use mdp::instructions::Instruction;
use mdp::state::features::FeaturesSet;
use mdp::state::record::{CountryCode, ErRecord};
use mdp::state::status::ErStatus;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_rpc_client::rpc_client::RpcClient;
use solana_signer::Signer;
use solana_system_program::id as system_program_id;
use solana_transaction::{Message, Transaction};
use std::str::FromStr;
use tracing::info;

/// Magic Domain Program ID
const PROGRAM_ID: &str = "DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1";
const ER_RECORD_SEED: &[u8] = b"er-record";

/// Validator keypair bytes (secret key)
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
    #[arg(long, default_value = "50")]
    block_time_ms: u16,

    /// Base fee in lamports
    #[arg(long, default_value = "0")]
    base_fee: u16,

    /// Country code (3 chars)
    #[arg(long, default_value = "USA")]
    country_code: String,
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

    // Load validator keypair from bytes (first 32 bytes are the secret key)
    let secret_key = &VALIDATOR_KEYPAIR_BYTES[..32];
    let secret_array: [u8; 32] = secret_key.try_into()?;
    let validator_keypair = Keypair::new_from_array(secret_array);
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

    // Verify RPC connection
    match client.get_version() {
        Ok(version) => {
            info!("✓ Connected to validator");
            info!("  Solana version: {}", version.solana_core);
        }
        Err(e) => {
            anyhow::bail!("✗ Failed to connect to validator: {}", e);
        }
    }

    // Build validator registration transaction
    info!("Building validator registration transaction...");

    // Get current blockhash
    let blockhash = client.get_latest_blockhash()?;
    info!("Latest blockhash: {}", blockhash);

    // Create the ER record
    let country_code = CountryCode::from(args.country_code.as_str());
    let fqdn = args.fqdn.clone();
    let er_record = ErRecord::V0(mdp::state::version::v0::RecordV0 {
        identity: validator_pubkey,
        status: ErStatus::Active,
        block_time_ms: args.block_time_ms,
        base_fee: args.base_fee,
        features: FeaturesSet::default(),
        load_average: 0,
        country_code,
        addr: fqdn,
    });

    // Serialize the instruction
    let instruction_data = Instruction::Register(er_record);
    let mut instruction_data_bytes = Vec::new();
    instruction_data.serialize(&mut instruction_data_bytes)?;

    // Build the instruction
    let ix = solana_program::instruction::Instruction {
        program_id,
        accounts: vec![
            solana_program::instruction::AccountMeta::new(validator_pubkey, true),
            solana_program::instruction::AccountMeta::new(record_pda, false),
            solana_program::instruction::AccountMeta::new_readonly(system_program_id(), false),
        ],
        data: instruction_data_bytes,
    };

    // Build and sign transaction
    let message = Message::new(&[ix], Some(&validator_pubkey));
    let transaction = Transaction::new(&[&validator_keypair], message, blockhash);

    // Submit transaction
    info!("Submitting registration transaction...");
    match client.send_and_confirm_transaction(&transaction) {
        Ok(signature) => {
            info!("✓ Transaction submitted successfully");
            info!("  Signature: {}", signature);
        }
        Err(e) => {
            info!("⚠️  Transaction submission failed: {}", e);
            info!("Checking if account already exists...");
        }
    }

    // Fetch and display the account content
    info!("");
    info!("Fetching ER Record account...");
    match client.get_account(&record_pda) {
        Ok(account) => {
            info!("✓ ER Record account found");
            info!("  Account: {}", record_pda);
            info!("  Lamports: {}", account.lamports);
            info!("  Owner: {}", account.owner);

            // Try to deserialize the account data
            match ErRecord::try_from_slice(&account.data) {
                Ok(fetched_record) => {
                    info!("✓ Successfully deserialized ER Record");
                    info!("");
                    info!("✓ Validator setup completed");
                    info!("Validator registration:");
                    info!("  Identity: {}", fetched_record.identity());
                    info!("  FQDN: {}", fetched_record.addr());
                    info!("  Block time: {}ms", fetched_record.block_time_ms());
                    info!("  Base fee: {} lamports", fetched_record.base_fee());
                    info!("  Country code: {:?}", fetched_record.country_code());
                    info!("  Status: {:?}", fetched_record.status());
                }
                Err(e) => {
                    info!("⚠️  Failed to deserialize ER Record: {}", e);
                    info!("Raw account data size: {} bytes", account.data.len());
                }
            }
        }
        Err(e) => {
            anyhow::bail!("✗ Failed to fetch ER Record account: {}", e);
        }
    }

    Ok(())
}
