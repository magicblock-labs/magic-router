//! types used by solana API, pull from solana-program so that we
//! don't have to pull billion dependencies for couple of types

use serde::Deserialize;
use solana_hash::Hash;

pub use pubkey::Pubkey;

#[derive(Deserialize)]
/// Transaction message
pub enum VersionedMessage {
    /// Legacy transaction message
    Legacy(LegacyMessage),
    /// V0 transaction message
    V0(V0Message),
}

/// Transaction version 0
#[derive(Deserialize)]
pub struct V0Message {
    /// The message header, identifying signed and read-only `account_keys`.
    /// Header values only describe static `account_keys`, they do not describe
    /// any additional account keys loaded via address table lookups.
    pub header: MessageHeader,

    /// List of accounts loaded by this transaction.
    #[serde(with = "shortvec")]
    pub account_keys: Vec<Pubkey>,

    /// The blockhash of a recent block.
    pub recent_blockhash: Hash,

    /// Instructions that invoke a designated program, are executed in sequence,
    /// and committed in one atomic transaction if all succeed.
    ///
    /// # Notes
    ///
    /// Program indexes must index into the list of message `account_keys` because
    /// program id's cannot be dynamically loaded from a lookup table.
    ///
    /// Account indexes must index into the list of addresses
    /// constructed from the concatenation of three key lists:
    ///   1) message `account_keys`
    ///   2) ordered list of keys loaded from `writable` lookup table indexes
    ///   3) ordered list of keys loaded from `readable` lookup table indexes
    #[serde(with = "shortvec")]
    pub instructions: Vec<CompiledInstruction>,

    /// List of address table lookups used to load additional accounts
    /// for this transaction.
    #[serde(with = "shortvec")]
    pub address_table_lookups: Vec<MessageAddressTableLookup>,
}

/// Original version Transaction
#[derive(Deserialize)]
pub struct LegacyMessage {
    /// The message header, identifying signed and read-only `account_keys`.
    // NOTE: Serialization-related changes must be paired with the direct read at sigverify.
    pub header: MessageHeader,

    /// All the account keys used by this transaction.
    #[serde(with = "shortvec")]
    pub account_keys: Vec<Pubkey>,

    /// The id of a recent ledger entry.
    pub recent_blockhash: Hash,

    /// Programs that will be executed in sequence and committed in one atomic transaction if all
    /// succeed.
    #[serde(with = "shortvec")]
    pub instructions: Vec<CompiledInstruction>,
}

/// Transaction message header
#[derive(Deserialize)]
pub struct MessageHeader {
    /// The number of signatures required for this message to be considered
    /// valid. The signers of those signatures must match the first
    /// `num_required_signatures` of [`Message::account_keys`].
    // NOTE: Serialization-related changes must be paired with the direct read at sigverify.
    pub num_required_signatures: u8,

    /// The last `num_readonly_signed_accounts` of the signed keys are read-only
    /// accounts.
    pub num_readonly_signed_accounts: u8,

    /// The last `num_readonly_unsigned_accounts` of the unsigned keys are
    /// read-only accounts.
    pub num_readonly_unsigned_accounts: u8,
}

/// Transaction instruction
#[derive(Deserialize)]
pub struct CompiledInstruction {
    /// Index into the transaction keys array indicating the program account that executes this instruction.
    pub program_id_index: u8,
    /// Ordered indices into the transaction keys array indicating which accounts to pass to the program.
    #[serde(with = "shortvec")]
    pub accounts: Vec<u8>,
    /// The program input data.
    #[serde(with = "shortvec")]
    pub data: Vec<u8>,
}

/// Account lookup tables for transaction
#[derive(Deserialize)]
pub struct MessageAddressTableLookup {
    /// Address lookup table account key
    pub account_key: Pubkey,
    /// List of indexes used to load writable account addresses
    #[serde(with = "shortvec")]
    pub writable_indexes: Vec<u8>,
    /// List of indexes used to load readonly account addresses
    #[serde(with = "shortvec")]
    pub readonly_indexes: Vec<u8>,
}

impl VersionedMessage {
    /// Return all pubkeys referenced in transaction
    pub fn static_account_keys(&self) -> &[Pubkey] {
        match self {
            Self::Legacy(message) => &message.account_keys,
            Self::V0(message) => &message.account_keys,
        }
    }

    /// Program instructions that will be executed in sequence and committed in
    /// one atomic transaction if all succeed.
    pub fn instructions(&self) -> &[CompiledInstruction] {
        match self {
            Self::Legacy(message) => &message.instructions,
            Self::V0(message) => &message.instructions,
        }
    }

    /// custom method to return all writeable account pubkeys
    pub fn writable_account_keys(&self) -> Vec<Pubkey> {
        match self {
            Self::Legacy(tx) => tx.writable_account_keys().copied().collect(),
            Self::V0(tx) => tx.writable_account_keys().copied().collect(),
        }
    }
}

impl LegacyMessage {
    /// custom method to return iterator over all writeable account keys
    pub fn writable_account_keys(&self) -> impl Iterator<Item = &Pubkey> {
        // Get writable signed accounts
        let num_writable_signed =
            self.header.num_required_signatures - self.header.num_readonly_signed_accounts;
        let writable_signed = &self.account_keys[0..num_writable_signed as usize];

        // Get writable unsigned accounts (like PDAs)
        let writable_unsigned_start = self.header.num_required_signatures as usize;
        let writable_unsigned_end =
            self.account_keys.len() - self.header.num_readonly_unsigned_accounts as usize;
        let writable_unsigned = &self.account_keys[writable_unsigned_start..writable_unsigned_end];

        writable_signed.into_iter().chain(writable_unsigned)
    }
}

impl V0Message {
    /// custom method to return iterator over all writeable account keys
    pub fn writable_account_keys(&self) -> impl Iterator<Item = &Pubkey> {
        // Get writable signed accounts
        let num_writable_signed =
            self.header.num_required_signatures - self.header.num_readonly_signed_accounts;
        let writable_signed = &self.account_keys[0..num_writable_signed as usize];

        // Get writable unsigned accounts (like PDAs)
        let writable_unsigned_start = self.header.num_required_signatures as usize;
        let writable_unsigned_end =
            self.account_keys.len() - self.header.num_readonly_unsigned_accounts as usize;
        let writable_unsigned = &self.account_keys[writable_unsigned_start..writable_unsigned_end];

        writable_signed.into_iter().chain(writable_unsigned)
    }
}
