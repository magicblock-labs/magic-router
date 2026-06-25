use solana_pubkey::{pubkey, Pubkey};

use crate::types::ParsedDelegationRecord;
pub const DELEGATION_PROGRAM_STR: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";
pub const DELEGATION_PROGRAM_ID: Pubkey = pubkey!("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");
/// The tag used as a seed for the delegation record PDA.
pub const DELEGATION_RECORD_TAG: &[u8] = b"delegation";

/// Delegation metadata for account
pub struct DelegationEntry {
    // Slot at which the state was fetched from chain
    pub slot: u64,
    /// Optional parsed delegation record
    pub record: Option<ParsedDelegationRecord>,
    /// Min context slot to use while a cache fill is pending.
    pub pending_min_context_slot: Option<u64>,
}

/// Derives the Delegation Record PDA from a delegated account address.
pub fn delegation_record_pda_from_delegated_account(delegated_account: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[DELEGATION_RECORD_TAG, delegated_account.as_ref()],
        &DELEGATION_PROGRAM_ID,
    )
    .0
}
