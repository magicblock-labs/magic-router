use crate::types::ParsedDelegationRecord;
pub const DELEGATION_PROGRAM_STR: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";

/// Delegation metadata for account
pub struct DelegationEntry {
    // Slot at which the state was fetched from chain
    pub slot: u64,
    /// Optional parsed delegation record
    pub record: Option<ParsedDelegationRecord>,
}
