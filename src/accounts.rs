use crate::types::{ParsedDelegationRecord, RequestId};
pub const DELEGATION_PROGRAM_STR: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";

/// Delegation metadata for account
pub struct DelegationEntry {
    /// Unique request ID associated with websocket subscription,
    /// which keeps track of any delegation status change
    pub request_id: RequestId,
    /// Optional parsed delegation record
    pub record: Option<ParsedDelegationRecord>,
}
