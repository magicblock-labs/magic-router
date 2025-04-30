use std::sync::Arc;

use solana_pubkey::Pubkey;
use url::Url;

use crate::types::RequestId;
pub const DELEGATION_PROGRAM_STR: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";
pub const DELEGATION_PROGRAM: Pubkey = Pubkey::from_str_const(DELEGATION_PROGRAM_STR);
/// Serialized size of delegation record PDA
/// NOTE: this is taken from the delegation program
pub const DELEGATION_RECORD_DATA_SIZE: usize = 96;

/// Delegation metadata for account
pub struct DelegationEntry {
    /// Unique request ID associated with websocket subscription,
    /// which keeps track of any delegation status change
    pub request_id: RequestId,
    /// FQDN of the upstream where subscription request has been sent
    pub destination: Arc<Url>,
    /// Delegation status of the account
    pub status: DelegationStatus,
}

#[derive(Clone, Copy, Debug)]
pub enum DelegationStatus {
    Delegated(Pubkey),
    NotDelegated,
}

impl DelegationStatus {
    #[inline(always)]
    pub fn is_delegated(&self) -> bool {
        matches!(self, Self::Delegated(_))
    }
}
