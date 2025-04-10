use std::sync::Arc;

use solana_pubkey::Pubkey;
use url::Url;

use crate::types::RequestId;
pub const DELEGATION_PROGRAM: Pubkey =
    Pubkey::from_str_const("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");
/// Serialized size of delegation record PDA
/// NOTE: this is taken from the delegation program
pub const DELEGATION_RECORD_DATA_SIZE: usize = 88;

pub struct DelegationEntry {
    pub request_id: RequestId,
    pub destination: Arc<Url>,
    pub status: DelegationStatus,
}

#[derive(Clone, Copy)]
pub enum DelegationStatus {
    Delegated(Pubkey),
    NotDelegated,
}

impl DelegationStatus {
    #[inline(always)]
    fn is_delegated(&self) -> bool {
        matches!(self, Self::Delegated(_))
    }
}
