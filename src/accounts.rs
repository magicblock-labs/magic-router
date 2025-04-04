use solana_pubkey::Pubkey;
pub const DELEGATION_PROGRAM: Pubkey =
    Pubkey::from_str_const("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");
/// Serialized size of delegation record PDA
/// NOTE: this is taken from the delegation program
pub const DELEGATION_RECORD_DATA_SIZE: usize = 88;

#[derive(Clone)]
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
