use std::{future::Future, sync::Arc};

use futures::{channel::oneshot, future::Shared, FutureExt};
use solana_pubkey::{pubkey, Pubkey};
use tokio::sync::Notify;

use crate::types::ParsedDelegationRecord;
pub const DELEGATION_PROGRAM_STR: &str = "DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh";
pub const DELEGATION_PROGRAM_ID: Pubkey = pubkey!("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");
/// The tag used as a seed for the delegation record PDA.
pub const DELEGATION_RECORD_TAG: &[u8] = b"delegation";

/// A handle to an in-progress cache fill, shared between the fill owner,
/// concurrent readers, and the stream updater.
#[derive(Clone)]
pub struct PendingDelegationFill {
    /// Resolved by the fill owner with the final record; dropped on cancellation.
    result: Shared<oneshot::Receiver<Option<ParsedDelegationRecord>>>,
    /// Signalled when a stream update installs the record ahead of the owner's fetch.
    filled: Arc<Notify>,
}

impl PendingDelegationFill {
    /// Creates a fill handle along with the sender the fill owner resolves it with.
    pub fn new() -> (oneshot::Sender<Option<ParsedDelegationRecord>>, Self) {
        let (tx, rx) = oneshot::channel();
        let fill = Self {
            result: rx.shared(),
            filled: Arc::new(Notify::new()),
        };
        (tx, fill)
    }

    /// Whether both handles refer to the same fill.
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.result.ptr_eq(&other.result)
    }

    /// Waits for the fill owner to resolve the fill; `Err` means it was cancelled.
    pub fn result(&self) -> Shared<oneshot::Receiver<Option<ParsedDelegationRecord>>> {
        self.result.clone()
    }

    /// Completes once `notify_filled` fires. Create the future before reading
    /// the cache so a concurrent stream fill is never missed.
    pub fn filled(&self) -> impl Future<Output = ()> + '_ {
        self.filled.notified()
    }

    /// Wakes the fill owner after a stream update installed the record.
    pub fn notify_filled(&self) {
        self.filled.notify_waiters();
    }
}

/// Delegation metadata for account
pub struct DelegationEntry {
    // Slot at which the state was fetched from chain
    pub slot: u64,
    /// Optional parsed delegation record
    pub record: Option<ParsedDelegationRecord>,
    /// Min context slot to use while a cache fill is pending.
    pub pending_min_context_slot: Option<u64>,
    /// Shared result for callers waiting on the cache fill owner.
    pub pending_fill: Option<PendingDelegationFill>,
}

/// Derives the Delegation Record PDA from a delegated account address.
pub fn delegation_record_pda_from_delegated_account(delegated_account: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[DELEGATION_RECORD_TAG, delegated_account.as_ref()],
        &DELEGATION_PROGRAM_ID,
    )
    .0
}
