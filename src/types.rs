use std::sync::atomic::AtomicU64;

use json::Deserialize;

pub type Slot = u64;
pub type SubscriptionId = u64;

#[derive(Debug, Hash, PartialEq, Eq)]
pub struct SubscriberId(pub u64);
#[derive(Debug, Hash, PartialEq, Eq, Deserialize)]
pub struct RequestId(pub u64);

impl From<u64> for SubscriberId {
    fn from(value: u64) -> Self {
        SubscriberId(value)
    }
}

impl From<u64> for RequestId {
    fn from(value: u64) -> Self {
        RequestId(value)
    }
}

trait UniqueId: Sized + From<u64> {
    fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let inner = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self::from(inner)
    }
}

impl UniqueId for SubscriberId {}
impl UniqueId for RequestId {}
