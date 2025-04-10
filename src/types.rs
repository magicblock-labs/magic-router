use std::sync::atomic::AtomicU64;

use json::{Deserialize, Serialize};

pub type Slot = u64;
pub type SubscriptionId = u64;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct SubscriberId(pub u64);
#[derive(Debug, Hash, PartialEq, Eq, Deserialize, Serialize, Clone, Copy)]
pub struct RequestId(pub u64);

macro_rules! impl_unique_id {
    ($t: ty) => {
        impl From<u64> for $t {
            fn from(value: u64) -> Self {
                Self(value)
            }
        }
        impl UniqueId for $t {}
    };
}

pub trait UniqueId: Sized + From<u64> {
    fn generate() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let inner = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self::from(inner)
    }
}

impl_unique_id!(RequestId);
impl_unique_id!(SubscriberId);
