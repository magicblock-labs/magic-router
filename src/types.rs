use std::fmt;
use std::sync::atomic::AtomicU64;

use json::{Deserialize, Serialize};
use serde::de::{self, Visitor};
use serde::{Deserializer, Serializer};
use solana_pubkey::Pubkey;

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

#[derive(Clone)]
pub struct SerdePubkey(pub Pubkey);

impl Serialize for SerdePubkey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buf = [0u8; 44]; // 32 bytes will expand to at most 44 base58 characters
        let size = bs58::encode(&self.0)
            .onto(buf.as_mut_slice())
            .expect("Buffer too small");
        // SAFETY:
        // bs58 always produces valid UTF-8
        serializer.serialize_str(unsafe { std::str::from_utf8_unchecked(&buf[..size]) })
    }
}

impl<'de> Deserialize<'de> for SerdePubkey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SerdePubkeyVisitor;

        impl Visitor<'_> for SerdePubkeyVisitor {
            type Value = SerdePubkey;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a base58 encoded string representing a 32-byte array")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let mut buffer = [0u8; 32];
                let decoded_len = bs58::decode(value)
                    .onto(&mut buffer)
                    .map_err(de::Error::custom)?;
                if decoded_len != 32 {
                    return Err(de::Error::custom("expected 32 bytes"));
                }
                Ok(SerdePubkey(Pubkey::new_from_array(buffer)))
            }
        }
        deserializer.deserialize_str(SerdePubkeyVisitor)
    }
}
