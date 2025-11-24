use std::fmt;
use std::sync::atomic::AtomicU64;

use json::{Deserialize, Serialize};
use mdp::state::record::ErRecord;
use serde::de::{self, Visitor};
use serde::{Deserializer, Serializer};
use solana_pubkey::Pubkey;

pub type SubscriptionId = u64;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy)]
pub struct SubscriberId(pub u64);
#[derive(Debug, Hash, PartialEq, Eq, Deserialize, Serialize, Clone, Copy, Default)]
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
        static COUNTER: AtomicU64 = AtomicU64::new(1);
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

#[derive(Serialize, Clone)]
pub struct RpcIdentity {
    pub identity: SerdePubkey,
    pub fqdn: String,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DelegationStatus {
    pub is_delegated: bool,
    pub fqdn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delegation_record: Option<ParsedDelegationRecord>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ParsedDelegationRecord {
    pub authority: SerdePubkey,
    pub owner: SerdePubkey,
    pub delegation_slot: u64,
    pub lamports: u64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RouteInfo {
    pub identity: SerdePubkey,
    pub fqdn: String,
    pub base_fee: u16,
    pub block_time_ms: u16,
    pub country_code: String,
}

impl From<&ErRecord> for RouteInfo {
    fn from(er_record: &ErRecord) -> Self {
        Self {
            identity: SerdePubkey(*er_record.identity()),
            fqdn: er_record.addr().to_string(),
            base_fee: er_record.base_fee(),
            block_time_ms: er_record.block_time_ms(),
            country_code: er_record.country_code().as_str().to_string(),
        }
    }
}
