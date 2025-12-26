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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fqdn: Option<String>,
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

impl ParsedDelegationRecord {
    const AUTHORITY_OFFSET: usize = 8;
    const AUTHORITY_SIZE: usize = 32; // Pubkey is 32 bytes

    const OWNER_OFFSET: usize = Self::AUTHORITY_OFFSET + Self::AUTHORITY_SIZE;
    const OWNER_SIZE: usize = 32;

    const SLOT_OFFSET: usize = Self::OWNER_OFFSET + Self::OWNER_SIZE;
    const SLOT_SIZE: usize = 8; // u64 is 8 bytes

    const LAMPORTS_OFFSET: usize = Self::SLOT_OFFSET + Self::SLOT_SIZE;
    const LAMPORTS_SIZE: usize = 8;

    // Minimum size required to read up to 'lamports'.
    // The actual account data is larger (it includes commit_frequency_ms), but we only need these bytes.
    const MIN_DATA_LEN: usize = Self::LAMPORTS_OFFSET + Self::LAMPORTS_SIZE;

    pub fn from_bytes(data: Vec<u8>) -> Option<Self> {
        // 1. Verify data length
        (data.len() >= Self::MIN_DATA_LEN).then_some(())?;

        // 3. Extract fields directly using slice indexing

        // Authority (Pubkey)
        let authority_bytes = &data[Self::AUTHORITY_OFFSET..Self::OWNER_OFFSET];
        let authority = SerdePubkey(Pubkey::try_from(authority_bytes).ok()?);

        // Owner (Pubkey)
        let owner_bytes = &data[Self::OWNER_OFFSET..Self::SLOT_OFFSET];
        let owner = SerdePubkey(Pubkey::try_from(owner_bytes).ok()?);

        // Delegation Slot (u64)
        let slot_bytes: [u8; 8] = data[Self::SLOT_OFFSET..Self::LAMPORTS_OFFSET]
            .try_into()
            .ok()?;
        let delegation_slot = u64::from_le_bytes(slot_bytes);

        // Lamports (u64)
        // Note: The field commit_frequency_ms exists after this in the source struct, but we ignore it.
        let lamports_bytes: [u8; 8] = data[Self::LAMPORTS_OFFSET..Self::LAMPORTS_OFFSET + 8]
            .try_into()
            .ok()?;
        let lamports = u64::from_le_bytes(lamports_bytes);

        Some(Self {
            authority,
            owner,
            delegation_slot,
            lamports,
        })
    }
}
