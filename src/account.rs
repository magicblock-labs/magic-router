//! Account related types that are used for deserializing JSON-RPC responses and notifications

use std::ops::Deref;

use json::{lazyvalue, Deserialize};
use solana::pubkey::Pubkey;

use crate::utils::deserialize_pubkey_from_base58;
use crate::DELEGATION_PROGRAM_ID;

/// Minimal deserialization of response for getAccountInfo HTTP request
/// contains reference to underlying memory buffer used for deserialization
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct GetAccountInfoResponse<'a> {
    /// Optional account info, `None` indicates absense of account on validator
    pub result: Option<AccountInfo<'a>>,
}

/// Wrapper around actual account state, used for deserialization
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct AccountInfo<'a> {
    /// actual account state
    pub value: AccountValue<'a>,
}

impl<'a> Deref for AccountInfo<'a> {
    type Target = AccountValue<'a>;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

/// State of account with extracted relevant fields.
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
pub struct AccountValue<'a> {
    /// Account owner
    #[serde(deserialize_with = "deserialize_pubkey_from_base58")]
    pub owner: Pubkey,
    /// Current account balance in SOL
    pub lamports: u64,
    // FIXME(bmuddha13): parse it to obtain delegation record data
    /// reference to underlying memory containing JSON serialization
    #[allow(unused)]
    pub data: lazyvalue::LazyValue<'a>,
}

impl<'a> AccountValue<'a> {
    /// Check if owner account is Delegation Program, and that account is not closed
    pub fn is_delegated(&self) -> bool {
        self.owner == DELEGATION_PROGRAM_ID && self.lamports != 0
    }

    // FIXME(bmuddha13): use once we need to extract delegation related info
    /// Deserializes data field of account and decodes it based on specified encoding
    #[allow(unused)]
    pub fn data(&self) -> Vec<u8> {
        // implement deserialization/decoding
        todo!()
    }
}
