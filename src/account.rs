//! Account related types that are used for deserializing JSON-RPC responses and notifications

use std::ops::Deref;

use solana::pubkey::Pubkey;
use json::Deserialize;

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
#[derive(Deserialize, Debug)]
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
#[derive(Deserialize, Debug)]
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
    pub data: [&'a str; 2],
}

impl<'a> AccountValue<'a> {
    /// Check if owner account is Delegation Program, and that account is not closed
    pub fn is_delegated(&self) -> bool {
        self.owner == DELEGATION_PROGRAM_ID && self.lamports != 0
    }

    // FIXME(bmuddha13): use once we need to extract delegation related info
    /// Deserializes data field of account and decodes it based on specified encoding
    #[allow(unused)]
    pub fn data(&self, buf: &mut [u8]) {
        // implement deserialization/decoding
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const OWNER: &str = "11111111111111111111111111111111";
    const ACCOUNT_INFO_RESPONSE: &[u8] = br#"{
      "jsonrpc": "2.0",
      "result": {
        "context": { "apiVersion": "2.0.15", "slot": 341197053 },
        "value": {
          "data": ["", "base58"],
          "executable": false,
          "lamports": 88849814690250,
          "owner": "11111111111111111111111111111111",
          "rentEpoch": 18446744073709551615,
          "space": 0
        }
      },
      "id": 1
    }"#;
    const NULL_RESPONSE: &[u8] = br#"{
      "jsonrpc": "2.0",
      "result": null,
      "id": 1
    }"#;

    #[test]
    fn test_deserialize_account_respone() {
        let response: GetAccountInfoResponse = json::from_slice(ACCOUNT_INFO_RESPONSE).unwrap();
        assert!(
            response.result.is_some(),
            "account info result should be present"
        );
        let account = response.result.unwrap();

        assert_eq!(account.owner.to_string(), OWNER);
        assert_eq!(account.lamports, 88849814690250);
        assert_eq!(account.data[0], "");
        assert_eq!(account.data[1], "base58");
    }

    #[test]
    fn test_deserialize_null_response() {
        let response: GetAccountInfoResponse = json::from_slice(NULL_RESPONSE).unwrap();
        assert!(
            response.result.is_none(),
            "account info result should be absent"
        );
    }
}
