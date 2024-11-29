//! satellite module for working with JSON-RPC method related logic

use std::str::FromStr;

use json::{lazyvalue, Deserialize, JsonValueTrait};
use solana::pubkey::Pubkey;
use solana::transaction::{Transaction, VersionedTransaction};

use crate::{error::Error, request::Encoding};

use super::Pubkeys;

const PARAMS_KEY: &str = "params";
const ENCODING_KEY: &str = "encoding";

/// Supported HTTP RPC request methods
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RequestMethod {
    /// sendTransaction request
    SendTransaction,
    /// simulateTransaction request
    SimulateTransaction,
    /// getAccountInfo request
    GetAccountInfo,
    /// getMultipleAccounts request
    GetMultipleAccounts,
    /// getBalance request
    GetBalance,
    /// getTokenAccountBalance request
    GetTokenAccountBalance,
    /// getLatestBlockhash request
    GetLatestBlockhash,
}

impl RequestMethod {
    /// Partially parse request body and extract account related meta data
    pub fn pubkeys(&self, payload: &[u8]) -> crate::Result<Pubkeys> {
        macro_rules! logerr {
            ($reason: expr) => {
                |error| {
                    #[cfg(test)]
                    eprintln!("{}: {error}", $reason);

                    #[cfg(not(test))]
                    tracing::warn!(%error, $reason);

                    Error::InvalidRequest
                }
            };
        }
        // use lazy loading, so we don't spend CPU time on parsing things we don't need
        let params =
            lazyvalue::get(payload, &[&PARAMS_KEY]).map_err(logerr!("params key missing"))?;

        match self {
            Self::GetLatestBlockhash => Ok(Pubkeys::None),
            Self::GetAccountInfo | Self::GetBalance | Self::GetTokenAccountBalance => params
                // these requests always contain the pubkey as a first argument of request
                .get(0)
                .and_then(|v| {
                    v.as_str()
                        .and_then(|s| Pubkey::from_str(s).map_err(logerr!("pubkey parsing")).ok())
                })
                .map(Pubkeys::Single)
                .ok_or(Error::InvalidRequest),
            Self::SendTransaction | Self::SimulateTransaction => {
                // we cannot parse encoding string representing
                // transaction without knowing the encoding used
                let encoding = if let Some(config) = params.get(1) {
                    config
                        .get(ENCODING_KEY)
                        .and_then(|v| {
                            json::from_str::<Encoding>(v.as_raw_str())
                                .map_err(logerr!("encoding parsing"))
                                .ok()
                        })
                        .unwrap_or(Encoding::Base58)
                } else {
                    Encoding::Base58
                };

                // extract the first arument from request params and
                // decode it using the specified encoding
                let tx = params.get(0).ok_or(Error::InvalidRequest)?;
                let tx = tx.as_str().ok_or(Error::InvalidRequest)?;
                let tx = match encoding {
                    Encoding::Base58 => bs58::decode(tx)
                        .into_vec()
                        .map_err(logerr!("base58 decoding transaction"))?,
                    Encoding::Base64 => {
                        base64::decode(tx).map_err(logerr!("base64 decoding transaction"))?
                    }
                    Encoding::Base64Zstd => base64::decode(tx)
                        .map_err(logerr!("base64 decoding transaction"))
                        .ok()
                        .and_then(|v| {
                            zstd::decode_all(v.as_slice())
                                .map_err(logerr!("zstd decompressing transaction"))
                                .ok()
                        })
                        .ok_or(Error::InvalidRequest)?,
                };

                let versioned = tx
                    .first()
                    .map(|b| (0x00..0x02).contains(b))
                    .unwrap_or(false);
                // deserialize transaction from binary representation
                let tx: VersionedTransaction = if versioned {
                    bincode::deserialize(&tx)
                        .or_else(|_| bincode::deserialize::<Transaction>(&tx).map(Into::into))
                        .map_err(logerr!("bincode deserializing versioned transaction"))?
                } else {
                    bincode::deserialize::<Transaction>(&tx)
                        .map(Into::into)
                        .map_err(logerr!("bincode deserializing transaction"))?
                };
                let accounts = tx.message.static_account_keys();
                Ok(Pubkeys::Multiple(accounts.to_vec()))
            }
            Self::GetMultipleAccounts => {
                // extract the first argument which is an array of Pubkey
                let pubkeys = params.get(0).ok_or(Error::InvalidRequest)?;

                let mut pks = Vec::new();
                for i in 0..100 {
                    if let Some(pk) = pubkeys.get(i) {
                        let pk = pk
                            .as_str()
                            .and_then(|s| Pubkey::from_str(s).ok())
                            .ok_or(Error::InvalidRequest)?;
                        pks.push(pk);
                    }
                }
                Ok(Pubkeys::Multiple(pks))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use solana::{
        hash::Hash, pubkey::Pubkey, signature::Keypair, signer::Signer,
        system_instruction::transfer,
    };

    const ACCOUNT1: Pubkey = solana::pubkey!("vines1vzrYbzLMRdu58ou5XTby4qAqVRLmqo36NKPTg");
    const ACCOUNT2: Pubkey = solana::pubkey!("4fYNw3dojWmQ4dXtSGE9epjRGy9pFSx62YypT7avPYvA");
    use super::*;

    macro_rules! jsonrpc {
        ($method: literal, $params: expr) => {
            jsonrpc!($method, $params, "base58")
        };
        ($method: literal, $params: expr, $encoding: expr) => {
            json::json! {{
                "jsonrpc": "2.0",
                "id": 1,
                "method": $method,
                "params": [$params, { "encoding":  $encoding }]
            }}
            .to_string()
            .into_bytes()
        };
    }

    #[test]
    fn parse_get_account_info() {
        let string = jsonrpc!("getAccountInfo", ACCOUNT1.to_string());
        let pubkeys = RequestMethod::GetAccountInfo.pubkeys(&string).unwrap();
        assert!(matches!(pubkeys, Pubkeys::Single(ACCOUNT1)));
    }

    #[test]
    fn parse_get_balance() {
        let string = jsonrpc!("getBalance", ACCOUNT1.to_string());
        let pubkeys = RequestMethod::GetBalance.pubkeys(&string).unwrap();
        assert!(matches!(pubkeys, Pubkeys::Single(ACCOUNT1)));
    }

    #[test]
    fn parse_get_token_account_balance_meta() {
        let string = jsonrpc!("getTokenAccountBalance", ACCOUNT1.to_string());
        let pubkeys = RequestMethod::GetTokenAccountBalance
            .pubkeys(&string)
            .unwrap();
        assert!(matches!(pubkeys, Pubkeys::Single(ACCOUNT1)));
    }

    #[test]
    fn parse_get_multiple_accounts() {
        let params = [ACCOUNT1.to_string(), ACCOUNT2.to_string()];
        let string = jsonrpc!("getTokenAccountBalance", params);
        let pubkeys = RequestMethod::GetMultipleAccounts.pubkeys(&string).unwrap();
        assert!(matches!(pubkeys, Pubkeys::Multiple(_)));
        let mut pubkeys = pubkeys.iter();
        assert_eq!(pubkeys.next(), Some(&ACCOUNT1));
        assert_eq!(pubkeys.next(), Some(&ACCOUNT2));
    }

    #[test]
    fn parse_send_transaction() {
        fn generation_transaction() -> (Transaction, Pubkey) {
            let payer = Keypair::new();
            let ix1 = transfer(&payer.pubkey(), &ACCOUNT1, 1);
            let ix2 = transfer(&payer.pubkey(), &ACCOUNT2, 1);
            let hash = Hash::new_unique();
            let tx = Transaction::new_signed_with_payer(
                &[ix1, ix2],
                Some(&payer.pubkey()),
                &[&payer],
                hash,
            );
            (tx, payer.pubkey())
        }

        let (legacy, payer) = generation_transaction();
        let versioned = VersionedTransaction::from(legacy.clone());
        let encoders = [
            |v: &[u8]| bs58::encode(v).into_string(),
            |v: &[u8]| base64::encode(v),
            |v: &[u8]| zstd::encode_all(v, 0).map(base64::encode).unwrap(),
        ];
        let encodings = ["base58", "base64", "base64+zstd"];
        let txs = [
            bincode::serialize(&legacy).unwrap(),
            bincode::serialize(&versioned).unwrap(),
        ];
        for tx in txs {
            for (encoder, encoding) in encoders.iter().zip(encodings) {
                let tx = encoder(&tx);
                let body = jsonrpc!("sendTransaction", tx, encoding);
                let pubkeys = RequestMethod::SendTransaction.pubkeys(&body).unwrap();
                assert!(matches!(pubkeys, Pubkeys::Multiple(_)));
                let mut pubkeys = pubkeys.iter();
                assert_eq!(pubkeys.next(), Some(&payer));
                assert_eq!(pubkeys.next(), Some(&ACCOUNT1));
                assert_eq!(pubkeys.next(), Some(&ACCOUNT2));
            }
        }
    }
}
