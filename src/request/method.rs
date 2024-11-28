//! satellite module for working with JSON-RPC method related logic

use std::str::FromStr;

use crate::solana::Pubkey;
use bytes::Bytes;
use json::{lazyvalue, Deserialize, JsonValueTrait};
use solana_sdk::transaction::{Transaction, VersionedTransaction};

use crate::{error::Error, request::Encoding, DELEGATION_PROGRAM_ID};

use super::{Pubkeys, RequestMeta, TransactionAction};

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
    pub fn meta(&self, payload: &Bytes) -> crate::Result<RequestMeta> {
        macro_rules! logerr {
            ($reason: expr) => {
                |error| {
                    tracing::warn!(%error, $reason);
                    Error::InvalidRequest
                }
            };
        }
        // use lazy loading, so we don't spend CPU time on parsing things we don't need
        let params =
            lazyvalue::get(payload, &[&PARAMS_KEY]).map_err(logerr!("params key missing"))?;

        match self {
            Self::GetLatestBlockhash => Ok(RequestMeta::ReadOnly(Pubkeys::None)),
            Self::GetAccountInfo | Self::GetBalance | Self::GetTokenAccountBalance => params
                // these requests always contain the pubkey as a first argument of request
                .get(0)
                .and_then(|v| {
                    v.as_str()
                        .and_then(|s| Pubkey::from_str(s).map_err(logerr!("pubkey parsing")).ok())
                })
                .map(Pubkeys::Single)
                .map(RequestMeta::ReadOnly)
                .ok_or(Error::InvalidRequest),
            Self::SendTransaction | Self::SimulateTransaction => {
                // we cannot parse encoding string representing
                // transaction without knowing the encoding used
                tracing::info!("params: {params}");
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
                let mut delegatable = None;

                // TODO/NOTES: this is highly hacky and unreliable method to figure out whether
                // account will be delegated or not, better way would be to subscribe to delegation
                // program and get updates on all accounts that get delegated, fetch their
                // delegation records afterwards and listen to changes on those, thus we don't have
                // to use this hack/approach

                // scan all accounts used in transaction, and try to find delegation program among
                // those, collect all writable accounts if found
                for pubkey in accounts {
                    if *pubkey == DELEGATION_PROGRAM_ID {
                        for i in 0..accounts.len() {
                            if tx.message.is_maybe_writable(i, None) {
                                match &mut delegatable {
                                    None => {
                                        delegatable.replace(Pubkeys::Single(accounts[i]));
                                    }
                                    Some(Pubkeys::Single(pk)) => {
                                        let prev = *pk;
                                        delegatable
                                            .replace(Pubkeys::Multiple(vec![prev, accounts[i]]));
                                    }
                                    Some(Pubkeys::Multiple(list)) => list.push(accounts[i]),
                                    _ => unreachable!(),
                                }
                            }
                        }
                        break;
                    }
                }

                // if no mentions of delegation program are found, just collect all accounts used by transaction
                let action = delegatable
                    .map(TransactionAction::Delegates)
                    .unwrap_or_else(|| {
                        TransactionAction::References(Pubkeys::Multiple(accounts.to_vec()))
                    });
                Ok(RequestMeta::Transaction(action))
            }
            Self::GetMultipleAccounts => {
                // extract the first argument which is an array of Pubkey
                let pubkeys = params.get(0).ok_or(Error::InvalidRequest)?;

                let mut pks = Vec::new();
                for i in 0..100 {
                    let pk = pubkeys
                        .get(i)
                        .and_then(|v| v.as_str().and_then(|s| Pubkey::from_str(s).ok()))
                        .ok_or(Error::InvalidRequest)?;
                    pks.push(pk);
                }
                Ok(RequestMeta::ReadOnly(Pubkeys::Multiple(pks)))
            }
        }
    }
}

// TODO: write parser tests
