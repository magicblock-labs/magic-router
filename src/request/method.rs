//! satellite module for working with JSON-RPC method related logic

use std::str::FromStr;

use bytes::Bytes;
use json::{lazyvalue, Deserialize, JsonValueTrait};
use crate::solana::{Pubkey, VersionedMessage};

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
}

impl RequestMethod {
    /// Partially parse request body and extract account related meta data
    pub fn meta(&self, payload: &Bytes) -> crate::Result<RequestMeta> {
        // use lazy loading, so we don't spend CPU time on parsing things we don't need
        let params = lazyvalue::get(payload, &[&PARAMS_KEY]).map_err(|_| Error::InvalidRequest)?;

        match self {
            Self::GetAccountInfo | Self::GetBalance | Self::GetTokenAccountBalance => params
                // these requests always contain the pubkey as a first argument of request
                .get(0)
                .and_then(|v| v.as_str().and_then(|s| Pubkey::from_str(s).ok()))
                .map(Pubkeys::Single)
                .map(RequestMeta::ReadOnly)
                .ok_or(Error::InvalidRequest),
            Self::SendTransaction | Self::SimulateTransaction => {
                // we cannot parse encoding string representing
                // transaction without knowing the encoding used
                let encoding = if let Some(config) = params.get(1) {
                    config
                        .get(1)
                        .map(|v| {
                            v.get(ENCODING_KEY)
                                .and_then(|v| {
                                    v.as_str().and_then(|s| json::from_str::<Encoding>(s).ok())
                                })
                                .unwrap_or(Encoding::Base58)
                        })
                        .ok_or(Error::InvalidRequest)?
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
                        .map_err(|_| Error::InvalidRequest)?,
                    Encoding::Base64 => base64::decode(tx).map_err(|_| Error::InvalidRequest)?,
                    Encoding::Base64Zstd => base64::decode(tx)
                        .ok()
                        .and_then(|v| zstd::decode_all(v.as_slice()).ok())
                        .ok_or(Error::InvalidRequest)?,
                };

                // deserialize transaction from binary representation
                let msg: VersionedMessage =
                    bincode::deserialize(&tx).map_err(|_| Error::InvalidRequest)?;
                let accounts = msg.static_account_keys();
                let mut delegatable = None;

                // scan all instructions used in transaction, and try to find any that
                // uses delegation program, and collect references accounts (if any)
                for ix in msg.instructions() {
                    let id = accounts
                        .get(ix.program_id_index as usize)
                        .ok_or(Error::InvalidRequest)?;
                    if *id == DELEGATION_PROGRAM_ID {
                        let i = *ix.accounts.get(1).ok_or(Error::InvalidRequest)? as usize;
                        let to_delegate = *accounts.get(i).ok_or(Error::InvalidRequest)?;
                        match &mut delegatable {
                            None => {
                                delegatable.replace(Pubkeys::Single(to_delegate));
                            }
                            Some(Pubkeys::Single(pk)) => {
                                let prev = *pk;
                                delegatable.replace(Pubkeys::Multiple(vec![prev, to_delegate]));
                            }
                            Some(Pubkeys::Multiple(list)) => list.push(to_delegate),
                        }
                    }
                }

                // if no delegation instruction found, just collect all accounts used by transaction
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
