//! module for working with requests received from client

use bytes::Bytes;
use json::{lazyvalue, Deserialize, LazyValue};
use method::RequestMethod;
use solana::pubkey::Pubkey;

use crate::error::Error;

const METHOD_KEY: &str = "method";

/// Partially parsed request from client
pub struct Request {
    /// metadata related to request routing
    meta: RequestMeta,
    /// full body of request
    payload: Bytes,
}

impl Request {
    /// Construct Request by partial parsing the body
    pub fn new(payload: Bytes) -> super::Result<Self> {
        println!("REQUEST: {}", unsafe {
            std::str::from_utf8_unchecked(&payload)
        });
        let meta = {
            let method: LazyValue =
                lazyvalue::get(&payload, &[METHOD_KEY]).map_err(|_| Error::InvalidRequest)?;
            let method = json::from_str::<RequestMethod>(method.as_raw_str())
                .map_err(|_| Error::UnsupportedMethod)?;
            method.meta(&payload)?
        };
        Ok(Self { meta, payload })
    }
}

/// Discriminator enum which indicates whether Transaction includes delegation instructions
pub enum TransactionAction {
    /// Transaction delegates some accounts
    Delegates(Pubkeys),
    /// Transaction doesn't use any instructions from Delegation Program
    References(Pubkeys),
}

/// Optimization type which avoids allocations when only one pubkey is used in request
pub enum Pubkeys {
    /// request doesn't contain any pubkeys
    None,
    /// single pubkey is used in request
    Single(Pubkey),
    /// multiple pubkey are used in request
    Multiple(Vec<Pubkey>),
}

impl Pubkeys {
    fn iter(&self) -> PubkeysIter<'_> {
        PubkeysIter {
            inner: self,
            index: 0,
        }
    }
}

/// Helper type to iterate over `Pubkeys`
pub struct PubkeysIter<'a> {
    inner: &'a Pubkeys,
    index: usize,
}

impl<'a> Iterator for PubkeysIter<'a> {
    type Item = &'a Pubkey;

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            Pubkeys::None => None,
            Pubkeys::Single(pk) => {
                if self.index == 0 {
                    self.index += 1;
                    Some(pk)
                } else {
                    None
                }
            }
            Pubkeys::Multiple(pks) => pks.get(self.index).inspect(|_| {
                self.index += 1;
            }),
        }
    }
}

/// Metadata of request related to routing logic
pub enum RequestMeta {
    /// Request only reads data from block chain
    ReadOnly(Pubkeys),
    /// Request contains transaction which potentialy can delegate accounts
    Transaction(TransactionAction),
}

impl RequestMeta {
    /// Pubkeys referenced in request
    pub fn pubkeys(&self) -> PubkeysIter<'_> {
        match self {
            RequestMeta::ReadOnly(pks) => pks.iter(),
            RequestMeta::Transaction(txa) => match txa {
                TransactionAction::Delegates(pks) => pks.iter(),
                TransactionAction::References(pks) => pks.iter(),
            },
        }
    }

    /// whether request contains transaction with instruction from Delegation Program
    /// returns optional iterator over all pubkeys to be used by Delegation Program
    pub fn delegates(&self) -> Option<PubkeysIter<'_>> {
        if let Self::Transaction(TransactionAction::Delegates(pubkeys)) = self {
            Some(pubkeys.iter())
        } else {
            None
        }
    }
}

/// Allowed encodings for binary data used in request/responses
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    /// Base58 encoding
    Base58,
    /// Base64 encoding
    Base64,
    /// Zstd compressed and then base64 encoded
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

pub mod handler;
pub mod method;
