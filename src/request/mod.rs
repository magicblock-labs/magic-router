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
    pubkeys: Pubkeys,
    /// full body of request
    payload: Bytes,
}

impl Request {
    /// Construct Request by partial parsing the body
    pub fn new(payload: Bytes) -> super::Result<Self> {
        println!("REQUEST: {}", unsafe {
            std::str::from_utf8_unchecked(&payload)
        });
        let pubkeys = {
            let method: LazyValue =
                lazyvalue::get(&payload, &[METHOD_KEY]).map_err(|_| Error::InvalidRequest)?;
            let method = json::from_str::<RequestMethod>(method.as_raw_str())
                .map_err(|_| Error::UnsupportedMethod)?;
            method.pubkeys(&payload)?
        };
        Ok(Self { pubkeys, payload })
    }

    /// Returns all the pubkeys referensed in this request
    pub fn pubkeys(&self) -> PubkeysIter {
        self.pubkeys.iter()
    }
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

/// Allowed encodings for binary data used in request/responses
#[derive(Deserialize)]
pub enum Encoding {
    /// Base58 encoding
    #[serde(rename = "base58")]
    Base58,
    /// Base64 encoding
    #[serde(rename = "base64")]
    Base64,
    /// Zstd compressed and then base64 encoded
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

pub mod handler;
pub mod method;
