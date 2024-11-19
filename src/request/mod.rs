use json::{serde, Deserialize, JsonContainerTrait};
use method::RequestMethod;
use solana::pubkey::Pubkey;

use crate::error::Error;

const METHOD_KEY: &str = "method";

pub enum Pubkeys {
    Single(Pubkey),
    Multiple(Vec<Pubkey>),
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Encoding {
    Base58,
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
}

pub struct Request {
    pubkeys: Pubkeys,
    payload: json::Value,
}

impl Request {
    pub fn new(payload: json::Value) -> super::Result<Self> {
        let Some(payload) = payload.as_object() else {
            return Err(Error::InvalidRequest);
        };
        let Some(method) = payload.get(&METHOD_KEY) else {
            return Err(Error::InvalidRequest);
        };
        let Ok(method) = json::from_value::<RequestMethod>(method) else {
            return Err(Error::UnsupportedMethod);
        };
        todo!()
    }

    pub fn method(&self) -> RequestMethod {
        todo!()
    }
}

pub mod handler;
pub mod method;
