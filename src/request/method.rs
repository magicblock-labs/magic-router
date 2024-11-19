use std::str::FromStr;

use json::{Deserialize, JsonContainerTrait, JsonValueTrait};
use solana::{message::VersionedMessage, pubkey::Pubkey};

use crate::{error::Error, request::Encoding};

use super::Pubkeys;

const PARAMS_KEY: &str = "params";
const ENCODING_KEY: &str = "encoding";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RequestMethod {
    SendTransaction,
    SimulateTransaction,
    GetAccountInfo,
    GetMultipleAccounts,
    GetBalance,
    GetTokenAccountBalance,
}

impl RequestMethod {
    pub fn extract_pubkeys(&self, payload: &json::Object) -> crate::Result<Pubkeys> {
        let params = payload
            .get(&PARAMS_KEY)
            .and_then(|v| v.as_array())
            .ok_or(Error::InvalidRequest)?;

        match self {
            Self::GetAccountInfo | Self::GetBalance | Self::GetTokenAccountBalance => params
                .first()
                .and_then(|v| v.as_str())
                .and_then(|s| Pubkey::from_str(s).ok())
                .map(Pubkeys::Single)
                .ok_or(Error::InvalidRequest),
            Self::SendTransaction | Self::SimulateTransaction => {
                let encoding = if let Some(c) = params.get(1) {
                    c.as_object()
                        .and_then(|v| v.get(&ENCODING_KEY))
                        .and_then(|v| v.as_str())
                        .and_then(|s| json::from_str::<Encoding>(s).ok())
                        .ok_or(Error::InvalidRequest)?
                } else {
                    Encoding::Base58
                };

                let tx = params
                    .first()
                    .and_then(|v| v.as_str())
                    .ok_or(Error::InvalidRequest)?;

                let tx = match encoding {
                    Encoding::Base58 => bs58::decode(tx)
                        .into_vec()
                        .map_err(|_| Error::InvalidRequest)?,
                    Encoding::Base64 => base64::decode(tx).map_err(|_| Error::InvalidRequest)?,
                    Encoding::Base64Zstd => {
                        todo!()
                    }
                };
                let msg: VersionedMessage =
                    bincode::deserialize(&tx).map_err(|_| Error::InvalidRequest)?;
                Ok(Pubkeys::Multiple(msg.static_account_keys().into()))
            }
            Self::GetMultipleAccounts => {
                let pubkeys = params
                    .iter()
                    .filter_map(|v| v.as_str())
                    .filter_map(|s| Pubkey::from_str(s).ok())
                    .collect();
                Ok(Pubkeys::Multiple(pubkeys))
            }
        }
    }
}
