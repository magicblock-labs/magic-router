use jsonrpsee::{client_transport::ws::WsHandshakeError, types::ErrorObject};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::client_error;
use solana_signature::Signature;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("upstream websocket handshake: {0}")]
    WsHandshake(#[from] WsHandshakeError),
    #[error("solana rpc request error: {0}")]
    Rpc(Box<client_error::Error>),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("account has been delegated to unknown ER node: {0}")]
    UnknownErNode(Pubkey),
    #[error("transaction with given signature has never been encountered: {0}")]
    UnknownTransaction(Signature),
    #[error("timeout trying to confirm subscription: {0}")]
    SubscriptionTimetout(&'static str),
    #[error("failed to decode request parameters: {0}")]
    DecodeError(Box<dyn std::error::Error + 'static>),
    #[error("transaction contains accounts that were delegated to different ER nodes")]
    ConflictingDelegations,
}

impl From<RouterError> for ErrorObject<'_> {
    fn from(value: RouterError) -> Self {
        ErrorObject::owned::<()>(-32604, value.to_string(), None)
    }
}

impl From<client_error::Error> for RouterError {
    fn from(value: client_error::Error) -> Self {
        Self::Rpc(Box::new(value))
    }
}

impl RouterError {
    pub fn decode_error<E: std::error::Error + 'static>(error: E) -> Self {
        Self::DecodeError(Box::new(error))
    }
}
