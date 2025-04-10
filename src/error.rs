use jsonrpsee::{client_transport::ws::WsHandshakeError, types::ErrorObject};
use solana_rpc_client_api::client_error;

#[derive(Debug, thiserror::Error)]
pub enum RouterError {
    #[error("upstream websocket handshake: {0}")]
    WsHandshake(#[from] WsHandshakeError),
    #[error("solana rpc request error: {0}")]
    Rpc(#[from] client_error::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

// TODO @@@ implement errors
impl From<RouterError> for ErrorObject<'_> {
    fn from(value: RouterError) -> Self {
        ErrorObject::owned::<()>(0, "", None)
    }
}
