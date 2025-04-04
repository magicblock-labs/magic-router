use jsonrpsee::{
    core::{async_trait, SubscriptionResult},
    PendingSubscriptionSink,
};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;

use crate::rpc::websocket::WebsocketRpcServer;

pub struct WebsocketServer {
    cache: (),
}

#[async_trait]
impl WebsocketRpcServer for WebsocketServer {
    async fn account_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        pubkey: Pubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        todo!()
    }
}
