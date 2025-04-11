use jsonrpsee::{core::SubscriptionResult, proc_macros::rpc};
use solana_rpc_client_api::config::RpcAccountInfoConfig;

use crate::types::SerdePubkey;

#[rpc(server)]
pub trait WebsocketRpc {
    #[subscription(name = "accountSubscribe", unsubscribe = "accountUnsubscribe", item = json::Value)]
    async fn account_subscribe(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult;
}
