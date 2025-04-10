use jsonrpsee::{core::SubscriptionResult, proc_macros::rpc};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;

#[rpc(server)]
pub trait WebsocketRpc {
    #[subscription(name = "accountSubscribe", unsubscribe = "accountUnsubscribe", item = json::Value)]
    async fn account_subscribe(
        &self,
        pubkey: Pubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult;
}
