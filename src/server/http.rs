use jsonrpsee::core::{async_trait, client::SubscriptionClientT, RpcResult};
use solana_account_decoder::UiAccount;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcContextConfig},
    response::{Response, RpcTokenAccountBalance},
};

use crate::rpc::http::RoHttpRpcServer;

pub struct HttpServer {
    cache: (),
}

#[async_trait]
impl RoHttpRpcServer for HttpServer {
    async fn account_info(
        &self,
        pubkey: Pubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<UiAccount>> {
        todo!()
    }

    async fn multiple_accounts(
        &self,
        pubkeys: Vec<Pubkey>,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Vec<Option<UiAccount>>>> {
        todo!()
    }

    async fn balance(
        &self,
        pubkey: Pubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<u64>> {
        todo!()
    }

    async fn token_account_balance(
        &self,
        pubkey: Pubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<RpcTokenAccountBalance>> {
        todo!()
    }
}
