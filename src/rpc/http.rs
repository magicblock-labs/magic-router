use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use solana_account_decoder::UiAccount;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcContextConfig},
    response::{Response, RpcTokenAccountBalance},
};

#[rpc(server, namespace = "get")]
pub trait RoHttpRpc {
    #[method(name = "getAccountInfo")]
    async fn account_info(
        &self,
        pubkey: Pubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<UiAccount>>;

    #[method(name = "getMultipleAccounts")]
    async fn multiple_accounts(
        &self,
        pubkeys: Vec<Pubkey>,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Vec<Option<UiAccount>>>>;

    #[method(name = "getBalance")]
    async fn balance(
        &self,
        pubkey: Pubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<u64>>;

    #[method(name = "getTokenAccountBalance")]
    async fn token_account_balance(
        &self,
        pubkey: Pubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<RpcTokenAccountBalance>>;
}
