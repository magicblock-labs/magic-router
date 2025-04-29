use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use solana_account_decoder::{parse_token::UiTokenAmount, UiAccount};
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcContextConfig},
    response::Response,
};

use crate::types::SerdePubkey;

#[rpc(server)]
pub trait RoHttpRpc {
    #[method(name = "getAccountInfo")]
    async fn account_info(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Option<UiAccount>>>;

    #[method(name = "getMultipleAccounts")]
    async fn multiple_accounts(
        &self,
        pubkeys: Vec<SerdePubkey>,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Vec<Option<UiAccount>>>>;

    #[method(name = "getBalance")]
    async fn balance(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<u64>>;

    #[method(name = "getTokenAccountBalance")]
    async fn token_account_balance(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<UiTokenAmount>>;
}
