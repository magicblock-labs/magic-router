use std::rc::Rc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use solana_account_decoder::{parse_token::UiTokenAmount, UiAccount};
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcContextConfig, RpcSendTransactionConfig, RpcTransactionConfig,
    },
    response::Response,
};
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionStatus,
};

use crate::types::{RouteInfo, RpcIdentity, SerdePubkey};

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

    #[method(name = "getIdentity")]
    async fn identity(&self) -> RpcResult<RpcIdentity>;

    #[method(name = "getSignatureStatuses")]
    async fn signature_statuses(
        &self,
        signatures: Vec<String>,
    ) -> RpcResult<Response<Vec<Option<TransactionStatus>>>>;

    #[method(name = "getTransaction")]
    async fn transaction(
        &self,
        signature: String,
        params: Option<RpcTransactionConfig>,
    ) -> RpcResult<Option<Rc<EncodedConfirmedTransactionWithStatusMeta>>>;

    #[method(name = "getRoutes")]
    async fn routes(&self) -> RpcResult<Vec<RouteInfo>>;
}

#[rpc(server)]
pub trait RwHttpRpc {
    #[method(name = "sendTransaction")]
    async fn send_transaction(
        &self,
        txn: String,
        params: Option<RpcSendTransactionConfig>,
    ) -> RpcResult<String>;
}
