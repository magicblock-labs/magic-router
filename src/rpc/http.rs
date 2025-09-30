use std::rc::Rc;

use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use solana_account_decoder::{parse_token::UiTokenAmount, UiAccount};
use solana_commitment_config::CommitmentConfig;
use solana_epoch_info::EpochInfo;
use solana_epoch_schedule::{
    EpochSchedule, DEFAULT_LEADER_SCHEDULE_SLOT_OFFSET, MINIMUM_SLOTS_PER_EPOCH,
};
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcContextConfig, RpcSendTransactionConfig, RpcTransactionConfig,
    },
    response::{Response, RpcBlockhash},
};
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionStatus,
};

use crate::types::{DelegationStatus, RouteInfo, RpcIdentity, SerdePubkey};

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

    #[method(name = "isBlockhashValid")]
    async fn is_blockhash_valid(
        &self,
        hash: String,
        params: Option<CommitmentConfig>,
    ) -> RpcResult<Response<bool>>;

    #[method(name = "getRoutes")]
    async fn routes(&self) -> RpcResult<Vec<RouteInfo>>;

    #[method(name = "getBlockhashForAccounts")]
    async fn blockhash_for_accounts(&self, accounts: Vec<SerdePubkey>) -> RpcResult<RpcBlockhash>;

    #[method(name = "getFirstAvailableBlock")]
    async fn first_available_block(&self) -> RpcResult<u64> {
        RpcResult::Ok(0)
    }

    #[method(name = "getEpochSchedule")]
    async fn epoch_schedule(&self) -> RpcResult<EpochSchedule> {
        RpcResult::Ok(EpochSchedule {
            slots_per_epoch: MINIMUM_SLOTS_PER_EPOCH,
            leader_schedule_slot_offset: DEFAULT_LEADER_SCHEDULE_SLOT_OFFSET,
            warmup: false,
            first_normal_epoch: 0,
            first_normal_slot: 0,
        })
    }

    #[method(name = "getEpochInfo")]
    async fn epoch_info(&self) -> RpcResult<EpochInfo> {
        RpcResult::Ok(EpochInfo {
            epoch: 0,
            slot_index: 0,
            slots_in_epoch: 0,
            absolute_slot: 0,
            block_height: 0,
            transaction_count: None,
        })
    }

    #[method(name = "getDelegationStatus")]
    async fn delegation_status(&self, pubkey: SerdePubkey) -> RpcResult<DelegationStatus>;

    #[method(name = "getLatestBlockhash")]
    async fn latest_blockhash(&self) -> RpcResult<Response<RpcBlockhash>>;
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
