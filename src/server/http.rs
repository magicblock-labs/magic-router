use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use jsonrpsee::core::{async_trait, RpcResult};
use solana_account_decoder::{
    encode_ui_account, parse_token::UiTokenAmount, UiAccount, UiAccountEncoding,
};
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcContextConfig},
    response::{Response, RpcResponseContext},
};

use crate::{
    accounts::DelegationStatus,
    cache::{delegations::DelegationsCache, routes::RoutingTable},
    error::RouterError,
    rpc::http::RoHttpRpcServer,
    types::SerdePubkey,
};

pub struct HttpServer {
    pub delegations: Arc<DelegationsCache>,
    pub routes: Arc<RoutingTable>,
}

#[async_trait]
impl RoHttpRpcServer for HttpServer {
    async fn account_info(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Option<UiAccount>>> {
        let pubkey = pubkey.0;
        let client = match self.delegations.get_delegation_status(pubkey).await {
            DelegationStatus::Delegated(identity) => self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            DelegationStatus::NotDelegated => self.routes.base_chain().client.clone(),
        };
        let config = params.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64Zstd);
        let data_slice_config = config.data_slice;
        let response = client
            .get_account_with_config(&pubkey, config)
            .await
            .map_err(RouterError::from)?;
        let uiaccount = response
            .value
            .map(|account| encode_ui_account(&pubkey, &account, encoding, None, data_slice_config));
        Ok(Response {
            context: response.context,
            value: uiaccount,
        })
    }

    async fn multiple_accounts(
        &self,
        pubkeys: Vec<SerdePubkey>,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Vec<Option<UiAccount>>>> {
        let mut delegated_pubkeys = HashMap::<_, Vec<_>>::new();
        let mut undelegated_pubkeys = Vec::new();

        let config = params.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64Zstd);
        let data_slice_config = config.data_slice;

        let mut response = vec![None; pubkeys.len()];
        for (i, pk) in pubkeys.into_iter().map(|k| k.0).enumerate() {
            match self.delegations.get_delegation_status(pk).await {
                DelegationStatus::Delegated(identity) => {
                    delegated_pubkeys.entry(identity).or_default().push((i, pk));
                }
                DelegationStatus::NotDelegated => {
                    undelegated_pubkeys.push((i, pk));
                }
            }
        }

        let slot = AtomicU64::new(0);
        let mut requests = FuturesUnordered::new();
        for (identity, pubkeys) in delegated_pubkeys {
            let client = self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?;
            let config = config.clone();
            let slot = &slot;
            let req = async move {
                let pks: Vec<_> = pubkeys.iter().map(|(_, pk)| *pk).collect();
                let response = client
                    .get_multiple_accounts_with_config(&pks, config)
                    .await?;
                let accounts = pubkeys.into_iter().zip(response.value);
                slot.fetch_max(response.context.slot, Ordering::Relaxed);
                Ok::<_, RouterError>(accounts)
            }
            .boxed();
            requests.push(req);
        }
        if !undelegated_pubkeys.is_empty() {
            let client = self.routes.base_chain().client.clone();
            let slot = &slot;
            let req = async move {
                let pks: Vec<_> = undelegated_pubkeys.iter().map(|(_, pk)| *pk).collect();
                let response = client
                    .get_multiple_accounts_with_config(&pks, config)
                    .await?;
                let accounts = undelegated_pubkeys.into_iter().zip(response.value);
                slot.fetch_max(response.context.slot, Ordering::Relaxed);
                Ok::<_, RouterError>(accounts)
            }
            .boxed();
            requests.push(req);
        }
        while let Some(accounts) = requests.next().await {
            for ((index, pubkey), account) in accounts? {
                response[index] = account.map(|account| {
                    encode_ui_account(&pubkey, &account, encoding, None, data_slice_config)
                });
            }
        }

        Ok(Response {
            context: RpcResponseContext::new(slot.load(Ordering::Relaxed)),
            value: response,
        })
    }

    async fn balance(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<u64>> {
        let pubkey = pubkey.0;
        let client = match self.delegations.get_delegation_status(pubkey).await {
            DelegationStatus::Delegated(identity) => self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            DelegationStatus::NotDelegated => self.routes.base_chain().client.clone(),
        };
        let commitment = params.unwrap_or_default().commitment.unwrap_or_default();
        client
            .get_balance_with_commitment(&pubkey, commitment)
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
    }

    async fn token_account_balance(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<UiTokenAmount>> {
        let pubkey = pubkey.0;
        let client = match self.delegations.get_delegation_status(pubkey).await {
            DelegationStatus::Delegated(identity) => self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            DelegationStatus::NotDelegated => self.routes.base_chain().client.clone(),
        };
        let commitment = params.unwrap_or_default().commitment.unwrap_or_default();
        client
            .get_token_account_balance_with_commitment(&pubkey, commitment)
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
    }
}
