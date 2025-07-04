use std::{
    collections::HashMap,
    rc::Rc,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use base64::Engine;
use futures::{stream::FuturesUnordered, FutureExt, StreamExt};
use jsonrpsee::{
    core::{async_trait, RpcResult},
    types::ErrorObjectOwned,
};
use solana_account_decoder::{
    encode_ui_account, parse_token::UiTokenAmount, UiAccount, UiAccountEncoding,
};
use solana_commitment_config::CommitmentConfig;
use solana_rpc_client::rpc_client::SerializableTransaction;
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcContextConfig, RpcSendTransactionConfig, RpcTransactionConfig,
    },
    response::{Response, RpcBlockhash, RpcResponseContext},
};
use solana_signature::Signature;
use solana_transaction::{versioned::VersionedTransaction, Transaction};
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionStatus, UiTransactionEncoding,
};

use crate::{
    accounts::DelegationStatus,
    cache::{
        delegations::DelegationsCache, routes::RoutingTable, transactions::ForwardedTransactions,
    },
    error::RouterError,
    rpc::http::{RoHttpRpcServer, RwHttpRpcServer},
    types::{RouteInfo, RpcIdentity, SerdePubkey},
};

/// Http server implementation for handling solana JSON-RPC requests
#[derive(Clone)]
pub struct HttpServer {
    /// Database of delegation states of accounts
    pub delegations: Arc<DelegationsCache>,
    /// Database of routes to upstream ER nodes or base layer chain
    pub routes: Arc<RoutingTable>,
    /// Fixed capacity cache of transactions, sent through the router
    pub transactions: Arc<ForwardedTransactions>,
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

    async fn identity(&self) -> RpcResult<RpcIdentity> {
        let (identity, fqdn) = self.routes.closest_node();
        Ok(RpcIdentity { identity, fqdn })
    }

    async fn signature_statuses(
        &self,
        signatures: Vec<String>,
    ) -> RpcResult<Response<Vec<Option<TransactionStatus>>>> {
        let signatures = signatures
            .into_iter()
            .map(|s| Signature::from_str(&s).map_err(RouterError::decode_error))
            .collect::<Result<Vec<_>, RouterError>>()?;
        let Some(sig) = signatures.first() else {
            return Ok(Response {
                context: RpcResponseContext::new(0),
                value: vec![],
            });
        };
        let Some(client) = self.transactions.get(sig).await else {
            return Ok(Response {
                context: RpcResponseContext::new(0),
                value: vec![],
            });
        };
        client
            .get_signature_statuses(&signatures)
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
    }

    async fn transaction(
        &self,
        signature: String,
        params: Option<RpcTransactionConfig>,
    ) -> RpcResult<Option<Rc<EncodedConfirmedTransactionWithStatusMeta>>> {
        let signature = Signature::from_str(&signature).map_err(RouterError::decode_error)?;
        let Some(client) = self.transactions.get(&signature).await else {
            return Ok(None);
        };
        client
            .get_transaction_with_config(&signature, params.unwrap_or_default())
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
            .map(|t| Some(Rc::new(t)))
    }

    async fn routes(&self) -> RpcResult<Vec<RouteInfo>> {
        Ok(self.routes.all_routes().await)
    }

    async fn blockhash_for_accounts(&self, accounts: Vec<SerdePubkey>) -> RpcResult<RpcBlockhash> {
        let mut delegated = None;
        for pk in accounts {
            let DelegationStatus::Delegated(validator) =
                self.delegations.get_delegation_status(pk.0).await
            else {
                continue;
            };

            let Some(old) = delegated.replace(validator) else {
                continue;
            };
            if old != validator {
                Err(RouterError::ConflictingDelegations)?;
            }
        }
        let client = match delegated {
            Some(identity) => self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            None => self.routes.base_chain().client.clone(),
        };
        let (hash, slot) = client
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await
            .map_err(RouterError::from)?;
        let response = RpcBlockhash {
            blockhash: hash.to_string(),
            last_valid_block_height: slot,
        };
        Ok(response)
    }
}

#[async_trait]
impl RwHttpRpcServer for HttpServer {
    async fn send_transaction(
        &self,
        txn: String,
        params: Option<RpcSendTransactionConfig>,
    ) -> RpcResult<String> {
        let params = params.unwrap_or_default();
        let encoding = params.encoding.unwrap_or(UiTransactionEncoding::Base58);
        let txn = match encoding {
            UiTransactionEncoding::Base58 | UiTransactionEncoding::Binary => bs58::decode(&txn)
                .into_vec()
                .map_err(RouterError::decode_error)?,
            UiTransactionEncoding::Base64 => base64::prelude::BASE64_STANDARD
                .decode(txn)
                .map_err(RouterError::decode_error)?,
            other => {
                return Err(ErrorObjectOwned::owned(
                    1,
                    format!("{other} transaction encoding is not supported"),
                    None::<()>,
                ))
            }
        };
        let txn = if let Ok(txn) = bincode::deserialize::<VersionedTransaction>(&txn) {
            txn
        } else {
            bincode::deserialize::<Transaction>(&txn)
                .map(VersionedTransaction::from)
                .map_err(RouterError::decode_error)?
        };
        let mut delegation = DelegationStatus::NotDelegated;
        for (i, pk) in txn.message.static_account_keys().iter().enumerate() {
            if !txn.message.is_maybe_writable(i, None) {
                continue;
            }
            let DelegationStatus::Delegated(validator) =
                self.delegations.get_delegation_status(*pk).await
            else {
                continue;
            };
            let replaced =
                std::mem::replace(&mut delegation, DelegationStatus::Delegated(validator));
            let DelegationStatus::Delegated(old) = replaced else {
                continue;
            };
            if old != validator {
                Err(RouterError::ConflictingDelegations)?;
            }
        }
        tracing::debug!(%delegation, "delegation status of transaction accounts");
        let client = match delegation {
            DelegationStatus::Delegated(identity) => self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            DelegationStatus::NotDelegated => self.routes.base_chain().client.clone(),
        };
        self.transactions
            .track(*txn.get_signature(), client.clone())
            .await;
        client
            .send_transaction_with_config(&txn, params)
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
            .map(|s| s.to_string())
    }
}
