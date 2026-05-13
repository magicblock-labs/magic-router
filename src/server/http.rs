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
use futures::{future::join_all, stream::FuturesUnordered, FutureExt, StreamExt};
use jsonrpsee::{
    core::{async_trait, RpcResult},
    types::ErrorObjectOwned,
};
use scc::HashCache;
use solana_account_decoder::{parse_token::UiTokenAmount, UiAccount};
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_rpc_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::{GetConfirmedSignaturesForAddress2Config, SerializableTransaction},
};
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcContextConfig, RpcSendTransactionConfig,
        RpcSignaturesForAddressConfig, RpcTransactionConfig,
    },
    response::{
        Response, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature, RpcResponseContext,
    },
};
use solana_signature::Signature;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, TransactionStatus, UiTransactionEncoding,
};

use crate::{
    cache::{
        delegations::DelegationsCache,
        routes::RoutingTable,
        transactions::{ForwardedTransactions, RemoteHandle},
    },
    error::RouterError,
    rpc::http::{RoHttpRpcServer, RwHttpRpcServer},
    types::{DelegationStatus, RouteInfo, RpcIdentity, SerdePubkey},
    RouterResult,
};

type BlockhashCache = Arc<HashCache<Hash, Arc<RpcClient>>>;

/// Http server implementation for handling solana JSON-RPC requests
#[derive(Clone)]
pub struct HttpServer {
    /// Database of delegation states of accounts
    pub delegations: Arc<DelegationsCache>,
    /// Database of routes to upstream ER nodes or base layer chain
    pub routes: Arc<RoutingTable>,
    /// Fixed capacity cache of transactions, sent through the router
    pub transactions: Arc<ForwardedTransactions>,
    /// Fixed capacity cache of blockhashes, requested through the router
    pub blockhashes: BlockhashCache,
}

impl HttpServer {
    async fn resolve_client(&self, pubkey: Pubkey) -> RouterResult<Arc<RpcClient>> {
        let client = match self.delegations.get_delegation_authority(pubkey).await {
            Some(identity) => self
                .routes
                .ephemeral_client(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            None => self.routes.base_chain().client.clone(),
        };
        Ok(client)
    }
}

#[async_trait]
impl RoHttpRpcServer for HttpServer {
    async fn account_info(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Option<UiAccount>>> {
        let pubkey = pubkey.0;
        let client = self.resolve_client(pubkey).await?;
        let config = params.unwrap_or_default();
        let response = client
            .get_ui_account_with_config(&pubkey, config)
            .await
            .map_err(RouterError::from)?;
        Ok(Response {
            context: response.context,
            value: response.value,
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

        let mut response = vec![None; pubkeys.len()];
        let mut futures = Vec::with_capacity(pubkeys.len());
        for pk in pubkeys.iter().map(|k| k.0) {
            let resolution = self.delegations.get_delegation_authority(pk);
            futures.push(resolution);
        }
        for (i, (status, pk)) in join_all(futures).await.into_iter().zip(pubkeys).enumerate() {
            match status {
                Some(identity) => {
                    delegated_pubkeys
                        .entry(identity)
                        .or_default()
                        .push((i, pk.0));
                }
                None => {
                    undelegated_pubkeys.push((i, pk.0));
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
                    .get_multiple_ui_accounts_with_config(&pks, config)
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
                    .get_multiple_ui_accounts_with_config(&pks, config)
                    .await?;
                let accounts = undelegated_pubkeys.into_iter().zip(response.value);
                slot.fetch_max(response.context.slot, Ordering::Relaxed);
                Ok::<_, RouterError>(accounts)
            }
            .boxed();
            requests.push(req);
        }
        while let Some(accounts) = requests.next().await {
            for ((index, _), account) in accounts? {
                response[index] = account;
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
        let client = self.resolve_client(pubkey).await?;
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
        let client = self.resolve_client(pubkey).await?;
        let commitment = params.unwrap_or_default().commitment.unwrap_or_default();
        client
            .get_token_account_balance_with_commitment(&pubkey, commitment)
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
    }

    async fn identity(&self) -> RpcResult<RpcIdentity> {
        let (identity, client) = self.routes.closest_node()?;
        Ok(RpcIdentity {
            identity,
            fqdn: client.url(),
        })
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
            .rpc
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
            .rpc
            .get_transaction_with_config(&signature, params.unwrap_or_default())
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
            .map(|t| Some(Rc::new(t)))
    }

    async fn is_blockhash_valid(
        &self,
        hash: String,
        params: Option<CommitmentConfig>,
    ) -> RpcResult<Response<bool>> {
        let hash = Hash::from_str(&hash).map_err(RouterError::decode_error)?;
        let mut response = Response {
            context: RpcResponseContext::new(0),
            value: false,
        };
        let Some(client) = self.blockhashes.get_async(&hash).await else {
            return Ok(response);
        };
        let commitment = params.unwrap_or_default();
        response.value = client
            .is_blockhash_valid(&hash, commitment)
            .await
            .map_err(RouterError::from)?;
        Ok(response)
    }

    async fn routes(&self) -> RpcResult<Vec<RouteInfo>> {
        Ok(self.routes.all_routes().await)
    }

    async fn blockhash_for_accounts(&self, accounts: Vec<SerdePubkey>) -> RpcResult<RpcBlockhash> {
        let mut delegated = None;
        for pk in accounts {
            let Some(validator) = self.delegations.get_delegation_authority(pk.0).await else {
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
        let _ = self.blockhashes.put_async(hash, client.clone()).await;
        let response = RpcBlockhash {
            blockhash: hash.to_string(),
            last_valid_block_height: slot,
        };
        Ok(response)
    }

    async fn delegation_status(&self, pubkey: SerdePubkey) -> RpcResult<DelegationStatus> {
        let record = self.delegations.get_record(pubkey.0).await;
        // Determine fqdn based on delegation status
        let fqdn: Option<String> = if let Some(ref rec) = record {
            let authority = rec.authority.0;
            let client = self
                .routes
                .ephemeral_client(&authority)
                .ok_or_else(|| RouterError::UnknownErNode(authority))?;
            Some(client.url())
        } else {
            None
        };
        let status = DelegationStatus {
            is_delegated: record.is_some(),
            fqdn,
            delegation_record: record,
        };
        Ok(status)
    }

    async fn latest_blockhash(&self) -> RpcResult<Response<RpcBlockhash>> {
        let (_, client) = self.routes.closest_node()?;
        let (hash, slot) = client
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await
            .map_err(RouterError::from)?;
        let _ = self.blockhashes.put_async(hash, client.clone()).await;
        let value = RpcBlockhash {
            blockhash: hash.to_string(),
            last_valid_block_height: slot + 150,
        };
        Ok(Response {
            context: RpcResponseContext::new(slot),
            value,
        })
    }

    async fn signatures_for_address(
        &self,
        pubkey: SerdePubkey,
        config: Option<RpcSignaturesForAddressConfig>,
    ) -> RpcResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let client = self.resolve_client(pubkey.0).await?;
        let config = config.unwrap_or_default();
        let before = if let Some(s) = config.before {
            Some(Signature::from_str(&s).map_err(RouterError::decode_error)?)
        } else {
            None
        };
        let until = if let Some(s) = config.until {
            Some(Signature::from_str(&s).map_err(RouterError::decode_error)?)
        } else {
            None
        };
        let config = GetConfirmedSignaturesForAddress2Config {
            before,
            until,
            limit: config.limit,
            commitment: config.commitment,
        };
        client
            .get_signatures_for_address_with_config(&pubkey.0, config)
            .await
            .map_err(RouterError::from)
            .map_err(Into::into)
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
        let txn = bincode::deserialize::<VersionedTransaction>(&txn)
            .map_err(RouterError::decode_error)?;
        let mut delegation = None;
        for (i, pk) in txn.message.static_account_keys().iter().enumerate() {
            if !txn.message.is_maybe_writable(i, None) {
                continue;
            }
            let Some(validator) = self.delegations.get_delegation_authority(*pk).await else {
                continue;
            };
            let replaced = delegation.replace(validator);
            let Some(old) = replaced else {
                continue;
            };
            if old != validator {
                Err(RouterError::ConflictingDelegations)?;
            }
        }
        tracing::debug!(?delegation, "transaction accounts have");
        let handle = match delegation {
            Some(identity) => self
                .routes
                .ephemeral_handle(&identity)
                .ok_or_else(|| RouterError::UnknownErNode(identity))?,
            None => {
                let upstream = self.routes.base_chain();
                RemoteHandle {
                    rpc: upstream.client.clone(),
                    ws: upstream.ws_url.clone(),
                }
            }
        };
        let result = handle
            .rpc
            .send_transaction_with_config(&txn, params)
            .await
            .map_err(RouterError::from)
            .map(|s| s.to_string())?;
        self.transactions.track(*txn.get_signature(), handle).await;
        Ok(result)
    }
}
