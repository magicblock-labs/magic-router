use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use json::JsonValueTrait;
use jsonrpsee::{
    core::{async_trait, SubscriptionResult},
    types::ErrorObject,
    PendingSubscriptionSink, SubscriptionMessage, SubscriptionSink,
};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcSignatureSubscribeConfig};
use solana_signature::Signature;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    accounts::DELEGATION_PROGRAM_STR,
    cache::{
        delegations::DelegationsCache, routes::RoutingTable, transactions::ForwardedTransactions,
    },
    error::RouterError,
    pubsub::{
        notification::{PubsubMessage, SubscriptionHandle},
        subscription::{
            account_subscription_json, signature_subscription_json, Subscription, Unsubscription,
        },
        PubSubUpstreamKind,
    },
    rpc::websocket::WebsocketRpcServer,
    types::{RequestId, SerdePubkey, SubscriberId, UniqueId},
};

const UPSTREAM_SUB_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Websocket server for handling solana JSON-RPC websocket subscriptions
pub struct WebsocketServer {
    /// Database of delegation states of accounts
    pub delegations: Arc<DelegationsCache>,
    /// Database of routes to upstream ER nodes or base layer chain
    pub routes: Arc<RoutingTable>,
    /// Channel endpoint to websocket subscriptions dispatcher
    pub dispatcher_tx: Sender<Subscription>,
    /// Cache of recently forwared transaction signatures and their remote handles
    pub transactions: Arc<ForwardedTransactions>,
}

#[async_trait]
impl WebsocketRpcServer for WebsocketServer {
    async fn account_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let pubkey = pubkey.0;
        let authority = self.delegations.get_delegation_authority(pubkey).await;

        let chain = self.routes.base_chain().ws_url.clone();
        let ephem = match authority {
            Some(validator) => {
                let ephem = self
                    .routes
                    .ephemeral_handle(&validator)
                    .ok_or_else(|| RouterError::UnknownErNode(validator))?;
                Some(ephem.ws)
            }
            None => None,
        };

        let (pubsub_tx, mut pubsub_rx) = mpsc::channel(1024);
        let subscriber_id = SubscriberId::generate();
        let request_id = RequestId::generate();
        let payload = account_subscription_json(request_id, pubkey, params.clone());
        let chain_subscription = Subscription {
            request_id,
            subscriber_id,
            payload,
            tx: pubsub_tx.clone(),
            destination: chain,
            upstream: PubSubUpstreamKind::Chain,
        };
        let ephem_subscription = ephem.map(|url| chain_subscription.clone_with_destination(url));
        if let Some(sub) = ephem_subscription.clone() {
            let _ = self.dispatcher_tx.send(sub).await;
        }
        let _ = self.dispatcher_tx.send(chain_subscription).await;
        let confirmation = pubsub_rx.recv();
        let message = tokio::time::timeout(UPSTREAM_SUB_CONFIRMATION_TIMEOUT, confirmation)
            .await
            .map_err(|_| "upstream failed to confirm the subscription")?;
        let mut handles = HashMap::new();
        if let Some(PubsubMessage::Subscribed(handle)) = message {
            tracing::debug!(
                id = handle.request_id.0,
                %pubkey,
                "account subscription has been confirmed"
            );
            handles.insert(handle.request_id, handle);
        } else {
            Err("upstream failed to confirm the subscription for account")?;
        }
        let sink = pending.accept().await?;

        let handler = AccountSubscriptionHandler {
            pubkey,
            sink,
            subscriber_id,
            pubsub_rx,
            dispatcher_tx: self.dispatcher_tx.clone(),
            routes: self.routes.clone(),
            delegations: self.delegations.clone(),
            pubsub_tx,
            handles,
            params,
        };
        tokio::spawn(handler.run());
        Ok(())
    }

    async fn signature_subscribe(
        &self,
        pending: PendingSubscriptionSink,
        signature: String,
        params: Option<RpcSignatureSubscribeConfig>,
    ) -> SubscriptionResult {
        let sink = pending.accept().await?;
        let signature = Signature::from_str(&signature).map_err(RouterError::decode_error)?;
        let Some(handle) = self.transactions.get(&signature).await else {
            let error = RouterError::UnknownTransaction(signature);
            let msg = SubscriptionMessage::from_json(&ErrorObject::from(error))?;
            sink.send(msg).await?;
            return Ok(());
        };
        let (pubsub_tx, mut pubsub_rx) = mpsc::channel(1024);
        let subscriber_id = SubscriberId::generate();
        let request_id = RequestId::generate();
        let payload = signature_subscription_json(request_id, signature, params);
        let subscription = Subscription {
            request_id,
            subscriber_id,
            payload,
            tx: pubsub_tx.clone(),
            destination: handle.ws,
            upstream: PubSubUpstreamKind::Chain,
        };
        let _ = self.dispatcher_tx.send(subscription).await;
        let confirmation = pubsub_rx.recv();
        let result = tokio::time::timeout(UPSTREAM_SUB_CONFIRMATION_TIMEOUT, confirmation).await;
        let Ok(message) = result else {
            let error = RouterError::SubscriptionTimetout("signatureSubscribe");
            let msg = SubscriptionMessage::from_json(&ErrorObject::from(error))?;
            sink.send(msg).await?;
            return Ok(());
        };
        let handle = if let Some(PubsubMessage::Subscribed(handle)) = message {
            tracing::debug!(
                id = handle.request_id.0,
                %signature,
                "signature subscription has been confirmed"
            );
            handle
        } else {
            let error = RouterError::SubscriptionTimetout("signatureSubscribe");
            let msg = SubscriptionMessage::from_json(&ErrorObject::from(error))?;
            sink.send(msg).await?;
            return Ok(());
        };

        let handler = SignatureSubscriptionHandler {
            signature,
            sink,
            subscriber_id,
            pubsub_rx,
            handle,
        };
        tokio::spawn(handler.run());
        Ok(())
    }
}

/// Account subscription handler
struct AccountSubscriptionHandler {
    pubkey: Pubkey,
    subscriber_id: SubscriberId,
    sink: SubscriptionSink,
    pubsub_rx: Receiver<PubsubMessage>,
    pubsub_tx: Sender<PubsubMessage>,
    routes: Arc<RoutingTable>,
    delegations: Arc<DelegationsCache>,
    dispatcher_tx: Sender<Subscription>,
    handles: HashMap<RequestId, SubscriptionHandle>,
    params: Option<RpcAccountInfoConfig>,
}

struct SignatureSubscriptionHandler {
    subscriber_id: SubscriberId,
    signature: Signature,
    pubsub_rx: Receiver<PubsubMessage>,
    sink: SubscriptionSink,
    handle: SubscriptionHandle,
}

impl AccountSubscriptionHandler {
    /// Try to resubscribe to a different upstream in case
    /// of the account's delegation status has been changed
    async fn handle_delegation_status_change(
        &mut self,
        notification: &json::Value,
        id: RequestId,
        upstream: PubSubUpstreamKind,
    ) {
        let Some(owner_str) = notification
            .get("value")
            .and_then(|v| v.get("owner"))
            .and_then(|o| o.as_str())
        else {
            tracing::warn!(
                ?notification,
                "invalid account notification has been received"
            );
            return;
        };
        if owner_str != DELEGATION_PROGRAM_STR {
            return;
        }
        match upstream {
            PubSubUpstreamKind::Chain => {
                let authority = self.delegations.get_delegation_authority(self.pubkey).await;
                if let Some(identity) = authority {
                    let Some(handle) = self.routes.ephemeral_handle(&identity) else {
                        tracing::warn!(
                            account=%self.pubkey, %identity,
                            "account has been redelegated to the unknown ER"
                        );
                        return;
                    };
                    tracing::debug!(account=%self.pubkey, %identity, "account has been delegated");
                    let subscriber_id = SubscriberId::generate();
                    let request_id = RequestId::generate();
                    let payload =
                        account_subscription_json(request_id, self.pubkey, self.params.clone());
                    let sub = Subscription {
                        request_id,
                        subscriber_id,
                        payload,
                        tx: self.pubsub_tx.clone(),
                        destination: handle.ws,
                        upstream: PubSubUpstreamKind::Ephem,
                    };
                    let _ = self.dispatcher_tx.send(sub).await;
                }
            }
            PubSubUpstreamKind::Ephem => {
                let Some(handler) = self.handles.remove(&id) else {
                    return;
                };
                let unsub = Unsubscription {
                    request_id: id,
                    subscriber_id: self.subscriber_id,
                    method: "accountUnsubscribe",
                };
                let _ = handler.unsub.send(unsub).await;
            }
        }
    }

    async fn run(mut self) {
        tracing::debug!(
            id = self.subscriber_id.0, pubkey=%self.pubkey,
            "starting the account subscription handler"
        );
        loop {
            tokio::select! {
                _ = self.sink.closed() => {
                    tracing::debug!(id=self.subscriber_id.0, "terminating client subscription handler");
                    for (request_id, h) in self.handles.drain() {
                        let unsub = Unsubscription {
                            request_id,
                            subscriber_id: self.subscriber_id,
                            method: "accountUnsubscribe",
                        };
                        if let Err(error) = h.unsub.send(unsub).await {
                            tracing::warn!(
                                %error,
                                id = self.subscriber_id.0,
                                "failed to send an unsubscribe request from the subscription handler"
                            );
                        }
                    }
                    break;
                }
                Some(msg) = self.pubsub_rx.recv() => {
                    match msg {
                        PubsubMessage::Subscribed(handle) => {
                            tracing::debug!(id=handle.request_id.0, "subscription has been confirmed");
                            self.handles.insert(handle.request_id, handle);
                        }
                        PubsubMessage::Notification { payload, id, upstream } => {
                            self.handle_delegation_status_change(&payload, id, upstream).await;
                            let Ok(msg) = SubscriptionMessage::new(
                                "accountNotification",
                                self.sink.subscription_id(),
                                &payload
                            ) else {
                                tracing::warn!("failed to serialize json value, should never happen");
                                continue;
                            };
                            if self.sink.send(msg).await.is_err() {
                                tracing::debug!(id=id.0, "websocket sink for subscription has been closed");
                                break;
                            }
                        }
                        PubsubMessage::Disconnected(id) => {
                            let Some(handle) = self.handles.remove(&id) else {
                                continue
                            };
                            tracing::warn!(id=id.0, "subscription has lost upstream connection, resubscribing");
                            let authority = self.delegations.get_delegation_authority(self.pubkey).await;
                            let payload = account_subscription_json(
                                handle.request_id,
                                self.pubkey,
                                self.params.clone()
                            );
                            let destination = match authority {
                                Some(validator) => {
                                    let Some(handle) = self.routes.ephemeral_handle(&validator) else {
                                        tracing::warn!(
                                            account = %self.pubkey, %validator,
                                            "account has been delegated to the unknown ER"
                                        );
                                        continue;

                                    };
                                    handle.ws
                                }
                                None => self.routes.base_chain().ws_url.clone(),
                            };
                            let sub = Subscription {
                                request_id: handle.request_id,
                                subscriber_id: self.subscriber_id,
                                payload,
                                tx: self.pubsub_tx.clone(),
                                destination,
                                upstream: handle.upstream,
                            };
                            let _ = self.dispatcher_tx.send(sub).await;
                        }
                    }
                }
            }
        }
        tracing::debug!(
            id = self.subscriber_id.0,
            pubkey = %self.pubkey,
            "account websocket subscription handler has terminated",
        );
    }
}

impl SignatureSubscriptionHandler {
    async fn run(mut self) {
        tracing::debug!(
            signature=%self.signature,
            "starting the signature subscription handler"
        );
        let mut receive_timeout = tokio::time::interval(Duration::from_secs(60));
        receive_timeout.tick().await;
        tokio::select! {
            _ = self.sink.closed() => {
                tracing::debug!(
                    id=self.subscriber_id.0,
                    "terminating client subscription handler"
                );
                let unsub = Unsubscription {
                    request_id: self.handle.request_id,
                    subscriber_id: self.subscriber_id,
                    method: "signatureUnsubscribe",
                };
                if let Err(error) = self.handle.unsub.send(unsub).await {
                    tracing::warn!(
                        %error,
                        id = self.subscriber_id.0,
                        "failed to send an unsubscribe request from \
                        the signature subscription handler"
                    );
                }

            }
            Some(msg) = self.pubsub_rx.recv() => {
                if let PubsubMessage::Notification { payload, ..} = msg {
                    let Ok(msg) = SubscriptionMessage::new(
                        "signatureNotification",
                        self.sink.subscription_id(),
                        &payload
                    ) else {
                        tracing::warn!("failed to serialize json value, should never happen");
                        return;
                    };
                    if self.sink.send(msg).await.is_err() {
                        tracing::debug!("websocket sink for subscription has been closed");
                    }
                }
            }
            _ = receive_timeout.tick() => {}
        }

        tracing::debug!(
            id = self.subscriber_id.0,
            signature = %self.signature,
            "signature websocket subscription handler has terminated",
        );
    }
}
