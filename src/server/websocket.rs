use std::{collections::HashMap, sync::Arc, time::Duration};

use json::JsonValueTrait;
use jsonrpsee::{
    core::{async_trait, SubscriptionResult},
    PendingSubscriptionSink, SubscriptionMessage, SubscriptionSink,
};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    accounts::DELEGATION_PROGRAM_STR,
    cache::{delegations::DelegationsCache, routes::RoutingTable},
    error::RouterError,
    pubsub::{
        notification::{PubsubMessage, SubscriptionHandle},
        subscription::{account_subscription_json, Subscription, Unsubscription},
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
                    .ephemeral_url(&validator)
                    .ok_or_else(|| RouterError::UnknownErNode(validator))?;
                Some(ephem)
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
        let _ = self.dispatcher_tx.send(chain_subscription.clone()).await;
        if let Some(sub) = ephem_subscription.clone() {
            let _ = self.dispatcher_tx.send(sub).await;
        }
        let confirmation = pubsub_rx.recv();
        let message = tokio::time::timeout(UPSTREAM_SUB_CONFIRMATION_TIMEOUT, confirmation)
            .await
            .map_err(|_| "upstream failed to confirm the subscription")?;
        let mut handles = HashMap::new();
        if let Some(PubsubMessage::Subscribed(handle)) = message {
            tracing::debug!(id = handle.request_id.0, %pubkey, "account subscription has been confirmed");
            handles.insert(handle.request_id, handle);
        } else {
            Err("upstream failed to confirm the subscription")?;
        }
        let sink = pending.accept().await?;

        let handler = SubscriptionHandler {
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
}

/// Client subscription handler
struct SubscriptionHandler {
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

impl SubscriptionHandler {
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
                    let Some(destination) = self.routes.ephemeral_url(&identity) else {
                        tracing::warn!(
                            account = %self.pubkey, %identity,
                            "account has been redelegated to the unknown ER"
                        );
                        return;
                    };
                    let subscriber_id = SubscriberId::generate();
                    let request_id = RequestId::generate();
                    let payload =
                        account_subscription_json(request_id, self.pubkey, self.params.clone());
                    let sub = Subscription {
                        request_id,
                        subscriber_id,
                        payload,
                        tx: self.pubsub_tx.clone(),
                        destination,
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
            id = self.subscriber_id.0,
            "starting the client subscription handler"
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
                                    let Some(url) = self.routes.ephemeral_url(&validator) else {
                                        tracing::warn!(
                                            account = %self.pubkey, %validator,
                                            "account has been delegated to the unknown ER"
                                        );
                                        continue;

                                    };
                                    url
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
            "client websocket subscription handler has terminated",
        );
    }
}
