use std::{sync::Arc, time::Duration};

use json::JsonValueTrait;
use jsonrpsee::{
    core::{async_trait, SubscriptionResult},
    PendingSubscriptionSink, SubscriptionMessage, SubscriptionSink,
};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcAccountInfoConfig;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    accounts::{DelegationStatus, DELEGATION_PROGRAM_STR},
    cache::{delegations::DelegationsCache, routes::RoutingTable},
    error::RouterError,
    pubsub::{
        notification::PubsubMessage,
        subscription::{account_subscription_json, Subscription, SubscriptionAction},
    },
    rpc::websocket::WebsocketRpcServer,
    types::{RequestId, SerdePubkey, SubscriberId, UniqueId},
};

const UPSTREAM_SUB_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(30);

pub struct WebsocketServer {
    delegations: Arc<DelegationsCache>,
    routes: Arc<RoutingTable>,
    dispatcher_tx: Sender<SubscriptionAction>,
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
        let status = self.delegations.get_delegation_status(pubkey).await;

        let chain = self.routes.base_chain().ws_url.clone();
        let ephem = match status {
            DelegationStatus::Delegated(validator) => {
                let ephem = self
                    .routes
                    .ephemeral_url(&validator)
                    .ok_or_else(|| RouterError::UnknownErNode(validator))?;
                Some(ephem)
            }
            DelegationStatus::NotDelegated => None,
        };

        let (pubsub_tx, mut pubsub_rx) = mpsc::channel(1024);
        let subscriber_id = SubscriberId::generate();
        let request_id = RequestId::generate();
        let payload = account_subscription_json(request_id, pubkey, params);
        let chain_subscription = Subscription {
            request_id,
            subscriber_id,
            payload,
            tx: pubsub_tx.clone(),
            destination: chain,
        };
        let ephem_subscription = ephem.map(|url| chain_subscription.clone_with_destination(url));
        let _ = self
            .dispatcher_tx
            .send(SubscriptionAction::Subscribe(chain_subscription.clone()))
            .await;
        if let Some(sub) = ephem_subscription.clone() {
            let _ = self
                .dispatcher_tx
                .send(SubscriptionAction::Subscribe(sub))
                .await;
        }
        let confirmation = pubsub_rx.recv();
        tokio::time::timeout(UPSTREAM_SUB_CONFIRMATION_TIMEOUT, confirmation)
            .await
            .map_err(|_| "upstream failed to confirm the subscription")?;
        let sink = pending.accept().await?;

        let handler = SubscriptionHandler {
            pubkey,
            sink,
            subscriber_id,
            pubsub_rx,
            pubsub_tx,
            chain_subscription,
            ephem_subscription,
            dispatcher_tx: self.dispatcher_tx.clone(),
            routes: self.routes.clone(),
            delegations: self.delegations.clone(),
        };
        tokio::spawn(handler.run());
        Ok(())
    }
}

struct SubscriptionHandler {
    pubkey: Pubkey,
    subscriber_id: SubscriberId,
    sink: SubscriptionSink,
    pubsub_rx: Receiver<PubsubMessage>,
    routes: Arc<RoutingTable>,
    delegations: Arc<DelegationsCache>,
    pubsub_tx: Sender<PubsubMessage>,
    dispatcher_tx: Sender<SubscriptionAction>,
    chain_subscription: Subscription,
    ephem_subscription: Option<Subscription>,
}

impl SubscriptionHandler {
    async fn handle_delegation_status_change(&mut self, notification: &json::Value, id: RequestId) {
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
        let status = self.delegations.get_delegation_status(self.pubkey).await;
        if let Some(ref sub) = self.ephem_subscription {
            if sub.request_id == id || !status.is_delegated() {
                let _ = self
                    .dispatcher_tx
                    .send(sub.to_unsubsciption("accountUnsubscribe"))
                    .await;
                self.ephem_subscription.take();
            }
        } else if let DelegationStatus::Delegated(identity) = status {
            let Some(url) = self.routes.ephemeral_url(&identity) else {
                return;
            };
            let sub = self.chain_subscription.clone_with_destination(url);
            let _ = self
                .dispatcher_tx
                .send(SubscriptionAction::Subscribe(sub.clone()))
                .await;
            self.ephem_subscription.replace(sub);
        }
    }

    async fn run(mut self) {
        tracing::debug!(
            id = self.subscriber_id.0,
            "starting the subscription handler"
        );
        loop {
            tokio::select! {
                _ = self.sink.closed() => {
                    tracing::debug!(id=self.subscriber_id.0, "terminating subscription handler");
                    let _ = self.dispatcher_tx.send(
                        self.chain_subscription.to_unsubsciption("accountUnsubscribe")
                    ).await;
                    if let Some(sub) = self.ephem_subscription {
                        let _ = self.dispatcher_tx.send(sub.to_unsubsciption("accountUnsubscribe")).await;
                    }
                    break;
                }
                Some(msg) = self.pubsub_rx.recv() => {
                    match msg {
                        PubsubMessage::Subscribed(id) => {
                            tracing::debug!(id=id.0, "subscription has been confirmed");
                        }
                        PubsubMessage::Notification { payload, id } => {
                            let Some(result) = payload.get("params").and_then(|p| p.get("result")) else {
                                tracing::warn!(
                                    id=id.0,
                                    ?payload,
                                    "received unparsable notification in subscription handler"
                                );
                                continue;
                            };
                            self.handle_delegation_status_change(result, id).await;
                            let Ok(msg) = SubscriptionMessage::new(
                                "accountNotification",
                                self.sink.subscription_id(),
                                result
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
                            let sub = if self.chain_subscription.request_id == id {
                                self.chain_subscription.clone()
                            } else if let Some(ref sub) = self.ephem_subscription {
                                sub.clone()
                            } else {
                                tracing::warn!(id=id.0, "lost connection to unknown subscription");
                                continue;
                            };
                            tracing::warn!(id=id.0, "subscription has lost upstream connection, resubscribing");
                            let _ = self
                                .dispatcher_tx
                                .send(SubscriptionAction::Subscribe(sub))
                                .await;
                        }
                    }
                }
            }
        }
    }
}
