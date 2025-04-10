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
    accounts::DelegationStatus,
    cache::{delegations::DelegationsCache, routes::RoutingTable},
    pubsub::{
        notification::PubsubMessage,
        subscription::{
            account_subscription_json, Subscription, SubscriptionAction, Unsubscription,
        },
    },
    rpc::websocket::WebsocketRpcServer,
    types::{RequestId, SubscriberId, UniqueId},
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
        pubkey: Pubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let status = self.delegations.get_delegation_status(pubkey).await;
        let destination = match status {
            DelegationStatus::Delegated(validator) => {
                self.routes.ephemeral_url(&validator).ok_or_else(|| {
                    format!("account {pubkey} has been delegated to an unknown ER node {validator}")
                })?
            }
            DelegationStatus::NotDelegated => self.routes.base_chain().ws_url.clone(),
        };
        let (pubsub_tx, mut pubsub_rx) = mpsc::channel(1024);
        let subscriber_id = SubscriberId::generate();
        let request_id = RequestId::generate();
        let payload = account_subscription_json(request_id, pubkey, params);
        let subscription = Subscription {
            request_id,
            subscriber_id,
            payload,
            tx: pubsub_tx.clone(),
            destination,
        };
        let _ = self
            .dispatcher_tx
            .send(SubscriptionAction::Subscribe(subscription.clone()))
            .await;
        let confirmation = pubsub_rx.recv();
        tokio::time::timeout(UPSTREAM_SUB_CONFIRMATION_TIMEOUT, confirmation)
            .await
            .map_err(|_| "upstream failed to confirm the subscription")?;
        let sink = pending.accept().await?;

        let handler = SubscriptionHandler {
            sink,
            subscriber_id,
            pubsub_rx,
            pubsub_tx,
            subscription,
            dispatcher_tx: self.dispatcher_tx.clone(),
            routes: self.routes.clone(),
        };
        tokio::spawn(handler.run());
        Ok(())
    }
}

struct SubscriptionHandler {
    subscriber_id: SubscriberId,
    sink: SubscriptionSink,
    pubsub_rx: Receiver<PubsubMessage>,
    routes: Arc<RoutingTable>,
    pubsub_tx: Sender<PubsubMessage>,
    dispatcher_tx: Sender<SubscriptionAction>,
    subscription: Subscription,
}

impl SubscriptionHandler {
    async fn run(mut self) {
        tracing::debug!(
            id = self.subscriber_id.0,
            "starting the subscription handler"
        );
        loop {
            tokio::select! {
                _ = self.sink.closed() => {
                    tracing::debug!(id=self.subscriber_id.0, "terminating subscription handler");
                    let _ = self.dispatcher_tx.send(SubscriptionAction::Unsubscribe(Unsubscription {
                        subscriber_id: self.subscriber_id,
                        request_id: self.subscription.request_id,
                        method: "accountUnsubscribe",
                        destination: self.subscription.destination,
                    })).await;
                    break;
                }
                Some(msg) = self.pubsub_rx.recv() => {
                    match msg {
                        PubsubMessage::Subscribed(id) => {
                            tracing::debug!(id=id.0, "subscription has been confirmed");
                        }
                        PubsubMessage::Notification { payload, id } => {
                            let Some(result) = payload.get("params").and_then(|p| p.get("result")) else {
                                tracing::warn!(id=id.0, ?payload, "received unparsable notification in subscription handler");
                                continue;
                            };
                            let Ok(msg) = SubscriptionMessage::new("accountNotification", self.sink.subscription_id(), result) else {
                                tracing::warn!("failed to serialize json value, should never happen");
                                continue;
                            };
                            if self.sink.send(msg).await.is_err() {
                                tracing::debug!(id=id.0, "websocket sink for subscription has been closed");
                                break;
                            }
                        }
                        PubsubMessage::Disconnected(id) => {
                            tracing::warn!(id=id.0, "subscription has lost upstream connection, resubscribing");
                            let _ = self
                                .dispatcher_tx
                                .send(SubscriptionAction::Subscribe(self.subscription.clone()))
                                .await;
                        }
                    }
                }
            }
        }
    }
}
