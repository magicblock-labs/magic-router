use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flume::Receiver as StealingReceiver;
use jsonrpsee::client_transport::ws::{
    EitherStream, Receiver, Sender, WsHandshakeError, WsTransportClientBuilder,
};
use jsonrpsee::core::client::{ReceivedMessage, TransportReceiverT, TransportSenderT};
use tokio::sync::mpsc::Sender as TokioSender;
use tokio::sync::mpsc::{self, Receiver as TokioReceiver};
use tokio_util::compat::Compat;
use url::Url;

use crate::pubsub::notification::{Notification, SubscriptionHandle, WebsocketMessage};
use crate::types::{RequestId, SubscriberId, SubscriptionId, UniqueId};

use super::notification::PubsubMessage;
use super::subscription::{Subscription, Unsubscription};
use super::PubSubUpstreamKind;

type SubscriptionsDB = HashMap<SubscriptionId, HashMap<SubscriberId, SubscriberHandle>>;

/// Single websocket connection handler
pub struct WebsocketConnection {
    /// Connection id, used for logging
    id: u32,
    /// Write endpoint for underlying websocket stream
    sender: Sender<Compat<EitherStream>>,
    /// Read endpoint for underlying websocket stream
    receiver: TokioReceiver<ReceivedMessage>,
    /// All active subscriptions on this websocket connection
    subscriptions: SubscriptionsDB,
    /// Subscriptions which haven't yet been confirmed and assinged an ID
    inflights: HashMap<RequestId, Subscription>,
    /// Mapping between internal request id (used by various actor components) and subscription
    /// id assigned by the upstream. This map is used for unsubscribe requests
    request_to_subs: HashMap<RequestId, SubscriptionId>,
    /// Channel endpoint for subscription requests
    requests_rx: StealingReceiver<Subscription>,
    /// Dedicated receiving channel endpoint for unsubscription requests
    unsubscriptions_rx: TokioReceiver<Unsubscription>,
    /// Dedicated sending channel endpoint for unsubscription requests
    unsubscriptions_tx: TokioSender<Unsubscription>,
    /// Url of this websocket connection
    url: Arc<Url>,
}

impl WebsocketConnection {
    pub async fn new(
        id: u32,
        url: Arc<Url>,
        requests_rx: StealingReceiver<Subscription>,
    ) -> Result<Self, WsHandshakeError> {
        let (sender, receiver) = WsTransportClientBuilder::default()
            .build(Url::clone(&url))
            .await?;
        let (tx, rx) = mpsc::channel(1024);
        let (unsubscriptions_tx, unsubscriptions_rx) = mpsc::channel(1024);
        tokio::spawn(receive(id, receiver, tx));
        Ok(Self {
            id,
            sender,
            receiver: rx,
            subscriptions: HashMap::new(),
            inflights: HashMap::new(),
            request_to_subs: HashMap::new(),
            requests_rx,
            url,
            unsubscriptions_rx,
            unsubscriptions_tx,
        })
    }

    pub async fn run(mut self) {
        let mut ping = tokio::time::interval(Duration::from_secs(30));
        loop {
            tokio::select! {
                Some(data) = self.receiver.recv() => {
                    let result = match data {
                        ReceivedMessage::Text(text) => {
                            WebsocketMessage::deserialize(text.as_bytes())
                        }
                        ReceivedMessage::Bytes(bytes) => {
                            WebsocketMessage::deserialize(&bytes)
                        }
                        ReceivedMessage::Pong => {
                            continue;
                        }
                    };
                    let message = match result {
                        Ok(msg) => msg,
                        Err(error) => {
                            tracing::warn!(%error, "failed to deserialize the websocket message");
                            continue
                        }
                    };
                    match message {
                        WebsocketMessage::Notification(Notification { params }) => {
                            let Some(listeners) = self.subscriptions.get_mut(&params.subscription) else {
                                tracing::warn!(
                                    id=?params.subscription,
                                    url=%self.url,
                                    "received unknown subscription with no listeners"
                                );
                                continue;
                            };
                            let notification = Arc::new(params.result);

                            let mut to_remove = Vec::new();
                            for (id, sh) in &mut *listeners {
                                let msg = PubsubMessage::Notification {
                                    id: sh.request_id,
                                    payload: notification.clone(),
                                    upstream: sh.upstream
                                };
                                if sh.tx.send(msg).await.is_err() {
                                    tracing::warn!(
                                        id=id.0,
                                        "subscriber has unxpectedly closed the channel"
                                    );
                                    to_remove.push(*id);
                                }
                            }
                            for id in to_remove {
                                listeners.remove(&id);
                            }
                            if listeners.is_empty() {
                                self.subscriptions.remove(&params.subscription);
                            }
                        }
                        WebsocketMessage::Subscribed(s) => {
                            let Some(sub) = self.inflights.remove(&s.id) else {
                                tracing::warn!(
                                    id=s.id.0,
                                    url=%self.url,
                                    "received sub confirmation for unknown request"
                                );
                                continue;
                            };
                            tracing::debug!(
                                id=s.id.0,
                                url=%self.url,
                                "received sub confirmation for pending request"
                            );

                            self.request_to_subs.insert(s.id, s.result);
                            let tx = sub.tx;
                            let handle = SubscriptionHandle {
                                request_id: s.id,
                                unsub: self.unsubscriptions_tx.clone(),
                                upstream: sub.upstream
                            };
                            if tx.send(PubsubMessage::Subscribed(handle)).await.is_err() {
                                tracing::warn!(id=?sub.subscriber_id, "subscriber stopped listening for subscription");
                                continue;
                            }
                            let handle = SubscriberHandle { tx, request_id: sub.request_id, upstream: sub.upstream };
                            self.subscriptions.entry(s.result).or_default().insert(sub.subscriber_id, handle);
                        }
                        WebsocketMessage::Unsubscribed(u) => {
                            tracing::debug!(id=u.id.0, "unsubscribed from subscription");
                        }
                    }
                }
                Some(u) = self.unsubscriptions_rx.recv() => {
                    let Some(subid) = self.request_to_subs.get(&u.request_id) else {
                        tracing::warn!(url=%self.url, "tried to unsubscribe from non-existent subscription");
                        continue;
                    };
                    let id = RequestId::generate();
                    let Some(subscribers) = self.subscriptions.get_mut(subid) else {
                        tracing::warn!(url=%self.url, "tried to unsubscribe from non-existent subscription");
                        continue;
                    };
                    subscribers.remove(&u.subscriber_id);
                    if subscribers.is_empty() {
                        self.subscriptions.remove(subid);
                    }
                    let unsubscription = format!(
                        r#"{{ "jsonrpc": "2.0", "id": {}, "method": "{}", "params": [ {} ] }}"#,
                        id.0, u.method, subid
                    );
                    if let Err(error) = self.sender.send(unsubscription).await {
                        tracing::error!(url=%self.url, %error, "failed to send unsubscription request to websocket");
                        if !self.reestablish().await {
                            break;
                        };
                    }

                }
                Ok(s) = self.requests_rx.recv_async() => {
                    let payload = s.payload.to_string();
                    tracing::debug!(
                        id=s.request_id.0,
                        url=%self.url,
                        "creating new subscription"
                    );
                    self.inflights.insert(s.request_id, s);
                    if let Err(error) = self.sender.send(payload).await {
                        tracing::error!(url=%self.url, %error, "failed to send subscription request to websocket");
                        if !self.reestablish().await {
                            break;
                        };
                    }
                }
                _ = ping.tick() => {
                    if let Err(error) = self.sender.send_ping().await {
                        tracing::error!(url=%self.url, %error, id = self.id, "failed to ping the connection");
                    } else {
                        continue;
                    }
                    if !self.reestablish().await {
                        break;
                    };
                }
                else => {
                    if self.requests_rx.is_disconnected() {
                        tracing::info!(id=self.id, url=%self.url, "server is shutting down, terminating websocket connection");
                        break;
                    }
                    if !self.reestablish().await {
                        break;
                    };
                }
            }
        }
    }

    /// Tries to reestablish the connection to upstream, there's a possibility that ER node is down
    /// (e.g. it went offline for maintanence or crashed), so we attempmt to reconnect several
    /// times to rule out network issues and then just signal the caller that all is lost, and we
    /// can terminate the connection task.
    async fn reestablish(&mut self) -> bool {
        const MAX_RECONNECT_ATTEMPTS: usize = 16;
        let mut attempt = 0;
        self.request_to_subs.clear();
        for (_, subscribers) in self.subscriptions.drain() {
            for subscriber in subscribers.values() {
                // it's ok to ignore ther result here, as we are dropping the whole thing anyway
                let _ = subscriber
                    .tx
                    .send(PubsubMessage::Disconnected(subscriber.request_id))
                    .await;
            }
        }
        let (sender, receiver) = loop {
            let result = WsTransportClientBuilder::default()
                .build(Url::clone(&self.url))
                .await;
            match result {
                Ok(c) => break c,
                Err(error) => {
                    attempt += 1;
                    tracing::error!(%attempt, %error, id=self.id, url=%self.url, "failed to reconnect to websocket");
                    if attempt > MAX_RECONNECT_ATTEMPTS {
                        return false;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        };
        self.sender = sender;
        let (tx, rx) = mpsc::channel(1024);
        tokio::spawn(receive(self.id, receiver, tx));
        self.receiver = rx;
        tracing::info!(
            id=self.id, url=%self.url,
            "connection has been reestablished"
        );
        true
    }
}

async fn receive(
    id: u32,
    mut receiver: Receiver<Compat<EitherStream>>,
    tx: TokioSender<ReceivedMessage>,
) {
    loop {
        match receiver.receive().await {
            Ok(m) => {
                if tx.send(m).await.is_err() {
                    tracing::info!(
                        "message receiver for ws connection {id} has been closed, terminating"
                    );
                    break;
                }
            }
            Err(e) => {
                tracing::warn!("ws connection {id} has been closed: {e}");
                break;
            }
        }
    }
}

struct SubscriberHandle {
    request_id: RequestId,
    upstream: PubSubUpstreamKind,
    tx: TokioSender<PubsubMessage>,
}
