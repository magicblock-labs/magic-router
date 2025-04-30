use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashMap, pin::Pin};

use flume::Receiver as StealingReceiver;
use jsonrpsee::client_transport::ws::{
    EitherStream, Receiver, Sender, WsHandshakeError, WsTransportClientBuilder,
};
use jsonrpsee::core::client::{ReceivedMessage, TransportReceiverT, TransportSenderT};
use tokio::sync::mpsc::Sender as TokioSender;
use tokio_util::compat::Compat;
use url::Url;

use crate::pubsub::notification::{Notification, WebsocketMessage};
use crate::types::{RequestId, SubscriberId, SubscriptionId, UniqueId};

use super::notification::PubsubMessage;
use super::subscription::{Subscription, SubscriptionAction};

type SubscriptionsDB = HashMap<SubscriptionId, HashMap<SubscriberId, SubscriberHandle>>;

/// Single websocket connection handler
pub struct WebsocketConnection {
    id: u32,
    sender: Sender<Compat<EitherStream>>,
    receiver: Receiver<Compat<EitherStream>>,
    subscriptions: SubscriptionsDB,
    inflights: HashMap<RequestId, Subscription>,
    request_to_subs: HashMap<RequestId, SubscriptionId>,
    requests_rx: StealingReceiver<SubscriptionAction>,
    url: Arc<Url>,
}

impl WebsocketConnection {
    pub async fn new(
        id: u32,
        url: Arc<Url>,
        requests_rx: StealingReceiver<SubscriptionAction>,
    ) -> Result<Self, WsHandshakeError> {
        let (sender, receiver) = WsTransportClientBuilder::default()
            .build(Url::clone(&url))
            .await?;

        Ok(Self {
            id,
            sender,
            receiver,
            subscriptions: HashMap::new(),
            inflights: HashMap::new(),
            request_to_subs: HashMap::new(),
            requests_rx,
            url,
        })
    }

    pub async fn run(mut self) {
        // we keep the future around, as it's not cancel safe and dropping
        // it in select! causes partial reads and all of the ensuing chaos
        let mut future = self.receiver.receive();
        let mut receiving = unsafe { Pin::new_unchecked(&mut future) };
        macro_rules! reset_receiving_future {
            () => {
                drop(future);
                future = self.receiver.receive();
                receiving = unsafe { Pin::new_unchecked(&mut future) };
            };
            (reestablish) => {
                drop(future);
                if !self.reestablish().await {
                    tracing::warn!("failed to reconnect to websocket, terminating");
                    break;
                }
                future = self.receiver.receive();
                receiving = unsafe { Pin::new_unchecked(&mut future) };
            };
        }

        loop {
            tokio::select! {
                Ok(data) = &mut receiving => {
                    reset_receiving_future!();

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
                                tracing::warn!(id=?params.subscription, "received unknown subscription with no listeners");
                                continue;
                            };
                            let notification = Arc::new(params.result);

                            let mut to_remove = Vec::new();
                            for (id, sh) in &mut *listeners {
                                let msg = PubsubMessage::Notification { id: sh.request_id, payload: notification.clone() };
                                if sh.tx.send(msg).await.is_err() {
                                    tracing::warn!(id=id.0, "subscriber has unxpectedly closed the channel");
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
                                tracing::warn!(id=s.id.0, "received sub confirmation for unknown request");
                                continue;
                            };
                            self.request_to_subs.insert(s.id, s.result);
                            let tx = sub.tx;
                            if tx.send(PubsubMessage::Subscribed(s.id)).await.is_err() {
                                tracing::warn!(id=?sub.subscriber_id, "subscriber stopped listening for subscription");
                                continue;
                            }
                            let handle = SubscriberHandle { tx, request_id: sub.request_id };
                            self.subscriptions.entry(s.result).or_default().insert(sub.subscriber_id, handle);
                        }
                        WebsocketMessage::Unsubscribed(u) => {
                            tracing::debug!(id=u.id.0, "unsubscribed from subscription");
                        }
                    }
                }
                Ok(action) = self.requests_rx.recv_async() => {
                    match action {
                        SubscriptionAction::Subscribe(s) => {
                            let payload = s.payload.to_string();
                            self.inflights.insert(s.request_id, s);
                            if let Err(error) = self.sender.send(payload).await {
                                tracing::error!(url=%self.url, %error, "failed to send subscription request to websocket");
                                reset_receiving_future!(reestablish);
                            }
                        }
                        SubscriptionAction::Unsubscribe(u) => {
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
                                reset_receiving_future!(reestablish);
                            }
                        }
                    }
                }
                else => {
                    if self.requests_rx.is_disconnected() {
                        tracing::info!(id=self.id, url=%self.url, "server is shutting down, terminating websocket connection");
                        break;
                    }
                    reset_receiving_future!(reestablish);
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
        self.receiver = receiver;
        true
    }
}

struct SubscriberHandle {
    request_id: RequestId,
    tx: TokioSender<PubsubMessage>,
}
