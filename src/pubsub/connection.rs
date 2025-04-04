use std::sync::Arc;
use std::{collections::HashMap, pin::Pin};

use jsonrpsee::client_transport::ws::{EitherStream, Receiver, Sender, WsTransportClientBuilder};
use jsonrpsee::core::client::{ReceivedMessage, TransportReceiverT};
use tokio::sync::mpsc::{Receiver as TokioReceiver, Sender as TokioSender};
use tokio_util::compat::Compat;
use url::Url;

use crate::pubsub::notification::{Notification, WebsocketMessage};
use crate::types::{RequestId, SubscriberId, SubscriptionId};

use super::subscription::SubscriptionAction;

type SubscriptionsDB =
    HashMap<SubscriptionId, HashMap<SubscriberId, TokioSender<Arc<json::Value>>>>;

struct WebsocketConnection {
    sender: Sender<Compat<EitherStream>>,
    receiver: Receiver<Compat<EitherStream>>,
    subscriptions: SubscriptionsDB,
    inflights: HashMap<RequestId, SubscriptionAction>,
    requests_rx: TokioReceiver<SubscriptionAction>,
}

impl WebsocketConnection {
    async fn new(
        url: Url,
        requests_rx: TokioReceiver<SubscriptionAction>,
    ) -> Result<Self, soketto::BoxedError> {
        let (sender, receiver) = WsTransportClientBuilder::default().build(url).await?;

        Ok(Self {
            sender,
            receiver,
            subscriptions: HashMap::new(),
            inflights: HashMap::new(),
            requests_rx,
        })
    }

    async fn run(&mut self) {
        // we keep the future around, as it's not cancel safe and dropping
        // it in select! causes partial reads and all of the ensuing chaos
        let mut future = self.receiver.receive();
        let mut receiving = unsafe { Pin::new_unchecked(&mut future) };

        macro_rules! reset_receiving_future {
            () => {};
        }
        loop {
            tokio::select! {
                Ok(data) = &mut receiving => {
                    drop(future);
                    future = self.receiver.receive();
                    receiving = unsafe { Pin::new_unchecked(&mut future) };

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
                                tracing::warn!(id=params.subscription, "received unknown subscription with no listeners");
                                continue;
                            };
                            let notification = Arc::new(params.result);
                            for (id, tx) in listeners {
                                if tx.send(notification.clone()).await.is_err() {
                                    tracing::warn!(id=id.0, "subscriber has unxpectedly closed the channel");
                                }
                            }
                        }
                        WebsocketMessage::Subscribed(s) => {
                            let Some(sub) = self.inflights.remove(&s.id) else {
                                tracing::warn!(id=s.id.0, "received sub confirmation for unknown request");
                                continue;
                            };
                            let SubscriptionAction::Subscribe(sub) = sub else {
                                tracing::warn!(id=s.id.0, "received sub confirmation for pending unsupscription");
                                continue;
                            };
                            self.subscriptions.entry(s.result).or_default().insert(sub.subscriber_id, sub.tx);
                        }
                        WebsocketMessage::Unsubscribed(u) => {
                            tracing::debug!(id=u.id.0, "unsubscribed from subscription");
                        }
                    }
                }
                Some(action) = self.requests_rx.recv() => {

                }
            }
        }
    }
}
