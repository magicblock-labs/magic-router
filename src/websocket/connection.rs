//! Websocket connection for handling cache maintenance subscriptions

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use flume::Receiver;
use futures::{SinkExt, StreamExt};
use solana::pubkey::Pubkey;
use tokio::{net::TcpStream, sync::mpsc::Sender, time::Interval};
use url::Url;
use websocket::{ClientBuilder, MaybeTlsStream, Message, Payload, WebSocketStream};

use crate::{
    account::GetAccountInfoResponse,
    config::WebsocketConf,
    error::{Error, InternalError},
    http::client::HttpClient,
    websocket::message::{Notification, WebsocketMessage},
};

use super::subscription::AccountSubscription;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const SLOT_SUBSCRIPTION: &str =
    r#"{ "jsonrpc": "2.0", "id": 4294967295, "method": "slotSubscribe" }"#;

/// Handle to a websocket connection
pub struct WsConnection {
    /// actual websocket connection stream over TCP
    inner: WsStream,
    /// new connection builder, used for reconnection events
    builder: ClientBuilder<'static>,
    /// HTTP client for base chain requests
    chain: HttpClient,
    /// periodicity with which to send PING frames to websocket server
    /// acts as connection health checker
    ping: Interval,
    /// subscription requests which were sent but not yet confirmed request ID -> subscription meta
    pending: HashMap<u64, AccountSubscription>,
    /// confirmed subscriptions, subscription ID -> subscription meta
    active: HashMap<u64, AccountSubscription>,
    /// unsubscription requests which were sent but not yet confirmed request ID -> subscription meta
    unsubs: HashMap<u64, AccountSubscription>,
    /// stealing MPMC receiver of new subscriptions for newly encountered accounts
    rx: Receiver<AccountSubscription>,
    /// tokio MPSC sender of Pubkeys which were undelegated and thus need to be removed from cache
    tx: Sender<Pubkey>,
    /// endpoint to which the connection is established
    url: Url,
    /// maximum slot lag between fastest connection in the websocket connection pool, and current
    /// one, which when exceeded will trigger reconnection
    lag: u64,
    /// maximum observed slot among all active websocket connections in the pool
    slot: Arc<AtomicU64>,
}

impl WsConnection {
    /// Try to establish new websocket connection to endpoint
    pub async fn establish(
        url: Url,
        config: &WebsocketConf,
        chain: HttpClient,
        rx: Receiver<AccountSubscription>,
        tx: Sender<Pubkey>,
        slot: Arc<AtomicU64>,
    ) -> crate::Result<Self> {
        let builder = ClientBuilder::new()
            .uri(url.as_str())
            .map_err(|_| InternalError::InvalidUrl("websocket", url.clone()))?;
        let (inner, _) = builder.connect().await?;
        let pending = HashMap::new();
        let active = HashMap::new();
        let unsubs = HashMap::new();
        let ping = tokio::time::interval(config.ping_interval);
        let lag = config.max_slot_lag;
        Ok(Self {
            inner,
            builder,
            chain,
            ping,
            pending,
            active,
            unsubs,
            rx,
            tx,
            url,
            lag,
            slot,
        })
    }

    /// Start handling websocket connection: processing notifications, and managing subscriptions
    pub async fn start(mut self) {
        // subcribe to slot
        self.send(SLOT_SUBSCRIPTION).await;
        // conveniece Result<T, E> unwrapper with reconnection on error
        macro_rules! check {
            ($result: expr) => {
                match $result {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::warn!(%error, "websocket message handling error, reconnecting");
                        self.reestablish().await;
                        continue;
                    }
                }
            };
        }

        loop {
            // use biased ordering to turn off select! RNG and handle events in prioritized manner
            tokio::select! {
                // process incoming websocket messages
                biased; Some(msg) = self.inner.next() => {
                    let msg = check!(msg);
                    if msg.is_pong() || msg.is_ping() {
                        // server pings are automatically ponged by library
                        continue;
                    }
                    if msg.is_close() {
                        self.reestablish().await;
                        continue;
                    }
                    let payload = msg.into_payload();
                    let msg = check!(WebsocketMessage::deserialize(&payload));
                    match msg {
                        WebsocketMessage::Subscribed(r) => {
                            if let Some(sub) = self.pending.remove(&r.id) {
                                tracing::info!(pubkey=%sub.pubkey, id=r.result, "subscribed to account");
                                sub.subscribed.store(true, Ordering::Release);
                                self.active.insert(r.result, sub);
                            }
                        }
                        WebsocketMessage::Unsubscribed(r) => {
                            if let Some(sub) = self.unsubs.remove(&r.id) {
                                sub.subscribed.store(false, Ordering::Release);
                                tracing::info!(pubkey=%sub.pubkey, "unsubscribed from account");
                            } else {
                                tracing::warn!(id=%r.id, "unsubscribed from unknown subscription");
                            }
                        }
                        WebsocketMessage::Notification(n) => {
                            match n {
                                Notification::Slot{ params } => {
                                    let slot = params.result.slot;
                                    let max = self.slot.fetch_max(slot, Ordering::Release);
                                    if slot < max && max - slot > self.lag {
                                        tracing::warn!(lag=(max - slot), "connection to websocket is lagging slot-wise");
                                        self.reestablish().await;
                                    }
                                }
                                Notification::Account{ params } => {
                                    let Some(account) = self.active.get(&params.subscription) else {
                                        tracing::warn!(sub=params.subscription,"received account update via unknown subscription");
                                        continue;
                                    };
                                    if !params.result.is_delegated() {
                                        account.delegated.store(false, Ordering::Release);
                                        // infallible: checked above that subscription exists in self.active
                                        let account = self.active.remove(&params.subscription).unwrap();
                                        self.unsubscribe(account.id, params.subscription).await;
                                        let _ = self.tx.send(account.pubkey).await;
                                        self.unsubs.insert(account.id, account);
                                    } else {
                                        account.delegated.store(true, Ordering::Release);
                                    }
                                }
                            }
                        }
                    }
                }
                // process websocket pings
                _ = self.ping.tick() => {
                    let msg = Message::ping("");
                    check!(self.inner.send(msg).await);
                }
                // process subscription requests
                Ok(sub) = self.rx.recv_async() => {
                    let msg = Message::text(sub.ws());
                    self.pending.insert(sub.id, sub);
                    check!(self.inner.send(msg).await);
                }
                // check if shutdown signal has been received
                _ = crate::SHUTDOWN.notified() => {
                    for (id, sub) in self.active.drain() {
                        let msg = format!(
                            r#"{{ "jsonrpc": "2.0", "id": {}, "method": "accountUnsubscribe", "params": [{id}] }}"#, sub.id
                        );
                        let msg = Message::text(msg);
                        let _ = self.inner.feed(msg).await;
                        sub.subscribed.store(false, Ordering::Release);
                    }
                    let _ = self.inner.flush().await;
                    let _ = self.inner.close().await;
                    tracing::info!(url=%self.url, "shutting down websocket connection");
                    break;
                }
            }
        }
    }

    async fn reestablish(&mut self) {
        tracing::info!(url=%self.url.as_str(), sub=self.active.len(), "reconnecting to websocket stream");
        let _ = self.inner.close().await;
        for sub in self.active.values_mut().chain(self.pending.values_mut()) {
            sub.subscribed.store(false, Ordering::Release);
        }
        'outer: loop {
            self.inner = Self::connect(&self.builder, self.url.as_str()).await;
            // little hack to avoid extra allocations,
            // we are not leaving `reastablish` method
            // before connection is active and consistent
            // state is restored, so it's acceptable
            self.active.extend(self.pending.drain());
            let mut active = self.active.drain();
            while let Some((_, sub)) = active.next() {
                let msg = Message::text(sub.ws());
                self.pending.insert(sub.id, sub);
                // realistically speaking, this should never happen
                if let Err(error) = self.inner.feed(msg).await {
                    tracing::error!(
                            %error,
                            url = self.url.as_str(),
                            "failed to resubscribe to account after reconnect");
                    self.pending.extend(active.map(|(_, s)| (s.id, s)));
                    continue 'outer;
                }
            }
            // don't forget to resubscribe to slot updates
            if let Err(error) = self.inner.feed(Message::text(SLOT_SUBSCRIPTION)).await {
                tracing::error!(%error, url = self.url.as_str(), "failed to slotSubscribe");
            }
            break;
        }
        for sub in self.pending.values_mut() {
            let delegated = sub.delegated.clone();
            let request = sub.http();
            let chain = self.chain.clone();
            let pubkey = sub.pubkey;
            let tx = self.tx.clone();
            // in order for reconnection to happen as fast as possible,
            // we spawn actual account fetching into separate tasks, that
            // way delegation status retrieval happens asynchronously
            tokio::spawn(async move {
                let err = move |error: &Error| tracing::warn!(%pubkey, %error, "failed to fetch account from base layer");
                let response = chain.fetch(request).await.inspect_err(err)?;
                let bytes = response
                    .bytes()
                    .await
                    .map_err(Into::into)
                    .inspect_err(err)?;
                let account = json::from_slice::<GetAccountInfoResponse>(&bytes)
                    .map_err(Into::<InternalError>::into)
                    .map_err(Into::into)
                    .inspect_err(err)?;
                let still_delegated = account.result.map(|a| a.is_delegated()).unwrap_or_default();
                delegated.store(still_delegated, Ordering::Release);
                if !still_delegated {
                    let _ = tx.send(pubkey).await;
                }

                Ok::<_, Error>(()) // just so we can use `?`
            });
        }
        let _ = self.inner.flush().await;
    }

    async fn unsubscribe(&mut self, id: u64, subid: u64) {
        let msg = format!(
            r#"{{ "jsonrpc": "2.0", "id": {id}, "method": "accountUnsubscribe", "params": [{subid}] }}"#
        );
        self.send(msg).await;
    }

    async fn send<P: Into<Payload>>(&mut self, payload: P) {
        let msg = Message::text(payload);
        if let Err(error) = self.inner.send(msg).await {
            tracing::warn!(%error, "failed to subscribe to slot");
            self.reestablish().await;
        }
    }

    async fn connect(builder: &ClientBuilder<'_>, url: &str) -> WsStream {
        loop {
            match builder.connect().await {
                Ok((ws, _)) => break ws,
                Err(error) => {
                    tracing::warn!(
                        %error,
                        url,
                        "failed to reconnect to websocket"
                    );
                }
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    }
}
