use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use flume::{SendError, Sender};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio::sync::{mpsc::Receiver, Notify};
use url::Url;

use crate::pubsub::connection::WebsocketConnection;

use super::subscription::Subscription;

/// A websocket subscription routing hub. It manages all of the upstream connections
/// and properly directs subscription requests to provided URLs
pub struct SubscriptionDispatcher {
    /// Channel endpoint to receive any updates on websocket
    /// upstreams, like url change or going offline
    upstream_state_rx: Receiver<WsUpstreamState>,
    /// Channel endpoint for subscription/unsubscription requests
    requests_rx: Receiver<Subscription>,
    /// A map between upstreams (identified by their URL) and
    /// channel for communicating with them
    upstreams: HashMap<Arc<Url>, Sender<Subscription>>,
    /// Number of websocket connections to spawn for each websocket upstream
    connections_per_upstream: u16,
}

/// Current state of websocket upstream
pub struct WsUpstreamState {
    pub is_online: bool,
    pub url: Arc<Url>,
}

impl SubscriptionDispatcher {
    pub fn new(
        upstream_state_rx: Receiver<WsUpstreamState>,
        requests_rx: Receiver<Subscription>,
        connections_per_upstream: u16,
    ) -> Self {
        Self {
            upstream_state_rx,
            requests_rx,
            upstreams: HashMap::default(),
            connections_per_upstream,
        }
    }

    async fn try_spawn_connections(url: Arc<Url>, count: u16) -> (Arc<Url>, Sender<Subscription>) {
        static CONNECTION_ID: AtomicU32 = AtomicU32::new(0);
        tracing::info!("spawning new websocket connections to {url}");
        let id = CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = flume::bounded(1024);

        // for each upstream, spawn the preconfigured number of connections
        let mut spawned = 0;
        for _ in 0..count {
            let result = WebsocketConnection::new(id, url.clone(), rx.clone()).await;
            match result {
                Ok(c) => tokio::spawn(c.run()),
                Err(error) => {
                    tracing::warn!(%error, "failed to websocket establish connection to {url}");
                    continue;
                }
            };
            spawned += 1;
        }
        tracing::info!("{spawned} new websocket connections to {url} has been established");
        (url, tx)
    }

    pub async fn run(mut self, mut ready: Option<Arc<Notify>>) {
        let mut connections = FuturesUnordered::new();
        loop {
            tokio::select! {
                Some(state) = self.upstream_state_rx.recv() => {
                    // if upstream went offline, remove it from possible routes
                    if !state.is_online {
                        self.upstreams.remove(&state.url);
                        continue;
                    }
                    if self.upstreams.contains_key(&state.url) {
                        continue;
                    }
                    // if upstream is new, spawn new connections to it
                    connections.push(Self::try_spawn_connections(state.url.clone(), self.connections_per_upstream));
                }
                Some(request) = self.requests_rx.recv() => {
                    let Some(tx) = self.upstreams.get_mut(&request.destination) else {
                        tracing::warn!(
                            url=%request.destination,
                            "subscription request was sent for unknown upstream"
                        );
                        continue;
                    };
                    if let Err(SendError(r)) = tx.send(request) {
                        tracing::warn!(url=%r.destination, "all connections to the upstream have been terminated");
                        self.upstreams.remove(&r.destination);
                    }
                }
                Some((url, tx)) = connections.next(), if !connections.is_empty() => {
                    self.upstreams.insert(url, tx);
                    if !connections.is_empty() {
                        continue;
                    }
                    if let Some(ready) = ready.take()  {
                        ready.notify_one();
                    }
                }
                else => {
                    tracing::info!("terminating subscriptions dispatcher");
                    break;
                }
            }
        }
    }
}
