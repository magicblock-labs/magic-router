use std::{collections::HashMap, sync::Arc};

use flume::{SendError, Sender};
use tokio::sync::mpsc::Receiver;
use url::Url;

use crate::{pubsub::connection::WebsocketConnection, RouterResult};

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
    /// Connection ID counter
    connection_id: u32,
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
            connection_id: 0,
        }
    }

    async fn try_spawn_connections(&mut self, url: Arc<Url>) -> RouterResult<()> {
        let id = self.connection_id;
        self.connection_id += 1;
        let (tx, rx) = flume::bounded(1024);

        // for each upstream, spawn the preconfigured number of connections
        for _ in 0..self.connections_per_upstream {
            let connection = WebsocketConnection::new(id, url.clone(), rx.clone()).await?;
            tokio::spawn(connection.run());
        }
        self.upstreams.insert(url, tx);
        Ok(())
    }

    pub async fn run(mut self) {
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
                    if let Err(error) = self.try_spawn_connections(state.url.clone()).await {
                        tracing::error!(%error, url=%state.url, "failed to init new ws connection to upstream");
                    }
                }
                Some(request) = self.requests_rx.recv() => {
                    let Some(tx) = self.upstreams.get_mut(&request.destination) else {
                        tracing::warn!(url=%request.destination, "subscription request was sent for unknown upstream");
                        continue;
                    };
                    if let Err(SendError(r)) = tx.send(request) {
                        tracing::warn!(url=%r.destination, "all connections to the upstream have been terminated");
                        self.upstreams.remove(&r.destination);
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
