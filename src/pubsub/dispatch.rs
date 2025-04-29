use std::{collections::HashMap, sync::Arc};

use flume::{SendError, Sender};
use tokio::sync::mpsc::Receiver;
use url::Url;

use crate::{pubsub::connection::WebsocketConnection, RouterResult};

use super::subscription::SubscriptionAction;

pub struct SubscriptionDispatcher {
    upstream_state_rx: Receiver<WsUpstreamState>,
    requests_rx: Receiver<SubscriptionAction>,
    upstreams: HashMap<Arc<Url>, Sender<SubscriptionAction>>,
    connections_per_upstream: u16,
    connection_id: u32,
}

pub struct WsUpstreamState {
    pub is_online: bool,
    pub url: Arc<Url>,
}

impl SubscriptionDispatcher {
    pub fn new(
        upstream_state_rx: Receiver<WsUpstreamState>,
        requests_rx: Receiver<SubscriptionAction>,
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
                    if !state.is_online {
                        self.upstreams.remove(&state.url);
                        continue;
                    }
                    if self.upstreams.contains_key(&state.url) {
                        continue;
                    }
                    if let Err(error) = self.try_spawn_connections(state.url.clone()).await {
                        tracing::error!(%error, "failed to init new ws connection to upstream");
                    }
                }
                Some(request) = self.requests_rx.recv() => {
                    let Some(tx) = self.upstreams.get_mut(request.destination()) else {
                        tracing::warn!(url=%request.destination(), "subscription request was sent for unknown upstream");
                        continue;
                    };
                    if let Err(SendError(r)) = tx.send(request) {
                        tracing::warn!(url=%r.destination(), "all connections to the upstream have been terminated");
                        self.upstreams.remove(r.destination());
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
