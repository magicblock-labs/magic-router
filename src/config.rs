use std::net::SocketAddr;

use serde::Deserialize;
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RouterConfig {
    pub listen_address: SocketAddr,
    pub base_chain_urls: Vec<Url>,
    pub max_cached_delegations: usize,
    pub max_cached_transactions: usize,
    pub max_connections: u32,
    pub max_subscriptions_per_connection: u32,
    pub websocket: WebsocketConnectionConfig,
    pub proximity_ping_frequency_sec: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WebsocketConnectionConfig {
    pub ping_interval_sec: u64,
    pub connections_per_upstream: u16,
}
