use std::net::SocketAddr;

use serde::Deserialize;
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RouterConfig {
    pub listen_address: SocketAddr,
    pub base_chain_urls: Vec<Url>,
    pub laser_stream: LaserStreamConfig,
    pub max_cached_delegations: usize,
    pub max_cached_transactions: usize,
    pub max_connections: u32,
    pub max_subscriptions_per_connection: u32,
    pub websocket: WebsocketConnectionConfig,
    #[serde(default)]
    pub routing: RoutingConfig,
    pub proximity_ping_frequency_sec: u64,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct LaserStreamConfig {
    pub api_key: String,
    pub endpoint: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WebsocketConnectionConfig {
    pub ping_interval_sec: u64,
    pub connections_per_upstream: u16,
}

#[derive(Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub struct RoutingConfig {
    pub static_er_identity: Option<String>,
}
