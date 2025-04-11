use serde::Deserialize;
use url::Url;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RouterConfig {
    base_chain_urls: Vec<Url>,
    max_cached_delegations: usize,
    websocket: WebsocketConnectionConfig,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct WebsocketConnectionConfig {
    ping_interval_sec: u64,
    connections_per_upstream: u16,
}
