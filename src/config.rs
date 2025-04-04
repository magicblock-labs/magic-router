use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
struct WebsocketConnectionConfig {
    ping_interval_sec: u64,
}
