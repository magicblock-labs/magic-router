//! Configuration used by various modules of router

use std::time::Duration;
use std::{net::SocketAddr, path::PathBuf};

use json::Deserialize;
use tracing_appender::rolling::Rotation;
use url::Url;

use crate::error::ConfigError;
use crate::utils::{deserialize_duration, deserialize_rotation};

/// General router configuration
#[derive(Deserialize)]
pub struct Configuration {
    /// server configuration
    pub server: ServerConf,
    /// logging configuration
    pub logging: LoggingConf,
    /// configuration of client connections to base chain
    pub chain: ClientConf,
    /// configuration of client connections to ephemeral rollups
    pub ephem: ClientConf,
    /// websocket connection pool configuration
    pub websocket: WebsocketConf,
}

impl Configuration {
    /// Try to read configuration from toml file specified in sole argument of program
    pub fn open() -> Result<Self, ConfigError> {
        let Some(path) = std::env::args().nth(1) else {
            eprintln!(
                "usage: {} <CONFIGURATION FILE PATH>",
                env!("CARGO_PKG_NAME")
            );
            std::process::exit(1);
        };
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }
}

/// Configuration for handling incoming connections
#[derive(Deserialize)]
pub struct ServerConf {
    /// TCP listen address
    pub bind: SocketAddr,
}

/// Configuration for the client.
#[derive(Deserialize)]
pub struct ClientConf {
    /// The endpoint URL for the client.
    pub endpoint: Url,
    /// The timeout duration for the client.
    #[serde(deserialize_with = "deserialize_duration")]
    pub timeout: Duration,
    /// The keepalive duration for the client.
    #[serde(deserialize_with = "deserialize_duration")]
    pub keepalive: Duration,
}

/// Configuration for the WebSocket connection.
#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct WebsocketConf {
    /// The list of WebSocket endpoint URLs and the number of connections for each.
    pub endpoints: Vec<WsEndpoint>,
    /// The interval at which ping messages are sent to keep the connection alive.
    #[serde(deserialize_with = "deserialize_duration")]
    pub ping_interval: Duration,
    /// The maximum allowed lag for a slot before it is considered stale.
    pub max_slot_lag: u64,
    /// The maximum number of subscriptions that can be queued before new ones are rejected.
    pub max_queued_subs: usize,
}

/// A WebSocket endpoint URL and the number of connections to establish.
#[derive(Deserialize)]
pub struct WsEndpoint {
    /// The WebSocket endpoint URL.
    pub url: Url,
    /// The number of connections to establish to this endpoint.
    pub connections: u16,
}

/// Configuration for logging.
#[derive(Deserialize)]
pub struct LoggingConf {
    /// The format for log messages.
    pub format: LogFormat,
    /// The log level to use (optional, will use ENV if not provided), can be specified on
    /// crate/module level.
    pub level: Option<String>,
    /// The logging mode (stdout, file, or rotating file).
    pub mode: LoggingMode,
}

/// The logging mode (stdout, file, or rotating file).
#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoggingMode {
    /// Log to stdout.
    Stdout,
    /// Log to a single file.
    File {
        /// The path to the log file.
        path: PathBuf,
    },
    /// Log to a rotating set of files.
    Rotating {
        /// The directory where log files will be stored.
        dir: PathBuf,
        /// The rotation policy for log files.
        #[serde(deserialize_with = "deserialize_rotation")]
        rotation: Rotation,
    },
}

/// Formatting directive to logger
#[derive(Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Don't format the output
    #[default]
    Plain,
    /// Format the output as JSON
    Json,
}
