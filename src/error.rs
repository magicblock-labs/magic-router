//! Errors used by router

use std::io;

use url::Url;

/// All errors that can be encountered during router operation
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Low level IO error
    #[error("io error during request: {0}")]
    Io(#[from] io::Error),
    /// Error encountered during request processing by router
    #[error("http error during request handling: {0}")]
    HttpServer(#[from] hyper::Error),
    /// Error encountered during forwarding the request to upstream
    #[error("http error during request to remote: {0}")]
    HttpClient(#[from] reqwest::Error),
    /// Error encountered during websocket connection handling
    #[error("websocket connection error: {0}")]
    Ws(#[from] websocket::Error),
    /// JSON-RPC method is not supported by router
    #[error("method is not supported by router")]
    UnsupportedMethod,
    /// JSON-RPC is malformed
    #[error("malformed or invalid request")]
    InvalidRequest,
    /// Internal router errors
    #[error("internal router error: {0}")]
    Internal(#[from] InternalError),
}

/// Internal router error
#[derive(thiserror::Error, Debug)]
pub enum InternalError {
    /// Error reading configuration file
    #[error("router configuration error: {0}")]
    Config(#[from] ConfigError),
    /// Provided url is invalid for the connection
    #[error("invalid connection url for {0}: {1}")]
    InvalidUrl(&'static str, Url),
    /// Error during deserialization
    #[error("deserialization error parsing value: {0}")]
    Serde(#[from] json::Error),
}

/// Configuration reading error
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    /// Io error during reading the configuration file
    #[error("io error reading config: {0}")]
    Io(#[from] io::Error),
    /// Error parsing configuration file
    #[error("deserialization error reading config: {0}")]
    Serde(#[from] toml::de::Error),
}

impl From<Error> for hyper::Response<reqwest::Body> {
    fn from(error: Error) -> Self {
        match error {
            Error::Io(_) | Error::HttpServer(_) | Error::HttpClient(_) | Error::Ws(_) => {
                hyper::Response::builder()
                    .status(hyper::StatusCode::BAD_GATEWAY)
                    .body(reqwest::Body::from("Upstream service error"))
                    .unwrap()
            }
            Error::UnsupportedMethod => hyper::Response::builder()
                .status(hyper::StatusCode::METHOD_NOT_ALLOWED)
                .body(reqwest::Body::from("JSON-RPC Method not supported"))
                .unwrap(),
            Error::InvalidRequest => hyper::Response::builder()
                .status(hyper::StatusCode::BAD_REQUEST)
                .body(reqwest::Body::from("Invalid request"))
                .unwrap(),
            Error::Internal(_) => hyper::Response::builder()
                .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
                .body(reqwest::Body::from("Internal server error"))
                .unwrap(),
        }
    }
}
