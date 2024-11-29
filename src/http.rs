//! module providing wrapper around reqwest's HTTP client

use std::sync::Arc;

use hyper::header::CONTENT_TYPE;
use reqwest::{Body, Client, Response, Url};

use crate::config::ClientConf;

/// Thin wrapper around reqwest::Client to streamline request processing
#[derive(Clone)]
pub struct HttpClient {
    inner: Client,
    endpoint: Arc<Url>,
}

impl HttpClient {
    /// Initialize new HTTP client
    pub fn new(config: ClientConf) -> Self {
        let endpoint = Arc::new(config.endpoint);
        let inner = Client::builder()
            .tcp_keepalive(Some(config.keepalive))
            .http2_keep_alive_timeout(config.keepalive)
            .timeout(config.timeout)
            .build()
            .expect("reqwest Client should build");
        Self { inner, endpoint }
    }

    /// Sends POST request to the configured endpoint with given body and returns Response
    pub async fn fetch(&self, body: impl Into<Body>) -> crate::Result<Response> {
        let request = self
            .inner
            .post(Url::clone(&self.endpoint))
            .header(CONTENT_TYPE, "application/json")
            .body(body)
            .build()
            .expect("POST request should always build");
        self.inner.execute(request).await.map_err(Into::into)
    }
}
