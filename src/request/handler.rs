//! module for handling/routing client requests

use std::future::Future;

use http_body_util::BodyExt;
use hyper::{body::Incoming, service::Service};

use crate::{cache::AccountsCache, error::Error, http::client::HttpClient};

use super::Request;

type IncomingRequest = hyper::Request<Incoming>;
type Response = hyper::Response<reqwest::Body>;

/// Container for various accessors for shared resources
#[derive(Clone)]
pub struct Accessors {
    /// HTTP client for base chain
    chain: HttpClient,
    /// HTTP client for ephemeral rollup
    ephem: HttpClient,
    /// cache of  accounts' delegation statuses
    cache: AccountsCache,
}

impl Accessors {
    /// Initialize shared resource accessors
    pub fn new(chain: HttpClient, ephem: HttpClient, cache: AccountsCache) -> Self {
        Self {
            chain,
            ephem,
            cache,
        }
    }
}

/// Wrapper type implementing hyper::Service, which wraps actual request handler which does not
/// implement hyper::Service, a separate type allows some composability when it comes to plugging
/// some extra request/response processing
pub struct RequestHandler<H, F>
where
    H: Fn(Accessors, Incoming) -> F,
    F: Future<Output = Result<Response, Error>>,
{
    accessors: Accessors,
    handler: H,
}

/// JSON-RPC HTTP request processor/router, parses request, looks up accounts referenced in request and
/// routes it an appropriate upstream/destination
pub async fn process(accessors: Accessors, req: Incoming) -> Result<Response, Error> {
    let payload = req.collect().await?.to_bytes();
    let request = Request::new(payload)?;
    let client = 'client: {
        if let Some(keys) = request.meta.delegates() {
            for &key in keys {
                accessors.cache.insert(key).await;
            }
            accessors.chain
        } else {
            for pubkey in request.meta.pubkeys() {
                if accessors.cache.contains(pubkey).await {
                    break 'client accessors.ephem;
                }
            }
            accessors.chain
        }
    };
    client.fetch(request.payload).await.map(Into::into)
}

impl<H, F> RequestHandler<H, F>
where
    H: Fn(Accessors, Incoming) -> F,
    F: Future<Output = Result<Response, Error>>,
{
    /// Helper method which allows to init generic RequestHandler
    /// over any type which impelements required Fn trait
    pub fn build(accessors: Accessors, handler: H) -> Self {
        Self { accessors, handler }
    }
}

impl<H, F> Service<IncomingRequest> for RequestHandler<H, F>
where
    H: Fn(Accessors, Incoming) -> F,
    F: Future<Output = Result<Response, Error>>,
{
    type Response = Response;
    type Error = Error;
    type Future = F;

    fn call(&self, req: IncomingRequest) -> Self::Future {
        (self.handler)(self.accessors.clone(), req.into_body())
    }
}
