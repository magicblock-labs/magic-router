//! module for handling/routing client requests

use std::{convert::Infallible, future::Future};

use http_body_util::BodyExt;
use hyper::{body::Incoming, service::Service};

use crate::{cache::AccountsCache, error::Error, http::HttpClient};

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
    F: Future<Output = Result<Response, Infallible>>,
{
    accessors: Accessors,
    handler: H,
}

macro_rules! map_error_to_response {
    ($result: expr) => {
        match $result {
            Ok(r) => r,
            Err(error) => return Ok(Response::from(Error::from(error))),
        }
    };
}

/// JSON-RPC HTTP request processor/router, parses request, looks up accounts referenced in request and
/// routes it an appropriate upstream/destination
pub async fn process(accessors: Accessors, req: Incoming) -> Result<Response, Infallible> {
    let payload = map_error_to_response!(req.collect().await).to_bytes();
    let request = map_error_to_response!(Request::new(payload));
    let client = 'client: {
        for pubkey in request.pubkeys() {
            if accessors.cache.contains(pubkey).await {
                tracing::info!("using ephem");
                break 'client accessors.ephem;
            }
        }
        tracing::info!("using chain");
        accessors.chain
    };
    Ok(map_error_to_response!(client
        .fetch(request.payload)
        .await
        .map(Into::into)))
}

impl<H, F> RequestHandler<H, F>
where
    H: Fn(Accessors, Incoming) -> F,
    F: Future<Output = Result<Response, Infallible>>,
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
    F: Future<Output = Result<Response, Infallible>>,
{
    type Response = Response;
    type Error = Infallible;
    type Future = F;

    fn call(&self, req: IncomingRequest) -> Self::Future {
        (self.handler)(self.accessors.clone(), req.into_body())
    }
}
