//! Process-wide router metrics and the HTTP/JSON-RPC collection boundaries.

use std::{
    collections::HashMap,
    future::{ready, Future, Ready},
    pin::Pin,
    sync::{LazyLock, OnceLock},
    task::{ready as poll_ready, Context, Poll},
    time::Instant,
};

use futures::future::Either;
use http::{header::CONTENT_TYPE, Method, StatusCode};
use jsonrpsee::{
    core::{async_trait, server::MethodResponse},
    server::{
        middleware::rpc::RpcServiceT, ws::is_upgrade_request, HttpBody, HttpRequest, HttpResponse,
    },
    types::Request,
};
use pin_project_lite::pin_project;
use prometheus::{
    core::Collector, default_registry, gather, Encoder, Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, TextEncoder,
};
use serde_json::Value;
use solana_rpc_client::{
    http_sender::HttpSender,
    nonblocking::rpc_client::RpcClient,
    rpc_sender::{RpcSender, RpcTransportStats},
};
use solana_rpc_client_api::{client_error::Result as ClientResult, request::RpcRequest};
use tower::Service;
use tracing::warn;

static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);
const SUBSCRIPTION_KINDS: &[&str] = &["account", "signature"];

struct Metrics {
    requests: IntCounterVec,
    durations: HistogramVec,
    errors: IntCounterVec,
    upstream: IntCounterVec,
    sync: IntCounterVec,
    transitions: IntCounterVec,
    subscriptions: IntGaugeVec,
    notifications: IntCounterVec,
    methods: OnceLock<HashMap<&'static str, RequestMetrics>>,
    unknown: RequestMetrics,
}

impl Metrics {
    fn new() -> Self {
        let requests = counter(
            "rpc_requests_total",
            "Dispatched RPC calls that ended.",
            "method",
            &[],
        );
        let durations = register(
            HistogramVec::new(
                HistogramOpts::new(
                    "rpc_request_duration_seconds",
                    "RPC processing duration in seconds.",
                )
                .namespace("router")
                .buckets(vec![
                    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 15.0, 60.0,
                ]),
                &["method"],
            )
            .expect("valid static histogram definition"),
        );
        let unknown = RequestMetrics::new(&requests, &durations, "unknown");
        let subscriptions = register(
            IntGaugeVec::new(
                Opts::new("subscriptions", "Active client subscription handlers.")
                    .namespace("router"),
                &["kind"],
            )
            .expect("valid static gauge definition"),
        );
        for kind in SUBSCRIPTION_KINDS {
            subscriptions.with_label_values(&[kind]);
        }
        Self {
            requests,
            durations,
            errors: counter(
                "errors_total",
                "Terminal failures at their owning boundary.",
                "source",
                &["rpc", "delegation", "pubsub"],
            ),
            upstream: counter(
                "upstream_requests_total",
                "Upstream HTTP RPC and WebSocket request sends.",
                "target",
                &["chain", "er"],
            ),
            sync: counter(
                "sync_events_total",
                "Events received from the delegation sync service, including replay.",
                "event",
                &[
                    "delegated_update",
                    "undelegated_update",
                    "interrupted",
                    "terminated",
                ],
            ),
            transitions: counter(
                "delegation_transitions_total",
                "State transitions applied to initialized delegation cache entries.",
                "transition",
                &["delegated", "undelegated", "redelegated"],
            ),
            subscriptions,
            notifications: counter(
                "notifications_total",
                "Data notifications successfully sent to clients.",
                "kind",
                SUBSCRIPTION_KINDS,
            ),
            methods: OnceLock::new(),
            unknown,
        }
    }
}

fn counter(name: &str, help: &str, label: &str, values: &[&str]) -> IntCounterVec {
    let counters = register(
        IntCounterVec::new(Opts::new(name, help).namespace("router"), &[label])
            .expect("valid static counter definition"),
    );
    for value in values {
        counters.with_label_values(&[value]);
    }
    counters
}

fn register<C: Collector + Clone + 'static>(collector: C) -> C {
    if let Err(error) = default_registry().register(Box::new(collector.clone())) {
        warn!(%error, "failed to register router metric");
    }
    collector
}

/// Initializes zero-valued series and bounds method labels to the registered RPC names.
pub(crate) fn init(methods: impl Iterator<Item = &'static str>) {
    METRICS.methods.get_or_init(|| {
        methods
            .map(|method| {
                (
                    method,
                    RequestMetrics::new(&METRICS.requests, &METRICS.durations, method),
                )
            })
            .collect()
    });
}

pub(crate) fn error(source: &'static str) {
    METRICS.errors.with_label_values(&[source]).inc();
}

pub(crate) fn sync_event(event: &'static str) {
    METRICS.sync.with_label_values(&[event]).inc();
}

pub(crate) fn transition(transition: &'static str) {
    METRICS.transitions.with_label_values(&[transition]).inc();
}

/// Holds one active client subscription until its handler exits or is cancelled.
pub(crate) struct SubscriptionGuard {
    active: IntGauge,
    notifications: IntCounter,
}

pub(crate) fn subscription(kind: &'static str) -> SubscriptionGuard {
    let active = METRICS.subscriptions.with_label_values(&[kind]);
    active.inc();
    SubscriptionGuard {
        active,
        notifications: METRICS.notifications.with_label_values(&[kind]),
    }
}

impl SubscriptionGuard {
    /// Records a successful data notification using the handler's resolved kind.
    pub(crate) fn notified(&self) {
        self.notifications.inc();
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.active.dec();
    }
}

struct RequestMetrics {
    requests: IntCounter,
    duration: Histogram,
}

impl RequestMetrics {
    fn new(requests: &IntCounterVec, durations: &HistogramVec, method: &str) -> Self {
        Self {
            requests: requests.with_label_values(&[method]),
            duration: durations.with_label_values(&[method]),
        }
    }
}

struct RequestTimer {
    metrics: &'static RequestMetrics,
    started: Instant,
}

impl Drop for RequestTimer {
    fn drop(&mut self) {
        self.metrics.requests.inc();
        self.metrics
            .duration
            .observe(self.started.elapsed().as_secs_f64());
    }
}

#[derive(Clone)]
pub(crate) struct RpcMetrics<S>(pub(crate) S);

impl<'a, S: RpcServiceT<'a>> RpcServiceT<'a> for RpcMetrics<S> {
    type Future = RpcFuture<S::Future>;

    fn call(&self, request: Request<'a>) -> Self::Future {
        let metrics = METRICS
            .methods
            .get()
            .and_then(|methods| methods.get(request.method_name()))
            .unwrap_or(&METRICS.unknown);
        let timer = Some(RequestTimer {
            metrics,
            started: Instant::now(),
        });
        RpcFuture {
            inner: self.0.call(request),
            timer,
        }
    }
}

pin_project! {
    pub(crate) struct RpcFuture<F> {
        #[pin]
        inner: F,
        timer: Option<RequestTimer>,
    }
}

impl<F: Future<Output = MethodResponse>> Future for RpcFuture<F> {
    type Output = MethodResponse;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let response = poll_ready!(this.inner.poll(cx));
        this.timer.take();
        if response.is_error() {
            error("rpc");
        }
        Poll::Ready(response)
    }
}

/// Serves scrapes before JSON-RPC dispatch, sharing the existing listener and shutdown.
#[derive(Clone)]
pub(crate) struct HttpMetrics<S>(pub(crate) S);

impl<S: Service<HttpRequest, Response = HttpResponse>> Service<HttpRequest> for HttpMetrics<S> {
    type Response = HttpResponse;
    type Error = S::Error;
    type Future = Either<Ready<Result<HttpResponse, S::Error>>, S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, request: HttpRequest) -> Self::Future {
        if request.method() != Method::GET
            || request.uri().path() != "/metrics"
            || is_upgrade_request(&request)
        {
            return Either::Right(self.0.call(request));
        }
        let encoder = TextEncoder::new();
        let response = match encoder.encode_to_string(&gather()) {
            Ok(body) => {
                let mut response = HttpResponse::new(HttpBody::from(body));
                response.headers_mut().insert(
                    CONTENT_TYPE,
                    encoder.format_type().parse().expect("static content type"),
                );
                response
            }
            Err(error) => {
                warn!(%error, "failed to encode router metrics");
                let mut response = HttpResponse::new(HttpBody::from("failed to encode metrics"));
                *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                response
            }
        };
        Either::Left(ready(Ok(response)))
    }
}

/// Counts logical HTTP RPC sends without duplicating instrumentation in callers.
struct UpstreamSender {
    inner: HttpSender,
    requests: IntCounter,
}

pub(crate) fn upstream(target: &'static str) -> IntCounter {
    METRICS.upstream.with_label_values(&[target])
}

pub(crate) fn client(url: &str, target: &'static str) -> RpcClient {
    RpcClient::new_sender(
        UpstreamSender {
            inner: HttpSender::new(url),
            requests: upstream(target),
        },
        Default::default(),
    )
}

#[async_trait]
impl RpcSender for UpstreamSender {
    async fn send(&self, request: RpcRequest, params: Value) -> ClientResult<Value> {
        self.requests.inc();
        self.inner.send(request, params).await
    }

    fn get_transport_stats(&self) -> RpcTransportStats {
        self.inner.get_transport_stats()
    }

    fn url(&self) -> String {
        self.inner.url()
    }
}
