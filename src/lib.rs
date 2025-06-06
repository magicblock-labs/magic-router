use std::time::Duration;

use cache::{
    delegations::DelegationsCache, routes::RoutingTable, transactions::ForwardedTransactions,
};
use config::RouterConfig;
use error::RouterError;
use jsonrpsee::server::{PingConfig, Server, ServerHandle};
use pubsub::dispatch::SubscriptionDispatcher;
use rpc::{
    http::{RoHttpRpcServer, RwHttpRpcServer},
    websocket::WebsocketRpcServer,
};
use server::{http::HttpServer, websocket::WebsocketServer};
use tokio::sync::mpsc;

pub mod accounts;
pub mod cache;
pub mod config;
pub mod error;
pub mod pubsub;
pub mod server;
pub mod types;

pub mod rpc;

pub type RouterResult<T> = Result<T, RouterError>;

/// Start the router service, this will start accpeting http and
/// websocket requests on the same provided port
pub async fn run(config: RouterConfig) -> RouterResult<ServerHandle> {
    let server = Server::builder()
        .enable_ws_ping(
            PingConfig::new()
                .ping_interval(Duration::from_secs(config.websocket.ping_interval_sec)),
        )
        .max_connections(config.max_connections)
        .max_subscriptions_per_connection(config.max_subscriptions_per_connection)
        .build(config.listen_address)
        .await?;
    let (upstream_state_tx, upstream_state_rx) = mpsc::channel(1024);
    let (requests_tx, requests_rx) = mpsc::channel(1024);
    let dispatcher = SubscriptionDispatcher::new(
        upstream_state_rx,
        requests_rx,
        config.websocket.connections_per_upstream,
    );
    tokio::spawn(dispatcher.run());
    let routes = RoutingTable::new(
        config.base_chain_urls,
        requests_tx.clone(),
        upstream_state_tx,
        config.proximity_ping_frequency_sec,
    )
    .await?;

    let delegations = DelegationsCache::new(
        requests_tx.clone(),
        routes.clone(),
        config.max_cached_delegations,
    );

    let handler = HttpServer {
        delegations: delegations.clone(),
        routes: routes.clone(),
        transactions: ForwardedTransactions::new(config.max_cached_transactions).into(),
    };
    let mut rpc_module = RoHttpRpcServer::into_rpc(handler.clone());
    rpc_module
        .merge(RwHttpRpcServer::into_rpc(handler.clone()))
        .expect("RW and RO servers have distinct method names");
    rpc_module
        .merge(
            WebsocketServer {
                delegations,
                routes,
                dispatcher_tx: requests_tx,
            }
            .into_rpc(),
        )
        .expect("ws and http servers have distinct method names");

    tracing::info!(
        "Listeninig for incoming connections on {}",
        config.listen_address
    );
    Ok(server.start(rpc_module))
}
