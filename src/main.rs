#![deny(missing_docs)]

//! RPC-Router for magicblock infrustructure [discussion](https://github.com/magicblock-labs/magicblock-validator/discussions/244)
//! The main rationale behind the project is to improve user experience by providing a single
//! endpoint for all client requests, and thus shifting responsibility for choosing correct
//! destination for request from client to infrustructure provider.
//!
//! Main idea is based on account's delegation status. When client decides to run some transactions
//! on ephemeral rollup, all the accounts to be mutated by those transactions need to be delegated
//! first. Router then uses that information to make decisions related to final endpoint to where
//! to route the request: for requests which reference undelegated accounts, this destination is
//! base chain, and for those that reference delegated ones it's an ephemeral rollup.
//!
//! This delegation based routing is primarily achieved via caching status of accounts which router
//! encounters, and keeping them up to date via websocket subscriptions to base layer.

use cache::AccountsCache;
use config::Configuration;
use error::InternalError;
use http::client::HttpClient;
use request::handler::Accessors;
use server::Server;
use solana::pubkey::Pubkey;
use tokio::{
    signal::unix::SignalKind,
    sync::{mpsc, Notify},
};
use websocket::pool::WebsocketPool;

type Result<T> = std::result::Result<T, error::Error>;

const DELEGATION_PROGRAM_ID: Pubkey =
    Pubkey::from_str_const("DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh");

static SHUTDOWN: Notify = Notify::const_new();

#[tokio::main]
async fn main() -> Result<()> {
    let config = Configuration::open().map_err(InternalError::from)?;
    let _log_guard = logging::init(config.logging);
    tracing::info!("configuration file has been parsed");

    let chain = HttpClient::new(config.chain);
    let ephem = HttpClient::new(config.ephem);

    tracing::info!("initiated chain/ephem HTTP clients");

    let undelegations = mpsc::channel(1 << 8);

    let (wspool, wsconnections) =
        WebsocketPool::new(config.websocket, chain.clone(), undelegations.0).await?;
    tracing::info!("created connections to websocket pools");
    let cache = AccountsCache::new(wspool, undelegations.1);
    let accessors = Accessors::new(chain, ephem, cache);

    let server = Server::new(config.server, accessors, wsconnections).await?;

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .expect("failed to register SIGTERM intercepter");
    tokio::spawn(async move {
        sigterm.recv().await;
        tracing::info!("received SIGTERM from system, initiating shutdown...");
        SHUTDOWN.notify_waiters()
    });
    tracing::info!("registered SIGTERM handler");

    server.run().await;
    Ok(())
}

pub mod account;
pub mod cache;
pub mod config;
pub mod error;
pub mod http;
pub mod logging;
pub mod request;
pub mod server;
pub mod utils;
pub mod websocket;
