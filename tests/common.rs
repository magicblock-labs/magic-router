#![allow(unused)]

use std::{
    collections::HashMap,
    fmt::Debug,
    str::FromStr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    time::Duration,
};

use jsonrpsee::{http_client::HttpClient, server::ServerHandle};
use router::config::{RouterConfig, WebsocketConnectionConfig};
use server::MockServer;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tracing_subscriber::FmtSubscriber;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(8080);

pub struct TestEnv {
    chain: MockServer,
    er_nodes: HashMap<Pubkey, MockServer>,
    delegations: HashMap<Pubkey, Pubkey>,
    pub router_client: RpcClient,
    pub router_pubsub: Arc<PubsubClient>,
    handles: Vec<ServerHandle>,
}

impl TestEnv {
    pub async fn init() -> Self {
        let subscriber = FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish();
        let _ = tracing::subscriber::set_global_default(subscriber);

        let chain_port = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let (chain, handle) = MockServer::start(chain_port).await;
        let mut handles = vec![handle];

        let router_port = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let listen_addr = host(router_port, None);
        let config = RouterConfig {
            listen_address: listen_addr,
            base_chain_urls: vec![host(chain_port, Some("http"))],
            max_connections: 128,
            max_cached_delegations: 1024,
            max_subscriptions_per_connection: 128,
            max_cached_transactions: 1024,
            websocket: WebsocketConnectionConfig {
                ping_interval_sec: 10,
                connections_per_upstream: 1,
            },
            proximity_ping_frequency_sec: 1,
        };
        let router_client = RpcClient::new(host(router_port, Some("http")));
        let handle = router::run(config).await.expect("failed to start router");
        handles.push(handle);
        // wait for the servers to finish init
        sleep().await;
        let router_pubsub = PubsubClient::new(&host::<String>(router_port, Some("ws")))
            .await
            .expect("failed to init pubsub client to router")
            .into();
        Self {
            chain,
            router_client,
            router_pubsub,
            er_nodes: HashMap::new(),
            delegations: HashMap::new(),
            handles,
        }
    }

    pub async fn add_route(&mut self, er_node: Pubkey) {
        let port = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let (ephemeral, handle) = MockServer::start(port).await;
        self.handles.push(handle);
        self.er_nodes.insert(er_node, ephemeral);
        self.chain.add_route(er_node, port).await;
    }

    pub fn add_account(&self, pubkey: Pubkey, owner: Pubkey) {
        self.chain.add_account(pubkey, owner);
    }

    pub async fn delegate_account(&mut self, pubkey: Pubkey, er_node: Pubkey) {
        self.delegations.insert(pubkey, er_node);
        let Some(node) = self.er_nodes.get_mut(&er_node) else {
            return;
        };
        let account = self.chain.delegate_account(pubkey, er_node).await;
        node.add_existing_account(pubkey, account);
        sleep().await;
    }

    pub async fn undelegate_account(&mut self, pubkey: Pubkey) {
        let Some(er_node) = self.delegations.remove(&pubkey) else {
            return;
        };
        let Some(server) = self.er_nodes.get(&er_node) else {
            return;
        };
        let Some(account) = server.account(&pubkey) else {
            return;
        };
        self.chain.undelegate_account(pubkey, account).await;
        sleep().await;
    }

    pub async fn update_account_balance(&self, pubkey: Pubkey, lamports: u64, on_chain: bool) {
        let endpoint = match (on_chain, self.delegations.get(&pubkey)) {
            (true, _) | (_, None) => &self.chain,
            (false, Some(er_node)) => self
                .er_nodes
                .get(er_node)
                .expect("account cannot be delegated to an unknown node"),
        };
        endpoint.update_account_balance(&pubkey, lamports).await;
    }

    pub async fn update_token_balance(&self, pubkey: Pubkey, tokens: u64, on_chain: bool) {
        let endpoint = match (on_chain, self.delegations.get(&pubkey)) {
            (true, _) | (_, None) => &self.chain,
            (false, Some(er_node)) => self
                .er_nodes
                .get(er_node)
                .expect("account cannot be delegated to an unknown node"),
        };
        endpoint.update_token_balance(&pubkey, tokens).await;
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        for h in &self.handles {
            let _ = h.stop();
        }
    }
}

pub async fn sleep() {
    tokio::time::sleep(Duration::from_millis(100)).await;
}

fn host<T>(port: u16, schema: Option<&str>) -> T
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    format!(
        "{}127.0.0.1:{port}",
        if let Some(schema) = schema {
            format!("{schema}://")
        } else {
            "".into()
        }
    )
    .parse()
    .unwrap()
}

#[path = "./server.rs"]
mod server;
