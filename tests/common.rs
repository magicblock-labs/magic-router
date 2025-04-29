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

use jsonrpsee::server::ServerHandle;
use magic_router::config::{RouterConfig, WebsocketConnectionConfig};
use server::MockServer;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(8080);

pub struct TestEnv {
    chain: MockServer,
    er_nodes: HashMap<Pubkey, MockServer>,
    delegations: HashMap<Pubkey, Pubkey>,
    pub router_client: Arc<RpcClient>,
    handles: Vec<ServerHandle>,
}

impl TestEnv {
    pub async fn init() -> Self {
        let chain_port = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let (chain, handle) = MockServer::start(chain_port).await;
        let mut handles = vec![handle];

        let router_port = PORT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let listen_addr = host(router_port, false);
        let config = RouterConfig {
            listen_address: listen_addr,
            base_chain_urls: vec![host(chain_port, true)],
            max_connections: 1,
            max_cached_delegations: 1024,
            max_subscriptions_per_connection: 128,
            websocket: WebsocketConnectionConfig {
                ping_interval_sec: 10,
                connections_per_upstream: 1,
            },
        };
        let router_client = RpcClient::new(host(router_port, true)).into();
        let handle = magic_router::run(config)
            .await
            .expect("failed to start router");
        handles.push(handle);
        // wait for the servers to finish init
        tokio::time::sleep(Duration::from_millis(200)).await;
        Self {
            chain,
            router_client,
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
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    pub fn add_account(&self, pubkey: Pubkey, owner: Pubkey) {
        self.chain.add_account(pubkey, owner);
    }

    pub async fn delegate_account(&mut self, pubkey: Pubkey, er_node: Pubkey) {
        self.delegations.insert(pubkey, er_node);
        let owner = self.chain.delegate_account(pubkey, er_node).await;
        let Some(node) = self.er_nodes.get_mut(&er_node) else {
            return;
        };
        node.add_account(pubkey, owner);
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

fn host<T>(port: u16, with_schema: bool) -> T
where
    T: FromStr,
    <T as FromStr>::Err: Debug,
{
    format!(
        "{}127.0.0.1:{port}",
        if with_schema { "http://" } else { "" }
    )
    .parse()
    .unwrap()
}

#[path = "./server.rs"]
mod server;
