use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use borsh::BorshDeserialize;
use futures::{stream::FuturesUnordered, StreamExt};
use mdp::state::{record::ErRecord, status::ErStatus};
use scc::HashMap;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::{
    sync::{
        mpsc::{self, Sender},
        Notify,
    },
    time,
};
use url::Url;

use crate::{
    cache::transactions::RemoteHandle,
    pubsub::{
        dispatch::WsUpstreamState,
        notification::{deserialize_account, deserialize_field, PubsubMessage},
        subscription::{program_subscription_json, Subscription},
        PubSubUpstreamKind,
    },
    types::{RequestId, RouteInfo, SerdePubkey, SubscriberId, UniqueId},
    RouterResult,
};

/// Routes manager, keeps an up to date mapping between ER identities and their FQDNs
pub struct RoutingTable {
    /// List of ER nodes and their connection handles
    inner: HashMap<Pubkey, UpstreamRecord>,
    /// Mapping between magic domain program PDA and ER identity
    pda_to_identity: HashMap<Pubkey, Pubkey>,
    /// List of connection handles to base layer chain endpoints
    base_chain: BaseChainUpstreams,
    /// Channel endpoint to websocket subscriptions dispatcher
    dispatcher_tx: Sender<Subscription>,
    /// Channel endpoint to send websocket updates on routes back to routes manager
    upstream_state_tx: Sender<WsUpstreamState>,
}

impl RoutingTable {
    pub async fn new(
        base_chain_urls: Vec<Url>,
        dispatcher_tx: Sender<Subscription>,
        upstream_state_tx: Sender<WsUpstreamState>,
        proximity_ping_frequency: u64,
        ready: Arc<Notify>,
    ) -> RouterResult<Arc<Self>> {
        let mut upstreams = Vec::with_capacity(base_chain_urls.len());
        for url in base_chain_urls {
            let record = UpstreamRecord::new_from_url(url, None)
                .await
                .expect("invalid base chain url was provided");
            upstreams.push(record);
        }

        for u in upstreams.iter() {
            let _ = upstream_state_tx
                .send(WsUpstreamState {
                    // base chain is always assumed to be online
                    is_online: true,
                    url: u.ws_url.clone(),
                })
                .await;
        }
        let base_chain = BaseChainUpstreams {
            upstreams,
            next: Default::default(),
        };

        let this = Arc::new(Self {
            inner: Default::default(),
            pda_to_identity: Default::default(),
            base_chain,
            dispatcher_tx,
            upstream_state_tx,
        });
        let accounts = this
            .base_chain()
            .client
            .get_program_accounts(&mdp::id())
            .await?;
        for (pubkey, account) in accounts {
            this.insert_entry(pubkey, account).await;
        }
        ready.notified().await;
        tokio::spawn(this.clone().updater(proximity_ping_frequency));
        Ok(this)
    }

    pub fn ephemeral_client(&self, identity: &Pubkey) -> Option<Arc<RpcClient>> {
        self.inner
            .get_sync(identity)
            .map(|e| e.get().client.clone())
    }

    pub fn ephemeral_handle(&self, identity: &Pubkey) -> Option<RemoteHandle> {
        self.inner.get_sync(identity).map(|e| RemoteHandle {
            rpc: e.client.clone(),
            ws: e.ws_url.clone(),
        })
    }

    pub fn base_chain(&self) -> &UpstreamRecord {
        let len = self.base_chain.upstreams.len();
        debug_assert_ne!(len, 0, "no base chain upstreams are present");
        let index = self
            .base_chain
            .next
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some((v + 1) % len)
            })
            .expect("fetch_update on atomic never returned None, cannot panic");
        &self.base_chain.upstreams[index]
    }

    pub fn closest_node(&self) -> (SerdePubkey, Arc<RpcClient>) {
        let mut node_id = Pubkey::default();
        let mut client = self.base_chain().client.clone();
        let mut min_proximity = u64::MAX;
        self.inner.iter_sync(|pubkey, record| {
            if min_proximity <= record.proximity_micros {
                return true;
            }
            min_proximity = record.proximity_micros;
            node_id = *pubkey;
            client = record.client.clone();
            true
        });
        (SerdePubkey(node_id), client)
    }

    pub async fn all_routes(&self) -> Vec<RouteInfo> {
        let mut routes = Vec::new();
        self.inner
            .iter_async(|_, r| {
                if let Some(ref info) = r.info {
                    routes.push(info.clone())
                }
                true
            })
            .await;
        routes
    }

    async fn insert_entry(&self, pubkey: Pubkey, account: Account) {
        let Ok(record) = ErRecord::deserialize(&mut account.data.as_slice()) else {
            if account.lamports == 0 {
                self.remove_entry(&pubkey).await;
            } else {
                tracing::warn!(%pubkey, "failed to deserialize account from domain registry");
            }
            return;
        };

        let Some(upstream) = UpstreamRecord::new_from_er_record(&record).await else {
            tracing::warn!(%pubkey, "domain registry account didn't have proper FQDN");
            return;
        };
        let identity = *record.identity();
        let _ = self.pda_to_identity.insert_sync(pubkey, identity);
        let _ = self
            .upstream_state_tx
            .send(WsUpstreamState {
                is_online: matches!(record.status(), ErStatus::Active),
                url: upstream.ws_url.clone(),
            })
            .await;
        let _ = self.inner.insert_sync(identity, upstream);
    }

    async fn remove_entry(&self, pda: &Pubkey) {
        let Some((_, identity)) = self.pda_to_identity.remove_sync(pda) else {
            return;
        };
        let Some((_, upstream)) = self.inner.remove_sync(&identity) else {
            return;
        };
        let _ = self
            .upstream_state_tx
            .send(WsUpstreamState {
                is_online: false,
                url: upstream.ws_url,
            })
            .await;
    }

    #[tracing::instrument(skip_all)]
    async fn updater(self: Arc<Self>, proximity_ping_frequency: u64) {
        let (tx, mut rx) = mpsc::channel(1024);
        let request_id = RequestId::generate();
        let mut subscription = Subscription {
            request_id,
            subscriber_id: SubscriberId::generate(),
            tx,
            payload: program_subscription_json(request_id, mdp::id(), None),
            destination: self.base_chain().ws_url.clone(),
            upstream: PubSubUpstreamKind::Chain,
        };
        let _ = self.dispatcher_tx.send(subscription.clone()).await;
        let mut ping_ticker = time::interval(Duration::from_secs(proximity_ping_frequency));
        let mut pings = FuturesUnordered::new();

        loop {
            tokio::select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            PubsubMessage::Subscribed(handle) => {
                                tracing::info!(id = handle.request_id.0, "subscribed to MDP program accounts");
                            }
                            PubsubMessage::Notification { ref payload, .. } => {
                                let Some(pubkey) = deserialize_field::<&str>(payload, &["value", "pubkey"])
                                    .and_then(|s| Pubkey::from_str(s).ok())
                                    else {
                                        tracing::warn!(?payload, "encounterd invalid websocket notification");
                                        continue;
                                    };

                                    let Some(account) = deserialize_account(payload, &["value", "account"]) else {
                                        tracing::warn!(?payload, "encounterd invalid websocket notification");
                                        continue;
                                    };

                                    self.insert_entry(pubkey, account).await;
                            }
                            PubsubMessage::Disconnected(id) => {
                                tracing::warn!(
                                    id = id.0,
                                    "MDP websocket subscription has been terminated, resubscribing"
                                );
                                subscription.destination = self.base_chain().ws_url.clone();
                                let _ = self
                                    .dispatcher_tx
                                    .send(subscription.clone())
                                    .await;
                                }
                        }
                    }
                    ping = pings.next(), if !pings.is_empty() => {
                        let Some(Ok((pubkey, duration))): Option<Result<(Pubkey, Duration), _>> = ping else {
                            tracing::warn!(?ping, "failed to perform ping request");
                            continue;
                        };
                        let Some(mut record) = self.inner.get_sync(&pubkey) else {
                            continue;
                        };

                        let record = record.get_mut();
                        let last = duration.as_micros() as u64;
                        if record.proximity_micros == u64::MAX {
                            record.proximity_micros = last;
                        } else {
                            record.proximity_micros =
                                ((record.proximity_micros * 85) + last * 15) / 100;
                        }
                        let host =  record.ws_url.host_str().unwrap_or_default();
                        tracing::info!(
                            "ping to {host} took {last}μs, avg: {}μs",
                            record.proximity_micros
                        );
                    }
                    _ = ping_ticker.tick() => {
                        self.inner.iter_sync(|&pubkey, record| {
                            let client = record.client.clone();
                            let task = async move {
                                let start = Instant::now();
                                client.get_identity().await.map(|_| (pubkey, start.elapsed()))
                            };
                            pings.push(task);
                            true
                        });
                    }
                    else => {
                        tracing::info!("routing table update loop is ternimating");
                        break;
                    }
            }
        }
    }
}

struct BaseChainUpstreams {
    upstreams: Vec<UpstreamRecord>,
    next: AtomicUsize,
}

pub struct UpstreamRecord {
    pub client: Arc<RpcClient>,
    pub ws_url: Arc<Url>,
    pub proximity_micros: u64,
    pub info: Option<RouteInfo>,
}

impl UpstreamRecord {
    async fn new_from_url(fqdn: Url, info: Option<RouteInfo>) -> Option<Self> {
        let client = Arc::new(RpcClient::new(fqdn.to_string()));
        client.get_identity().await.ok()?;
        
        let scheme = if fqdn.scheme() == "https" {
            "wss"
        } else {
            "ws"
        };
        
        let mut ws_url = fqdn.clone();
        ws_url.set_scheme(scheme).ok()?;
        
        // Handle local validator WebSocket port offset (solana-test-validator uses RPC port + 1)
        let is_local = fqdn.host_str().map(|h| h == "localhost" || h == "127.0.0.1").unwrap_or(false);
        if is_local {
            if let Some(port) = fqdn.port() {
                ws_url.set_port(Some(port + 1)).ok()?;
            }
        }
        
        Some(UpstreamRecord {
            client,
            ws_url: Arc::new(ws_url),
            proximity_micros: u64::MAX,
            info,
        })
    }
    async fn new_from_er_record(er_record: &ErRecord) -> Option<Self> {
        let fqdn = er_record.addr();
        let Ok(fqdn) = Url::parse(fqdn) else {
            tracing::warn!(
                fqdn,
                "failed to parse FQDN of the account from domain registry"
            );
            return None;
        };
        let info = RouteInfo::from(er_record);
        Self::new_from_url(fqdn, Some(info)).await
    }
}
