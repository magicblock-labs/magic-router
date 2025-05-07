use std::{
    net::IpAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use borsh::BorshDeserialize;
use futures::{stream::FuturesUnordered, StreamExt};
use mdp::state::{record::ErRecord, status::ErStatus};
use scc::HashMap;
use solana_account::Account;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use tokio::{
    sync::mpsc::{self, Sender},
    time,
};
use url::Url;

use crate::{
    pubsub::{
        dispatch::WsUpstreamState,
        notification::{deserialize_account, deserialize_field, PubsubMessage},
        subscription::{program_subscription_json, Subscription, SubscriptionAction},
    },
    types::{RequestId, SubscriberId, UniqueId},
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
    dispatcher_tx: Sender<SubscriptionAction>,
    /// Channel endpoint to send websocket updates on routes back to routes manager
    upstream_state_tx: Sender<WsUpstreamState>,
    /// Shared client, to perform ICMP ping request simultaneously to multiple upstream nodes
    ping_client: Arc<ping::Client>,
}

impl RoutingTable {
    pub async fn new(
        base_chain_urls: Vec<Url>,
        dispatcher_tx: Sender<SubscriptionAction>,
        upstream_state_tx: Sender<WsUpstreamState>,
        proximity_ping_frequency: u64,
    ) -> RouterResult<Arc<Self>> {
        let upstreams = base_chain_urls
            .into_iter()
            .map(|url| {
                UpstreamRecord::new_from_url(url).expect("invalid base chain url was provided")
            })
            .collect::<Vec<_>>();

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
            ping_client: ping::Client::new(&Default::default())?.into(),
        });
        let accounts = this
            .base_chain()
            .client
            .get_program_accounts(&mdp::id())
            .await?;
        for (pubkey, account) in accounts {
            this.insert_entry(pubkey, account).await;
        }
        tokio::spawn(this.clone().updater(proximity_ping_frequency));
        Ok(this)
    }

    pub fn ephemeral_client(&self, pubkey: &Pubkey) -> Option<Arc<RpcClient>> {
        self.inner.get(pubkey).map(|e| e.get().client.clone())
    }

    pub fn ephemeral_url(&self, pubkey: &Pubkey) -> Option<Arc<Url>> {
        self.inner.get(pubkey).map(|e| e.get().ws_url.clone())
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

    pub fn closest_node(&self) -> Pubkey {
        let mut node_id = Pubkey::default();
        let mut min_proximity = u64::MAX;
        self.inner.scan(|pubkey, record| {
            if min_proximity <= record.proximity_ms {
                return;
            }
            min_proximity = record.proximity_ms;
            node_id = *pubkey;
        });
        node_id
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
        let fqdn = record.addr();

        let Some(upstream) = UpstreamRecord::new_from_str(fqdn) else {
            tracing::warn!(%pubkey, "domain registry account didn't have proper FQDN");
            return;
        };
        let identity = *record.identity();
        let _ = self.pda_to_identity.insert(pubkey, identity);
        let _ = self
            .upstream_state_tx
            .send(WsUpstreamState {
                is_online: matches!(record.status(), ErStatus::Active),
                url: upstream.ws_url.clone(),
            })
            .await;
        let _ = self.inner.insert(identity, upstream);
    }

    async fn remove_entry(&self, pda: &Pubkey) {
        let Some((_, identity)) = self.pda_to_identity.remove(pda) else {
            return;
        };
        let Some((_, upstream)) = self.inner.remove(&identity) else {
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
        };
        let _ = self
            .dispatcher_tx
            .send(SubscriptionAction::Subscribe(subscription.clone()))
            .await;
        let mut ping_ticker = time::interval(Duration::from_millis(proximity_ping_frequency));
        let mut pings = FuturesUnordered::new();

        let mut ping_id = 0u16;
        loop {
            tokio::select! {
                    Some(msg) = rx.recv() => {
                        match msg {
                            PubsubMessage::Subscribed(id) => {
                                tracing::info!(id = id.0, "subscribed to MDP program accounts");
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
                                    .send(SubscriptionAction::Subscribe(subscription.clone()))
                                    .await;
                                }
                        }
                    }
                    ping = pings.next(), if !pings.is_empty() => {
                        let Some(Ok((pubkey, duration))): Option<Result<(Pubkey, Duration), _>> = ping else {
                            tracing::warn!(?ping, "failed to perform ping request");
                            continue;
                        };
                        let Some(mut record) = self.inner.get(&pubkey) else {
                            continue;
                        };
                        record.get_mut().proximity_ms = duration.as_millis() as u64;
                    }
                    _ = ping_ticker.tick() => {
                        self.inner.scan(|&pubkey, record| {
                            let client = self.ping_client.clone();
                            ping_id = ping_id.wrapping_add(1);
                            let addr = record.ip;
                            let task = async move {
                                let mut pinger = client.pinger(addr, ping_id.into()).await;
                                pinger
                                    .ping(ping_id.into(), b"")
                                    .await
                                    .map(|(_, dur)| (pubkey, dur))
                            };
                            pings.push(task);
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
    pub ip: IpAddr,
    pub proximity_ms: u64,
}

impl UpstreamRecord {
    fn new_from_url(mut fqdn: Url) -> Option<Self> {
        let client = Arc::new(RpcClient::new(fqdn.to_string()));
        let port = fqdn.port_or_known_default();
        let scheme = if fqdn.scheme() == "https" {
            "wss"
        } else {
            "ws"
        };
        fqdn.set_scheme(scheme).ok()?;
        let ip = fqdn.socket_addrs(|| port).ok()?.first()?.ip();
        Some(UpstreamRecord {
            client,
            ws_url: Arc::new(fqdn),
            proximity_ms: u64::MAX,
            ip,
        })
    }
    fn new_from_str(fqdn: &str) -> Option<Self> {
        let Ok(fqdn) = Url::parse(fqdn) else {
            tracing::warn!(
                fqdn,
                "failed to parse FQDN of the account from domain registry"
            );
            return None;
        };
        Self::new_from_url(fqdn)
    }
}
