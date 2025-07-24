use std::{sync::Arc, time::Duration};

use scc::{hash_cache::Entry, HashCache, HashMap};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{config::RpcAccountInfoConfig, response::Response};
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    accounts::{
        DelegationEntry, DelegationStatus, DELEGATION_PROGRAM, DELEGATION_RECORD_DATA_SIZE,
    },
    pubsub::{
        notification::{deserialize_account, PubsubMessage, SubscriptionHandle},
        subscription::{account_subscription_json, Subscription, Unsubscription},
        PubSubUpstreamKind,
    },
    types::{RequestId, SubscriberId, UniqueId},
};

use super::routes::RoutingTable;

const MAX_ACCOUNT_REFETCH_ATTEMPTS: u64 = 3;
/// We use HashCache to keep the number of entries bounded
type DelegationsDB = Arc<HashCache<Pubkey, DelegationEntry>>;

/// In memory store for delegation statuses of all encountered accounts
pub struct DelegationsCache {
    subscriber_id: SubscriberId,
    /// Channel endpoint for the wesocket connection to
    /// send notification to delegations cache manager
    pubsub_tx: Sender<PubsubMessage>,
    /// Cache of delegation states
    db: DelegationsDB,
    /// Channel endpoint to wesocket subscriptions dispatcher
    dispatcher_tx: Sender<Subscription>,
    /// Routes manager, mapping ER identity to FQDNs
    routes: Arc<RoutingTable>,
    /// List of active subscriptions to delegation states of given accounts
    subscriptions: Arc<HashMap<RequestId, SubMeta>>,
}

struct SubMeta {
    account: Pubkey,
    handle: Option<SubscriptionHandle>,
}

impl DelegationsCache {
    pub fn new(
        dispatcher_tx: Sender<Subscription>,
        routes: Arc<RoutingTable>,
        max_cached_delegations: usize,
    ) -> Arc<Self> {
        let (pubsub_tx, pubsub_rx) = mpsc::channel(1024);
        let min_capacity = 1024.min(max_cached_delegations);
        let this = Arc::new(Self {
            db: HashCache::with_capacity(min_capacity, max_cached_delegations).into(),
            subscriber_id: SubscriberId::generate(),
            dispatcher_tx,
            pubsub_tx,
            routes,
            subscriptions: Default::default(),
        });
        let updater = this.clone().updater(pubsub_rx);
        tokio::spawn(updater);
        this
    }

    pub async fn get_delegation_status(&self, pubkey: Pubkey) -> DelegationStatus {
        let pda = delegation_record_pda(pubkey);
        let entry = self.db.get_async(&pda).await;
        let status = {
            if let Some(e) = entry {
                e.get().status
            } else {
                drop(entry);
                let status = self.fetch(pda).await;
                let entry = self.db.entry_async(pda).await;
                self.insert(entry, pda, status, true).await;
                status
            }
        };
        tracing::debug!("account {pubkey} delegation status has been resolved to {status}");
        status
    }

    pub async fn fetch(&self, pda: Pubkey) -> DelegationStatus {
        let chain = &self.routes.base_chain().client;
        let mut attempt = 0;
        loop {
            let response = chain
                .get_account_with_commitment(&pda, CommitmentConfig::confirmed())
                .await;
            let record = match response {
                Ok(Response { value: Some(a), .. }) => a,
                Ok(Response { value: None, .. }) => {
                    break DelegationStatus::NotDelegated;
                }
                Err(error) => {
                    // this indicates an actual error, not found was handled in the previous arm
                    tracing::error!(%error, "failed to fetch account {pda} from chain");
                    attempt += 1;
                    if attempt > MAX_ACCOUNT_REFETCH_ATTEMPTS {
                        break DelegationStatus::NotDelegated;
                    }
                    tokio::time::sleep(Duration::from_secs(attempt * 2)).await;
                    continue;
                }
            };

            let Some(identity) = extract_delegation_identity(&record.data) else {
                break DelegationStatus::NotDelegated;
            };
            let status = DelegationStatus::Delegated(identity);

            break status;
        }
    }

    async fn insert(
        &self,
        entry: Entry<'_, Pubkey, DelegationEntry>,
        pda: Pubkey,
        status: DelegationStatus,
        subscribe: bool,
    ) {
        let request_id = RequestId::generate();
        match entry {
            Entry::Vacant(e) => {
                let entry = DelegationEntry { status, request_id };
                if let (Some(evicted), _) = e.put_entry(entry) {
                    let unsub = Unsubscription {
                        subscriber_id: self.subscriber_id,
                        request_id: evicted.1.request_id,
                        method: "accountUnsubscribe",
                    };
                    let Some((_, meta)) = self.subscriptions.remove(&evicted.1.request_id) else {
                        return;
                    };
                    let Some(tx) = meta.handle.map(|h| h.unsub) else {
                        return;
                    };
                    let _ = tx.send(unsub).await;
                };
            }
            Entry::Occupied(mut e) => e.status = status,
        }
        if subscribe {
            let params = RpcAccountInfoConfig {
                commitment: CommitmentConfig::confirmed().into(),
                ..Default::default()
            };
            let meta = SubMeta {
                account: pda,
                handle: None,
            };
            let _ = self.subscriptions.insert(request_id, meta);
            let payload = account_subscription_json(request_id, pda, Some(params));
            let destination = self.routes.base_chain().ws_url.clone();
            let subscription = Subscription {
                request_id,
                subscriber_id: self.subscriber_id,
                payload: payload.clone(),
                tx: self.pubsub_tx.clone(),
                destination,
                upstream: PubSubUpstreamKind::Chain,
            };
            let _ = self.dispatcher_tx.send(subscription).await;
        }
        tracing::debug!(
            id = request_id.0,
            %pda,
            "cache coherence subscription sent for the account"
        );
    }

    async fn updater(self: Arc<Self>, mut rx: Receiver<PubsubMessage>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                PubsubMessage::Subscribed(handle) => {
                    let Some(mut meta) = self.subscriptions.get(&handle.request_id) else {
                        continue;
                    };
                    let account = meta.account;
                    meta.handle.replace(handle);
                    drop(meta);
                    let status = self.fetch(account).await;
                    let entry = self.db.entry_async(account).await;
                    self.insert(entry, account, status, false).await;
                    tracing::debug!(
                        %account,
                        "delegations cache coherence subscription confirmed"
                    );
                }
                PubsubMessage::Notification { id, payload, .. } => {
                    let account = deserialize_account(&payload, &["value"]);

                    let Some(account) = account else {
                        tracing::warn!(
                            ?payload,
                            "delegations cache coherence manager received garbage update"
                        );
                        continue;
                    };
                    let status = if account.lamports == 0 {
                        DelegationStatus::NotDelegated
                    } else {
                        extract_delegation_identity(&account.data)
                            .map(DelegationStatus::Delegated)
                            .unwrap_or(DelegationStatus::NotDelegated)
                    };
                    let Some(entry) = self.subscriptions.get(&id) else {
                        tracing::warn!(
                            id = id.0,
                            ?payload,
                            "unknown subscription update was received"
                        );
                        continue;
                    };
                    let pubkey = entry.get().account;
                    drop(entry);
                    let Some(mut sts) = self.db.get(&pubkey) else {
                        tracing::warn!("received subscription for unknown pubkey");
                        self.subscriptions.remove(&id);
                        continue;
                    };
                    let sts = sts.get_mut();
                    tracing::debug!(
                        "account {pubkey} has changed its delegation status from {} to {status}",
                        sts.status
                    );
                    sts.status = status;
                }
                PubsubMessage::Disconnected(id) => {
                    let Some((_, meta)) = self.subscriptions.remove(&id) else {
                        tracing::warn!(id = id.0, "unknown subscription was terminated");
                        continue;
                    };
                    self.db.remove(&meta.account);
                    tracing::debug!(pda=%meta.account, "delegation record has been removed due to ws disconnect");
                }
            }
        }
    }
}

/// One to one PDA derivation logic for delegation record pubkey
pub fn delegation_record_pda(pubkey: Pubkey) -> Pubkey {
    let seeds: &[&[u8]] = &[b"delegation", pubkey.as_ref()];
    Pubkey::find_program_address(seeds, &DELEGATION_PROGRAM).0
}

fn extract_delegation_identity(data: &[u8]) -> Option<Pubkey> {
    let size = data.len();
    if size != DELEGATION_RECORD_DATA_SIZE {
        tracing::error!(%size, "unexpected delegation record size");
        return None;
    }
    let mut buffer = [0u8; 32];
    // first 8 bytes is a discriminator, followed by 32 bytes
    // representing the validator identity
    buffer.copy_from_slice(&data[8..40]);
    Some(Pubkey::new_from_array(buffer))
}
