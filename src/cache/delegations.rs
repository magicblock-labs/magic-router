use std::{future::Future, sync::Arc, time::Duration};

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
        let this = Self {
            db: HashCache::with_capacity(min_capacity, max_cached_delegations).into(),
            subscriber_id: SubscriberId::generate(),
            dispatcher_tx,
            pubsub_tx,
            routes,
            subscriptions: Default::default(),
        };
        let updater = this.updater(pubsub_rx);
        tokio::spawn(updater);
        Arc::new(this)
    }

    pub async fn get_delegation_status(&self, pubkey: Pubkey) -> DelegationStatus {
        let pda = delegation_record_pda(pubkey);
        let status = 'status: {
            if let Some(status) = self.db.read(&pda, |_, v| v.status) {
                break 'status status;
            }
            let mut attempt = 0;
            let chain = &self.routes.base_chain().client;
            loop {
                let response = chain
                    .get_account_with_commitment(&pda, CommitmentConfig::confirmed())
                    .await;
                let record = match response {
                    Ok(Response { value: Some(a), .. }) => a,
                    Ok(Response { value: None, .. }) => {
                        let status = DelegationStatus::NotDelegated;
                        self.insert(pda, status).await;
                        break 'status status;
                    }
                    Err(error) => {
                        // this indicates an actual error, not found was handled in the previous arm
                        tracing::error!(%error, "failed to fetch account {pubkey} from chain");
                        attempt += 1;
                        if attempt > MAX_ACCOUNT_REFETCH_ATTEMPTS {
                            break 'status DelegationStatus::NotDelegated;
                        }
                        tokio::time::sleep(Duration::from_secs(attempt * 2)).await;
                        continue;
                    }
                };
                let Some(identity) = extract_delegation_identity(&record.data) else {
                    return DelegationStatus::NotDelegated;
                };
                let status = DelegationStatus::Delegated(identity);

                self.insert(pda, status).await;

                break 'status status;
            }
        };
        tracing::debug!("account's delegation status has been resolved to {status}");
        status
    }

    async fn insert(&self, pda: Pubkey, status: DelegationStatus) {
        let request_id = RequestId::generate();
        let params = RpcAccountInfoConfig {
            commitment: CommitmentConfig::confirmed().into(),
            ..Default::default()
        };
        let destination = self.routes.base_chain().ws_url.clone();
        let entry = DelegationEntry {
            status,
            request_id,
            destination: destination.clone(),
        };
        match self.db.entry(pda) {
            Entry::Vacant(e) => {
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
            Entry::Occupied(mut e) => {
                e.put(entry);
                return;
            }
        }
        let payload = account_subscription_json(request_id, pda, Some(params));
        let subscription = Subscription {
            request_id,
            subscriber_id: self.subscriber_id,
            payload: payload.clone(),
            tx: self.pubsub_tx.clone(),
            destination: destination.clone(),
            upstream: PubSubUpstreamKind::Chain,
        };
        let _ = self.dispatcher_tx.send(subscription).await;
        tracing::debug!(
            id = request_id.0,
            %pda,
            "cache coherence subscription sent for the account"
        );

        let meta = SubMeta {
            account: pda,
            handle: None,
        };
        let _ = self.subscriptions.insert(request_id, meta);
    }

    fn updater(&self, mut rx: Receiver<PubsubMessage>) -> impl Future<Output = ()> {
        let db = self.db.clone();
        let subscriptions = self.subscriptions.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    PubsubMessage::Subscribed(handle) => {
                        let Some(mut meta) = subscriptions.get(&handle.request_id) else {
                            continue;
                        };
                        tracing::debug!(
                            id = handle.request_id.0,
                            pubkey = %meta.account,
                            "delegations cache coherence subscription confirmed"
                        );
                        meta.handle.replace(handle);
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
                        let Some(entry) = subscriptions.get(&id) else {
                            tracing::warn!(
                                id = id.0,
                                ?payload,
                                "unknown subscription update was received"
                            );
                            continue;
                        };
                        let pubkey = entry.get().account;
                        let Some(mut sts) = db.get(&pubkey) else {
                            tracing::warn!("received subscription for unknown pubkey");
                            subscriptions.remove(&id);
                            continue;
                        };
                        let sts = sts.get_mut();
                        tracing::debug!("account {pubkey} has changed its delegation status from {} to {status}", sts.status);
                        sts.status = status;
                    }
                    PubsubMessage::Disconnected(id) => {
                        let Some((_, meta)) = subscriptions.remove(&id) else {
                            tracing::warn!(id = id.0, "unknown subscription was terminated");
                            continue;
                        };
                        db.remove(&meta.account);
                    }
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
