use std::{future::Future, sync::Arc, time::Duration};

use scc::{hash_cache::Entry, HashCache, HashMap};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::response::Response;
use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::{
    accounts::{
        DelegationEntry, DelegationStatus, DELEGATION_PROGRAM, DELEGATION_RECORD_DATA_SIZE,
    },
    pubsub::{
        notification::{deserialize_account, PubsubMessage},
        subscription::{
            account_subscription_json, Subscription, SubscriptionAction, Unsubscription,
        },
    },
    types::{RequestId, SubscriberId, UniqueId},
};

use super::routes::RoutingTable;

const MAX_ACCOUNT_REFETCH_ATTEMPTS: u64 = 3;
type DelegationsDB = Arc<HashCache<Pubkey, DelegationEntry>>;

pub struct DelegationsCache {
    subscriber_id: SubscriberId,
    pubsub_tx: Sender<PubsubMessage>,
    db: DelegationsDB,
    dispatcher_tx: Sender<SubscriptionAction>,
    routes: Arc<RoutingTable>,
    subscriptions: Arc<HashMap<RequestId, Pubkey>>,
}

impl DelegationsCache {
    pub fn new(
        dispatcher_tx: Sender<SubscriptionAction>,
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
        if let Some(entry) = self.db.get(&pda) {
            return entry.get().status;
        }
        let mut attempt = 0;
        let chain = &self.routes.base_chain().client;
        loop {
            let response = chain
                .get_account_with_commitment(&pda, CommitmentConfig::default())
                .await;
            let record = match response {
                Ok(Response { value: Some(a), .. }) => a,
                Ok(Response { value: None, .. }) => {
                    let status = DelegationStatus::NotDelegated;
                    self.insert(pda, status).await;
                    return status;
                }
                Err(error) => {
                    // this indicates an actual error, not found was handled in the previous arm
                    tracing::error!(%error, "failed to fetch account {pubkey} from chain");
                    attempt += 1;
                    if attempt > MAX_ACCOUNT_REFETCH_ATTEMPTS {
                        return DelegationStatus::NotDelegated;
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

            break status;
        }
    }

    async fn insert(&self, pubkey: Pubkey, status: DelegationStatus) {
        let request_id = RequestId::generate();
        let payload = account_subscription_json(request_id, pubkey, None);
        let destination = self.routes.base_chain().ws_url.clone();
        let subscription = SubscriptionAction::Subscribe(Subscription {
            request_id,
            subscriber_id: self.subscriber_id,
            payload: payload.clone(),
            tx: self.pubsub_tx.clone(),
            destination: destination.clone(),
        });
        let entry = DelegationEntry {
            status,
            request_id,
            destination,
        };
        let result = self.dispatcher_tx.send(subscription).await;

        let _ = self.subscriptions.insert(request_id, pubkey);
        match self.db.entry(pubkey) {
            Entry::Vacant(e) => {
                if let (Some(evicted), _) = e.put_entry(entry) {
                    let unsub = SubscriptionAction::Unsubscribe(Unsubscription {
                        subscriber_id: self.subscriber_id,
                        request_id: evicted.1.request_id,
                        destination: evicted.1.destination,
                        method: "accountUnsubscribe",
                    });
                    self.subscriptions.remove(&evicted.1.request_id);
                    let _ = self.dispatcher_tx.send(unsub).await;
                }
            }
            Entry::Occupied(mut e) => {
                e.put(entry);
            }
        }
    }

    fn updater(&self, mut rx: Receiver<PubsubMessage>) -> impl Future<Output = ()> {
        let db = self.db.clone();
        let subscriptions = self.subscriptions.clone();
        async move {
            while let Some(msg) = rx.recv().await {
                match msg {
                    PubsubMessage::Subscribed(id) => {
                        tracing::debug!(
                            id = id.0,
                            "delegations cache coherence subscription confirmed"
                        );
                    }
                    PubsubMessage::Notification { id, payload } => {
                        let account = deserialize_account(&payload, &["value"]);

                        let Some(account) = account else {
                            tracing::warn!(
                                ?payload,
                                "delegations cache coherence manager received garbage update"
                            );
                            continue;
                        };
                        let Some(entry) = subscriptions.get(&id) else {
                            tracing::warn!(
                                id = id.0,
                                ?payload,
                                "unknown subscription update was received"
                            );
                            continue;
                        };
                        let pubkey = entry.get();
                        let status = if account.lamports == 0 {
                            DelegationStatus::NotDelegated
                        } else {
                            extract_delegation_identity(&account.data)
                                .map(DelegationStatus::Delegated)
                                .unwrap_or(DelegationStatus::NotDelegated)
                        };
                        let Some(mut sts) = db.get(pubkey) else {
                            tracing::warn!("received subscription for unknown pubkey");
                            subscriptions.remove(&id);
                            continue;
                        };
                        sts.get_mut().status = status;
                    }
                    PubsubMessage::Disconnected(id) => {
                        let Some((_, pubkey)) = subscriptions.remove(&id) else {
                            tracing::warn!(id = id.0, "unknown subscription was terminated");
                            continue;
                        };
                        db.remove(&pubkey);
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
