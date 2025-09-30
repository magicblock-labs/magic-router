//! This module provides a thread-safe, in-memory cache for Solana account delegation statuses.
//!
//! The `DelegationsCache` is designed to efficiently track whether a given account has delegated
//! its authority by storing and managing its "delegation record" PDA (Program Derived Address).
//! To maintain data freshness, it integrates with a real-time streaming service (Helius Laser)
//! for live updates and employs a cache coherence strategy to ensure that RPC fetches retrieve
//! the most up-to-date information.

use std::{sync::Arc, time::Duration};

use dlp::{pda::delegation_record_pda_from_delegated_account, state::DelegationRecord};
use scc::{
    hash_cache::{Entry, VacantEntry},
    HashCache,
};
use solana_account_decoder::UiAccountEncoding;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{config::RpcAccountInfoConfig, response::Response as RpcResponse};
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    oneshot,
};

use crate::{
    accounts::DelegationEntry,
    config::LaserStreamConfig,
    pubsub::laser::{LaserNotification, LaserRequest, LaserSubscriber},
    types::{ParsedDelegationRecord, SerdePubkey},
};

use super::routes::RoutingTable;

const CHANNEL_CAPACITY: usize = 1024;
const MAX_ACCOUNT_REFETCH_ATTEMPTS: u64 = 3;
const RETRY_BACKOFF_SECONDS: u64 = 2;

/// A concurrent, bounded hash map used as the backing store for the cache.
type DelegationsDB = Arc<HashCache<Pubkey, DelegationEntry>>;

/// An in-memory, thread-safe store for the delegation status of all encountered accounts.
///
/// This cache is responsible for:
/// 1.  **Caching**: Storing `DelegationEntry` data for quick access.
/// 2.  **Fetching**: On a cache miss, fetching the delegation record from the Solana chain.
/// 3.  **Updating**: Running a background task that listens to a real-time stream (Laser) for
///     account updates to keep the cache coherent.
pub struct DelegationsCache {
    /// The underlying concurrent hash map for storing delegation entries.
    db: DelegationsDB,
    /// A sender channel to dispatch requests to the `LaserSubscriber` task.
    requests_tx: Sender<LaserRequest>,
    /// A reference to the routing table, used to get an RPC client for fetching account data.
    routes: Arc<RoutingTable>,
}

impl DelegationsCache {
    /// Creates a new `DelegationsCache` and spawns the necessary background tasks.
    ///
    /// This constructor initializes the cache and spawns two background tasks:
    /// - A `LaserSubscriber` to manage real-time subscriptions.
    /// - An `updater` task to process notifications and update the cache.
    pub fn new(
        routes: Arc<RoutingTable>,
        max_cached_delegations: usize,
        laser: LaserStreamConfig,
    ) -> Arc<Self> {
        let (requests_tx, requests_rx) = channel(CHANNEL_CAPACITY);
        let (notifications_tx, notifications_rx) = channel(CHANNEL_CAPACITY);

        // Spawn the LaserSubscriber task to handle real-time subscriptions.
        let laser_subscriber = LaserSubscriber::new(laser, requests_rx, notifications_tx);
        tokio::spawn(tokio::task::unconstrained(laser_subscriber.run()));

        let min_capacity = CHANNEL_CAPACITY.min(max_cached_delegations);
        let this = Arc::new(Self {
            db: HashCache::with_capacity(min_capacity, max_cached_delegations).into(),
            requests_tx,
            routes,
        });

        // Spawn the cache updater task to process notifications from LaserSubscriber.
        let updater = this.clone().updater(notifications_rx);
        tokio::spawn(tokio::task::unconstrained(updater));

        this
    }

    /// Gets the delegation authority for a given account, if it exists.
    pub async fn get_delegation_authority(&self, pubkey: Pubkey) -> Option<Pubkey> {
        self.get_record(pubkey).await.map(|r| r.authority.0)
    }

    /// Retrieves the delegation record for an account, using the cache.
    ///
    /// On a cache miss, it subscribes to real-time updates and fetches the initial state,
    /// ensuring that subsequent reads are fast and subsequent updates are pushed to the cache.
    pub async fn get_record(&self, pubkey: Pubkey) -> Option<ParsedDelegationRecord> {
        let pda = delegation_record_pda_from_delegated_account(&pubkey);
        let entry = self.db.entry_async(pda).await;

        match entry {
            Entry::Occupied(occupied_entry) => occupied_entry.get().record.clone(),
            Entry::Vacant(vacant_entry) => {
                tracing::debug!(%pubkey, %pda, "tracking delegation for");
                // On a cache miss, subscribe to get a recent slot, then fetch and insert.
                let slot = self.subscribe(pda).await;
                let new_entry_data = self.fetch(pda, slot).await;
                let record_to_return = new_entry_data.record.clone();
                self.insert_new(vacant_entry, new_entry_data).await;
                record_to_return
            }
        }
    }

    /// Fetches a delegation record from the chain with a retry mechanism.
    ///
    /// It uses `min_context_slot` to ensure the RPC response is not stale.
    pub async fn fetch(&self, pda: Pubkey, min_slot: u64) -> DelegationEntry {
        let rpc_client = &self.routes.base_chain().client;

        for attempt in 0..=MAX_ACCOUNT_REFETCH_ATTEMPTS {
            if attempt > 0 {
                let backoff_duration = Duration::from_secs(RETRY_BACKOFF_SECONDS * attempt);
                tokio::time::sleep(backoff_duration).await;
            }

            let response = rpc_client
                .get_account_with_config(
                    &pda,
                    RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot: Some(min_slot),
                        ..Default::default()
                    },
                )
                .await;

            match response {
                Ok(RpcResponse {
                    value: Some(account),
                    context,
                }) => {
                    return DelegationEntry {
                        record: extract_delegation_record(&account.data),
                        slot: context.slot,
                    };
                }
                Ok(RpcResponse {
                    value: None,
                    context,
                }) => {
                    // Account not existing is a valid state (not delegated).
                    return DelegationEntry {
                        record: None,
                        slot: context.slot,
                    };
                }
                Err(error) => {
                    tracing::warn!(%error, attempt, %pda, "Failed to fetch account, retrying...");
                    continue;
                }
            }
        }

        tracing::error!(%pda, "All fetch attempts failed");
        DelegationEntry {
            record: None,
            slot: 0,
        }
    }

    /// Inserts a new record into a vacant cache entry and handles eviction.
    async fn insert_new(
        &self,
        vacant_entry: VacantEntry<'_, Pubkey, DelegationEntry>,
        record: DelegationEntry,
    ) {
        if let (Some(evicted), _) = vacant_entry.put_entry(record) {
            // If an entry was evicted, unsubscribe from its real-time updates.
            let unsub_req = LaserRequest::Unsubscribe(evicted.0);
            if let Err(e) = self.requests_tx.send(unsub_req).await {
                tracing::error!(error = %e, "Failed to send unsubscribe for evicted entry");
            }
        };
    }

    /// Subscribes to real-time updates for an account PDA.
    async fn subscribe(&self, account: Pubkey) -> u64 {
        let (slot_tx, slot_rx) = oneshot::channel();
        let sub_req = LaserRequest::Subscribe { account, slot_tx };

        if self.requests_tx.send(sub_req).await.is_err() {
            tracing::error!("Failed to send subscription request: Laser task may have panicked.");
            return 0;
        }

        slot_rx.await.unwrap_or_else(|_| {
            tracing::error!("Laser subscriber failed to send subscription slot");
            0
        })
    }

    /// The background task that processes notifications from the `LaserSubscriber`.
    async fn updater(self: Arc<Self>, mut rx: Receiver<LaserNotification>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                LaserNotification::Delegated { pubkey, data, slot } => {
                    let new_record = extract_delegation_record(&data);
                    self.update_cached_entry(pubkey, slot, new_record);
                }
                LaserNotification::Undelegated { pubkey, slot } => {
                    self.update_cached_entry(pubkey, slot, None);
                }
                LaserNotification::Disconnected(pubkey) => {
                    // On disconnect, remove entry to prevent serving stale data.
                    // They will be re-fetched on next access.
                    self.db.remove(&pubkey);
                    tracing::debug!(%pubkey, "Removed delegation record due to disconnect");
                }
            }
        }
    }

    /// Helper to update an entry in the cache with new data from a notification.
    fn update_cached_entry(
        &self,
        pubkey: Pubkey,
        slot: u64,
        new_record: Option<ParsedDelegationRecord>,
    ) {
        let Some(mut cached_entry) = self.db.get(&pubkey) else {
            tracing::warn!(%pubkey, "Received update for unknown or evicted pubkey");
            return;
        };

        // Only update if the incoming data is from a newer slot to prevent race conditions.
        if cached_entry.get().slot >= slot {
            return;
        }

        let old_status = if cached_entry.record.is_some() {
            "delegated"
        } else {
            "not delegated"
        };
        let new_status = if new_record.is_some() {
            "delegated"
        } else {
            "not delegated"
        };

        if old_status != new_status {
            tracing::debug!(
                %pubkey,
                "Delegation status changed from '{old_status}' to '{new_status}' at slot {slot}"
            );
        }

        let entry_mut = cached_entry.get_mut();
        entry_mut.record = new_record;
        entry_mut.slot = slot;
    }
}

/// Attempts to deserialize a byte slice into a `ParsedDelegationRecord`.
fn extract_delegation_record(data: &[u8]) -> Option<ParsedDelegationRecord> {
    if data.len() != DelegationRecord::size_with_discriminator() {
        tracing::error!(
            size = data.len(),
            expected = DelegationRecord::size_with_discriminator(),
            "Unexpected delegation record size"
        );
        return None;
    }

    match DelegationRecord::try_from_bytes_with_discriminator(data) {
        Ok(record) => Some(ParsedDelegationRecord {
            authority: SerdePubkey(record.authority),
            owner: SerdePubkey(record.owner),
            delegation_slot: record.delegation_slot,
            lamports: record.lamports,
        }),
        Err(error) => {
            tracing::error!(%error, "Failed to parse delegation record");
            None
        }
    }
}
