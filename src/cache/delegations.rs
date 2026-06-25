//! This module provides a thread-safe, in-memory cache for Solana account delegation statuses.
//!
//! The `DelegationsCache` is designed to efficiently track whether a given account has delegated
//! its authority by storing and managing its "delegation record" PDA (Program Derived Address).
//! To maintain data freshness, it integrates with a real-time streaming service (Helius Laser)
//! for live updates and employs a cache coherence strategy to ensure that RPC fetches retrieve
//! the most up-to-date information.

use std::{sync::Arc, time::Duration};

use magicblock_sync::{AccountUpdate, DlpSyncChannelsRequester, DlpSyncer};
use scc::{
    hash_cache::{Entry, VacantEntry},
    HashCache,
};
use solana_account_decoder::UiAccountEncoding;
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{config::RpcAccountInfoConfig, response::Response as RpcResponse};
use tokio::sync::mpsc::Receiver;

use crate::{
    accounts::{delegation_record_pda_from_delegated_account, DelegationEntry},
    config::LaserStreamConfig,
    types::ParsedDelegationRecord,
};

use super::routes::RoutingTable;

const CHANNEL_CAPACITY: usize = 1024;
const MAX_ACCOUNT_REFETCH_ATTEMPTS: u64 = 3;
const RETRY_BACKOFF_SECONDS: u64 = 2;
/// The defensive shift for min context slot. This is needed due to
/// laserstream (slot source) usually being ahead of the RPC providers
const MIN_CONTEXT_SLOT_ROLLBACK: u64 = 8;

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
    syncer: DlpSyncChannelsRequester,
    /// A reference to the routing table, used to get an RPC client for fetching account data.
    routes: Arc<RoutingTable>,
}

impl DelegationsCache {
    /// Creates a new `DelegationsCache` and spawns the necessary background tasks.
    ///
    /// This constructor initializes the cache and spawns two background tasks:
    /// - A `LaserSubscriber` to manage real-time subscriptions.
    /// - An `updater` task to process notifications and update the cache.
    pub async fn new(
        routes: Arc<RoutingTable>,
        max_cached_delegations: usize,
        laser: LaserStreamConfig,
    ) -> Arc<Self> {
        // Spawn the LaserSubscriber task to handle real-time subscriptions.
        let syncer = DlpSyncer::start(laser.endpoint, laser.api_key)
            .await
            .unwrap();
        let (syncer, updates_rx) = syncer.split();

        let min_capacity = CHANNEL_CAPACITY.min(max_cached_delegations);
        let this = Arc::new(Self {
            db: HashCache::with_capacity(min_capacity, max_cached_delegations).into(),
            syncer,
            routes,
        });

        // Spawn the cache updater task to process notifications from LaserSubscriber.
        let updater = this.clone().updater(updates_rx);
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

        if let Some((slot, record)) = self
            .db
            .read_async(&pda, |_, entry| (entry.slot, entry.record.clone()))
            .await
        {
            if slot != 0 {
                return record;
            }

            return self.merge_fetched(pda, self.fetch(pda, None).await).await;
        }

        tracing::debug!(%pubkey, %pda, "tracking delegation for");
        // On a cache miss, subscribe to get a recent slot, then fetch and insert.
        let Some(subscription_slot) = self.subscribe(pda).await else {
            return self.fetch(pda, None).await.record;
        };
        let min_context_slot = subscription_slot.checked_sub(MIN_CONTEXT_SLOT_ROLLBACK);

        if let Entry::Vacant(vacant_entry) = self.db.entry_async(pda).await {
            // Slot 0 marks the cache fill as pending so Laser updates can merge before the RPC fetch returns.
            self.insert_new(
                vacant_entry,
                DelegationEntry {
                    record: None,
                    slot: 0,
                },
            )
            .await;
        }

        self.merge_fetched(pda, self.fetch(pda, min_context_slot).await)
            .await
    }

    /// Fetches a delegation record from the chain with a retry mechanism.
    ///
    /// It uses `min_context_slot` to ensure the RPC response is not stale, when available.
    pub async fn fetch(&self, pda: Pubkey, min_context_slot: Option<u64>) -> DelegationEntry {
        let rpc_client = &self.routes.base_chain().client;

        for attempt in 0..=MAX_ACCOUNT_REFETCH_ATTEMPTS {
            if attempt > 0 {
                let backoff_duration = Duration::from_secs(RETRY_BACKOFF_SECONDS * attempt);
                tokio::time::sleep(backoff_duration).await;
            }

            let response = rpc_client
                .get_ui_account_with_config(
                    &pda,
                    RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        commitment: Some(CommitmentConfig::confirmed()),
                        min_context_slot,
                        ..Default::default()
                    },
                )
                .await;

            match response {
                Ok(RpcResponse {
                    value: Some(account),
                    context,
                }) => {
                    let record = account
                        .data
                        .decode()
                        .and_then(ParsedDelegationRecord::from_bytes);
                    return DelegationEntry {
                        record,
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
        let evicted = vacant_entry.put_entry(record).0;
        if let Some(evicted) = evicted {
            // If an entry was evicted, unsubscribe from its real-time updates.
            if self
                .syncer
                .unsubscribe(evicted.0.to_bytes())
                .await
                .is_none()
            {
                tracing::error!("Failed to send unsubscribe for evicted entry, syncer terminated");
            }
        };
    }

    async fn merge_fetched(
        &self,
        pda: Pubkey,
        new_entry_data: DelegationEntry,
    ) -> Option<ParsedDelegationRecord> {
        match self.db.entry_async(pda).await {
            Entry::Occupied(mut occupied_entry) => {
                if occupied_entry.get().slot < new_entry_data.slot {
                    let record_to_return = new_entry_data.record.clone();
                    *occupied_entry.get_mut() = new_entry_data;
                    record_to_return
                } else {
                    occupied_entry.get().record.clone()
                }
            }
            Entry::Vacant(vacant_entry) => {
                let record_to_return = new_entry_data.record.clone();
                self.insert_new(vacant_entry, new_entry_data).await;
                record_to_return
            }
        }
    }

    /// Subscribes to real-time updates for an account PDA.
    async fn subscribe(&self, account: Pubkey) -> Option<u64> {
        let slot = self.syncer.subscribe(account.to_bytes()).await;
        if slot.is_none() {
            tracing::error!(%account, "Failed to send subscribe for delegation account, syncer terminated");
        }
        slot
    }

    /// The background task that processes notifications from the `LaserSubscriber`.
    async fn updater(self: Arc<Self>, mut rx: Receiver<AccountUpdate>) {
        while let Some(msg) = rx.recv().await {
            match msg {
                AccountUpdate::Delegated { record, data, slot } => {
                    let new_record = ParsedDelegationRecord::from_bytes(data);
                    self.update_cached_entry(record.into(), slot, new_record);
                }
                AccountUpdate::Undelegated { record, slot } => {
                    self.update_cached_entry(record.into(), slot, None);
                }
                AccountUpdate::SyncTerminated => {
                    // On disconnect, remove entry to prevent serving stale data.
                    // They will be re-fetched on next access.
                    // self.db.remove(&pubkey);
                    // tracing::debug!(%pubkey, "Removed delegation record due to disconnect");
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
        let Some(mut cached_entry) = self.db.get_sync(&pubkey) else {
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
