//! Manages real-time subscriptions to the Helius Laserstream service.
//!
//! This module defines the `LaserSubscriber`, an actor that handles individual gRPC streams
//! for each subscribed Solana account. This simplified model ensures that a failure in one
//! stream does not impact any other active subscriptions.
//!
//! # Key Features
//! - **One Stream Per Account**: Each account subscription is managed in its own isolated gRPC stream.
//! - **Slot Tracking**: A dedicated stream provides a recent, confirmed slot for cache-coherent fetches.
//! - **Automatic Recovery**: The underlying client handles reconnections. If a stream fails terminally,
//!   it is cleaned up, and the cache is notified to take action.

use std::{collections::HashMap, pin::Pin};

use futures::{Stream, StreamExt};
use helius_laserstream::{
    client,
    grpc::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts,
        SubscribeUpdate,
    },
    ChannelOptions, LaserstreamConfig, LaserstreamError,
};
use solana_pubkey::Pubkey;
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tokio_stream::StreamMap;

use crate::config::LaserStreamConfig;

type LaserResult = Result<SubscribeUpdate, LaserstreamError>;
type LaserStreamUpdate = (Pubkey, LaserResult);
type LaserStream = Pin<Box<dyn Stream<Item = LaserResult> + Send>>;

/// A conservative slot offset to ensure the slot provided for RPC fetches is confirmed.
const SLOT_DELAY_WINDOW: u64 = 8;

/// Requests sent from the `DelegationsCache` to the `LaserSubscriber`.
pub enum LaserRequest {
    /// Request to subscribe to updates for a given `Pubkey`.
    Subscribe {
        account: Pubkey,
        /// A channel to send back the current slot number for cache coherence.
        slot_tx: oneshot::Sender<u64>,
    },
    /// Request to unsubscribe from updates for a `Pubkey`.
    Unsubscribe(Pubkey),
}

/// Notifications sent from the `LaserSubscriber` back to the `DelegationsCache`.
pub enum LaserNotification {
    /// A delegation record account has been created/updated.
    Delegated {
        pubkey: Pubkey,
        data: Vec<u8>,
        slot: u64,
    },
    /// A delegation record account has been closed.
    Undelegated { pubkey: Pubkey, slot: u64 },
    /// A gRPC stream has disconnected and was cleaned up.
    Disconnected(Pubkey),
}

/// The main actor that manages all Laserstream subscriptions.
pub struct LaserSubscriber {
    config: LaserstreamConfig,
    accounts: StreamMap<Pubkey, LaserStream>,
    slots: LaserStream,
    notifications_tx: Sender<LaserNotification>,
    requests_rx: Receiver<LaserRequest>,
    latest_slot: u64,
}

impl LaserSubscriber {
    /// Creates a new `LaserSubscriber`.
    pub fn new(
        config: LaserStreamConfig,
        requests_rx: Receiver<LaserRequest>,
        notifications_tx: Sender<LaserNotification>,
    ) -> Self {
        let channel_options = ChannelOptions {
            connect_timeout_secs: Some(5),
            http2_keep_alive_interval_secs: Some(15),
            tcp_keepalive_secs: Some(30),
            ..Default::default()
        };
        let laser_config = LaserstreamConfig {
            api_key: config.api_key,
            endpoint: config.endpoint,
            channel_options,
            // Configure the client to handle retries internally.
            max_reconnect_attempts: Some(4),
            replay: true,
        };
        let slots = Self::create_slot_stream(laser_config.clone());

        Self {
            latest_slot: 0,
            config: laser_config,
            requests_rx,
            notifications_tx,
            slots,
            accounts: Default::default(),
        }
    }

    /// The main event loop for the actor.
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(update) = self.accounts.next(), if !self.accounts.is_empty() => {
                    self.handle_account_update(update).await;
                }
                Some(update) = self.slots.next() => {
                    self.handle_slot_update(update);
                }
                Some(request) = self.requests_rx.recv() => {
                    self.handle_request(request).await;
                }
                else => {
                    tracing::info!("All channels closed. LaserSubscriber is shutting down.");
                    break;
                }
            }
        }
    }

    /// Handles a new request from the `DelegationsCache`.
    async fn handle_request(&mut self, request: LaserRequest) {
        match request {
            LaserRequest::Subscribe { account, slot_tx } => {
                self.process_subscribe(account, slot_tx);
            }
            LaserRequest::Unsubscribe(pubkey) => {
                self.process_unsubscribe(pubkey);
            }
        }
    }

    /// Processes an incoming subscription request by creating a new stream.
    fn process_subscribe(&mut self, account: Pubkey, slot_tx: oneshot::Sender<u64>) {
        if self.accounts.contains_key(&account) {
            tracing::warn!(%account, "Received a duplicate subscription request.");
        } else {
            tracing::debug!(%account, "Creating new subscription stream.");
            let stream = Self::create_account_stream(self.config.clone(), account);
            self.accounts.insert(account, Box::pin(stream));
        }

        if slot_tx
            .send(self.latest_slot.saturating_sub(SLOT_DELAY_WINDOW))
            .is_err()
        {
            tracing::warn!(%account, "Failed to send slot back; requester may have dropped.");
        }
    }

    /// Processes an unsubscribe request by removing the corresponding stream.
    fn process_unsubscribe(&mut self, pubkey: Pubkey) {
        if self.accounts.remove(&pubkey).is_some() {
            tracing::debug!(%pubkey, "Unsubscribed and removed stream locally.");
        }
    }

    /// Handles an update from the dedicated slot stream.
    fn handle_slot_update(&mut self, update: LaserResult) {
        if let Ok(SubscribeUpdate {
            update_oneof: Some(UpdateOneof::Slot(slot)),
            ..
        }) = update
        {
            self.latest_slot = slot.slot;
        } else if let Err(err) = update {
            tracing::warn!(%err, "Slot stream disconnected. Reconnecting...");
            self.slots = Self::create_slot_stream(self.config.clone());
        }
    }

    /// Handles an update from one of the account data streams.
    async fn handle_account_update(&mut self, (pubkey, result): LaserStreamUpdate) {
        match result {
            Ok(SubscribeUpdate {
                update_oneof: Some(UpdateOneof::Account(acc)),
                ..
            }) => {
                let (Some(account), slot) = (acc.account, acc.slot) else {
                    return;
                };

                // Defensive check to ensure the update corresponds to the stream's key.
                if pubkey.as_ref() != account.pubkey {
                    tracing::warn!(
                        stream_key = %pubkey,
                        "Received mismatched pubkey in account update stream."
                    );
                    return;
                }

                let notification = if account.lamports != 0 {
                    LaserNotification::Delegated {
                        pubkey,
                        data: account.data,
                        slot,
                    }
                } else {
                    LaserNotification::Undelegated { pubkey, slot }
                };

                let _ = self.notifications_tx.send(notification).await;
            }
            Err(err) => {
                tracing::warn!(%pubkey, %err, "Stream for account failed terminally. Cleaning up.");
                // Remove the failed stream. The cache will be notified to take action.
                self.accounts.remove(&pubkey);
                let notification = LaserNotification::Disconnected(pubkey);
                if let Err(e) = self.notifications_tx.send(notification).await {
                    tracing::error!("Failed to send Disconnected notification to cache: {e}");
                }
            }
            _ => { /* Ignore other message types */ }
        }
    }

    /// Helper to create a dedicated stream for a single account.
    fn create_account_stream(
        config: LaserstreamConfig,
        account: Pubkey,
    ) -> impl Stream<Item = LaserResult> {
        let mut accounts = HashMap::new();
        accounts.insert(
            "delegations".into(),
            SubscribeRequestFilterAccounts {
                account: vec![account.to_string()],
                ..Default::default()
            },
        );
        let request = SubscribeRequest {
            accounts,
            ..Default::default()
        };
        client::subscribe(config, request).0
    }

    /// Helper to create the dedicated stream for slot updates.
    fn create_slot_stream(config: LaserstreamConfig) -> LaserStream {
        let mut slots = HashMap::new();
        slots.insert("slots".into(), Default::default());
        let request = SubscribeRequest {
            slots,
            ..Default::default()
        };
        Box::pin(client::subscribe(config, request).0)
    }
}
