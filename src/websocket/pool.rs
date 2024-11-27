//! Websocket connection pool, which is used to load balance
//! subscriptions between existing connections. This pool is
//! used for maintaining the delegation status cache in an
//! up to date state, and not for serving websocket subscriptions
//! coming from clients

use std::{
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
};

use flume::Sender;
use crate::solana::Pubkey;
use tokio::{sync::mpsc::Sender as TokioSender, task::JoinHandle};

use crate::{config::WebsocketConf, http::client::HttpClient};

use super::{connection::WsConnection, subscription::AccountSubscription};

/// Handle to websocket connection pool
pub struct WebsocketPool {
    tx: Sender<AccountSubscription>,
}

impl WebsocketPool {
    /// Initialize websocket connections to given endpoints, each connection handler is spawned
    /// into separate async task
    pub async fn new(
        mut config: WebsocketConf,
        chain: HttpClient,
        undelegations: TokioSender<Pubkey>,
    ) -> crate::Result<(Self, Vec<JoinHandle<()>>)> {
        let (tx, rx) = flume::bounded(config.max_queued_subs);
        let slot = Arc::new(AtomicU64::default());
        let endpoints = mem::take(&mut config.endpoints);
        let mut connections =
            Vec::with_capacity(endpoints.iter().map(|e| e.connections as usize).sum());

        for endpoint in endpoints {
            for _ in 0..endpoint.connections {
                let ws = WsConnection::establish(
                    endpoint.url.clone(),
                    &config,
                    chain.clone(),
                    rx.clone(),
                    undelegations.clone(),
                    slot.clone(),
                )
                .await?;
                connections.push(tokio::spawn(ws.start()));
            }
        }
        Ok((Self { tx }, connections))
    }

    /// Send subscription for account's delegation record PDA down the
    /// channel where websocket connection handlers are listening
    /// args:
    /// pubkey - account's pubkey, delegation record PDA will be generated upon subscription
    /// delegated - delegation status indicator, copy is stored in AccountsCache
    /// subscribed - active websocket subscription indicator, copy is stored in AccountsCache
    pub async fn subscribe(
        &self,
        pubkey: Pubkey,
        delegated: Arc<AtomicBool>,
        subscribed: Arc<AtomicBool>,
    ) {
        let sub = AccountSubscription::new(pubkey, delegated, subscribed);
        let _ = self.tx.send_async(sub).await;
    }
}
