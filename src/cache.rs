//! Accounts' status cache, it provides functionality to insert, check for existence, and remove
//! accounts from the cache. Additionally, it handles garbage collection of undelegated accounts.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use solana::pubkey::Pubkey;
use tokio::sync::mpsc::Receiver;

use crate::websocket::pool::WebsocketPool;

/// Cheaply clonable cache (meant to be used concurrently) for delegation status of accounts
#[derive(Clone)]
pub struct AccountsCache {
    /// handle for the pool of websocket connections, that can be used
    /// for subscribing to changes in delegation status of accounts
    wspool: Arc<WebsocketPool>,
    /// concurrent HashMap that is used as in-memory key-value
    /// store for delegation statuses of accounts
    delegations: Arc<scc::HashMap<Pubkey, DelegatedAccount>>,
}

/// Local delegation record of account
pub struct DelegatedAccount {
    // FIXME(bmuddha13): should be used for routing
    // between known ERs, by using Pubkey->IP mapping
    /// validator identity that given account has been delegated to
    #[allow(unused)]
    validator: Option<Pubkey>,
    /// flag indicating whether given account is delegated or not
    delegated: Arc<AtomicBool>,
    /// flag, used to inicate whether given account has an active websocket subscription
    subscribed: Arc<AtomicBool>,
}

impl AccountsCache {
    /// Initialize cache with given websocket pool, and channel handle to receive undelegation
    /// notifications from websocket connection handlers
    pub fn new(wspool: WebsocketPool, undelegations: Receiver<Pubkey>) -> Self {
        let delegations = Default::default();
        let this = Self {
            delegations,
            wspool: wspool.into(),
        };
        // run garbage collection in a seprate task
        tokio::spawn(this.clone().gc(undelegations));
        this
    }

    /// Create a new cache record for given account
    pub async fn insert(&self, key: Pubkey) {
        // we use `entry` API, as it alows to mutate the hashmap in an atomic fashion, preventing
        // race conditions if multiple requests try to insert the same account
        let scc::hash_map::Entry::Vacant(e) = self.delegations.entry_async(key).await else {
            return;
        };
        let subscribed = Arc::new(AtomicBool::default());
        let delegated = Arc::new(AtomicBool::new(false));
        let val = DelegatedAccount {
            validator: None,
            delegated: delegated.clone(),
            subscribed: subscribed.clone(),
        };
        e.insert_entry(val);
        self.wspool.subscribe(key, delegated, subscribed).await;
    }

    /// Check whether a cache record exists for given account, the account is delegated and it has
    /// an active websocket subscription to keep the state up to date
    #[inline(always)]
    pub async fn contains(&self, key: &Pubkey) -> bool {
        self.delegations
            .get_async(key)
            .await
            .map(|e| e.delegated.load(Ordering::Acquire) && e.subscribed.load(Ordering::Acquire))
            .unwrap_or_default()
    }

    /// Remove account from cache
    #[inline(always)]
    pub async fn remove(&self, key: &Pubkey) {
        self.delegations.remove_async(key).await;
    }

    /// Garbage collection, removes accounts from cache which are reported as being undelegated by
    /// websocket connection handler
    async fn gc(self, mut undelegations: Receiver<Pubkey>) {
        while let Some(pubkey) = undelegations.recv().await {
            self.remove(&pubkey).await;
        }
    }
}
