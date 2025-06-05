use std::sync::Arc;

use scc::HashCache;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_signature::Signature;

const MIN_KEPT_TRANSACTION_COUNT: usize = 16384;

/// Cache for transactions which were sent through the router
pub struct ForwardedTransactions {
    /// A mapping between transaction signature and the solana client used to send it upstream
    cache: HashCache<Signature, Arc<RpcClient>>,
}

impl ForwardedTransactions {
    /// Initialize the cache with given capacity, which acts as a soft upper limit for  
    pub fn new(capacity: usize) -> Self {
        let max = MIN_KEPT_TRANSACTION_COUNT.max(capacity);
        let cache = HashCache::with_capacity(MIN_KEPT_TRANSACTION_COUNT, max);
        Self { cache }
    }

    /// Track given sinature in cache before it's evicted by other inserts
    pub async fn track(&self, signature: Signature, client: Arc<RpcClient>) {
        let _ = self.cache.put_async(signature, client).await;
    }

    /// Get the client which was used to forward the given signature to the upstream, if exists
    pub async fn get(&self, signature: &Signature) -> Option<Arc<RpcClient>> {
        self.cache.get_async(signature).await.map(|c| c.clone())
    }
}
