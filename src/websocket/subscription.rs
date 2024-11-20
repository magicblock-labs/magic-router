//! Types for working with WebSocket subscription messages in the Solana blockchain.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use solana::pubkey::Pubkey;

use crate::DELEGATION_PROGRAM_ID;

/// Represents a subscription to an account on the Solana blockchain.
pub struct AccountSubscription {
    /// JSON-RPC ID of request sent to upstream, used for both HTTP and WS
    pub id: u64,
    /// Indicator of presence of an active websocket subscription
    pub subscribed: Arc<AtomicBool>,
    /// Indicator of current delegation status of given account
    pub delegated: Arc<AtomicBool>,
    /// Solana pubkey of account
    pub pubkey: Pubkey,
}

impl AccountSubscription {
    /// Creates a new `AccountSubscription` for the given `pubkey`.
    pub fn new(pubkey: Pubkey, delegated: Arc<AtomicBool>, subscribed: Arc<AtomicBool>) -> Self {
        /// Generates a unique request ID.
        fn id() -> u64 {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            COUNTER.fetch_add(1, Ordering::Relaxed)
        }
        Self {
            id: id(),
            pubkey,
            subscribed,
            delegated,
        }
    }

    /// Generate JSON-RPC request for websocket subscription
    pub fn ws(&self) -> Vec<u8> {
        self.json("accountSubscribe")
    }

    /// Generate JSON-RPC representation for HTTP request
    pub fn http(&self) -> Vec<u8> {
        self.json("getAccountInfo")
    }

    /// Returns a JSON representation (as slice) of the account request
    fn json(&self, method: &str) -> Vec<u8> {
        let value = json::json!({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": method,
            "params": [
                // we don't use the account itself as a subscription target, but rather its
                // delegation record PDA, which allows us to obtain some extra data, like
                // identity of the validator which was used in the delegation process, and still
                // uniquely identify delegated accoounts
                self.delegation_record_pda().to_string(),
                {
                    "commitment": "confirmed",
                    // use the most compact form to reduce latency on network transmissions
                    "encoding": "base64+zstd"
                }
            ]
        });
        json::to_vec(&value).expect("acc sub should always serialize")
    }

    /// Find the PDA associated with the delegation record for given account
    fn delegation_record_pda(&self) -> Pubkey {
        let seeds: &[&[u8]] = &[b"delegation", self.pubkey.as_ref()];
        Pubkey::find_program_address(seeds, &DELEGATION_PROGRAM_ID).0
    }
}
