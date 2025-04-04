use std::time::Duration;

use scc::{hash_cache::Entry, HashCache};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_api::response::Response;

use crate::accounts::{DelegationStatus, DELEGATION_PROGRAM, DELEGATION_RECORD_DATA_SIZE};

const MAX_ACCOUNT_REFETCH_ATTEMPTS: u64 = 3;

struct DelegationsCache {
    db: HashCache<Pubkey, DelegationStatus>,
    chain: RpcClient,
}

impl DelegationsCache {
    fn new(chain: RpcClient) -> Self {
        Self {
            db: Default::default(),
            chain,
        }
    }

    async fn get_delegation_status(&self, pubkey: Pubkey) -> Option<DelegationStatus> {
        if let Some(entry) = self.db.get(&pubkey) {
            return Some(entry.get().clone());
        }
        let pda = delegation_record_pda(pubkey);
        let mut attempt = 0;
        loop {
            let response = self
                .chain
                .get_account_with_commitment(&pubkey, CommitmentConfig::default())
                .await;
            let record = match response {
                Ok(Response { value: Some(a), .. }) => a,
                Ok(Response { value: None, .. }) => {
                    self.insert(pubkey, DelegationStatus::NotDelegated);
                    return None;
                }
                Err(error) => {
                    tracing::error!(%error, "failed to fetch account {pubkey} from chain");
                    attempt += 1;
                    if attempt > MAX_ACCOUNT_REFETCH_ATTEMPTS {
                        return None;
                    }
                    tokio::time::sleep(Duration::from_secs(attempt * 2)).await;
                    continue;
                }
            };
            let size = record.data.len();
            if size == DELEGATION_RECORD_DATA_SIZE {
                tracing::error!(%size, "unexpected delegation record size")
            }
            let mut buffer = [0u8; 32];
            // first 8 bytes is a discriminator, followed by 32 bytes
            // representing the validator identity
            buffer.copy_from_slice(&record.data[8..40]);
            let status = DelegationStatus::Delegated(Pubkey::new_from_array(buffer));

            self.insert(pubkey, status.clone());

            break Some(status);
        }
    }

    fn insert(&self, pubkey: Pubkey, status: DelegationStatus) {
        match self.db.entry(pubkey) {
            Entry::Vacant(e) => {
                e.put_entry(status);
            }
            Entry::Occupied(mut e) => {
                e.put(status);
            }
        }
    }

    async fn run_updater() {}
}

/// One to one PDA derivation logic for delegation record pubkey
fn delegation_record_pda(pubkey: Pubkey) -> Pubkey {
    let seeds: &[&[u8]] = &[b"delegation", pubkey.as_ref()];
    Pubkey::find_program_address(seeds, &DELEGATION_PROGRAM).0
}
