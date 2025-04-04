use scc::HashMap;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

pub struct RoutingTable {
    inner: HashMap<Pubkey, RpcClient>,
}
