use std::sync::Arc;

use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use tokio::sync::mpsc::Sender;
use url::Url;

use crate::types::{RequestId, SerdePubkey, SubscriberId};

use super::notification::PubsubMessage;

#[derive(Clone)]
pub enum SubscriptionAction {
    Subscribe(Subscription),
    Unsubscribe(Unsubscription),
}

#[derive(Clone)]
pub struct Subscription {
    pub request_id: RequestId,
    pub subscriber_id: SubscriberId,
    pub payload: json::Value,
    pub tx: Sender<PubsubMessage>,
    pub destination: Arc<Url>,
}

impl Subscription {
    pub fn clone_with_destination(&self, url: Arc<Url>) -> Self {
        let mut clone = self.clone();
        clone.destination = url;
        clone
    }

    pub fn to_unsubsciption(&self, method: &'static str) -> SubscriptionAction {
        SubscriptionAction::Unsubscribe(Unsubscription {
            subscriber_id: self.subscriber_id,
            request_id: self.request_id,
            destination: self.destination.clone(),
            method,
        })
    }
}

#[derive(Clone)]
pub struct Unsubscription {
    pub subscriber_id: SubscriberId,
    pub request_id: RequestId,
    pub method: &'static str,
    pub destination: Arc<Url>,
}

impl SubscriptionAction {
    pub fn destination(&self) -> &Url {
        match self {
            Self::Subscribe(s) => &s.destination,
            Self::Unsubscribe(u) => &u.destination,
        }
    }
}

#[inline(always)]
pub fn account_subscription_json(
    id: RequestId,
    pubkey: Pubkey,
    params: Option<RpcAccountInfoConfig>,
) -> json::Value {
    json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "accountSubscribe",
        "params": [
            SerdePubkey(pubkey),
            params
        ]
    })
}

#[inline(always)]
pub fn program_subscription_json(
    id: RequestId,
    pubkey: Pubkey,
    params: Option<RpcProgramAccountsConfig>,
) -> json::Value {
    json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "programSubscribe",
        "params": [
            SerdePubkey(pubkey),
            params
        ]
    })
}
