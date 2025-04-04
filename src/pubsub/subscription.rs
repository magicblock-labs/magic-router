use std::sync::Arc;

use tokio::sync::mpsc::Sender;

use crate::types::{RequestId, SubscriberId, SubscriptionId};

pub enum SubscriptionAction {
    Subscribe(Subscription),
    Unsubscribe(Unsubscription),
}

pub struct Subscription {
    pub request_id: RequestId,
    pub subscriber_id: SubscriberId,
    pub payload: json::Value,
    pub tx: Sender<Arc<json::Value>>,
}

pub struct Unsubscription {
    pub subscriber_id: SubscriberId,
    pub subscription_id: SubscriptionId,
}
