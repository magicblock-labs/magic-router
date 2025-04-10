use std::sync::Arc;

use json::JsonValueTrait;
use serde::Deserialize;
use solana_account::Account;
use solana_account_decoder::UiAccount;

use crate::types::{RequestId, SubscriptionId};

/// Represents a WebSocket message received from the Solana blockchain.
/// can be either a result of subscription with ID or an actual state update
#[derive(Debug)]
pub enum WebsocketMessage {
    /// A subscription result message.
    Subscribed(SubscriptionResult),
    /// An unsubscription result message.
    Unsubscribed(UnsubscriptionResult),
    /// A notification message.
    Notification(Notification),
}

/// Internal message sent by WebSocket connection handler to all of its subscribers
/// (entities listening for notifications)
pub enum PubsubMessage {
    /// Successful subscription to given request
    Subscribed(RequestId),
    /// WebSocket notification received by connection
    Notification {
        id: RequestId,
        payload: Arc<json::Value>,
    },
    /// A notification indicating that connection is down, and all subscribers
    /// should choose another connection to revive the given subscription
    Disconnected(RequestId),
}

impl WebsocketMessage {
    /// Deserializes a WebSocket message from a byte buffer.
    pub fn deserialize(buffer: &[u8]) -> Result<Self, json::Error> {
        let result = json::lazyvalue::get(buffer, &["result"]);
        let msg = if let Ok(result) = result {
            if result.is_u64() {
                WebsocketMessage::Subscribed(json::from_slice::<SubscriptionResult>(buffer)?)
            } else {
                WebsocketMessage::Unsubscribed(json::from_slice::<UnsubscriptionResult>(buffer)?)
            }
        } else {
            WebsocketMessage::Notification(json::from_slice::<Notification>(buffer)?)
        };
        Ok(msg)
    }
}

#[derive(Deserialize, Debug)]
pub struct Notification {
    pub params: NotificationParams,
}

/// Represents the parameters of a notification message.
#[derive(Deserialize, Debug)]
pub struct NotificationParams {
    /// Result of notification
    pub result: json::Value,
    /// Subscription ID
    pub subscription: SubscriptionId,
}

/// Represents a subscription result message.
#[derive(Deserialize, Debug)]
pub struct SubscriptionResult {
    /// ID of subscription request, not a subscription ID
    pub id: RequestId,
    /// resultant subscription ID
    pub result: SubscriptionId,
}

/// Represents an unsubscription result message.
#[derive(Deserialize, Debug)]
pub struct UnsubscriptionResult {
    /// ID of unsubscription request
    pub id: RequestId,
}

#[inline(always)]
pub fn deserialize_account(payload: &json::Value, path: &[&str]) -> Option<Account> {
    deserialize_field::<UiAccount>(payload, path)
        .as_ref()
        .and_then(UiAccount::decode::<Account>)
}

#[inline(always)]
pub fn deserialize_field<'a, F: Deserialize<'a>>(
    payload: &'a json::Value,
    path: &[&str],
) -> Option<F> {
    let mut value = payload;

    for p in path {
        value = value.get(p)?;
    }

    json::from_value::<F>(value).ok()
}
