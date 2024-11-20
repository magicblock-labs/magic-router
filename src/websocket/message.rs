//! Websocket messages received from remote

use json::{Deserialize, JsonValueTrait};

use crate::account::AccountInfo;

/// Represents a WebSocket message received from the Solana blockchain.
/// can be either a result of subscription with ID or an actual state update
pub enum WebsocketMessage<'a> {
    /// A subscription result message.
    Subscribed(SubscriptionResult),
    /// An unsubscription result message.
    Unsubscribed(UnsubscriptionResult),
    /// A notification message.
    Notification(Notification<'a>),
}

impl<'a> WebsocketMessage<'a> {
    /// Deserializes a WebSocket message from a byte buffer.
    pub fn deserialize(buffer: &'a [u8]) -> Result<Self, json::Error> {
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

/// Represents the parameters of a notification message.
#[derive(Deserialize)]
pub struct NotificationParams<T> {
    /// Result of notification
    pub result: T,
    /// Subscription ID
    pub subscription: u64,
}

/// Represents a notification message received from the Solana blockchain.
#[derive(Deserialize)]
#[serde(bound(deserialize = "'de: 'a"))]
#[serde(tag = "method")]
pub enum Notification<'a> {
    /// A slot notification.
    #[serde(rename = "slotNotification")]
    Slot {
        /// slot notification parameters
        params: NotificationParams<SlotInfo>,
    },
    /// An account notification.
    #[serde(rename = "accountNotification")]
    Account {
        /// account notification parameters
        params: NotificationParams<AccountInfo<'a>>,
    },
}

/// Represents a subscription result message.
#[derive(Deserialize)]
pub struct SubscriptionResult {
    /// ID of subscription request, not a subscription ID
    pub id: u64,
    /// resultant subscription ID
    pub result: u64,
}

/// Represents an unsubscription result message.
#[derive(Deserialize)]
pub struct UnsubscriptionResult {
    /// ID of unsubscription request
    pub id: u64,
}

/// Represents the payload of slot notification.
#[derive(Deserialize)]
pub struct SlotInfo {
    /// current slot
    pub slot: u64,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use json::from_str;
    use solana::pubkey::Pubkey;

    const ACCOUNT_NOTIFICATION: &str = r#"{
            "jsonrpc": "2.0",
            "method": "accountNotification",
            "params": {
                "result": {
                    "context": {
                        "slot": 5199307
                    },
                    "value": {
                        "data": [
                            "11116bv5nS2h3y12kD1yUKeMZvGcKLSjQgX6BeV7u1FrjeJcKfsHPXHRDEHrBesJhZyqnnq9qJeUuF7WHxiuLuL5twc38w2TXNLxnDbjmuR",
                            "base58"
                        ],
                        "executable": false,
                        "lamports": 33594,
                        "owner": "11111111111111111111111111111111",
                        "rentEpoch": 635,
                        "space": 80
                    }
                },
                "subscription": 23784
            }
        }"#;
    const SUBSCRIPTION_RESULT: &str = r#"{ "jsonrpc": "2.0", "result": 23784, "id": 1 }"#;
    const UNSUBSCRIPTION_RESULT: &str = r#"{ "jsonrpc": "2.0", "result": true, "id": 1 }"#;
    const SLOT_NOTIFICATION: &str = r#"{
        "jsonrpc": "2.0",
        "method": "slotNotification",
        "params": {
            "result": {
                "parent": 75,
                "root": 44,
                "slot": 76
            },
            "subscription": 0
        }
    }"#;

    #[test]
    fn test_deserialize_subscription_result() {
        let message: SubscriptionResult = from_str(SUBSCRIPTION_RESULT).unwrap();
        assert_eq!(message.id, 1);
        assert_eq!(message.result, 23784);
    }

    #[test]
    fn test_deserialize_unsubscription_result() {
        let message: UnsubscriptionResult = from_str(UNSUBSCRIPTION_RESULT).unwrap();
        assert_eq!(message.id, 1);
    }

    #[test]
    fn test_deserialize_account_notification() {
        let message: Notification = from_str(ACCOUNT_NOTIFICATION).unwrap();
        if let Notification::Account { params } = message {
            assert_eq!(params.subscription, 23784);
            let AccountInfo { value } = params.result;
            assert_eq!(value.lamports, 33594);
            assert_eq!(
                value.owner,
                Pubkey::from_str("11111111111111111111111111111111").unwrap()
            );
        } else {
            panic!("Invalid message type");
        }
    }

    #[test]
    fn test_deserialize_slot_notification() {
        let message: Notification = from_str(SLOT_NOTIFICATION).unwrap();
        if let Notification::Slot { params } = message {
            assert_eq!(params.subscription, 0);
            let SlotInfo { slot } = params.result;
            assert_eq!(slot, 76);
        } else {
            panic!("Invalid message type");
        }
    }

    #[test]
    fn test_deserialize_ws_message() {
        let mut msg = WebsocketMessage::deserialize(SUBSCRIPTION_RESULT.as_bytes());
        assert!(matches!(msg, Ok(WebsocketMessage::Subscribed(_))));
        msg = WebsocketMessage::deserialize(UNSUBSCRIPTION_RESULT.as_bytes());
        assert!(matches!(msg, Ok(WebsocketMessage::Unsubscribed(_))));
        msg = WebsocketMessage::deserialize(ACCOUNT_NOTIFICATION.as_bytes());
        assert!(matches!(
            msg,
            Ok(WebsocketMessage::Notification(Notification::Account { .. }))
        ));
        msg = WebsocketMessage::deserialize(SLOT_NOTIFICATION.as_bytes());
        assert!(matches!(
            msg,
            Ok(WebsocketMessage::Notification(Notification::Slot { .. }))
        ));
    }
}
