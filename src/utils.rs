//! Various deserializion utilities

use std::{fmt, str::FromStr, time::Duration};

use json::Deserialize;
use serde::{
    de::{Error, Visitor},
    Deserializer,
};
use solana::pubkey::Pubkey;
use tracing_appender::rolling::Rotation;

/// Deserialize solana Pubkey from base58 encoded string
pub fn deserialize_pubkey_from_base58<'de, D>(deserializer: D) -> Result<Pubkey, D::Error>
where
    D: Deserializer<'de>,
{
    let string = <&str as Deserialize>::deserialize(deserializer)?;
    Pubkey::from_str(string).map_err(D::Error::custom)
}

/// Deserialize log rotation policy
pub fn deserialize_rotation<'de, D>(deserializer: D) -> Result<Rotation, D::Error>
where
    D: Deserializer<'de>,
{
    struct RotationVisitor;

    impl<'de> Visitor<'de> for RotationVisitor {
        type Value = Rotation;

        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a string representing a rotation kind")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            match value {
                "minutely" => Ok(Rotation::MINUTELY),
                "hourly" => Ok(Rotation::HOURLY),
                "daily" => Ok(Rotation::DAILY),
                "never" => Ok(Rotation::NEVER),
                _ => Err(E::custom(format!("Invalid rotation kind: {}", value))),
            }
        }
    }

    deserializer.deserialize_str(RotationVisitor)
}

/// Deserialize std::time::Duration from human readable string
pub fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let string = String::deserialize(deserializer)?;
    humantime::parse_duration(&string).map_err(D::Error::custom)
}
