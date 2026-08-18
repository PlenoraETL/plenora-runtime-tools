use std::sync::Arc;

use chrono::{DateTime, Utc};

use crate::SerializedMessage;

/// Portable dead-letter representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeadLetter {
    /// Serialized message that could not be processed.
    pub message: SerializedMessage,
    /// Redaction-safe operator-facing reason.
    pub reason: Arc<str>,
    /// Number of processing attempts observed.
    pub attempts: u32,
    /// Time at which processing was abandoned.
    pub failed_at: DateTime<Utc>,
}

/// Requested replay origin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplaySource {
    /// Start from a broker sequence number.
    FromSequence(u64),
    /// Start from a UTC timestamp.
    FromTimestamp(DateTime<Utc>),
    /// Replay all data retained by the broker.
    All,
}

/// Broker-neutral replay request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayRequest {
    /// Requested replay origin.
    pub source: ReplaySource,
}

/// Capabilities explicitly reported by a broker adapter.
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent broker features are intentionally reported without implied coupling"
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BrokerCapabilities {
    /// Durable consumer state is supported.
    pub durable_consumers: bool,
    /// At least one replay origin is supported.
    pub replay: bool,
    /// Ordered delivery can be requested.
    pub ordered_delivery: bool,
    /// The broker has a native dead-letter facility.
    pub dead_letter_native: bool,
    /// The broker claims an exactly-once mode.
    pub exactly_once_claimed: bool,
}
