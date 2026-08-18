//! Tests for broker capability and replay data structures.

use std::error::Error;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use plenora_runtime_messaging::{
    BrokerCapabilities, DeadLetter, ReplayRequest, ReplaySource, SerializedMessage,
};

#[test]
fn capabilities_make_unsupported_features_explicit() {
    let capabilities = BrokerCapabilities::default();

    assert!(!capabilities.durable_consumers);
    assert!(!capabilities.replay);
    assert!(!capabilities.ordered_delivery);
    assert!(!capabilities.dead_letter_native);
    assert!(!capabilities.exactly_once_claimed);
}

#[test]
fn replay_and_dead_letter_models_remain_transport_neutral() -> Result<(), Box<dyn Error>> {
    let timestamp = DateTime::parse_from_rfc3339("2026-08-15T10:30:00Z")?.with_timezone(&Utc);
    let request = ReplayRequest {
        source: ReplaySource::FromTimestamp(timestamp),
    };
    let dead_letter = DeadLetter {
        message: SerializedMessage::new("application/octet-stream", Bytes::from_static(b"body")),
        reason: "processing failed".into(),
        attempts: 4,
        failed_at: timestamp,
    };

    assert_eq!(request.source, ReplaySource::FromTimestamp(timestamp));
    assert_eq!(dead_letter.attempts, 4);
    assert_eq!(dead_letter.message.len(), 4);
    Ok(())
}
