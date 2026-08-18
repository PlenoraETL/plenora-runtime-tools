//! Tests for serialization-neutral messages and envelopes.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use bytes::Bytes;
use chrono::{DateTime, Utc};
use plenora_runtime_messaging::{
    CausationId, CorrelationId, MessageCodec, MessageEnvelope, MessageMetadata, SerializedMessage,
};

#[derive(Debug)]
enum TextCodecError {
    UnexpectedContentType,
    InvalidUtf8(std::string::FromUtf8Error),
}

impl Display for TextCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedContentType => formatter.write_str("unexpected content type"),
            Self::InvalidUtf8(_) => formatter.write_str("payload is not valid UTF-8"),
        }
    }
}

impl Error for TextCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnexpectedContentType => None,
            Self::InvalidUtf8(source) => Some(source),
        }
    }
}

struct PlainTextCodec;

impl MessageCodec<String> for PlainTextCodec {
    type Error = TextCodecError;

    fn encode(&self, value: &String) -> Result<SerializedMessage, Self::Error> {
        Ok(SerializedMessage::new(
            "text/plain; charset=utf-8",
            Bytes::copy_from_slice(value.as_bytes()),
        ))
    }

    fn decode(&self, message: &SerializedMessage) -> Result<String, Self::Error> {
        if message.content_type.as_ref() != "text/plain; charset=utf-8" {
            return Err(TextCodecError::UnexpectedContentType);
        }

        String::from_utf8(message.bytes.to_vec()).map_err(TextCodecError::InvalidUtf8)
    }
}

fn occurred_at() -> Result<DateTime<Utc>, chrono::ParseError> {
    DateTime::parse_from_rfc3339("2026-08-15T10:30:00Z")
        .map(|timestamp| timestamp.with_timezone(&Utc))
}

#[test]
fn codec_round_trip_preserves_bytes_without_prescribing_a_format() -> Result<(), Box<dyn Error>> {
    let codec = PlainTextCodec;
    let encoded = codec.encode(&"hello".to_owned())?;

    assert_eq!(encoded.content_type.as_ref(), "text/plain; charset=utf-8");
    assert_eq!(encoded.bytes, Bytes::from_static(b"hello"));
    assert_eq!(codec.decode(&encoded)?, "hello");
    Ok(())
}

#[test]
fn mapping_payload_preserves_envelope_context() -> Result<(), Box<dyn Error>> {
    let correlation_id = CorrelationId::random();
    let causation_id = CausationId::random();
    let mut metadata = MessageMetadata::new();
    assert!(metadata.insert_text("myapp.tenant", "tenant-17")?.is_none());

    let original = MessageEnvelope::new(
        "orders.created",
        "1",
        occurred_at()?,
        correlation_id,
        "42".to_owned(),
    )
    .with_causation(causation_id)
    .with_metadata(metadata);
    let message_id = original.message_id;

    let mapped = original.map_payload(|payload| payload.parse::<u64>());

    assert_eq!(mapped.message_id, message_id);
    assert_eq!(mapped.correlation_id, correlation_id);
    assert_eq!(mapped.causation_id, Some(causation_id));
    assert_eq!(mapped.message_type.as_ref(), "orders.created");
    assert_eq!(mapped.schema_version.as_ref(), "1");
    assert_eq!(mapped.metadata.get_text("myapp.tenant")?, Some("tenant-17"));
    assert_eq!(mapped.payload?, 42);
    Ok(())
}

#[test]
fn message_debug_output_redacts_payload_bytes_and_typed_payload() -> Result<(), Box<dyn Error>> {
    let serialized = SerializedMessage::new(
        "application/private",
        Bytes::from_static(b"secret-wire-value"),
    );
    let envelope = MessageEnvelope::new(
        "private.event",
        "1",
        occurred_at()?,
        CorrelationId::random(),
        "secret-domain-value",
    );

    let serialized_debug = format!("{serialized:?}");
    let envelope_debug = format!("{envelope:?}");

    assert!(serialized_debug.contains("byte_len"));
    assert!(!serialized_debug.contains("secret-wire-value"));
    assert!(envelope_debug.contains("<redacted>"));
    assert!(!envelope_debug.contains("secret-domain-value"));
    Ok(())
}
