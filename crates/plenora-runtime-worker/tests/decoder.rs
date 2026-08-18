//! Canonical broker-message decoder tests.

use std::{error::Error, fmt, fmt::Display};

use bytes::Bytes;
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY, CORRELATION_ID_METADATA_KEY, CausationId, MESSAGE_ID_METADATA_KEY,
    MessageCodec, MessageId, MessageMetadata, SerializedMessage,
};
use plenora_runtime_worker::{
    MetadataMessageDecodeError, MetadataMessageDecoder, WorkerMessageDecoder,
};

#[derive(Clone, Copy, Debug)]
struct Utf8Codec;

#[derive(Clone, Copy, Debug)]
struct DecodeError;

impl Display for DecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sensitive codec detail")
    }
}

impl Error for DecodeError {}

impl MessageCodec<String> for Utf8Codec {
    type Error = DecodeError;

    fn encode(&self, value: &String) -> Result<SerializedMessage, Self::Error> {
        Ok(SerializedMessage::new("text/plain", value.clone()))
    }

    fn decode(&self, message: &SerializedMessage) -> Result<String, Self::Error> {
        String::from_utf8(message.bytes.to_vec()).map_err(|_error| DecodeError)
    }
}

#[test]
fn canonical_decoder_preserves_identity_and_redacts_payload() -> Result<(), Box<dyn Error>> {
    let message_id = MessageId::random();
    let correlation_id = plenora_runtime_messaging::CorrelationId::random();
    let causation_id = CausationId::random();
    let message = message(
        Bytes::from_static(b"secret-payload"),
        Some(message_id),
        Some(correlation_id),
        Some(causation_id),
    )?;

    let decoded = MetadataMessageDecoder::<_, String>::new(Utf8Codec).decode(&message)?;
    let debug = format!("{decoded:?}");

    assert_eq!(decoded.identity.message_id, message_id);
    assert_eq!(decoded.identity.correlation_id, correlation_id);
    assert_eq!(decoded.identity.causation_id, Some(causation_id));
    assert_eq!(decoded.message, "secret-payload");
    assert!(!debug.contains("secret-payload"));

    Ok(())
}

#[test]
fn canonical_decoder_requires_stable_message_and_correlation_ids() -> Result<(), Box<dyn Error>> {
    let correlation_id = plenora_runtime_messaging::CorrelationId::random();
    let missing_message = message(
        Bytes::from_static(b"payload"),
        None,
        Some(correlation_id),
        None,
    )?;

    let result = MetadataMessageDecoder::<_, String>::new(Utf8Codec).decode(&missing_message);
    let Err(error) = result else {
        return Err(Box::new(DecodeError) as Box<dyn Error>);
    };

    assert!(matches!(
        error,
        MetadataMessageDecodeError::MissingIdentity(MESSAGE_ID_METADATA_KEY)
    ));

    Ok(())
}

#[test]
fn codec_failure_preserves_source_but_redacts_debug_and_display() -> Result<(), Box<dyn Error>> {
    let message = message(
        Bytes::from_static(&[0xff]),
        Some(MessageId::random()),
        Some(plenora_runtime_messaging::CorrelationId::random()),
        None,
    )?;

    let result = MetadataMessageDecoder::<_, String>::new(Utf8Codec).decode(&message);
    let Err(error) = result else {
        return Err(Box::new(DecodeError) as Box<dyn Error>);
    };
    let debug = format!("{error:?}");
    let display = error.to_string();

    assert!(error.source().is_some());
    assert!(!debug.contains("sensitive codec detail"));
    assert!(!display.contains("sensitive codec detail"));

    Ok(())
}

fn message(
    payload: Bytes,
    message_id: Option<MessageId>,
    correlation_id: Option<plenora_runtime_messaging::CorrelationId>,
    causation_id: Option<CausationId>,
) -> Result<SerializedMessage, Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    if let Some(message_id) = message_id {
        let _previous = metadata.insert_text(MESSAGE_ID_METADATA_KEY, message_id.to_string())?;
    }
    if let Some(correlation_id) = correlation_id {
        let _previous =
            metadata.insert_text(CORRELATION_ID_METADATA_KEY, correlation_id.to_string())?;
    }
    if let Some(causation_id) = causation_id {
        let _previous =
            metadata.insert_text(CAUSATION_ID_METADATA_KEY, causation_id.to_string())?;
    }
    Ok(SerializedMessage::new("text/plain", payload).with_headers(metadata))
}
