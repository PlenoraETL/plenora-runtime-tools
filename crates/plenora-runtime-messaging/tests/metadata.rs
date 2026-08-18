//! Tests for typed identifiers and namespaced message metadata.

use std::{error::Error, str::FromStr};

use bytes::Bytes;
use plenora_runtime_messaging::{
    CausationId, MAX_METADATA_ENTRIES, MAX_METADATA_KEY_BYTES, MAX_METADATA_VALUE_BYTES, MessageId,
    MessageMetadata, MetadataKeyErrorKind,
};

#[test]
fn identifier_round_trip_preserves_uuid() -> Result<(), Box<dyn Error>> {
    let parsed = MessageId::from_str("67e55044-10b1-426f-9247-bb680e5fe0c8")?;
    let reparsed = MessageId::from_str(&parsed.to_string())?;
    let causation = CausationId::from(parsed);

    assert_eq!(parsed, reparsed);
    assert_eq!(causation.as_uuid(), parsed.as_uuid());
    Ok(())
}

#[test]
fn namespaced_metadata_accepts_application_and_plenora_keys() -> Result<(), Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();

    assert!(
        metadata
            .insert_text("myapp.key", "application-value")?
            .is_none()
    );
    assert!(
        metadata
            .insert("plenora.trace.traceparent", Bytes::from_static(b"trace"))?
            .is_none()
    );

    assert_eq!(metadata.get_text("myapp.key")?, Some("application-value"));
    assert_eq!(metadata.len(), 2);
    Ok(())
}

#[test]
fn metadata_rejects_non_namespaced_or_non_portable_keys() {
    let mut metadata = MessageMetadata::new();

    let missing_namespace = metadata.insert_text("traceparent", "value");
    let empty_segment = metadata.insert_text("plenora..trace", "value");
    let invalid_character = metadata.insert_text("plenora.trace value", "value");

    assert!(matches!(
        missing_namespace,
        Err(error) if error.kind() == MetadataKeyErrorKind::MissingNamespace
    ));
    assert!(matches!(
        empty_segment,
        Err(error) if error.kind() == MetadataKeyErrorKind::EmptySegment
    ));
    assert!(matches!(
        invalid_character,
        Err(error) if error.kind() == MetadataKeyErrorKind::InvalidCharacter
    ));
    assert!(metadata.is_empty());
}

#[test]
fn metadata_debug_output_redacts_values() -> Result<(), Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    assert!(
        metadata
            .insert_text("private.token", "do-not-log-this")?
            .is_none()
    );

    let debug = format!("{metadata:?}");

    assert!(debug.contains("private.token"));
    assert!(!debug.contains("do-not-log-this"));
    Ok(())
}

#[test]
fn metadata_rejects_oversized_keys_values_and_entry_counts() -> Result<(), Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    let oversized_key = format!("app.{}", "x".repeat(MAX_METADATA_KEY_BYTES));
    let key_error = metadata.insert_text(oversized_key, "value");
    assert!(matches!(
        key_error,
        Err(error) if error.kind() == MetadataKeyErrorKind::KeyTooLong
    ));

    let value_error = metadata.insert(
        "app.large",
        vec![0_u8; MAX_METADATA_VALUE_BYTES.saturating_add(1)],
    );
    assert!(matches!(
        value_error,
        Err(error) if error.kind() == MetadataKeyErrorKind::ValueTooLarge
    ));

    for index in 0..MAX_METADATA_ENTRIES {
        metadata.insert_text(format!("app.key{index}"), "value")?;
    }
    let capacity_error = metadata.insert_text("app.extra", "value");
    assert!(matches!(
        capacity_error,
        Err(error) if error.kind() == MetadataKeyErrorKind::EntryCapacityExceeded
    ));
    assert_eq!(metadata.len(), MAX_METADATA_ENTRIES);
    Ok(())
}

#[test]
fn metadata_total_byte_bound_is_atomic() -> Result<(), Box<dyn Error>> {
    let mut metadata = MessageMetadata::new();
    let value = vec![1_u8; MAX_METADATA_VALUE_BYTES];
    metadata.insert("app.one", value.clone())?;
    metadata.insert("app.two", value.clone())?;
    metadata.insert("app.three", value.clone())?;

    let error = metadata.insert("app.four", value);

    assert!(matches!(
        error,
        Err(error) if error.kind() == MetadataKeyErrorKind::TotalBytesExceeded
    ));
    assert!(!metadata.contains_key("app.four"));
    assert_eq!(metadata.len(), 3);
    Ok(())
}
