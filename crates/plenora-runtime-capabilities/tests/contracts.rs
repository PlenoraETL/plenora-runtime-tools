//! Capability identifier and portable wire-codec contracts.

use std::error::Error;

use plenora_runtime_capabilities::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, CapabilityId, CapabilityIdentifierErrorKind,
    CapabilityIdentifierField, CapabilityMessageCodec, CapabilityMessageCodecError,
    CapabilityRequest, ContractId, ContractIdentifierErrorKind, INPUT_CONTRACT_METADATA_KEY,
    MAX_CAPABILITY_NAME_BYTES, MAX_CONTRACT_ID_BYTES, MAX_OPERATION_NAME_BYTES,
    OPERATION_VERSION_METADATA_KEY, OperationName, OperationVersion,
};
use plenora_runtime_messaging::{MessageCodec, MessageMetadata, SerializedMessage};

#[test]
fn identifiers_are_namespaced_versioned_and_portable() -> Result<(), Box<dyn Error>> {
    let id = CapabilityId::new("plenora.data-tools", 1)?;
    assert_eq!(id.name(), "plenora.data-tools");
    assert_eq!(id.version(), 1);
    assert_eq!(id.to_string(), "plenora.data-tools@v1");
    assert_eq!(
        OperationName::new("dataset.export")?.as_str(),
        "dataset.export"
    );

    assert_eq!(
        CapabilityId::new("data-tools", 1)
            .err()
            .map(plenora_runtime_capabilities::CapabilityIdentifierError::kind),
        Some(CapabilityIdentifierErrorKind::MissingNamespace)
    );
    assert_eq!(
        CapabilityId::new("plenora.IO-tools", 1)
            .err()
            .map(plenora_runtime_capabilities::CapabilityIdentifierError::kind),
        Some(CapabilityIdentifierErrorKind::InvalidCharacter)
    );
    assert_eq!(
        CapabilityId::new("plenora.io-tools", 0)
            .err()
            .map(plenora_runtime_capabilities::CapabilityIdentifierError::kind),
        Some(CapabilityIdentifierErrorKind::ZeroVersion)
    );
    Ok(())
}

#[test]
fn message_codec_round_trips_routing_without_exposing_payload() -> Result<(), Box<dyn Error>> {
    let request = CapabilityRequest::new(
        CapabilityId::new("plenora.data-tools", 2)?,
        OperationName::new("dataset.convert")?,
        OperationVersion::new(3)?,
        ContractId::new("plenora-data-convert-input-v3")?,
        SerializedMessage::new("application/octet-stream", "sensitive-payload"),
    );
    let codec = CapabilityMessageCodec;
    let encoded = codec.encode(&request)?;
    assert_eq!(
        encoded.headers.get_text(CAPABILITY_NAME_METADATA_KEY)?,
        Some("plenora.data-tools")
    );
    assert_eq!(
        encoded.headers.get_text(CAPABILITY_VERSION_METADATA_KEY)?,
        Some("2")
    );
    assert_eq!(
        encoded
            .headers
            .get_text(CAPABILITY_OPERATION_METADATA_KEY)?,
        Some("dataset.convert")
    );
    assert_eq!(
        encoded.headers.get_text(OPERATION_VERSION_METADATA_KEY)?,
        Some("3")
    );
    assert_eq!(
        encoded.headers.get_text(INPUT_CONTRACT_METADATA_KEY)?,
        Some("plenora-data-convert-input-v3")
    );
    assert_eq!(codec.decode(&encoded)?, request);
    assert!(!format!("{request:?}").contains("sensitive-payload"));
    Ok(())
}

#[test]
fn message_codec_rejects_missing_invalid_and_non_utf8_routing() -> Result<(), Box<dyn Error>> {
    let codec = CapabilityMessageCodec;
    let empty = SerializedMessage::new("application/octet-stream", "payload");
    assert!(matches!(
        codec.decode(&empty),
        Err(CapabilityMessageCodecError::Missing(
            CAPABILITY_NAME_METADATA_KEY
        ))
    ));

    let mut metadata = MessageMetadata::new();
    let _previous = metadata.insert_text(CAPABILITY_NAME_METADATA_KEY, "plenora.data-tools")?;
    let _previous = metadata.insert_text(CAPABILITY_VERSION_METADATA_KEY, "not-a-version")?;
    let _previous = metadata.insert_text(CAPABILITY_OPERATION_METADATA_KEY, "convert")?;
    let invalid_version = empty.clone().with_headers(metadata);
    assert!(matches!(
        codec.decode(&invalid_version),
        Err(CapabilityMessageCodecError::InvalidVersion(
            CAPABILITY_VERSION_METADATA_KEY
        ))
    ));

    let mut metadata = MessageMetadata::new();
    let _previous = metadata.insert(CAPABILITY_NAME_METADATA_KEY, vec![0xff])?;
    let _previous = metadata.insert_text(CAPABILITY_VERSION_METADATA_KEY, "1")?;
    let _previous = metadata.insert_text(CAPABILITY_OPERATION_METADATA_KEY, "convert")?;
    let invalid_encoding = empty.with_headers(metadata);
    assert!(matches!(
        codec.decode(&invalid_encoding),
        Err(CapabilityMessageCodecError::InvalidEncoding(
            CAPABILITY_NAME_METADATA_KEY
        ))
    ));
    Ok(())
}

#[test]
fn every_identifier_rejection_is_field_specific_and_redaction_safe() -> Result<(), Box<dyn Error>> {
    let cases = [
        CapabilityId::new("", 1).err(),
        CapabilityId::new("x".repeat(MAX_CAPABILITY_NAME_BYTES + 1), 1).err(),
        CapabilityId::new("single", 1).err(),
        CapabilityId::new("plenora..tools", 1).err(),
        CapabilityId::new("plenora.-tools", 1).err(),
        CapabilityId::new("plenora.tools!", 1).err(),
    ];
    let expected = [
        CapabilityIdentifierErrorKind::Empty,
        CapabilityIdentifierErrorKind::TooLong,
        CapabilityIdentifierErrorKind::MissingNamespace,
        CapabilityIdentifierErrorKind::EmptySegment,
        CapabilityIdentifierErrorKind::InvalidSegmentBoundary,
        CapabilityIdentifierErrorKind::InvalidCharacter,
    ];
    for (error, expected_kind) in cases.into_iter().zip(expected) {
        let error = error.ok_or("invalid capability identifier unexpectedly accepted")?;
        assert_eq!(error.field(), CapabilityIdentifierField::Capability);
        assert_eq!(error.kind(), expected_kind);
        assert!(error.to_string().contains("Capability"));
    }

    for value in [
        String::new(),
        String::from("run"),
        String::from("-run"),
        String::from("run!"),
        "x".repeat(MAX_OPERATION_NAME_BYTES + 1),
    ] {
        let error = OperationName::new(value).err();
        assert!(error.is_some_and(|error| error.field() == CapabilityIdentifierField::Operation));
    }

    for (value, expected_kind) in [
        (String::new(), ContractIdentifierErrorKind::Empty),
        (
            "x".repeat(MAX_CONTRACT_ID_BYTES + 1),
            ContractIdentifierErrorKind::TooLong,
        ),
        (
            String::from("data-convert-input-v1"),
            ContractIdentifierErrorKind::InvalidPrefix,
        ),
        (
            String::from("plenora-data_CONVERT-v1"),
            ContractIdentifierErrorKind::InvalidCharacter,
        ),
        (
            String::from("plenora-data-convert"),
            ContractIdentifierErrorKind::MissingVersion,
        ),
        (
            String::from("plenora-data-convert-v0"),
            ContractIdentifierErrorKind::InvalidVersion,
        ),
    ] {
        let error = ContractId::new(value)
            .err()
            .ok_or("invalid contract identifier unexpectedly accepted")?;
        assert_eq!(error.kind(), expected_kind);
        assert_eq!(error.to_string(), "public contract identifier is invalid");
    }
    assert!(OperationVersion::new(0).is_err());
    Ok(())
}

#[test]
fn codec_requires_operation_version_and_input_contract() -> Result<(), Box<dyn Error>> {
    let codec = CapabilityMessageCodec;
    let base = SerializedMessage::new("application/json", "{}");
    let mut routing = MessageMetadata::new();
    routing.insert_text(CAPABILITY_NAME_METADATA_KEY, "plenora.data-tools")?;
    routing.insert_text(CAPABILITY_VERSION_METADATA_KEY, "1")?;
    routing.insert_text(CAPABILITY_OPERATION_METADATA_KEY, "data.run")?;

    assert!(matches!(
        codec.decode(&base.clone().with_headers(routing.clone())),
        Err(CapabilityMessageCodecError::Missing(
            OPERATION_VERSION_METADATA_KEY
        ))
    ));

    routing.insert_text(OPERATION_VERSION_METADATA_KEY, "0")?;
    assert!(matches!(
        codec.decode(&base.clone().with_headers(routing.clone())),
        Err(CapabilityMessageCodecError::InvalidVersion(
            OPERATION_VERSION_METADATA_KEY
        ))
    ));

    routing.insert_text(OPERATION_VERSION_METADATA_KEY, "1")?;
    assert!(matches!(
        codec.decode(&base.clone().with_headers(routing.clone())),
        Err(CapabilityMessageCodecError::Missing(
            INPUT_CONTRACT_METADATA_KEY
        ))
    ));

    routing.insert_text(INPUT_CONTRACT_METADATA_KEY, "not-versioned")?;
    let error = codec
        .decode(&base.with_headers(routing))
        .err()
        .ok_or("invalid input contract unexpectedly decoded")?;
    assert_eq!(error.field(), INPUT_CONTRACT_METADATA_KEY);
    assert!(error.source().is_some());
    Ok(())
}

#[test]
fn codec_reports_every_routing_field_and_preserves_metadata_sources() -> Result<(), Box<dyn Error>>
{
    let codec = CapabilityMessageCodec;
    let base = SerializedMessage::new("application/octet-stream", "payload");

    let mut name_only = MessageMetadata::new();
    name_only.insert_text(CAPABILITY_NAME_METADATA_KEY, "plenora.data-tools")?;
    let missing_version = codec.decode(&base.clone().with_headers(name_only));
    assert!(matches!(
        missing_version,
        Err(CapabilityMessageCodecError::Missing(
            CAPABILITY_VERSION_METADATA_KEY
        ))
    ));

    let mut missing_operation = MessageMetadata::new();
    missing_operation.insert_text(CAPABILITY_NAME_METADATA_KEY, "plenora.data-tools")?;
    missing_operation.insert_text(CAPABILITY_VERSION_METADATA_KEY, "1")?;
    assert!(matches!(
        codec.decode(&base.clone().with_headers(missing_operation)),
        Err(CapabilityMessageCodecError::Missing(
            CAPABILITY_OPERATION_METADATA_KEY
        ))
    ));

    for (name, version, operation, expected_field) in [
        ("invalid", "1", "run", CAPABILITY_NAME_METADATA_KEY),
        (
            "plenora.data-tools",
            "0",
            "run",
            CAPABILITY_VERSION_METADATA_KEY,
        ),
        (
            "plenora.data-tools",
            "1",
            "-run",
            CAPABILITY_OPERATION_METADATA_KEY,
        ),
    ] {
        let mut metadata = MessageMetadata::new();
        metadata.insert_text(CAPABILITY_NAME_METADATA_KEY, name)?;
        metadata.insert_text(CAPABILITY_VERSION_METADATA_KEY, version)?;
        metadata.insert_text(CAPABILITY_OPERATION_METADATA_KEY, operation)?;
        metadata.insert_text(OPERATION_VERSION_METADATA_KEY, "1")?;
        metadata.insert_text(INPUT_CONTRACT_METADATA_KEY, "plenora-test-input-v1")?;
        let error = codec
            .decode(&base.clone().with_headers(metadata))
            .err()
            .ok_or("invalid routing unexpectedly decoded")?;
        assert_eq!(error.field(), expected_field);
        assert!(error.source().is_some());
        assert!(format!("{error:?}").contains("redacted"));
        assert_eq!(
            error.to_string(),
            format!("capability routing metadata field '{expected_field}' is invalid")
        );
    }

    let request = CapabilityRequest::new(
        CapabilityId::new("plenora.data-tools", 1)?,
        OperationName::new("data.run")?,
        OperationVersion::new(1)?,
        ContractId::new("plenora-data-run-input-v1")?,
        base,
    );
    assert_eq!(request.capability().name(), "plenora.data-tools");
    assert_eq!(request.operation().as_str(), "data.run");
    assert_eq!(request.operation_version().get(), 1);
    assert_eq!(
        request.input_contract().as_str(),
        "plenora-data-run-input-v1"
    );
    assert_eq!(request.input().len(), "payload".len());
    assert_eq!(request.clone().into_input().len(), "payload".len());
    Ok(())
}
