//! Black-box checks against the pinned Runtime Binding 1.0 conformance matrix.

use std::{collections::BTreeMap, error::Error, io};

use chrono::{DateTime, Utc};
use plenora_runtime_capabilities::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, CapabilityMessageCodec, CapabilityResponse, ContractId,
    INPUT_CONTRACT_METADATA_KEY, OPERATION_VERSION_METADATA_KEY, OUTPUT_CONTRACT_METADATA_KEY,
    PlenoraError, PlenoraErrorCategory, PlenoraErrorPhase, PlenoraErrorRemoteEffect,
    PlenoraErrorRetry, TRACE_CORRELATION_ID_METADATA_KEY,
};
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY, CORRELATION_ID_METADATA_KEY, CausationId, CorrelationId,
    MESSAGE_ID_METADATA_KEY, MessageCodec, MessageEnvelope, MessageId, MessageMetadata,
    SerializedMessage,
};
use serde::Deserialize;
use serde_json::{Map, Value};

const CONTRACTS_REVISION: &str = "90cffb2b78cace16edad352a19d86f674930c133";
const SOURCE: &str = include_str!("../../../contracts/source.json");
const REST_CAPABILITIES_VECTOR: &str =
    include_str!("../../../contracts/capabilities-v2/rest-tools-v1.json");

type VectorEntry = (&'static str, &'static str);

const REQUEST_VECTORS: &[VectorEntry] = &[
    (
        "runtime-v1/data-validate-request.json",
        include_str!("../../../contracts/runtime-v1/data-validate-request.json"),
    ),
    (
        "runtime-v1/database-read-request.json",
        include_str!("../../../contracts/runtime-v1/database-read-request.json"),
    ),
    (
        "runtime-v1/io-read-request.json",
        include_str!("../../../contracts/runtime-v1/io-read-request.json"),
    ),
    (
        "runtime-v1/rest-upload-request.json",
        include_str!("../../../contracts/runtime-v1/rest-upload-request.json"),
    ),
];

const SUCCESS_VECTORS: &[VectorEntry] = &[
    (
        "runtime-v1/data-run-success.json",
        include_str!("../../../contracts/runtime-v1/data-run-success.json"),
    ),
    (
        "runtime-v1/database-transaction-commit-success.json",
        include_str!("../../../contracts/runtime-v1/database-transaction-commit-success.json"),
    ),
    (
        "runtime-v1/io-read-success.json",
        include_str!("../../../contracts/runtime-v1/io-read-success.json"),
    ),
    (
        "runtime-v1/rest-download-success.json",
        include_str!("../../../contracts/runtime-v1/rest-download-success.json"),
    ),
];

const ERROR_VECTORS: &[VectorEntry] = &[
    (
        "runtime-v1/data-run-timeout-error.json",
        include_str!("../../../contracts/runtime-v1/data-run-timeout-error.json"),
    ),
    (
        "runtime-v1/database-write-error.json",
        include_str!("../../../contracts/runtime-v1/database-write-error.json"),
    ),
    (
        "runtime-v1/io-read-cancelled-error.json",
        include_str!("../../../contracts/runtime-v1/io-read-cancelled-error.json"),
    ),
    (
        "runtime-v1/rest-upload-unknown-error.json",
        include_str!("../../../contracts/runtime-v1/rest-upload-unknown-error.json"),
    ),
];

const EXPECTED_VECTOR_PATHS: &[&str] = &[
    "capabilities-v2/rest-tools-v1.json",
    "runtime-v1/data-run-success.json",
    "runtime-v1/data-run-timeout-error.json",
    "runtime-v1/data-validate-request.json",
    "runtime-v1/database-read-request.json",
    "runtime-v1/database-transaction-commit-success.json",
    "runtime-v1/database-write-error.json",
    "runtime-v1/io-read-cancelled-error.json",
    "runtime-v1/io-read-request.json",
    "runtime-v1/io-read-success.json",
    "runtime-v1/rest-download-success.json",
    "runtime-v1/rest-upload-request.json",
    "runtime-v1/rest-upload-unknown-error.json",
];

#[derive(Debug, Deserialize)]
struct SourcePin {
    repository: String,
    revision: String,
    contracts: Vec<String>,
    vectors: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeVector {
    schema_version: u8,
    contract: String,
    kind: String,
    content_type: String,
    metadata: BTreeMap<String, String>,
    payload: Value,
}

#[derive(Debug, Deserialize)]
struct ErrorFixture {
    category: String,
    phase: String,
    remote_effect: String,
    retry: RetryFixture,
    code: Option<String>,
    provider: Option<String>,
    execution_id: Option<String>,
    message: String,
    #[serde(default)]
    details: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct RetryFixture {
    kind: String,
    delay_ms: Option<u64>,
}

#[test]
fn source_pin_is_immutable_and_inventory_is_complete() -> Result<(), Box<dyn Error>> {
    let pin: SourcePin = serde_json::from_str(SOURCE)?;
    assert_eq!(
        pin.repository,
        "https://github.com/PlenoraETL/plenora-contracts.git"
    );
    assert_eq!(pin.revision, CONTRACTS_REVISION);
    assert_eq!(
        pin.contracts,
        [
            "plenora-runtime-binding-v1",
            "plenora-runtime-vector-v1",
            "plenora-rest-execution-request-v1",
            "plenora-rest-execution-result-v1",
            "plenora-rest-file-transfer-input-v1",
            "plenora-rest-file-transfer-result-v1",
            "plenora-rest-capability-attributes-v1",
        ]
    );
    assert_eq!(pin.vectors, EXPECTED_VECTOR_PATHS);

    for (path, document) in all_runtime_vectors() {
        let vector: RuntimeVector = serde_json::from_str(document)?;
        assert_eq!(vector.schema_version, 1, "{path}");
        assert_eq!(vector.contract, "plenora-runtime-vector-v1", "{path}");
    }
    assert_eq!(
        serde_json::from_str::<Value>(REST_CAPABILITIES_VECTOR)?["schema_version"],
        2
    );
    Ok(())
}

#[test]
fn every_request_vector_builds_a_canonical_envelope_and_round_trips() -> Result<(), Box<dyn Error>>
{
    let codec = CapabilityMessageCodec;
    for (path, document) in REQUEST_VECTORS {
        let vector: RuntimeVector = serde_json::from_str(document)?;
        assert_eq!(vector.kind, "request", "{path}");
        let (message_id, causation_id, wire) = vector_message(&vector)?;
        let correlation_text = metadata_value(&vector, CORRELATION_ID_METADATA_KEY)?;
        let correlation_id: CorrelationId = correlation_text.parse()?;
        let occurred_at: DateTime<Utc> = "2030-01-01T00:00:00Z".parse()?;
        let mut envelope = MessageEnvelope::new(
            "plenora.capability.request",
            "1",
            occurred_at,
            correlation_id,
            wire,
        );
        envelope.message_id = message_id;
        if let Some(causation_id) = causation_id {
            envelope = envelope.with_causation(causation_id);
        }

        let request = codec.decode(&envelope.payload)?;
        assert_eq!(
            request.capability().name(),
            metadata_value(&vector, CAPABILITY_NAME_METADATA_KEY)?,
            "{path}"
        );
        assert_eq!(
            request.capability().version().to_string(),
            metadata_value(&vector, CAPABILITY_VERSION_METADATA_KEY)?,
            "{path}"
        );
        assert_eq!(
            request.operation().as_str(),
            metadata_value(&vector, CAPABILITY_OPERATION_METADATA_KEY)?,
            "{path}"
        );
        assert_eq!(
            request.operation_version().to_string(),
            metadata_value(&vector, OPERATION_VERSION_METADATA_KEY)?,
            "{path}"
        );
        assert_eq!(
            request.input_contract().as_str(),
            metadata_value(&vector, INPUT_CONTRACT_METADATA_KEY)?,
            "{path}"
        );
        assert_eq!(request.input().content_type.as_ref(), vector.content_type);
        assert_eq!(
            request.input().bytes.as_ref(),
            serde_json::to_vec(&vector.payload)?
        );
        assert_eq!(
            request
                .input()
                .headers
                .get_text(TRACE_CORRELATION_ID_METADATA_KEY)?,
            Some(correlation_text)
        );

        let encoded = codec.encode(&request)?;
        assert_request_metadata(&encoded, &vector)?;
        assert_eq!(envelope.message_id, message_id);
        assert_eq!(envelope.correlation_id, correlation_id);
        assert_eq!(envelope.causation_id, causation_id);
    }
    Ok(())
}

#[test]
fn every_request_vector_fails_closed_for_invalid_routing() -> Result<(), Box<dyn Error>> {
    let codec = CapabilityMessageCodec;
    for (path, document) in REQUEST_VECTORS {
        let vector: RuntimeVector = serde_json::from_str(document)?;
        let (_, _, wire) = vector_message(&vector)?;
        for key in [
            CAPABILITY_NAME_METADATA_KEY,
            CAPABILITY_VERSION_METADATA_KEY,
            CAPABILITY_OPERATION_METADATA_KEY,
            OPERATION_VERSION_METADATA_KEY,
            INPUT_CONTRACT_METADATA_KEY,
        ] {
            let mut missing = wire.clone();
            let _removed = missing.headers.remove(key);
            let error = codec
                .decode(&missing)
                .err()
                .ok_or("missing routing metadata was accepted")?;
            assert_eq!(error.field(), key, "{path}");
        }

        for (key, value) in [
            (CAPABILITY_NAME_METADATA_KEY, "invalid"),
            (CAPABILITY_VERSION_METADATA_KEY, "0"),
            (CAPABILITY_OPERATION_METADATA_KEY, "invalid"),
            (OPERATION_VERSION_METADATA_KEY, "0"),
            (INPUT_CONTRACT_METADATA_KEY, "invalid"),
        ] {
            let mut invalid = wire.clone();
            invalid.headers.insert_text(key, value)?;
            let error = codec
                .decode(&invalid)
                .err()
                .ok_or("invalid routing metadata was accepted")?;
            assert_eq!(error.field(), key, "{path}");
        }
    }
    Ok(())
}

#[test]
fn success_vectors_preserve_contract_content_and_correlation() -> Result<(), Box<dyn Error>> {
    for (path, document) in SUCCESS_VECTORS {
        let vector: RuntimeVector = serde_json::from_str(document)?;
        assert_eq!(vector.kind, "success", "{path}");
        let (_, causation_id, message) = vector_message(&vector)?;
        assert!(causation_id.is_none(), "{path}");
        let output_contract =
            ContractId::new(metadata_value(&vector, OUTPUT_CONTRACT_METADATA_KEY)?)?;
        let response = CapabilityResponse::new(output_contract.clone(), message.clone());
        assert_eq!(response.output_contract(), Some(&output_contract), "{path}");
        assert_eq!(response.output(), Some(&message), "{path}");
        assert_eq!(message.content_type.as_ref(), vector.content_type, "{path}");
        assert_eq!(message.bytes.as_ref(), serde_json::to_vec(&vector.payload)?);
        assert_result_metadata(&message, &vector)?;
    }
    Ok(())
}

#[test]
fn error_vectors_round_trip_through_the_bounded_public_model() -> Result<(), Box<dyn Error>> {
    for (path, document) in ERROR_VECTORS {
        let vector: RuntimeVector = serde_json::from_str(document)?;
        assert_eq!(vector.kind, "error", "{path}");
        assert_eq!(
            vector.content_type, "application/vnd.plenora.error+json",
            "{path}"
        );
        assert_eq!(
            metadata_value(&vector, OUTPUT_CONTRACT_METADATA_KEY)?,
            "plenora-error-v1",
            "{path}"
        );
        let (_, _, message) = vector_message(&vector)?;
        assert_result_metadata(&message, &vector)?;

        let public_error = public_error_from_fixture(&vector.payload)?;
        let encoded: Value = serde_json::from_slice(&public_error.to_json()?)?;
        assert_eq!(encoded, vector.payload, "{path}");
        let public_message = public_error.to_message()?;
        assert_eq!(public_message.content_type.as_ref(), vector.content_type);
        assert_eq!(
            serde_json::from_slice::<Value>(&public_message.bytes)?,
            vector.payload,
            "{path}"
        );
    }
    Ok(())
}

fn all_runtime_vectors() -> impl Iterator<Item = VectorEntry> {
    REQUEST_VECTORS
        .iter()
        .chain(SUCCESS_VECTORS)
        .chain(ERROR_VECTORS)
        .copied()
}

fn vector_message(
    vector: &RuntimeVector,
) -> Result<(MessageId, Option<CausationId>, SerializedMessage), Box<dyn Error>> {
    let mut metadata = vector.metadata.clone();
    let message_id = metadata
        .remove(MESSAGE_ID_METADATA_KEY)
        .ok_or("runtime vector lacks message identity")?
        .parse()?;
    let causation_id = metadata
        .remove(CAUSATION_ID_METADATA_KEY)
        .map(|value| value.parse())
        .transpose()?;
    let mut headers = MessageMetadata::new();
    for (key, value) in metadata {
        headers.insert_text(key, value)?;
    }
    let message = SerializedMessage::new(
        vector.content_type.clone(),
        serde_json::to_vec(&vector.payload)?,
    )
    .with_headers(headers);
    Ok((message_id, causation_id, message))
}

fn metadata_value<'a>(vector: &'a RuntimeVector, key: &str) -> Result<&'a str, Box<dyn Error>> {
    vector
        .metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| io::Error::other(format!("runtime vector lacks {key}")).into())
}

fn assert_request_metadata(
    message: &SerializedMessage,
    vector: &RuntimeVector,
) -> Result<(), Box<dyn Error>> {
    for (key, expected) in &vector.metadata {
        if matches!(
            key.as_str(),
            MESSAGE_ID_METADATA_KEY | CAUSATION_ID_METADATA_KEY
        ) {
            assert!(!message.headers.contains_key(key), "{key}");
        } else {
            assert_eq!(
                message.headers.get_text(key)?,
                Some(expected.as_str()),
                "{key}"
            );
        }
    }
    assert!(
        !message
            .headers
            .contains_key("plenora.message.correlation_id")
    );
    Ok(())
}

fn assert_result_metadata(
    message: &SerializedMessage,
    vector: &RuntimeVector,
) -> Result<(), Box<dyn Error>> {
    let correlation_text = metadata_value(vector, TRACE_CORRELATION_ID_METADATA_KEY)?;
    let correlation_id: CorrelationId = correlation_text.parse()?;
    assert_eq!(correlation_id.to_string(), correlation_text);
    assert_eq!(
        message
            .headers
            .get_text(CAPABILITY_OPERATION_METADATA_KEY)?,
        Some(metadata_value(vector, CAPABILITY_OPERATION_METADATA_KEY)?)
    );
    assert_eq!(
        message.headers.get_text(OPERATION_VERSION_METADATA_KEY)?,
        Some(metadata_value(vector, OPERATION_VERSION_METADATA_KEY)?)
    );
    assert_eq!(
        message.headers.get_text(OUTPUT_CONTRACT_METADATA_KEY)?,
        Some(metadata_value(vector, OUTPUT_CONTRACT_METADATA_KEY)?)
    );
    assert_eq!(
        message
            .headers
            .get_text(TRACE_CORRELATION_ID_METADATA_KEY)?,
        Some(correlation_text)
    );
    assert!(!message.headers.contains_key(MESSAGE_ID_METADATA_KEY));
    Ok(())
}

fn public_error_from_fixture(payload: &Value) -> Result<PlenoraError, Box<dyn Error>> {
    let fixture: ErrorFixture = serde_json::from_value(payload.clone())?;
    let mut error = PlenoraError::new(
        error_category(&fixture.category)?,
        error_phase(&fixture.phase)?,
        remote_effect(&fixture.remote_effect)?,
        retry_disposition(&fixture.retry)?,
        fixture.message,
    )?;
    if let Some(code) = fixture.code {
        error = error.with_code(code)?;
    }
    if let Some(provider) = fixture.provider {
        error = error.with_provider(provider)?;
    }
    if let Some(execution_id) = fixture.execution_id {
        error = error.with_execution_id(execution_id)?;
    }
    if !fixture.details.is_empty() {
        error = error.with_details(fixture.details)?;
    }
    Ok(error)
}

fn error_category(value: &str) -> Result<PlenoraErrorCategory, Box<dyn Error>> {
    Ok(match value {
        "invalid_plan" => PlenoraErrorCategory::InvalidPlan,
        "invalid_configuration" => PlenoraErrorCategory::InvalidConfiguration,
        "schema" => PlenoraErrorCategory::Schema,
        "data_mapping" => PlenoraErrorCategory::DataMapping,
        "crs" => PlenoraErrorCategory::Crs,
        "unsupported" => PlenoraErrorCategory::Unsupported,
        "not_found" => PlenoraErrorCategory::NotFound,
        "conflict" => PlenoraErrorCategory::Conflict,
        "concurrent_modification" => PlenoraErrorCategory::ConcurrentModification,
        "authentication" => PlenoraErrorCategory::Authentication,
        "authorization" => PlenoraErrorCategory::Authorization,
        "timeout" => PlenoraErrorCategory::Timeout,
        "cancelled" => PlenoraErrorCategory::Cancelled,
        "resource_limit" => PlenoraErrorCategory::ResourceLimit,
        "io" => PlenoraErrorCategory::Io,
        "protocol" => PlenoraErrorCategory::Protocol,
        "transient" => PlenoraErrorCategory::Transient,
        "execution" => PlenoraErrorCategory::Execution,
        "internal" => PlenoraErrorCategory::Internal,
        _ => return Err(io::Error::other("unknown public error category").into()),
    })
}

fn error_phase(value: &str) -> Result<PlenoraErrorPhase, Box<dyn Error>> {
    Ok(match value {
        "validate" => PlenoraErrorPhase::Validate,
        "connect" => PlenoraErrorPhase::Connect,
        "probe" => PlenoraErrorPhase::Probe,
        "prepare" => PlenoraErrorPhase::Prepare,
        "read" => PlenoraErrorPhase::Read,
        "write" => PlenoraErrorPhase::Write,
        "finalize" => PlenoraErrorPhase::Finalize,
        "commit" => PlenoraErrorPhase::Commit,
        "rollback" => PlenoraErrorPhase::Rollback,
        "cleanup" => PlenoraErrorPhase::Cleanup,
        _ => return Err(io::Error::other("unknown public error phase").into()),
    })
}

fn remote_effect(value: &str) -> Result<PlenoraErrorRemoteEffect, Box<dyn Error>> {
    Ok(match value {
        "none" => PlenoraErrorRemoteEffect::None,
        "rolled_back" => PlenoraErrorRemoteEffect::RolledBack,
        "partial" => PlenoraErrorRemoteEffect::Partial,
        "committed" => PlenoraErrorRemoteEffect::Committed,
        "unknown" => PlenoraErrorRemoteEffect::Unknown,
        _ => return Err(io::Error::other("unknown public remote effect").into()),
    })
}

fn retry_disposition(fixture: &RetryFixture) -> Result<PlenoraErrorRetry, Box<dyn Error>> {
    Ok(match fixture.kind.as_str() {
        "never" => PlenoraErrorRetry::Never,
        "quarantine" => PlenoraErrorRetry::Quarantine,
        "safe" => PlenoraErrorRetry::Safe,
        "requires_idempotency_key" => PlenoraErrorRetry::RequiresIdempotencyKey,
        "requires_recovery" => PlenoraErrorRetry::RequiresRecovery,
        "after" => PlenoraErrorRetry::After {
            delay_ms: fixture
                .delay_ms
                .ok_or("after retry fixture lacks delay_ms")?,
        },
        _ => return Err(io::Error::other("unknown public retry disposition").into()),
    })
}
