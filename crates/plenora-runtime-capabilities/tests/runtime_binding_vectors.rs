//! Black-box checks against the pinned Runtime Binding 1.0 request vector.

use std::{collections::BTreeMap, error::Error};

use chrono::{DateTime, Utc};
use plenora_runtime_capabilities::{
    CapabilityMessageCodec, INPUT_CONTRACT_METADATA_KEY, OPERATION_VERSION_METADATA_KEY,
};
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY, CORRELATION_ID_METADATA_KEY, CausationId, CorrelationId,
    MESSAGE_ID_METADATA_KEY, MessageCodec, MessageEnvelope, MessageId, MessageMetadata,
    SerializedMessage,
};
use serde::Deserialize;
use serde_json::Value;

const CONTRACTS_REVISION: &str = "e0484e54c96c5441ea09f44a2419bcabbe7f7242";
const SOURCE: &str = include_str!("../../../contracts/source.json");
const REQUEST_VECTOR: &str =
    include_str!("../../../contracts/runtime-v1/database-read-request.json");
const SUCCESS_VECTOR: &str = include_str!("../../../contracts/runtime-v1/data-run-success.json");
const ERROR_VECTOR: &str = include_str!("../../../contracts/runtime-v1/database-write-error.json");
const REST_CAPABILITIES_VECTOR: &str =
    include_str!("../../../contracts/capabilities-v2/rest-tools-v1.json");

#[derive(Debug, Deserialize)]
struct SourcePin {
    repository: String,
    revision: String,
    contracts: Vec<String>,
    vectors: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeVector {
    contract: String,
    kind: String,
    content_type: String,
    metadata: BTreeMap<String, String>,
    payload: Value,
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
    assert_eq!(
        pin.vectors,
        [
            "capabilities-v2/rest-tools-v1.json",
            "runtime-v1/database-read-request.json",
            "runtime-v1/data-run-success.json",
            "runtime-v1/database-write-error.json",
        ]
    );
    for document in [REQUEST_VECTOR, SUCCESS_VECTOR, ERROR_VECTOR] {
        let vector: RuntimeVector = serde_json::from_str(document)?;
        assert_eq!(vector.contract, "plenora-runtime-vector-v1");
    }
    assert_eq!(
        serde_json::from_str::<Value>(REST_CAPABILITIES_VECTOR)?["schema_version"],
        2
    );
    Ok(())
}

#[test]
fn request_vector_builds_a_canonical_envelope_and_round_trips_payload() -> Result<(), Box<dyn Error>>
{
    let mut vector: RuntimeVector = serde_json::from_str(REQUEST_VECTOR)?;
    assert_eq!(vector.kind, "request");

    let message_id_text = vector
        .metadata
        .remove(MESSAGE_ID_METADATA_KEY)
        .ok_or("request vector lacks message identity")?;
    let message_id: MessageId = message_id_text.parse()?;
    assert_eq!(message_id.to_string(), message_id_text);
    let correlation_id_text = vector
        .metadata
        .get(CORRELATION_ID_METADATA_KEY)
        .ok_or("request vector lacks correlation identity")?
        .clone();
    let correlation_id: CorrelationId = correlation_id_text.parse()?;
    assert_eq!(correlation_id.to_string(), correlation_id_text);
    let causation_id = vector
        .metadata
        .remove(CAUSATION_ID_METADATA_KEY)
        .map(|value| value.parse::<CausationId>())
        .transpose()?;

    let mut headers = MessageMetadata::new();
    for (key, value) in &vector.metadata {
        headers.insert_text(key.as_str(), value.as_str())?;
    }
    let payload = serde_json::to_vec(&vector.payload)?;
    let wire = SerializedMessage::new(vector.content_type, payload.clone()).with_headers(headers);
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
    assert_eq!(envelope.message_id.to_string(), message_id_text);
    assert_eq!(envelope.correlation_id.to_string(), correlation_id_text);
    assert_eq!(envelope.causation_id, causation_id);

    let codec = CapabilityMessageCodec;
    let request = codec.decode(&envelope.payload)?;

    assert_eq!(request.capability().name(), "plenora.database-tools");
    assert_eq!(request.capability().version(), 1);
    assert_eq!(request.operation().as_str(), "database.read");
    assert_eq!(request.operation_version().get(), 1);
    assert_eq!(
        request.input_contract().as_str(),
        "plenora-database-read-input-v1"
    );
    assert_eq!(request.input().content_type.as_ref(), "application/json");
    assert_eq!(request.input().bytes.as_ref(), payload);
    assert_eq!(
        request
            .input()
            .headers
            .get_text("plenora.trace.correlation_id")?,
        Some("018f3d84-7b2c-7f00-8000-000000000001")
    );
    assert_eq!(
        request
            .input()
            .headers
            .get_text("plenora.execution.deadline")?,
        Some("2030-01-01T00:00:00Z")
    );
    assert!(
        !request
            .input()
            .headers
            .contains_key(OPERATION_VERSION_METADATA_KEY)
    );
    assert!(
        !request
            .input()
            .headers
            .contains_key(INPUT_CONTRACT_METADATA_KEY)
    );

    let encoded = codec.encode(&request)?;
    assert_eq!(encoded.bytes.as_ref(), payload);
    for (key, expected) in &vector.metadata {
        assert_eq!(encoded.headers.get_text(key)?, Some(expected.as_str()));
    }
    assert!(
        !encoded
            .headers
            .contains_key("plenora.message.correlation_id")
    );
    assert!(!encoded.headers.contains_key(MESSAGE_ID_METADATA_KEY));
    assert!(!encoded.headers.contains_key(CAUSATION_ID_METADATA_KEY));
    Ok(())
}
