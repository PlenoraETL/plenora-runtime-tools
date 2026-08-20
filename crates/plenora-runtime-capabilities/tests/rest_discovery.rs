//! Capability Discovery 2.0 and required REST runtime-profile conformance.

use std::{
    error::Error,
    future,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use plenora_runtime_capabilities::{
    CapabilityDiscovery, CapabilityDiscoveryError, CapabilityDiscoveryErrorKind,
    CapabilityDispatchErrorCategory, CapabilityDispatcher, CapabilityDispatcherConfig,
    CapabilityFailure, CapabilityHandler, CapabilityId, CapabilityRegistryBuilder,
    CapabilityRegistryError, CapabilityRemoteEffect, CapabilityRequest, CapabilityRequestRejection,
    CapabilityResponse, CapabilityResponseRejection, CapabilityResult,
    CapabilityResultPublishError, CapabilityResultSink, CapabilitySideEffect, CapabilitySurface,
    ContractId, EXECUTION_DEADLINE_METADATA_KEY, IDEMPOTENCY_KEY_METADATA_KEY,
    MAX_DISCOVERED_INTERFACES, MAX_DISCOVERED_OPERATIONS, OPERATION_VERSION_METADATA_KEY,
    OUTPUT_CONTRACT_METADATA_KEY, OperationName, OperationVersion, PlenoraError,
    PlenoraErrorCategory, PlenoraErrorPhase, PlenoraErrorRemoteEffect, PlenoraErrorRetry,
    REST_ATTRIBUTES_CONTRACT, REST_DOWNLOAD_OPERATION, REST_EXECUTION_REQUEST_CONTRACT,
    REST_EXECUTION_RESULT_CONTRACT, REST_FILE_TRANSFER_INPUT_CONTRACT,
    REST_FILE_TRANSFER_RESULT_CONTRACT, REST_RUNTIME_CAPABILITY, REST_RUNTIME_VERSION,
    REST_UPLOAD_OPERATION, RestCapabilityProfile, RestProfileErrorKind,
    TRACE_CORRELATION_ID_METADATA_KEY,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    ClassifyRetry, CorrelationId, MessageId, MessageMetadata, MessageProducer, PublishOutcome,
    RetryErrorClass, SerializedMessage,
};
use plenora_runtime_worker::{
    TaskCancellationReason, TaskCancellationToken, WorkerContext, WorkerHandler,
};
use serde_json::Value;

const REST_DISCOVERY: &str = include_str!("../../../contracts/capabilities-v2/rest-tools-v1.json");

#[derive(Clone, Debug, Eq, PartialEq)]
struct Invocation {
    operation: OperationName,
    correlation_id: CorrelationId,
    cancellation: Option<TaskCancellationReason>,
}

#[derive(Clone, Debug)]
struct RecordingHandler {
    invocations: Arc<Mutex<Vec<Invocation>>>,
}

#[derive(Clone, Debug)]
struct BlockingHandler {
    cancellations: Arc<Mutex<Vec<TaskCancellationToken>>>,
}

#[derive(Clone, Debug)]
struct CapturingResultSink {
    results: Arc<Mutex<Vec<CapabilityResult>>>,
}

#[derive(Clone, Debug)]
struct FailingHandler {
    failure: CapabilityFailure,
}

#[derive(Clone)]
struct StaticResponseHandler {
    response: CapabilityResponse,
}

#[derive(Clone, Debug)]
struct RecordingProducer {
    messages: Arc<Mutex<Vec<SerializedMessage>>>,
    fail: bool,
}

#[async_trait]
impl MessageProducer for RecordingProducer {
    type Error = std::io::Error;

    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, Self::Error> {
        if self.fail {
            return Err(std::io::Error::other("private producer failure"));
        }
        lock(&self.messages).push(message);
        Ok(PublishOutcome::Confirmed)
    }
}

#[async_trait]
impl CapabilityHandler for FailingHandler {
    async fn invoke(
        &self,
        _context: WorkerContext,
        _request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        Err(self.failure.clone())
    }
}

#[async_trait]
impl CapabilityHandler for StaticResponseHandler {
    async fn invoke(
        &self,
        _context: WorkerContext,
        _request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        Ok(self.response.clone())
    }
}

#[async_trait]
impl CapabilityResultSink for CapturingResultSink {
    async fn publish_result(
        &self,
        result: CapabilityResult,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        lock(&self.results).push(result);
        Ok(PublishOutcome::Confirmed)
    }
}

#[async_trait]
impl CapabilityHandler for BlockingHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        _request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        lock(&self.cancellations).push(context.cancellation);
        future::pending().await
    }
}

#[async_trait]
impl CapabilityHandler for RecordingHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        let output_contract = match request.operation().as_str() {
            REST_DOWNLOAD_OPERATION | REST_UPLOAD_OPERATION => REST_FILE_TRANSFER_RESULT_CONTRACT,
            _ => REST_EXECUTION_RESULT_CONTRACT,
        };
        lock(&self.invocations).push(Invocation {
            operation: request.operation().clone(),
            correlation_id: context.correlation_id,
            cancellation: context.cancellation.reason(),
        });
        let output_contract = ContractId::new(output_contract).map_err(|source| {
            CapabilityFailure::new(
                RetryErrorClass::Permanent,
                CapabilityRemoteEffect::NotStarted,
                source,
            )
        })?;
        Ok(CapabilityResponse::new(
            output_contract,
            SerializedMessage::new("application/json", r#"{"ok":true}"#),
        ))
    }
}

#[test]
fn pinned_rest_discovery_resolves_the_complete_required_profile() -> Result<(), Box<dyn Error>> {
    let discovery = CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?;
    RestCapabilityProfile::validate(&discovery)?;
    assert_eq!(discovery.component(), "plenora-rest-tools");
    assert_eq!(discovery.component_version(), "1.0.0");
    assert_eq!(discovery.operations().len(), 5);

    let identity = discovery.runtime_capability()?;
    assert_eq!(identity.name(), REST_RUNTIME_CAPABILITY);
    assert_eq!(identity.version(), REST_RUNTIME_VERSION);

    let download = discovery
        .operation_named(REST_DOWNLOAD_OPERATION)
        .ok_or("rest.download is missing")?;
    assert_eq!(download.side_effect(), CapabilitySideEffect::Remote);
    assert_eq!(
        download.input().contract().as_str(),
        REST_FILE_TRANSFER_INPUT_CONTRACT
    );
    assert_eq!(
        download.attributes().string("contract"),
        Some(REST_ATTRIBUTES_CONTRACT)
    );
    assert_eq!(download.attributes().string("direction"), Some("download"));

    let upload = discovery
        .operation_named(REST_UPLOAD_OPERATION)
        .ok_or("rest.upload is missing")?;
    assert_eq!(upload.input().contract(), download.input().contract());
    assert!(upload.input().supports_content_type("application/json"));
    assert!(
        !upload
            .input()
            .supports_content_type("application/octet-stream")
    );
    Ok(())
}

#[tokio::test]
async fn discovered_rest_dispatches_all_five_operations_and_preserves_context()
-> Result<(), Box<dyn Error>> {
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let results = Arc::new(Mutex::new(Vec::new()));
    let discovery = CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?;
    let mut builder = CapabilityRegistryBuilder::default();
    let identity = builder.register_discovered(
        discovery,
        RecordingHandler {
            invocations: Arc::clone(&invocations),
        },
    )?;
    let registry = builder.build();
    assert!(registry.discovery(&identity).is_some());
    let dispatcher = CapabilityDispatcher::with_result_sink(
        registry,
        CapabilityDispatcherConfig::default(),
        CapturingResultSink {
            results: Arc::clone(&results),
        },
    )?;
    let correlation_id: CorrelationId = "018f3d84-7b2c-7f00-8000-000000000099".parse()?;

    for (operation, contract) in [
        ("rest.test", REST_EXECUTION_REQUEST_CONTRACT),
        ("rest.generate", REST_EXECUTION_REQUEST_CONTRACT),
        ("rest.enrich", REST_EXECUTION_REQUEST_CONTRACT),
        (REST_DOWNLOAD_OPERATION, REST_FILE_TRANSFER_INPUT_CONTRACT),
        (REST_UPLOAD_OPERATION, REST_FILE_TRANSFER_INPUT_CONTRACT),
    ] {
        let cancellation = TaskCancellationToken::new();
        assert!(cancellation.cancel(TaskCancellationReason::Requested));
        dispatcher
            .handle(
                worker_context(correlation_id).with_cancellation(cancellation),
                rest_request(operation, 1, contract, "application/json")?,
            )
            .await?;
    }

    let recorded = lock(&invocations);
    assert_eq!(recorded.len(), 5);
    assert!(
        recorded
            .iter()
            .all(|invocation| invocation.correlation_id == correlation_id)
    );
    assert!(
        recorded.iter().all(|invocation| {
            invocation.cancellation == Some(TaskCancellationReason::Requested)
        })
    );
    let results = lock(&results);
    assert_eq!(results.len(), 5);
    assert!(
        results
            .iter()
            .all(|result| result.correlation_id() == correlation_id)
    );
    assert!(
        results
            .iter()
            .all(|result| result.operation_version().get() == 1)
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| {
                result.output_contract().as_str() == REST_FILE_TRANSFER_RESULT_CONTRACT
            })
            .count(),
        2
    );
    for result in results.iter() {
        assert_eq!(
            result
                .message()
                .headers
                .get_text(OPERATION_VERSION_METADATA_KEY)?,
            Some("1")
        );
        assert_eq!(
            result
                .message()
                .headers
                .get_text(OUTPUT_CONTRACT_METADATA_KEY)?,
            Some(result.output_contract().as_str())
        );
        assert_eq!(
            result
                .message()
                .headers
                .get_text(TRACE_CORRELATION_ID_METADATA_KEY)?,
            Some("018f3d84-7b2c-7f00-8000-000000000099")
        );
        assert_eq!(result.message().content_type.as_ref(), "application/json");
    }
    Ok(())
}

#[tokio::test]
async fn public_results_and_broker_neutral_sink_are_fully_observable() -> Result<(), Box<dyn Error>>
{
    let response = CapabilityResponse::new(
        ContractId::new(REST_EXECUTION_RESULT_CONTRACT)?,
        SerializedMessage::new("application/json", r#"{"ok":true}"#),
    );
    assert_eq!(
        response.output_contract().map(ContractId::as_str),
        Some(REST_EXECUTION_RESULT_CONTRACT)
    );
    assert_eq!(
        response
            .output()
            .map(|message| message.content_type.as_ref()),
        Some("application/json")
    );
    assert!(format!("{response:?}").contains("CapabilityResponse"));
    let acknowledged = CapabilityResponse::acknowledged();
    assert!(acknowledged.output_contract().is_none());
    assert!(acknowledged.output().is_none());
    assert!(format!("{acknowledged:?}").contains("CapabilityResponse"));

    let results = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        StaticResponseHandler { response },
    )?;
    let dispatcher = CapabilityDispatcher::with_result_sink(
        builder.build(),
        CapabilityDispatcherConfig::default(),
        CapturingResultSink {
            results: Arc::clone(&results),
        },
    )?;
    let correlation_id: CorrelationId = "018f3d84-7b2c-7f00-8000-000000000098".parse()?;
    dispatcher
        .handle(
            worker_context(correlation_id),
            rest_request(
                "rest.test",
                1,
                REST_EXECUTION_REQUEST_CONTRACT,
                "application/json",
            )?,
        )
        .await?;
    let result = lock(&results)
        .pop()
        .ok_or("dispatcher did not publish a result")?;
    assert_eq!(result.operation().as_str(), "rest.test");
    assert_eq!(result.operation_version().get(), 1);
    assert_eq!(
        result.output_contract().as_str(),
        REST_EXECUTION_RESULT_CONTRACT
    );
    assert_eq!(result.correlation_id(), correlation_id);
    assert_eq!(result.message().content_type.as_ref(), "application/json");
    assert!(format!("{result:?}").contains("CapabilityResult"));
    let expected_message = result.clone().into_message();

    let messages = Arc::new(Mutex::new(Vec::new()));
    let producer = RecordingProducer {
        messages: Arc::clone(&messages),
        fail: false,
    };
    assert_eq!(
        producer.publish_result(result.clone()).await?,
        PublishOutcome::Confirmed
    );
    assert_eq!(lock(&messages).as_slice(), &[expected_message]);

    let failing = RecordingProducer {
        messages: Arc::new(Mutex::new(Vec::new())),
        fail: true,
    };
    let error = failing
        .publish_result(result)
        .await
        .err()
        .ok_or("failing producer returned success")?;
    assert_eq!(error.source_error().to_string(), "private producer failure");
    assert!(error.source().is_some());
    assert_eq!(error.to_string(), "capability result publication failed");
    assert!(!format!("{error:?}").contains("private producer failure"));
    assert_eq!(error.clone().to_string(), error.to_string());
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_incompatible_requests_before_invoking_rest() -> Result<(), Box<dyn Error>>
{
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        RecordingHandler {
            invocations: Arc::clone(&invocations),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;

    let cases = [
        (
            rest_request(
                "rest.unknown",
                1,
                REST_EXECUTION_REQUEST_CONTRACT,
                "application/json",
            )?,
            CapabilityRequestRejection::UnknownOperation,
        ),
        (
            rest_request(
                "rest.test",
                2,
                REST_EXECUTION_REQUEST_CONTRACT,
                "application/json",
            )?,
            CapabilityRequestRejection::OperationVersionMismatch,
        ),
        (
            rest_request("rest.test", 1, "plenora-wrong-input-v1", "application/json")?,
            CapabilityRequestRejection::InputContractMismatch,
        ),
        (
            rest_request(
                REST_UPLOAD_OPERATION,
                1,
                REST_FILE_TRANSFER_INPUT_CONTRACT,
                "application/octet-stream",
            )?,
            CapabilityRequestRejection::InputContentTypeMismatch,
        ),
    ];

    for (request, expected) in cases {
        let error = dispatcher
            .handle(worker_context(CorrelationId::random()), request)
            .await
            .err()
            .ok_or("incompatible REST request was accepted")?;
        assert_eq!(
            error.category(),
            CapabilityDispatchErrorCategory::IncompatibleRequest
        );
        assert_eq!(error.request_rejection(), Some(expected));
        assert_eq!(error.retry_class(), RetryErrorClass::DeadLetter);
        assert_eq!(error.remote_effect(), CapabilityRemoteEffect::NotStarted);
        assert!(error.handler_failure().is_none());
    }
    assert!(lock(&invocations).is_empty());
    Ok(())
}

#[tokio::test]
async fn discovery_rejects_incompatible_rest_outputs_and_unconfigured_publication()
-> Result<(), Box<dyn Error>> {
    let cases = [
        (
            CapabilityResponse::acknowledged(),
            CapabilityResponseRejection::MissingOutput,
        ),
        (
            CapabilityResponse::new(
                ContractId::new("plenora-wrong-result-v1")?,
                SerializedMessage::new("application/json", r#"{"ok":true}"#),
            ),
            CapabilityResponseRejection::OutputContractMismatch,
        ),
        (
            CapabilityResponse::new(
                ContractId::new(REST_EXECUTION_RESULT_CONTRACT)?,
                SerializedMessage::new("application/octet-stream", "opaque"),
            ),
            CapabilityResponseRejection::OutputContentTypeMismatch,
        ),
    ];

    for (response, expected) in cases {
        let mut builder = CapabilityRegistryBuilder::default();
        builder.register_discovered(
            CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
            StaticResponseHandler { response },
        )?;
        let dispatcher = CapabilityDispatcher::with_result_sink(
            builder.build(),
            CapabilityDispatcherConfig::default(),
            CapturingResultSink {
                results: Arc::new(Mutex::new(Vec::new())),
            },
        )?;
        let error = dispatcher
            .handle(
                worker_context(CorrelationId::random()),
                rest_request(
                    "rest.test",
                    1,
                    REST_EXECUTION_REQUEST_CONTRACT,
                    "application/json",
                )?,
            )
            .await
            .err()
            .ok_or("incompatible REST output was accepted")?;
        assert_eq!(
            error.category(),
            CapabilityDispatchErrorCategory::IncompatibleResponse
        );
        assert_eq!(error.response_rejection(), Some(expected));
        assert_eq!(error.retry_class(), RetryErrorClass::OutcomeUnknown);
        assert_eq!(error.remote_effect(), CapabilityRemoteEffect::Unknown);
    }

    let invocations = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        RecordingHandler {
            invocations: Arc::clone(&invocations),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request(
                "rest.test",
                1,
                REST_EXECUTION_REQUEST_CONTRACT,
                "application/json",
            )?,
        )
        .await
        .err()
        .ok_or("REST result was accepted without a configured sink")?;
    assert_eq!(
        error.category(),
        CapabilityDispatchErrorCategory::ResultPublication
    );
    assert_eq!(error.retry_class(), RetryErrorClass::OutcomeUnknown);
    assert_eq!(error.remote_effect(), CapabilityRemoteEffect::Unknown);
    assert_eq!(lock(&invocations).len(), 1);
    Ok(())
}

#[tokio::test]
async fn absolute_deadlines_and_unsupported_controls_fail_closed() -> Result<(), Box<dyn Error>> {
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        RecordingHandler {
            invocations: Arc::clone(&invocations),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;

    let mut expired = MessageMetadata::new();
    expired.insert_text(EXECUTION_DEADLINE_METADATA_KEY, "2000-01-01T00:00:00Z")?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request_with_metadata("rest.test", REST_EXECUTION_REQUEST_CONTRACT, expired)?,
        )
        .await
        .err()
        .ok_or("expired deadline was accepted")?;
    assert_eq!(
        error.category(),
        CapabilityDispatchErrorCategory::DeadlineExceeded
    );
    assert_eq!(error.remote_effect(), CapabilityRemoteEffect::NotStarted);
    assert_eq!(error.retry_class(), RetryErrorClass::DeadLetter);

    let mut invalid = MessageMetadata::new();
    invalid.insert_text(EXECUTION_DEADLINE_METADATA_KEY, "tomorrow")?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request_with_metadata("rest.test", REST_EXECUTION_REQUEST_CONTRACT, invalid)?,
        )
        .await
        .err()
        .ok_or("invalid deadline was accepted")?;
    assert_eq!(
        error.request_rejection(),
        Some(CapabilityRequestRejection::InvalidDeadline)
    );

    let mut idempotency = MessageMetadata::new();
    idempotency.insert_text(IDEMPOTENCY_KEY_METADATA_KEY, "opaque-key")?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request_with_metadata("rest.test", REST_EXECUTION_REQUEST_CONTRACT, idempotency)?,
        )
        .await
        .err()
        .ok_or("unsupported idempotency key was accepted")?;
    assert_eq!(
        error.request_rejection(),
        Some(CapabilityRequestRejection::IdempotencyKeyUnsupported)
    );
    assert!(lock(&invocations).is_empty());

    let cancellations = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        BlockingHandler {
            cancellations: Arc::clone(&cancellations),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
    let deadline: DateTime<Utc> = (SystemTime::now() + Duration::from_millis(50)).into();
    let mut metadata = MessageMetadata::new();
    metadata.insert_text(
        EXECUTION_DEADLINE_METADATA_KEY,
        deadline.to_rfc3339_opts(SecondsFormat::Millis, true),
    )?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request_with_metadata("rest.test", REST_EXECUTION_REQUEST_CONTRACT, metadata)?,
        )
        .await
        .err()
        .ok_or("running REST operation ignored its deadline")?;
    assert_eq!(
        error.category(),
        CapabilityDispatchErrorCategory::DeadlineExceeded
    );
    assert_eq!(error.remote_effect(), CapabilityRemoteEffect::Unknown);
    assert_eq!(error.retry_class(), RetryErrorClass::OutcomeUnknown);
    let cancellations = lock(&cancellations);
    assert_eq!(cancellations.len(), 1);
    assert_eq!(
        cancellations[0].reason(),
        Some(TaskCancellationReason::Timeout)
    );
    Ok(())
}

#[tokio::test]
async fn discovered_rest_preserves_typed_errors_and_flags_missing_mappings()
-> Result<(), Box<dyn Error>> {
    let mapped = PlenoraError::new(
        PlenoraErrorCategory::Transient,
        PlenoraErrorPhase::Connect,
        PlenoraErrorRemoteEffect::None,
        PlenoraErrorRetry::After { delay_ms: 250 },
        "REST dependency is temporarily unavailable.",
    )?;
    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        FailingHandler {
            failure: CapabilityFailure::with_public_error(
                mapped.clone(),
                std::io::Error::other("private REST failure"),
            ),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request(
                "rest.test",
                1,
                REST_EXECUTION_REQUEST_CONTRACT,
                "application/json",
            )?,
        )
        .await
        .err()
        .ok_or("mapped REST failure unexpectedly succeeded")?;
    assert_eq!(error.public_error()?, mapped);
    assert_eq!(error.retry_class(), RetryErrorClass::Retryable);
    assert!(!format!("{error:?}").contains("private REST failure"));

    let mut builder = CapabilityRegistryBuilder::default();
    builder.register_discovered(
        CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?,
        FailingHandler {
            failure: CapabilityFailure::new(
                RetryErrorClass::OutcomeUnknown,
                CapabilityRemoteEffect::Unknown,
                std::io::Error::other("unmapped private failure"),
            ),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
    let error = dispatcher
        .handle(
            worker_context(CorrelationId::random()),
            rest_request(
                "rest.test",
                1,
                REST_EXECUTION_REQUEST_CONTRACT,
                "application/json",
            )?,
        )
        .await
        .err()
        .ok_or("unmapped REST failure unexpectedly succeeded")?;
    let public = error.public_error()?;
    assert_eq!(public.category(), PlenoraErrorCategory::Protocol);
    assert_eq!(public.phase(), PlenoraErrorPhase::Finalize);
    assert_eq!(public.remote_effect(), PlenoraErrorRemoteEffect::Unknown);
    assert_eq!(public.retry(), PlenoraErrorRetry::RequiresRecovery);
    Ok(())
}

#[test]
fn rest_registration_requires_discovery_and_rejects_profile_drift() -> Result<(), Box<dyn Error>> {
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let handler = RecordingHandler {
        invocations: Arc::clone(&invocations),
    };
    let mut legacy = CapabilityRegistryBuilder::default();
    let error = legacy
        .register(
            CapabilityId::new(REST_RUNTIME_CAPABILITY, REST_RUNTIME_VERSION)?,
            handler.clone(),
        )
        .err()
        .ok_or("REST registered without discovery")?;
    assert!(matches!(
        error,
        CapabilityRegistryError::DiscoveryRequired(_)
    ));

    for (operation, field, replacement, expected) in [
        (
            REST_DOWNLOAD_OPERATION,
            "side_effect",
            Value::String(String::from("local")),
            RestProfileErrorKind::SideEffectMismatch,
        ),
        (
            REST_UPLOAD_OPERATION,
            "input.content_types",
            serde_json::json!(["application/json", "application/octet-stream"]),
            RestProfileErrorKind::EnvelopeContentTypeMismatch,
        ),
        (
            "rest.test",
            "output.contract",
            Value::String(String::from("plenora-wrong-output-v1")),
            RestProfileErrorKind::PayloadContractMismatch,
        ),
        (
            "rest.generate",
            "attributes.contract",
            Value::String(String::from("plenora-wrong-attributes-v1")),
            RestProfileErrorKind::AttributesContractMismatch,
        ),
    ] {
        let changed = changed_operation(operation, field, replacement)?;
        let discovery = CapabilityDiscovery::from_json(&changed)?;
        let profile_error = RestCapabilityProfile::validate(&discovery)
            .err()
            .ok_or("incompatible REST profile was accepted")?;
        assert_eq!(profile_error.kind(), expected);

        let mut builder = CapabilityRegistryBuilder::default();
        let registry_error = builder
            .register_discovered(discovery, handler.clone())
            .err()
            .ok_or("incompatible REST registration was accepted")?;
        assert!(matches!(
            registry_error,
            CapabilityRegistryError::RestProfile(_)
        ));
    }
    assert!(lock(&invocations).is_empty());
    Ok(())
}

#[test]
fn discovery_enforces_document_and_attribute_bounds() -> Result<(), Box<dyn Error>> {
    let oversized = vec![b' '; plenora_runtime_capabilities::MAX_CAPABILITY_DISCOVERY_BYTES + 1];
    assert_eq!(
        CapabilityDiscovery::from_json(&oversized)
            .err()
            .map(CapabilityDiscoveryError::kind),
        Some(CapabilityDiscoveryErrorKind::DocumentTooLarge)
    );

    let mut document: Value = serde_json::from_str(REST_DISCOVERY)?;
    let operation = operation_mut(&mut document, "rest.test")?;
    operation.insert(String::from("attributes"), nested_attribute(10));
    let encoded = serde_json::to_vec(&document)?;
    assert_eq!(
        CapabilityDiscovery::from_json(&encoded)
            .err()
            .map(CapabilityDiscoveryError::kind),
        Some(CapabilityDiscoveryErrorKind::InvalidAttributes)
    );
    Ok(())
}

#[test]
fn discovery_rejects_invalid_document_and_interface_shapes() -> Result<(), Box<dyn Error>> {
    let invalid_json = CapabilityDiscovery::from_json(b"{")
        .err()
        .ok_or("invalid JSON was accepted")?;
    assert_eq!(
        invalid_json.kind(),
        CapabilityDiscoveryErrorKind::InvalidJson
    );
    assert!(invalid_json.to_string().contains("InvalidJson"));

    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::UnsupportedSchemaVersion,
        |document| {
            document["schema_version"] = serde_json::json!(1);
            Ok(())
        },
    )?;
    assert_discovery_mutation(CapabilityDiscoveryErrorKind::InvalidComponent, |document| {
        document["component"] = serde_json::json!("invalid");
        Ok(())
    })?;
    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::InvalidComponentVersion,
        |document| {
            document["component_version"] = serde_json::json!("1.0");
            Ok(())
        },
    )?;
    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::MissingInterfaces,
        |document| {
            document["interfaces"] = serde_json::json!([]);
            Ok(())
        },
    )?;
    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::TooManyInterfaces,
        |document| {
            let interface = document["interfaces"][0].clone();
            document["interfaces"] = Value::Array(vec![interface; MAX_DISCOVERED_INTERFACES + 1]);
            Ok(())
        },
    )?;
    assert_discovery_mutation(CapabilityDiscoveryErrorKind::InvalidInterface, |document| {
        document["interfaces"][0]["version"] = serde_json::json!(0);
        Ok(())
    })?;
    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::DuplicateInterface,
        |document| {
            let interface = document["interfaces"][0].clone();
            document["interfaces"]
                .as_array_mut()
                .ok_or("interfaces array is missing")?
                .push(interface);
            Ok(())
        },
    )?;
    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::TooManyOperations,
        |document| {
            let operation = document["operations"][0].clone();
            document["operations"] = Value::Array(vec![operation; MAX_DISCOVERED_OPERATIONS + 1]);
            Ok(())
        },
    )?;
    assert_discovery_mutation(
        CapabilityDiscoveryErrorKind::DuplicateOperation,
        |document| {
            let operation = document["operations"][0].clone();
            document["operations"]
                .as_array_mut()
                .ok_or("operations array is missing")?
                .push(operation);
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn discovery_rejects_invalid_operation_payload_and_attribute_shapes() -> Result<(), Box<dyn Error>>
{
    for (field, value) in [
        ("id", serde_json::json!("invalid")),
        ("version", serde_json::json!(0)),
        ("surfaces", serde_json::json!([])),
        ("surfaces", serde_json::json!(["runtime", "runtime"])),
        ("status", serde_json::json!("unavailable")),
    ] {
        assert_discovery_mutation(CapabilityDiscoveryErrorKind::InvalidOperation, |document| {
            operation_mut(document, "rest.test")?.insert(String::from(field), value);
            Ok(())
        })?;
    }
    for (field, value) in [
        ("contract", serde_json::json!("not a contract")),
        ("content_types", serde_json::json!([])),
        (
            "content_types",
            serde_json::json!(["application/json", "application/json"]),
        ),
        ("content_types", serde_json::json!(["invalid"])),
    ] {
        assert_discovery_mutation(CapabilityDiscoveryErrorKind::InvalidOperation, |document| {
            operation_mut(document, "rest.test")?
                .get_mut("input")
                .and_then(Value::as_object_mut)
                .ok_or("input payload is missing")?
                .insert(String::from(field), value);
            Ok(())
        })?;
    }
    for attributes in [
        serde_json::json!({"items": vec![Value::Null; 65]}),
        serde_json::json!({"value": "x".repeat(513)}),
        serde_json::json!({"x".repeat(65): true}),
    ] {
        assert_discovery_mutation(
            CapabilityDiscoveryErrorKind::InvalidAttributes,
            |document| {
                operation_mut(document, "rest.test")?
                    .insert(String::from("attributes"), attributes);
                Ok(())
            },
        )?;
    }
    Ok(())
}

#[test]
fn runtime_identity_resolution_rejects_every_incompatible_interface() -> Result<(), Box<dyn Error>>
{
    assert_runtime_identity_mutation(
        CapabilityDiscoveryErrorKind::MissingRuntimeInterface,
        |document| {
            document["interfaces"]
                .as_array_mut()
                .ok_or("interfaces array is missing")?
                .retain(|interface| interface["kind"] != "runtime");
            Ok(())
        },
    )?;
    assert_runtime_identity_mutation(
        CapabilityDiscoveryErrorKind::DuplicateRuntimeInterface,
        |document| {
            let mut runtime = document["interfaces"][2].clone();
            runtime["artifact"] = serde_json::json!("plenora.other-tools");
            document["interfaces"]
                .as_array_mut()
                .ok_or("interfaces array is missing")?
                .push(runtime);
            Ok(())
        },
    )?;
    assert_runtime_identity_mutation(
        CapabilityDiscoveryErrorKind::UnsupportedRuntimeBinding,
        |document| {
            document["interfaces"][2]["contract"] = serde_json::json!("plenora-other-runtime-v1");
            Ok(())
        },
    )?;
    assert_runtime_identity_mutation(
        CapabilityDiscoveryErrorKind::MissingRuntimeArtifact,
        |document| {
            document["interfaces"][2]
                .as_object_mut()
                .ok_or("runtime interface is missing")?
                .remove("artifact");
            Ok(())
        },
    )?;
    assert_runtime_identity_mutation(
        CapabilityDiscoveryErrorKind::InvalidRuntimeCapability,
        |document| {
            document["interfaces"][2]["artifact"] = serde_json::json!("invalid");
            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn discovery_accessors_and_request_controls_are_complete() -> Result<(), Box<dyn Error>> {
    let discovery = CapabilityDiscovery::from_json(REST_DISCOVERY.as_bytes())?;
    let rust = discovery
        .interfaces()
        .first()
        .ok_or("Rust interface is missing")?;
    assert_eq!(rust.kind(), CapabilitySurface::Rust);
    assert_eq!(rust.contract().as_str(), "plenora-rust-public-v1");
    assert_eq!(rust.version(), 1);
    assert_eq!(rust.artifact(), Some("plenora-rest-core"));

    let operation = discovery
        .operation_named("rest.test")
        .ok_or("rest.test is missing")?;
    assert_eq!(operation.attributes().len(), 1);
    assert!(!operation.attributes().is_empty());
    assert_eq!(operation.reason(), None);

    let mut no_attributes: Value = serde_json::from_str(REST_DISCOVERY)?;
    operation_mut(&mut no_attributes, "rest.test")?.remove("attributes");
    let no_attributes = CapabilityDiscovery::from_json(&serde_json::to_vec(&no_attributes)?)?;
    assert!(
        no_attributes
            .operation_named("rest.test")
            .ok_or("rest.test is missing")?
            .attributes()
            .is_empty()
    );

    let unavailable = mutated_discovery(|document| {
        let operation = operation_mut(document, "rest.test")?;
        operation.insert(String::from("status"), serde_json::json!("unavailable"));
        operation.insert(String::from("reason"), serde_json::json!("maintenance"));
        Ok(())
    })?;
    let unavailable_operation = unavailable
        .operation_named("rest.test")
        .ok_or("rest.test is missing")?;
    assert_eq!(unavailable_operation.reason(), Some("maintenance"));
    assert_eq!(
        unavailable.validate_request(&rest_request(
            "rest.test",
            1,
            REST_EXECUTION_REQUEST_CONTRACT,
            "application/json",
        )?),
        Err(CapabilityRequestRejection::OperationUnavailable)
    );

    let no_runtime = mutated_discovery(|document| {
        operation_mut(document, "rest.test")?.insert(
            String::from("surfaces"),
            serde_json::json!(["rust", "python_sdk"]),
        );
        Ok(())
    })?;
    assert_eq!(
        no_runtime.validate_request(&rest_request(
            "rest.test",
            1,
            REST_EXECUTION_REQUEST_CONTRACT,
            "application/json",
        )?),
        Err(CapabilityRequestRejection::RuntimeSurfaceUnsupported)
    );

    let no_deadline = mutated_discovery(|document| {
        operation_mut(document, "rest.test")?["controls"]["deadline"] = serde_json::json!(false);
        Ok(())
    })?;
    let mut metadata = MessageMetadata::new();
    metadata.insert_text(EXECUTION_DEADLINE_METADATA_KEY, "2030-01-01T00:00:00Z")?;
    assert_eq!(
        no_deadline.validate_request(&rest_request_with_metadata(
            "rest.test",
            REST_EXECUTION_REQUEST_CONTRACT,
            metadata,
        )?),
        Err(CapabilityRequestRejection::DeadlineUnsupported)
    );
    Ok(())
}

#[test]
fn rest_profile_reports_every_uncovered_public_drift() -> Result<(), Box<dyn Error>> {
    assert_profile_mutation(RestProfileErrorKind::ComponentMismatch, None, |document| {
        document["component"] = serde_json::json!("plenora-other-tools");
        Ok(())
    })?;
    assert_profile_mutation(
        RestProfileErrorKind::RuntimeBindingMismatch,
        None,
        |document| {
            document["interfaces"][2]["artifact"] = serde_json::json!("plenora.other-tools");
            Ok(())
        },
    )?;
    assert_profile_mutation(
        RestProfileErrorKind::RequiredOperationMissing,
        Some("rest.test"),
        |document| {
            document["operations"]
                .as_array_mut()
                .ok_or("operations array is missing")?
                .retain(|operation| operation["id"] != "rest.test");
            Ok(())
        },
    )?;
    assert_profile_mutation(
        RestProfileErrorKind::OperationVersionMismatch,
        Some("rest.test"),
        |document| {
            operation_mut(document, "rest.test")?["version"] = serde_json::json!(2);
            Ok(())
        },
    )?;
    assert_profile_mutation(
        RestProfileErrorKind::OperationUnavailable,
        Some("rest.test"),
        |document| {
            let operation = operation_mut(document, "rest.test")?;
            operation.insert(String::from("status"), serde_json::json!("unavailable"));
            operation.insert(String::from("reason"), serde_json::json!("maintenance"));
            Ok(())
        },
    )?;
    assert_profile_mutation(
        RestProfileErrorKind::RuntimeSurfaceMissing,
        Some("rest.test"),
        |document| {
            operation_mut(document, "rest.test")?.insert(
                String::from("surfaces"),
                serde_json::json!(["rust", "python_sdk"]),
            );
            Ok(())
        },
    )?;
    assert_profile_mutation(
        RestProfileErrorKind::ControlsMismatch,
        Some("rest.test"),
        |document| {
            operation_mut(document, "rest.test")?["controls"]["cancellation"] =
                serde_json::json!(false);
            Ok(())
        },
    )?;
    assert_profile_mutation(
        RestProfileErrorKind::TransferAttributesMismatch,
        Some(REST_DOWNLOAD_OPERATION),
        |document| {
            operation_mut(document, REST_DOWNLOAD_OPERATION)?["attributes"]["direction"] =
                serde_json::json!("upload");
            Ok(())
        },
    )?;
    Ok(())
}

fn rest_request(
    operation: &str,
    version: u16,
    contract: &str,
    content_type: &str,
) -> Result<CapabilityRequest, Box<dyn Error>> {
    rest_request_with_metadata_parts(
        operation,
        version,
        contract,
        content_type,
        MessageMetadata::new(),
    )
}

fn rest_request_with_metadata(
    operation: &str,
    contract: &str,
    metadata: MessageMetadata,
) -> Result<CapabilityRequest, Box<dyn Error>> {
    rest_request_with_metadata_parts(operation, 1, contract, "application/json", metadata)
}

fn rest_request_with_metadata_parts(
    operation: &str,
    version: u16,
    contract: &str,
    content_type: &str,
    metadata: MessageMetadata,
) -> Result<CapabilityRequest, Box<dyn Error>> {
    Ok(CapabilityRequest::new(
        CapabilityId::new(REST_RUNTIME_CAPABILITY, REST_RUNTIME_VERSION)?,
        OperationName::new(operation)?,
        OperationVersion::new(version)?,
        ContractId::new(contract)?,
        SerializedMessage::new(content_type, "{}").with_headers(metadata),
    ))
}

fn worker_context(correlation_id: CorrelationId) -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "rest-discovery-test",
        "0.1.0",
        "test-instance",
    ));
    WorkerContext::new(
        MessageId::random(),
        correlation_id,
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn changed_operation(
    operation: &str,
    field: &str,
    replacement: Value,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut document: Value = serde_json::from_str(REST_DISCOVERY)?;
    let operation = operation_mut(&mut document, operation)?;
    let (parent, field) = field
        .split_once('.')
        .map_or((None, field), |(parent, field)| (Some(parent), field));
    let target = if let Some(parent) = parent {
        operation
            .get_mut(parent)
            .and_then(Value::as_object_mut)
            .ok_or("operation object field is missing")?
    } else {
        operation
    };
    target.insert(String::from(field), replacement);
    Ok(serde_json::to_vec(&document)?)
}

fn mutated_discovery(
    mutate: impl FnOnce(&mut Value) -> Result<(), Box<dyn Error>>,
) -> Result<CapabilityDiscovery, Box<dyn Error>> {
    let mut document: Value = serde_json::from_str(REST_DISCOVERY)?;
    mutate(&mut document)?;
    Ok(CapabilityDiscovery::from_json(&serde_json::to_vec(
        &document,
    )?)?)
}

fn assert_discovery_mutation(
    expected: CapabilityDiscoveryErrorKind,
    mutate: impl FnOnce(&mut Value) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut document: Value = serde_json::from_str(REST_DISCOVERY)?;
    mutate(&mut document)?;
    let error = CapabilityDiscovery::from_json(&serde_json::to_vec(&document)?)
        .err()
        .ok_or("invalid discovery mutation was accepted")?;
    assert_eq!(error.kind(), expected);
    assert!(error.to_string().contains(&format!("{expected:?}")));
    Ok(())
}

fn assert_runtime_identity_mutation(
    expected: CapabilityDiscoveryErrorKind,
    mutate: impl FnOnce(&mut Value) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let discovery = mutated_discovery(mutate)?;
    let error = discovery
        .runtime_capability()
        .err()
        .ok_or("invalid runtime identity was accepted")?;
    assert_eq!(error.kind(), expected);
    Ok(())
}

fn assert_profile_mutation(
    expected: RestProfileErrorKind,
    operation: Option<&str>,
    mutate: impl FnOnce(&mut Value) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let discovery = mutated_discovery(mutate)?;
    let error = RestCapabilityProfile::validate(&discovery)
        .err()
        .ok_or("incompatible REST profile was accepted")?;
    assert_eq!(error.kind(), expected);
    assert_eq!(error.operation(), operation);
    assert_eq!(
        error.to_string(),
        "REST capability discovery is incompatible with the required profile"
    );
    Ok(())
}

fn operation_mut<'a>(
    document: &'a mut Value,
    operation: &str,
) -> Result<&'a mut serde_json::Map<String, Value>, Box<dyn Error>> {
    document
        .get_mut("operations")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| Box::<dyn Error>::from("operations array is missing"))?
        .iter_mut()
        .find(|candidate| candidate.get("id").and_then(Value::as_str) == Some(operation))
        .and_then(Value::as_object_mut)
        .ok_or_else(|| Box::<dyn Error>::from("required operation is missing"))
}

fn nested_attribute(depth: usize) -> Value {
    let mut value = Value::String(String::from("bounded"));
    for _ in 0..depth {
        value = serde_json::json!({"nested": value});
    }
    serde_json::json!({"contract": REST_ATTRIBUTES_CONTRACT, "value": value})
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
