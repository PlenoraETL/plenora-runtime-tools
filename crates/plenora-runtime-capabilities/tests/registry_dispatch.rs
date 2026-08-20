//! Bounded registry, generic dispatch, and extension contracts.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
};

use async_trait::async_trait;
use plenora_runtime_capabilities::{
    CapabilityDispatchErrorCategory, CapabilityDispatcher, CapabilityDispatcherConfig,
    CapabilityFailure, CapabilityHandler, CapabilityId, CapabilityRegistryBuilder,
    CapabilityRegistryConfig, CapabilityRegistryError, CapabilityRemoteEffect, CapabilityRequest,
    CapabilityResponse, ContractId, MAX_CAPABILITY_PAYLOAD_BYTES, MAX_REGISTERED_CAPABILITIES,
    OperationName, OperationVersion, PlenoraErrorCategory, PlenoraErrorPhase,
    PlenoraErrorRemoteEffect, PlenoraErrorRetry,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    ClassifyRetry, CorrelationId, MessageId, MessageMetadata, RetryErrorClass, SerializedMessage,
};
use plenora_runtime_worker::{
    TaskCancellationReason, TaskCancellationToken, WorkerContext, WorkerHandler,
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct InvocationRecord {
    adapter: &'static str,
    capability: CapabilityId,
    operation: OperationName,
    message_id: MessageId,
    attempt: u32,
    payload_bytes: usize,
    cancellation: Option<TaskCancellationReason>,
}

#[derive(Clone, Debug)]
struct RecordingHandler {
    adapter: &'static str,
    records: Arc<Mutex<Vec<InvocationRecord>>>,
}

#[async_trait]
impl CapabilityHandler for RecordingHandler {
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        lock(&self.records).push(InvocationRecord {
            adapter: self.adapter,
            capability: request.capability().clone(),
            operation: request.operation().clone(),
            message_id: context.message_id,
            attempt: context.attempt,
            payload_bytes: request.input().len(),
            cancellation: context.cancellation.reason(),
        });
        Ok(CapabilityResponse::acknowledged())
    }
}

#[derive(Clone, Copy, Debug)]
struct AdapterError;

impl Display for AdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("sensitive concrete adapter detail")
    }
}

impl Error for AdapterError {}

#[derive(Clone, Copy, Debug)]
struct FailingHandler;

#[async_trait]
impl CapabilityHandler for FailingHandler {
    async fn invoke(
        &self,
        _context: WorkerContext,
        _request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        Err(CapabilityFailure::new(
            RetryErrorClass::OutcomeUnknown,
            CapabilityRemoteEffect::Unknown,
            AdapterError,
        ))
    }
}

#[tokio::test]
async fn a_fourth_library_registers_and_dispatches_without_runtime_changes()
-> Result<(), Box<dyn Error>> {
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(4)?)?;
    for (name, adapter) in [
        ("plenora.data-tools", "data"),
        ("plenora.database-tools", "database"),
        ("plenora.io-tools", "io"),
        ("plenora.future-tools", "future"),
    ] {
        builder.register(
            CapabilityId::new(name, 1)?,
            RecordingHandler {
                adapter,
                records: Arc::clone(&records),
            },
        )?;
    }
    let registry = builder.build();
    assert_eq!(registry.len(), 4);
    let dispatcher = CapabilityDispatcher::new(registry, CapabilityDispatcherConfig::new(32)?)?;
    assert_eq!(dispatcher.registry().len(), 4);
    assert_eq!(dispatcher.config().max_payload_bytes, 32);
    assert!(format!("{dispatcher:?}").contains("CapabilityDispatcher"));
    let cancellation = TaskCancellationToken::new();
    assert!(cancellation.cancel(TaskCancellationReason::Requested));
    let context = worker_context(3).with_cancellation(cancellation);
    let expected_message_id = context.message_id;

    dispatcher
        .handle(
            context,
            request("plenora.future-tools", "future.analyze", "opaque-input")?,
        )
        .await?;

    assert_eq!(
        lock(&records).as_slice(),
        &[InvocationRecord {
            adapter: "future",
            capability: CapabilityId::new("plenora.future-tools", 1)?,
            operation: OperationName::new("future.analyze")?,
            message_id: expected_message_id,
            attempt: 3,
            payload_bytes: "opaque-input".len(),
            cancellation: Some(TaskCancellationReason::Requested),
        }]
    );
    Ok(())
}

#[test]
fn registry_rejects_duplicates_and_capacity_then_freezes() -> Result<(), Box<dyn Error>> {
    let id = CapabilityId::new("plenora.data-tools", 1)?;
    let records = Arc::new(Mutex::new(Vec::new()));
    let handler = RecordingHandler {
        adapter: "data",
        records,
    };
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    builder.register(id.clone(), handler.clone())?;
    assert_eq!(
        builder.register(id.clone(), handler.clone()),
        Err(CapabilityRegistryError::Duplicate(id.clone()))
    );
    assert!(matches!(
        builder.register(CapabilityId::new("plenora.io-tools", 1)?, handler),
        Err(CapabilityRegistryError::CapacityReached { limit: 1 })
    ));
    let registry = builder.build();
    assert_eq!(registry.capabilities(), vec![id]);
    Ok(())
}

#[tokio::test]
async fn dispatch_rejects_unknown_and_oversized_requests_before_invocation()
-> Result<(), Box<dyn Error>> {
    let records = Arc::new(Mutex::new(Vec::new()));
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    builder.register(
        CapabilityId::new("plenora.data-tools", 1)?,
        RecordingHandler {
            adapter: "data",
            records: Arc::clone(&records),
        },
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::new(4)?)?;

    let unknown = dispatcher
        .handle(
            worker_context(1),
            request("plenora.unknown-tools", "unknown.run", "ok")?,
        )
        .await;
    let unknown = unknown.err().ok_or("unknown capability was accepted")?;
    assert_eq!(
        unknown.category(),
        CapabilityDispatchErrorCategory::UnknownCapability
    );
    assert_eq!(unknown.retry_class(), RetryErrorClass::DeadLetter);
    assert_eq!(unknown.remote_effect(), CapabilityRemoteEffect::NotStarted);
    assert_eq!(unknown.capability().name(), "plenora.unknown-tools");
    assert_eq!(unknown.operation().as_str(), "unknown.run");
    assert!(unknown.handler_failure().is_none());
    assert!(unknown.source().is_none());
    assert_eq!(
        unknown.to_string(),
        "requested capability is not registered"
    );
    assert!(format!("{unknown:?}").contains("UnknownCapability"));

    let oversized = dispatcher
        .handle(
            worker_context(1),
            request("plenora.data-tools", "data.run", "12345")?,
        )
        .await;
    let oversized = oversized.err().ok_or("oversized payload was accepted")?;
    assert_eq!(
        oversized.category(),
        CapabilityDispatchErrorCategory::PayloadTooLarge
    );
    assert_eq!(oversized.retry_class(), RetryErrorClass::DeadLetter);
    assert_eq!(
        oversized.remote_effect(),
        CapabilityRemoteEffect::NotStarted
    );
    assert!(oversized.handler_failure().is_none());
    assert!(oversized.source().is_none());
    assert!(oversized.to_string().contains("configured bound"));
    let diagnostics = format!("{oversized:?}");
    assert!(diagnostics.contains("actual"));
    assert!(diagnostics.contains("limit"));
    assert!(lock(&records).is_empty());
    Ok(())
}

#[tokio::test]
async fn handler_failure_preserves_semantics_and_redacts_source() -> Result<(), Box<dyn Error>> {
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    builder.register(CapabilityId::new("plenora.io-tools", 1)?, FailingHandler)?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;

    let result = dispatcher
        .handle(
            worker_context(1),
            request("plenora.io-tools", "io.write", "payload")?,
        )
        .await;
    let error = result.err().ok_or("failing handler returned success")?;
    assert_eq!(error.category(), CapabilityDispatchErrorCategory::Handler);
    assert_eq!(error.retry_class(), RetryErrorClass::OutcomeUnknown);
    assert_eq!(error.remote_effect(), CapabilityRemoteEffect::Unknown);
    assert!(error.source().is_some());
    assert!(error.handler_failure().is_some());
    assert_eq!(error.capability().name(), "plenora.io-tools");
    assert_eq!(error.operation().as_str(), "io.write");
    assert_eq!(error.to_string(), "capability adapter failed");
    let public = error.public_error()?;
    assert_eq!(public.category(), PlenoraErrorCategory::Internal);
    assert_eq!(public.phase(), PlenoraErrorPhase::Finalize);
    assert_eq!(public.remote_effect(), PlenoraErrorRemoteEffect::Unknown);
    assert_eq!(public.retry(), PlenoraErrorRetry::RequiresRecovery);
    assert!(!format!("{error:?}").contains("sensitive concrete adapter detail"));
    assert!(
        !error
            .to_string()
            .contains("sensitive concrete adapter detail")
    );
    Ok(())
}

#[test]
fn unknown_outcome_forces_conservative_remote_effect() {
    let failure = CapabilityFailure::new(
        RetryErrorClass::OutcomeUnknown,
        CapabilityRemoteEffect::NotStarted,
        AdapterError,
    );
    assert_eq!(failure.remote_effect(), CapabilityRemoteEffect::Unknown);
}

#[test]
fn configuration_registry_and_failure_diagnostics_cover_all_public_contracts()
-> Result<(), Box<dyn Error>> {
    let default_registry = CapabilityRegistryConfig::default();
    assert_eq!(default_registry.max_capabilities, 64);
    assert!(matches!(
        CapabilityRegistryConfig::new(0),
        Err(CapabilityRegistryError::ZeroCapacity)
    ));
    let above = CapabilityRegistryConfig::new(MAX_REGISTERED_CAPABILITIES + 1)
        .err()
        .ok_or("registry accepted capacity above hard maximum")?;
    assert!(matches!(
        above,
        CapabilityRegistryError::CapacityAboveMaximum { .. }
    ));
    assert!(above.to_string().contains("hard maximum"));
    assert!(
        CapabilityRegistryError::ZeroCapacity
            .to_string()
            .contains("positive")
    );

    let builder = CapabilityRegistryBuilder::default();
    assert!(format!("{builder:?}").contains("type-erased"));
    let registry = builder.build();
    assert!(registry.is_empty());
    assert_eq!(registry.len(), 0);
    assert!(registry.capabilities().is_empty());
    assert!(format!("{registry:?}").contains("type-erased"));

    let default_dispatch = CapabilityDispatcherConfig::default();
    assert_eq!(default_dispatch.max_payload_bytes, 1024 * 1024);
    let zero = CapabilityDispatcherConfig::new(0)
        .err()
        .ok_or("dispatcher accepted zero payload bound")?;
    assert!(zero.to_string().contains("positive"));
    let above = CapabilityDispatcherConfig::new(MAX_CAPABILITY_PAYLOAD_BYTES + 1)
        .err()
        .ok_or("dispatcher accepted payload bound above maximum")?;
    assert!(above.to_string().contains("hard maximum"));

    let failure = CapabilityFailure::new(
        RetryErrorClass::Retryable,
        CapabilityRemoteEffect::NotStarted,
        AdapterError,
    );
    assert_eq!(failure.retry_classification(), RetryErrorClass::Retryable);
    assert_eq!(failure.retry_class(), RetryErrorClass::Retryable);
    assert_eq!(failure.remote_effect(), CapabilityRemoteEffect::NotStarted);
    assert_eq!(
        failure.source_error().to_string(),
        "sensitive concrete adapter detail"
    );
    assert!(failure.source().is_some());
    assert!(
        !failure
            .to_string()
            .contains("sensitive concrete adapter detail")
    );
    assert!(!format!("{failure:?}").contains("sensitive concrete adapter detail"));
    let cloned = failure.clone();
    assert_eq!(cloned.retry_classification(), RetryErrorClass::Retryable);
    Ok(())
}

#[test]
fn shared_registration_path_freezes_sorted_capabilities() -> Result<(), Box<dyn Error>> {
    let records = Arc::new(Mutex::new(Vec::new()));
    let handler: Arc<dyn CapabilityHandler> = Arc::new(RecordingHandler {
        adapter: "shared",
        records,
    });
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(2)?)?;
    builder.register_shared(
        CapabilityId::new("plenora.z-tools", 1)?,
        Arc::clone(&handler),
    )?;
    builder.register_shared(CapabilityId::new("plenora.a-tools", 2)?, handler)?;
    let registry = builder.build();
    assert_eq!(
        registry.capabilities(),
        vec![
            CapabilityId::new("plenora.a-tools", 2)?,
            CapabilityId::new("plenora.z-tools", 1)?,
        ]
    );
    Ok(())
}

fn request(
    capability: &str,
    operation: &str,
    payload: &str,
) -> Result<CapabilityRequest, Box<dyn Error>> {
    Ok(CapabilityRequest::new(
        CapabilityId::new(capability, 1)?,
        OperationName::new(operation)?,
        OperationVersion::new(1)?,
        ContractId::new("plenora-test-input-v1")?,
        SerializedMessage::new("application/octet-stream", payload.to_owned()),
    ))
}

fn worker_context(attempt: u32) -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "capability-test",
        "0.1.0",
        "test-instance",
    ));
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
