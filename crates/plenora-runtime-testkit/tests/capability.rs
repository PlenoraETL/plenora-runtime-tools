//! Deterministic generic capability fake contracts.

use std::{error::Error, sync::Arc};

use plenora_runtime_capabilities::{
    CapabilityDispatcher, CapabilityDispatcherConfig, CapabilityId, CapabilityRegistryBuilder,
    CapabilityRegistryConfig, CapabilityRemoteEffect, CapabilityRequest, OperationName,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    ClassifyRetry, CorrelationId, MessageId, MessageMetadata, RetryErrorClass, SerializedMessage,
};
use plenora_runtime_testkit::{
    FakeCapabilityConfig, FakeCapabilityError, FakeCapabilityErrorKind, FakeCapabilityHandler,
    FakeCapabilityOutcome, MAX_FAKE_CAPABILITY_HISTORY, MAX_FAKE_CAPABILITY_OUTCOMES,
};
use plenora_runtime_worker::{WorkerContext, WorkerHandler};

#[tokio::test]
async fn fake_records_no_payload_and_consumes_scripted_outcomes_fifo() -> Result<(), Box<dyn Error>>
{
    let fake = FakeCapabilityHandler::new(FakeCapabilityConfig::new(2, 2)?)?;
    fake.script(FakeCapabilityOutcome::Failure {
        retry_class: RetryErrorClass::Retryable,
        remote_effect: CapabilityRemoteEffect::NotStarted,
    })?;
    fake.script(FakeCapabilityOutcome::Success)?;
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    builder.register_shared(
        CapabilityId::new("plenora.data-tools", 1)?,
        Arc::new(fake.clone()),
    )?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;

    let first = dispatcher
        .handle(worker_context(1), request("first-secret-payload")?)
        .await;
    let first = first.err().ok_or("scripted failure returned success")?;
    assert_eq!(first.retry_class(), RetryErrorClass::Retryable);
    dispatcher
        .handle(worker_context(2), request("second-secret-payload")?)
        .await?;

    let invocations = fake.invocations();
    assert_eq!(invocations.len(), 2);
    assert_eq!(invocations[0].payload_bytes, "first-secret-payload".len());
    assert_eq!(invocations[1].attempt, 2);
    assert!(!format!("{fake:?}").contains("secret-payload"));
    assert_eq!(fake.snapshot().pending_outcomes, 0);
    Ok(())
}

#[tokio::test]
async fn fake_bounds_history_and_outcome_scripts() -> Result<(), Box<dyn Error>> {
    let fake = FakeCapabilityHandler::new(FakeCapabilityConfig::new(1, 1)?)?;
    fake.script(FakeCapabilityOutcome::Success)?;
    assert_eq!(
        fake.script(FakeCapabilityOutcome::Success)
            .err()
            .map(plenora_runtime_testkit::FakeCapabilityError::kind),
        Some(FakeCapabilityErrorKind::OutcomeCapacityReached)
    );
    let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
    builder.register(CapabilityId::new("plenora.io-tools", 1)?, fake.clone())?;
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
    dispatcher
        .handle(worker_context(1), io_request("one")?)
        .await?;
    let second = dispatcher
        .handle(worker_context(2), io_request("two")?)
        .await;
    let second = second
        .err()
        .ok_or("full invocation history accepted work")?;
    let source = second
        .handler_failure()
        .ok_or("capacity failure did not preserve a handler source")?
        .source_error();
    assert_eq!(
        source
            .downcast_ref::<plenora_runtime_testkit::FakeCapabilityError>()
            .map(|error| error.kind()),
        Some(FakeCapabilityErrorKind::InvocationCapacityReached)
    );
    Ok(())
}

#[test]
fn fake_configuration_defaults_bounds_and_errors_are_fully_observable() -> Result<(), Box<dyn Error>>
{
    let default = FakeCapabilityConfig::default();
    assert_eq!(default.invocation_capacity, 1_024);
    assert_eq!(default.outcome_capacity, 256);
    let fake = FakeCapabilityHandler::default();
    assert_eq!(fake.snapshot().invocation_count, 0);
    assert_eq!(fake.snapshot().pending_outcomes, 0);
    assert!(format!("{fake:?}").contains("<not retained>"));

    let zero = FakeCapabilityConfig::new(0, 1)
        .err()
        .ok_or("zero fake capability bound was accepted")?;
    let excessive = FakeCapabilityConfig::new(
        MAX_FAKE_CAPABILITY_HISTORY + 1,
        MAX_FAKE_CAPABILITY_OUTCOMES + 1,
    )
    .err()
    .ok_or("excessive fake capability bounds were accepted")?;
    assert_fake_error(
        zero,
        FakeCapabilityErrorKind::ZeroCapacity,
        "fake capability capacities must be positive",
    );
    assert_fake_error(
        excessive,
        FakeCapabilityErrorKind::CapacityAboveMaximum,
        "fake capability capacity exceeds the hard maximum",
    );

    for (kind, expected) in [
        (
            FakeCapabilityErrorKind::InvocationCapacityReached,
            "fake capability invocation history is full",
        ),
        (
            FakeCapabilityErrorKind::OutcomeCapacityReached,
            "fake capability outcome script is full",
        ),
        (
            FakeCapabilityErrorKind::ScriptedFailure,
            "fake capability returned a scripted failure",
        ),
    ] {
        let error = error_for_kind(kind)?;
        assert_fake_error(error, kind, expected);
    }
    Ok(())
}

fn error_for_kind(kind: FakeCapabilityErrorKind) -> Result<FakeCapabilityError, Box<dyn Error>> {
    match kind {
        FakeCapabilityErrorKind::InvocationCapacityReached => {
            let fake = FakeCapabilityHandler::new(FakeCapabilityConfig::new(1, 1)?)?;
            let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
            builder.register(CapabilityId::new("plenora.io-tools", 1)?, fake)?;
            let dispatcher =
                CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(dispatcher.handle(worker_context(1), io_request("one")?))?;
            let failure = runtime
                .block_on(dispatcher.handle(worker_context(2), io_request("two")?))
                .err()
                .ok_or("missing invocation capacity failure")?;
            downcast_fake_error(&failure)
        }
        FakeCapabilityErrorKind::OutcomeCapacityReached => {
            let fake = FakeCapabilityHandler::new(FakeCapabilityConfig::new(1, 1)?)?;
            fake.script(FakeCapabilityOutcome::Success)?;
            fake.script(FakeCapabilityOutcome::Success)
                .err()
                .ok_or_else(|| "missing outcome capacity failure".into())
        }
        FakeCapabilityErrorKind::ScriptedFailure => {
            let fake = FakeCapabilityHandler::new(FakeCapabilityConfig::new(1, 1)?)?;
            fake.script(FakeCapabilityOutcome::Failure {
                retry_class: RetryErrorClass::Permanent,
                remote_effect: CapabilityRemoteEffect::NotStarted,
            })?;
            let mut builder = CapabilityRegistryBuilder::new(CapabilityRegistryConfig::new(1)?)?;
            builder.register(CapabilityId::new("plenora.io-tools", 1)?, fake)?;
            let dispatcher =
                CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            let failure = runtime
                .block_on(dispatcher.handle(worker_context(1), io_request("one")?))
                .err()
                .ok_or("missing scripted failure")?;
            downcast_fake_error(&failure)
        }
        FakeCapabilityErrorKind::ZeroCapacity | FakeCapabilityErrorKind::CapacityAboveMaximum => {
            Err("configuration error requested through runtime path".into())
        }
    }
}

fn downcast_fake_error(
    failure: &plenora_runtime_capabilities::CapabilityDispatchError,
) -> Result<FakeCapabilityError, Box<dyn Error>> {
    failure
        .handler_failure()
        .ok_or("dispatch error did not preserve a handler failure")?
        .source_error()
        .downcast_ref::<FakeCapabilityError>()
        .copied()
        .ok_or_else(|| "handler source was not a fake capability error".into())
}

fn assert_fake_error(error: FakeCapabilityError, kind: FakeCapabilityErrorKind, expected: &str) {
    assert_eq!(error.kind(), kind);
    assert_eq!(error.to_string(), expected);
}

fn request(payload: &str) -> Result<CapabilityRequest, Box<dyn Error>> {
    Ok(CapabilityRequest::new(
        CapabilityId::new("plenora.data-tools", 1)?,
        OperationName::new("convert")?,
        SerializedMessage::new("application/octet-stream", payload.to_owned()),
    ))
}

fn io_request(payload: &str) -> Result<CapabilityRequest, Box<dyn Error>> {
    Ok(CapabilityRequest::new(
        CapabilityId::new("plenora.io-tools", 1)?,
        OperationName::new("write")?,
        SerializedMessage::new("application/octet-stream", payload.to_owned()),
    ))
}

fn worker_context(attempt: u32) -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new(
        "fake-capability-test",
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
