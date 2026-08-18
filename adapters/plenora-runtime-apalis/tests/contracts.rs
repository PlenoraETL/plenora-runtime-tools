//! Public adapter contract tests.

use std::{error::Error, time::Duration};

use async_trait::async_trait;
use plenora_runtime_apalis::{
    ApalisAdapterConfig, ApalisAdapterConfigError, ApalisDisposition, ApalisExecutionOutcome,
    ApalisJob, ApalisWorkerService, DEFAULT_PAUSED_ADMISSION_RETRY_DELAY,
};
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CorrelationId, MessageId, MessageMetadata, RetryDecision, RetryPolicy,
};
use plenora_runtime_worker::{
    TaskCancellationReason, WorkerAdmissionReason, WorkerAdmissionState, WorkerConcurrency,
    WorkerConfig, WorkerConfigError, WorkerContext, WorkerErrorCategory, WorkerHandler,
};
use tower::ServiceExt;

#[derive(Clone, Copy, Debug)]
struct HandlerError;

impl std::fmt::Display for HandlerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("sensitive handler failure")
    }
}

impl Error for HandlerError {}

#[derive(Clone, Copy, Debug)]
struct FixedPolicy(RetryDecision);

impl RetryPolicy<HandlerError> for FixedPolicy {
    fn decide(&self, _attempt: u32, _error: &HandlerError) -> RetryDecision {
        self.0
    }
}

#[derive(Clone, Copy, Debug)]
struct FailingHandler;

#[async_trait]
impl WorkerHandler<u64> for FailingHandler {
    type Error = HandlerError;

    async fn handle(&self, _ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        Err(HandlerError)
    }
}

#[derive(Clone, Copy, Debug)]
struct CooperativeTimeoutHandler;

#[async_trait]
impl WorkerHandler<u64> for CooperativeTimeoutHandler {
    type Error = HandlerError;

    async fn handle(&self, ctx: WorkerContext, _message: u64) -> Result<(), Self::Error> {
        assert_eq!(
            ctx.cancellation.cancelled().await,
            TaskCancellationReason::Timeout
        );
        Ok(())
    }
}

#[test]
fn configuration_validates_name_and_worker_bounds() -> Result<(), Box<dyn Error>> {
    let valid = ApalisAdapterConfig::new("example-worker", worker_config(3))?;

    assert_eq!(valid.worker_name(), "example-worker");
    assert_eq!(valid.worker().concurrency.max_in_flight, 3);
    assert_eq!(
        ApalisAdapterConfig::new("  ", worker_config(1)),
        Err(ApalisAdapterConfigError::EmptyWorkerName)
    );
    assert_eq!(
        ApalisAdapterConfig::new(
            "invalid",
            WorkerConfig::new(
                WorkerConcurrency { max_in_flight: 0 },
                Duration::from_secs(1),
            ),
        ),
        Err(ApalisAdapterConfigError::Worker(
            WorkerConfigError::ZeroMaxInFlight
        ))
    );

    let empty = ApalisAdapterConfigError::EmptyWorkerName;
    assert_eq!(empty.to_string(), "Apalis worker name must not be blank");
    assert!(empty.source().is_none());
    let worker_error = ApalisAdapterConfigError::from(WorkerConfigError::ZeroMaxInFlight);
    assert_eq!(
        worker_error.to_string(),
        WorkerConfigError::ZeroMaxInFlight.to_string()
    );
    assert!(worker_error.source().is_some());

    Ok(())
}

#[tokio::test]
async fn completed_outcome_and_failure_accessors_cover_owned_contracts()
-> Result<(), Box<dyn Error>> {
    let completed = ApalisExecutionOutcome::<HandlerError>::Completed;
    assert_eq!(completed.disposition(), ApalisDisposition::Completed);
    assert!(completed.failure().is_none());
    assert!(format!("{completed:?}").contains("Completed"));
    assert!(completed.into_failure().is_none());

    let runtime = runtime();
    let service = ApalisWorkerService::new(
        FailingHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        ApalisAdapterConfig::new("owned-failure-worker", worker_config(1))?,
    )?;
    let outcome = service
        .execute(ApalisJob::new(context(&runtime, 1), 1))
        .await;
    assert_eq!(outcome.disposition(), ApalisDisposition::DoNotRetry);
    assert!(outcome.failure().is_some());
    assert!(format!("{outcome:?}").contains("Failed"));
    let error = outcome
        .into_failure()
        .ok_or_else(|| std::io::Error::other("missing failure"))?
        .into_error();
    assert_eq!(error.category(), WorkerErrorCategory::Handler);
    Ok(())
}

#[test]
fn adapter_job_debug_redacts_typed_payload() {
    let runtime = runtime();
    let job = ApalisJob::new(context(&runtime, 1), "secret-payload");
    let debug = format!("{job:?}");

    assert!(!debug.contains("secret-payload"));
    assert!(debug.contains("<redacted>"));
}

#[tokio::test]
async fn tower_service_preserves_retry_disposition_and_source() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let service = ApalisWorkerService::new(
        FailingHandler,
        FixedPolicy(RetryDecision::RetryAfter(Duration::from_secs(4))),
        ApalisAdapterConfig::new("retry-worker", worker_config(1))?,
    )?;

    let outcome = service
        .oneshot(ApalisJob::new(context(&runtime, 2), 42))
        .await?;
    let Some(failure) = outcome.into_failure() else {
        return Err(Box::new(HandlerError) as Box<dyn Error>);
    };
    let debug = format!("{failure:?}");

    assert_eq!(
        failure.disposition(),
        ApalisDisposition::RetryAfter(Duration::from_secs(4))
    );
    assert!(failure.error().source_error().is_some());
    assert!(!debug.contains("sensitive handler failure"));

    Ok(())
}

#[tokio::test]
async fn apalis_request_bridge_maps_dead_letter_without_native_retry() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let service = ApalisWorkerService::new(
        FailingHandler,
        FixedPolicy(RetryDecision::DeadLetter),
        ApalisAdapterConfig::new("dead-letter-worker", worker_config(1))?,
    )?;
    let request = apalis::prelude::Request::<_, ()>::new(ApalisJob::new(context(&runtime, 1), 7));

    let outcome = ServiceExt::oneshot(service, request).await?;

    assert_eq!(outcome.disposition(), ApalisDisposition::DeadLetter);

    Ok(())
}

#[tokio::test(start_paused = true)]
async fn execution_timeout_preserves_configured_adapter_retry() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let config = worker_config(1).with_execution_timeout(
        Duration::from_secs(2),
        Duration::from_secs(1),
        RetryDecision::RetryAfter(Duration::from_secs(3)),
    )?;
    let service = ApalisWorkerService::new(
        CooperativeTimeoutHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        ApalisAdapterConfig::new("timeout-worker", config)?,
    )?;
    let execution = tokio::spawn(async move {
        service
            .execute(ApalisJob::new(context(&runtime, 1), 9))
            .await
    });
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    let outcome = execution.await?;
    let failure = outcome
        .into_failure()
        .ok_or_else(|| std::io::Error::other("timed execution unexpectedly succeeded"))?;

    assert_eq!(
        failure.disposition(),
        ApalisDisposition::RetryAfter(Duration::from_secs(3))
    );
    assert_eq!(failure.error().category(), WorkerErrorCategory::Timeout);
    Ok(())
}

#[tokio::test]
async fn reversible_pause_maps_to_delayed_retry_without_invoking_handler()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let service = ApalisWorkerService::new(
        FailingHandler,
        FixedPolicy(RetryDecision::DoNotRetry),
        ApalisAdapterConfig::new("paused-worker", worker_config(1))?,
    )?;

    assert!(service.pause_admission());
    assert_eq!(service.admission_state(), WorkerAdmissionState::Paused);
    let outcome = service
        .execute(ApalisJob::new(context(&runtime, 1), 7))
        .await;
    let failure = outcome
        .failure()
        .ok_or_else(|| std::io::Error::other("paused worker accepted a job"))?;
    assert_eq!(
        failure.disposition(),
        ApalisDisposition::RetryAfter(DEFAULT_PAUSED_ADMISSION_RETRY_DELAY)
    );
    assert_eq!(
        failure.error().admission_reason(),
        Some(WorkerAdmissionReason::Paused)
    );

    assert!(service.resume_admission());
    assert_eq!(service.admission_state(), WorkerAdmissionState::Accepting);
    Ok(())
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new(
        "apalis-adapter-test",
        "0.1.0",
        "test-1",
    ))
}

fn context(runtime: &RuntimeHandle, attempt: u32) -> WorkerContext {
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        attempt,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}

fn worker_config(max_in_flight: usize) -> WorkerConfig {
    WorkerConfig::new(WorkerConcurrency { max_in_flight }, Duration::from_secs(1))
}
