//! Tests for lifecycle coordination and supervised task outcomes.

use std::{error::Error, fmt, future::pending, time::Duration};

use plenora_runtime_core::{
    DrainOutcome, HealthStatus, OptionalTaskFailurePolicy, ReadinessStatus, RuntimeConfig,
    RuntimeHandle, RuntimePhase, ServiceMetadata, SpawnError, TaskCompletionError, TaskCriticality,
    TaskFailureKind, TaskOutcome, TaskSpec,
};

#[derive(Debug)]
struct TestError(&'static str);

impl fmt::Display for TestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

fn metadata() -> ServiceMetadata {
    ServiceMetadata::new("test-service", "0.1.0", "test-instance").with_environment("test")
}

#[tokio::test]
async fn shutdown_is_idempotent() {
    let runtime = RuntimeHandle::new(metadata());
    let signal = runtime.shutdown_signal();

    assert!(runtime.request_shutdown());
    assert!(!runtime.request_shutdown());
    signal.cancelled().await;

    assert!(signal.is_cancelled());
    assert_eq!(runtime.phase(), RuntimePhase::Draining);
    assert_eq!(runtime.shutdown().await, DrainOutcome::Completed);
    assert_eq!(runtime.phase(), RuntimePhase::Stopped);
}

#[tokio::test]
async fn critical_task_failure_starts_shutdown() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(metadata());
    let signal = runtime.shutdown_signal();
    let completion = runtime.spawn(
        TaskSpec::new("critical-loop", TaskCriticality::Critical),
        async { Err(TestError("critical failure")) },
    )?;

    let report = completion.wait().await?;
    let failure = report.outcome.failure();

    assert!(matches!(
        failure.map(plenora_runtime_core::TaskFailure::kind),
        Some(TaskFailureKind::Error)
    ));
    assert!(failure.is_some_and(|failure| failure.source_error().is_some()));
    assert!(failure.and_then(Error::source).is_some());
    signal.cancelled().await;
    assert_eq!(runtime.phase(), RuntimePhase::Draining);
    assert_eq!(
        runtime.health_registry().health().status,
        HealthStatus::Unhealthy
    );
    assert_eq!(
        runtime.health_registry().readiness().status,
        ReadinessStatus::NotReady
    );
    assert_eq!(runtime.shutdown().await, DrainOutcome::Completed);
    Ok(())
}

#[tokio::test]
async fn optional_task_failure_degrades_without_stopping() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(metadata());
    let completion = runtime.spawn(
        TaskSpec::new("optional-reporter", TaskCriticality::Optional),
        async { Err(TestError("optional failure")) },
    )?;

    let report = completion.wait().await?;

    assert!(matches!(report.outcome, TaskOutcome::Failed(_)));
    assert_eq!(runtime.phase(), RuntimePhase::Running);
    assert!(!runtime.shutdown_signal().is_cancelled());
    assert_eq!(
        runtime.health_registry().health().status,
        HealthStatus::Degraded
    );
    assert_eq!(
        runtime.health_registry().readiness().status,
        ReadinessStatus::Ready
    );
    assert_eq!(runtime.task_reports().len(), 1);
    Ok(())
}

#[tokio::test]
async fn optional_failure_policy_can_ignore_health_impact() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::with_config(
        metadata(),
        RuntimeConfig {
            optional_task_failure: OptionalTaskFailurePolicy::Ignore,
            ..RuntimeConfig::default()
        },
    );
    let completion = runtime.spawn(
        TaskSpec::new("best-effort-task", TaskCriticality::Optional),
        async { Err(TestError("ignored health impact")) },
    )?;

    completion.wait().await?;

    assert_eq!(
        runtime.health_registry().health().status,
        HealthStatus::Healthy
    );
    assert!(!runtime.task_reports().is_empty());
    Ok(())
}

#[tokio::test]
async fn optional_failure_policy_can_start_shutdown() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::with_config(
        metadata(),
        RuntimeConfig {
            optional_task_failure: OptionalTaskFailurePolicy::Shutdown,
            ..RuntimeConfig::default()
        },
    );
    let completion = runtime.spawn(
        TaskSpec::new("guarded-optional-task", TaskCriticality::Optional),
        async { Err(TestError("shutdown policy")) },
    )?;

    completion.wait().await?;

    assert!(runtime.shutdown_signal().is_cancelled());
    assert_eq!(runtime.phase(), RuntimePhase::Draining);
    assert_eq!(
        runtime.health_registry().health().status,
        HealthStatus::Unhealthy
    );
    assert_eq!(runtime.shutdown().await, DrainOutcome::Completed);
    Ok(())
}

#[tokio::test]
async fn required_task_failure_removes_readiness() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(metadata());
    let completion = runtime.spawn(
        TaskSpec::new("required-listener", TaskCriticality::Required),
        async { Err(TestError("required failure")) },
    )?;

    completion.wait().await?;

    assert_eq!(runtime.phase(), RuntimePhase::Running);
    assert_eq!(
        runtime.health_registry().health().status,
        HealthStatus::Degraded
    );
    assert_eq!(
        runtime.health_registry().readiness().status,
        ReadinessStatus::NotReady
    );
    Ok(())
}

#[tokio::test]
async fn cooperative_task_finishes_during_shutdown() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(metadata());
    let shutdown = runtime.shutdown_signal();
    let completion = runtime.spawn(
        TaskSpec::new("cooperative-task", TaskCriticality::Required),
        async move {
            shutdown.cancelled().await;
            Ok::<(), TestError>(())
        },
    )?;

    assert_eq!(runtime.active_tasks(), 1);
    assert_eq!(runtime.shutdown().await, DrainOutcome::Completed);
    assert!(completion.wait().await?.outcome.is_completed());
    assert_eq!(runtime.active_tasks(), 0);
    Ok(())
}

#[tokio::test]
async fn drain_timeout_is_explicit_and_rejects_new_tasks() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::with_config(
        metadata(),
        RuntimeConfig {
            shutdown_grace_period: Duration::from_millis(10),
            ..RuntimeConfig::default()
        },
    );

    let completion = runtime.spawn(
        TaskSpec::new("hung-task", TaskCriticality::Required),
        pending::<Result<(), TestError>>(),
    )?;

    assert_eq!(
        runtime.shutdown().await,
        DrainOutcome::TimedOut { remaining_tasks: 1 }
    );
    assert_eq!(runtime.phase(), RuntimePhase::Stopped);
    let cancelled = tokio::time::timeout(Duration::from_secs(1), completion.wait()).await??;
    assert!(matches!(
        cancelled
            .outcome
            .failure()
            .map(plenora_runtime_core::TaskFailure::kind),
        Some(TaskFailureKind::Cancelled)
    ));
    assert_eq!(runtime.active_tasks(), 0);
    assert!(matches!(
        runtime.spawn(
            TaskSpec::new("late-task", TaskCriticality::Optional),
            async { Ok::<(), TestError>(()) }
        ),
        Err(SpawnError::RuntimeNotRunning(RuntimePhase::Stopped))
    ));
    Ok(())
}

#[allow(clippy::panic)]
async fn panicking_task() -> Result<(), TestError> {
    tokio::task::yield_now().await;
    panic!("supervised boom")
}

#[tokio::test]
async fn task_panic_is_captured_as_a_report() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(metadata());
    let completion = runtime.spawn(
        TaskSpec::new("panicking-task", TaskCriticality::Optional),
        panicking_task(),
    )?;

    let report = completion.wait().await?;
    let failure = report.outcome.failure();

    assert!(matches!(
        failure.map(plenora_runtime_core::TaskFailure::kind),
        Some(TaskFailureKind::Panicked)
    ));
    assert!(failure.is_some_and(|failure| failure.message() == "supervised task panicked"));
    assert!(!format!("{report:?}").contains("supervised boom"));
    assert_eq!(runtime.active_tasks(), 0);
    Ok(())
}

#[tokio::test]
async fn supervised_task_admission_has_an_explicit_bound() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::with_config(
        metadata(),
        RuntimeConfig {
            max_concurrent_tasks: 1,
            shutdown_grace_period: Duration::from_millis(10),
            ..RuntimeConfig::default()
        },
    );
    let completion = runtime.spawn(
        TaskSpec::new("capacity-holder", TaskCriticality::Optional),
        pending::<Result<(), TestError>>(),
    )?;

    assert!(matches!(
        runtime.spawn(
            TaskSpec::new("over-capacity", TaskCriticality::Optional),
            async { Ok::<(), TestError>(()) }
        ),
        Err(SpawnError::TaskCapacityExceeded { limit: 1 })
    ));

    assert_eq!(
        runtime.shutdown().await,
        DrainOutcome::TimedOut { remaining_tasks: 1 }
    );
    let _cancelled = tokio::time::timeout(Duration::from_secs(1), completion.wait()).await??;
    Ok(())
}

#[tokio::test]
async fn task_report_history_drops_the_oldest_entry_at_capacity() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::with_config(
        metadata(),
        RuntimeConfig {
            task_report_capacity: 2,
            ..RuntimeConfig::default()
        },
    );

    for name in ["first", "second", "third"] {
        runtime
            .spawn(TaskSpec::new(name, TaskCriticality::Optional), async {
                Ok::<(), TestError>(())
            })?
            .wait()
            .await?;
    }

    let names: Vec<_> = runtime
        .task_reports()
        .iter()
        .map(|report| report.spec.name.to_string())
        .collect();
    assert_eq!(names, ["second", "third"]);
    Ok(())
}

#[tokio::test]
async fn task_failure_diagnostics_redact_application_error_text() -> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(metadata());
    let report = runtime
        .spawn(
            TaskSpec::new("redaction-test", TaskCriticality::Optional),
            async { Err(TestError("secret-error-value")) },
        )?
        .wait()
        .await?;
    let failure = report
        .outcome
        .failure()
        .ok_or(TestError("missing supervised failure"))?;

    assert_eq!(failure.message(), "supervised task returned an error");
    assert!(!format!("{report:?}").contains("secret-error-value"));
    assert!(!failure.to_string().contains("secret-error-value"));
    assert!(
        failure
            .source_error()
            .is_some_and(|source| source.to_string() == "secret-error-value")
    );
    Ok(())
}

#[test]
fn spawning_without_an_async_runtime_is_rejected() {
    let runtime = RuntimeHandle::new(metadata());
    let result = runtime.spawn(
        TaskSpec::new("no-runtime", TaskCriticality::Optional),
        async { Ok::<(), TestError>(()) },
    );

    assert!(matches!(result, Err(SpawnError::NoRuntime)));
    assert_eq!(runtime.active_tasks(), 0);
}

#[test]
fn supervision_public_error_messages_cover_every_stable_category() {
    assert_eq!(
        TaskCompletionError.to_string(),
        "task supervisor closed before producing a report"
    );
    assert_eq!(
        SpawnError::NoRuntime.to_string(),
        "no Tokio runtime is active"
    );
    assert_eq!(
        SpawnError::RuntimeNotRunning(RuntimePhase::Draining).to_string(),
        "runtime is not accepting tasks in phase Draining"
    );
    assert_eq!(
        SpawnError::TaskCapacityExceeded { limit: 7 }.to_string(),
        "supervised task capacity 7 was reached"
    );
    assert_eq!(
        SpawnError::TaskIdentifierExhausted.to_string(),
        "supervised task identifier space was exhausted"
    );
}
