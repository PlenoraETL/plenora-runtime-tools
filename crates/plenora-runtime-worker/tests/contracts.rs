//! Public worker contract tests.

use std::{error::Error, time::Duration};

use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CausationId, CorrelationId, MessageId, MessageMetadata, RetryDecision,
};
use plenora_runtime_worker::{WorkerConcurrency, WorkerConfig, WorkerConfigError, WorkerContext};

#[test]
fn worker_configuration_has_explicit_validated_bounds() -> Result<(), Box<dyn Error>> {
    let concurrency = WorkerConcurrency::new(4)?;
    let config = WorkerConfig::new(concurrency, Duration::from_secs(7));

    assert_eq!(config.concurrency.max_in_flight, 4);
    assert_eq!(config.shutdown_grace_period, Duration::from_secs(7));
    assert_eq!(config.execution_timeout, None);
    assert_eq!(config.lifecycle_heartbeat_interval, None);
    assert_eq!(config.validate(), Ok(()));
    assert_eq!(
        WorkerConcurrency::new(0),
        Err(WorkerConfigError::ZeroMaxInFlight)
    );
    assert_eq!(
        WorkerConfig::new(WorkerConcurrency { max_in_flight: 1 }, Duration::ZERO,).validate(),
        Err(WorkerConfigError::ZeroShutdownGracePeriod)
    );

    let configured = WorkerConfig::new(concurrency, Duration::from_secs(7))
        .with_execution_timeout(
            Duration::from_secs(30),
            Duration::from_secs(2),
            RetryDecision::RetryAfter(Duration::from_secs(5)),
        )?
        .with_lifecycle_heartbeat(Duration::from_secs(3))?;
    assert_eq!(configured.execution_timeout, Some(Duration::from_secs(30)));
    assert_eq!(
        configured.task_cancellation_grace_period,
        Duration::from_secs(2)
    );
    assert_eq!(
        configured.timeout_retry_decision,
        RetryDecision::RetryAfter(Duration::from_secs(5))
    );
    assert_eq!(
        configured.lifecycle_heartbeat_interval,
        Some(Duration::from_secs(3))
    );
    assert_eq!(
        WorkerConfig::new(concurrency, Duration::from_secs(7)).with_execution_timeout(
            Duration::ZERO,
            Duration::from_secs(1),
            RetryDecision::DoNotRetry,
        ),
        Err(WorkerConfigError::ZeroExecutionTimeout)
    );
    assert_eq!(
        WorkerConfig::new(concurrency, Duration::from_secs(7)).with_execution_timeout(
            Duration::from_secs(1),
            Duration::ZERO,
            RetryDecision::DoNotRetry,
        ),
        Err(WorkerConfigError::ZeroTaskCancellationGracePeriod)
    );
    assert_eq!(
        WorkerConfig::new(concurrency, Duration::from_secs(7))
            .with_lifecycle_heartbeat(Duration::ZERO),
        Err(WorkerConfigError::ZeroLifecycleHeartbeatInterval)
    );

    Ok(())
}

#[test]
fn worker_context_preserves_message_identity_and_redacts_metadata_debug()
-> Result<(), Box<dyn Error>> {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("worker-test", "0.1.0", "test-1"));
    let message_id = MessageId::random();
    let correlation_id = CorrelationId::random();
    let causation_id = CausationId::random();
    let mut metadata = MessageMetadata::new();
    metadata.insert_text("application.secret", "do-not-print")?;

    let context = WorkerContext::new(
        message_id,
        correlation_id,
        Some(causation_id),
        3,
        metadata,
        runtime.shutdown_signal(),
    );
    let debug = format!("{context:?}");

    assert_eq!(context.message_id, message_id);
    assert_eq!(context.correlation_id, correlation_id);
    assert_eq!(context.causation_id, Some(causation_id));
    assert_eq!(context.attempt, 3);
    assert!(debug.contains("application.secret"));
    assert!(!debug.contains("do-not-print"));

    Ok(())
}
