//! Shutdown harness and deterministic retry-observation tests.

use std::{io, time::Duration};

use plenora_runtime_core::{DrainOutcome, RuntimePhase, TaskCriticality, TaskSpec};
use plenora_runtime_messaging::{
    ClassifyRetry, ExponentialBackoff, ExponentialBackoffConfig, RetryDecision, RetryErrorClass,
};
use plenora_runtime_testkit::{ShutdownHarness, observe_retry_decisions};

#[derive(Clone, Copy, Debug)]
struct RetryableError;

impl ClassifyRetry for RetryableError {
    fn retry_class(&self) -> RetryErrorClass {
        RetryErrorClass::Retryable
    }
}

#[tokio::test]
async fn shutdown_harness_exposes_an_idempotent_signal() {
    let harness = ShutdownHarness::new(Duration::from_secs(1));
    let signal = harness.signal();

    assert!(harness.trigger());
    assert!(!harness.trigger());
    signal.cancelled().await;
    assert!(signal.is_cancelled());
    assert_eq!(harness.phase(), RuntimePhase::Draining);
    assert_eq!(harness.shutdown().await, DrainOutcome::Completed);
    assert_eq!(harness.phase(), RuntimePhase::Stopped);
}

#[tokio::test]
async fn shutdown_harness_reports_a_bounded_drain_timeout() -> Result<(), Box<dyn std::error::Error>>
{
    let harness = ShutdownHarness::new(Duration::from_millis(1));
    let completion = harness.runtime().spawn(
        TaskSpec::new("blocked", TaskCriticality::Required),
        std::future::pending::<Result<(), io::Error>>(),
    )?;

    assert_eq!(
        harness.shutdown().await,
        DrainOutcome::TimedOut { remaining_tasks: 1 }
    );
    drop(completion);
    Ok(())
}

#[test]
fn retry_observations_use_only_explicit_attempts_and_elapsed_time()
-> Result<(), Box<dyn std::error::Error>> {
    let policy = ExponentialBackoff::new(ExponentialBackoffConfig {
        initial_delay: Duration::from_millis(10),
        max_delay: Duration::from_millis(40),
        max_attempts: 4,
        ..ExponentialBackoffConfig::default()
    })?;
    let observations = observe_retry_decisions(
        &policy,
        &RetryableError,
        [
            (1, Duration::ZERO),
            (2, Duration::from_millis(10)),
            (4, Duration::from_millis(30)),
        ],
    );

    assert_eq!(observations.len(), 3);
    assert_eq!(
        observations[0].decision,
        RetryDecision::RetryAfter(Duration::from_millis(10))
    );
    assert_eq!(
        observations[1].decision,
        RetryDecision::RetryAfter(Duration::from_millis(20))
    );
    assert_eq!(observations[2].decision, RetryDecision::DeadLetter);
    Ok(())
}
