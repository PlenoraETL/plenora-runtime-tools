use std::time::Duration;

use plenora_runtime_messaging::{RetryDecision, RetryPolicy};

/// One deterministic observation of a retry policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryObservation {
    /// One-based attempt number supplied to the policy.
    pub attempt: u32,
    /// Elapsed retry budget supplied to the policy.
    pub elapsed: Duration,
    /// Decision returned by the policy.
    pub decision: RetryDecision,
}

/// Evaluates a retry policy for an explicit sequence of attempts and elapsed durations.
///
/// The helper never sleeps or reads wall-clock time, so the returned observations are stable.
#[must_use]
pub fn observe_retry_decisions<E, P>(
    policy: &P,
    error: &E,
    inputs: impl IntoIterator<Item = (u32, Duration)>,
) -> Vec<RetryObservation>
where
    P: RetryPolicy<E>,
{
    inputs
        .into_iter()
        .map(|(attempt, elapsed)| RetryObservation {
            attempt,
            elapsed,
            decision: policy.decide_with_elapsed(attempt, elapsed, error),
        })
        .collect()
}
