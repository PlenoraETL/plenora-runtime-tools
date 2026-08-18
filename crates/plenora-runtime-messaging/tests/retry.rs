//! Tests for retry classification and deterministic exponential backoff.

use std::{error::Error, time::Duration};

use plenora_runtime_messaging::{
    BackoffConfigError, ClassifyRetry, ExponentialBackoff, ExponentialBackoffConfig, JitterConfig,
    RetryDecision, RetryErrorClass, RetryExhaustedAction, RetryPolicy,
};

#[derive(Clone, Copy)]
struct ClassifiedError(RetryErrorClass);

impl ClassifyRetry for ClassifiedError {
    fn retry_class(&self) -> RetryErrorClass {
        self.0
    }
}

struct MinimalPolicy;

impl RetryPolicy<ClassifiedError> for MinimalPolicy {
    fn decide(&self, attempt: u32, _error: &ClassifiedError) -> RetryDecision {
        RetryDecision::RetryAfter(Duration::from_millis(u64::from(attempt)))
    }
}

fn policy(
    update: impl FnOnce(&mut ExponentialBackoffConfig),
) -> Result<ExponentialBackoff, BackoffConfigError> {
    let mut config = ExponentialBackoffConfig::default();
    update(&mut config);
    ExponentialBackoff::new(config)
}

#[test]
fn retryable_errors_use_capped_exponential_delays() -> Result<(), Box<dyn Error>> {
    let policy = policy(|config| {
        config.initial_delay = Duration::from_millis(100);
        config.max_delay = Duration::from_millis(250);
        config.multiplier = 2;
        config.max_attempts = 5;
    })?;
    let error = ClassifiedError(RetryErrorClass::Retryable);

    assert_eq!(
        policy.decide(1, &error),
        RetryDecision::RetryAfter(Duration::from_millis(100))
    );
    assert_eq!(
        policy.decide(2, &error),
        RetryDecision::RetryAfter(Duration::from_millis(200))
    );
    assert_eq!(
        policy.decide(3, &error),
        RetryDecision::RetryAfter(Duration::from_millis(250))
    );
    assert_eq!(policy.decide(5, &error), RetryDecision::DeadLetter);
    Ok(())
}

#[test]
fn classifications_have_distinct_safe_defaults() -> Result<(), Box<dyn Error>> {
    let policy = policy(|_| {})?;

    assert_eq!(
        policy.decide(1, &ClassifiedError(RetryErrorClass::Permanent)),
        RetryDecision::DoNotRetry
    );
    assert_eq!(
        policy.decide(1, &ClassifiedError(RetryErrorClass::DeadLetter)),
        RetryDecision::DeadLetter
    );
    assert_eq!(
        policy.decide(1, &ClassifiedError(RetryErrorClass::OutcomeUnknown)),
        RetryDecision::DoNotRetry
    );
    Ok(())
}

#[test]
fn unknown_outcome_retry_requires_explicit_opt_in() -> Result<(), Box<dyn Error>> {
    let policy = policy(|config| config.retry_unknown_outcome = true)?;

    assert_eq!(
        policy.decide(1, &ClassifiedError(RetryErrorClass::OutcomeUnknown)),
        RetryDecision::RetryAfter(Duration::from_millis(100))
    );
    Ok(())
}

#[test]
fn elapsed_budget_and_exhaustion_action_are_enforced() -> Result<(), Box<dyn Error>> {
    let policy = policy(|config| {
        config.initial_delay = Duration::from_millis(100);
        config.max_elapsed = Some(Duration::from_millis(250));
        config.exhausted_action = RetryExhaustedAction::DoNotRetry;
    })?;
    let error = ClassifiedError(RetryErrorClass::Retryable);

    assert_eq!(
        policy.decide_with_elapsed(1, Duration::from_millis(100), &error),
        RetryDecision::RetryAfter(Duration::from_millis(100))
    );
    assert_eq!(
        policy.decide_with_elapsed(2, Duration::from_millis(100), &error),
        RetryDecision::DoNotRetry
    );
    Ok(())
}

#[test]
fn jitter_is_deterministic_and_stays_within_its_bound() -> Result<(), Box<dyn Error>> {
    let policy = policy(|config| {
        config.initial_delay = Duration::from_secs(1);
        config.max_delay = Duration::from_secs(10);
        config.jitter = Some(JitterConfig {
            percent: 20,
            seed: 7,
        });
    })?;

    let first = policy.delay_for_attempt(1);
    let repeated = policy.delay_for_attempt(1);

    assert_eq!(first, repeated);
    assert!(first >= Duration::from_millis(800));
    assert!(first <= Duration::from_secs(1));
    assert_ne!(first, policy.delay_for_attempt(2));
    Ok(())
}

#[test]
fn unit_multiplier_handles_maximum_attempt_in_constant_time() -> Result<(), Box<dyn Error>> {
    let policy = policy(|config| {
        config.initial_delay = Duration::from_nanos(26);
        config.max_delay = Duration::from_nanos(u64::MAX);
        config.multiplier = 1;
        config.max_attempts = u32::MAX;
    })?;

    assert_eq!(policy.delay_for_attempt(u32::MAX), Duration::from_nanos(26));
    Ok(())
}

#[test]
fn invalid_configuration_is_rejected() {
    let cases = [
        (
            policy(|config| config.initial_delay = Duration::ZERO),
            BackoffConfigError::ZeroInitialDelay,
            "initial retry delay must be greater than zero",
        ),
        (
            policy(|config| config.max_delay = Duration::ZERO),
            BackoffConfigError::ZeroMaxDelay,
            "maximum retry delay must be greater than zero",
        ),
        (
            policy(|config| {
                config.initial_delay = Duration::from_secs(2);
                config.max_delay = Duration::from_secs(1);
            }),
            BackoffConfigError::InitialDelayExceedsMaximum,
            "initial retry delay must not exceed maximum delay",
        ),
        (
            policy(|config| config.multiplier = 0),
            BackoffConfigError::InvalidMultiplier,
            "retry multiplier must be at least one",
        ),
        (
            policy(|config| config.max_attempts = 0),
            BackoffConfigError::ZeroMaxAttempts,
            "maximum retry attempts must be at least one",
        ),
        (
            policy(|config| {
                config.jitter = Some(JitterConfig {
                    percent: 101,
                    seed: 0,
                });
            }),
            BackoffConfigError::InvalidJitterPercent,
            "retry jitter percentage must not exceed one hundred",
        ),
    ];

    for (result, expected, message) in cases {
        assert!(matches!(result, Err(error) if error == expected));
        assert_eq!(expected.to_string(), message);
    }
}

#[test]
fn public_config_and_default_elapsed_policy_are_observable() -> Result<(), Box<dyn Error>> {
    let jitter_policy = policy(|config| {
        config.jitter = Some(JitterConfig {
            percent: 0,
            seed: 99,
        });
    })?;
    assert_eq!(
        jitter_policy.config().jitter.map(|jitter| jitter.percent),
        Some(0)
    );
    assert_eq!(
        jitter_policy.delay_for_attempt(1),
        Duration::from_millis(100)
    );

    let error = ClassifiedError(RetryErrorClass::Retryable);
    assert_eq!(
        MinimalPolicy.decide_with_elapsed(7, Duration::MAX, &error),
        RetryDecision::RetryAfter(Duration::from_millis(7))
    );

    let elapsed_policy = policy(|config| {
        config.max_elapsed = Some(Duration::from_millis(100));
        config.exhausted_action = RetryExhaustedAction::DoNotRetry;
    })?;
    assert_eq!(
        elapsed_policy.decide_with_elapsed(1, Duration::from_millis(100), &error),
        RetryDecision::DoNotRetry
    );
    Ok(())
}
