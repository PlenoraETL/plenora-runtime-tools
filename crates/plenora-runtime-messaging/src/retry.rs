use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    time::Duration,
};

/// Decision returned by a retry policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryDecision {
    /// Retry after the supplied delay.
    RetryAfter(Duration),
    /// Stop processing without retry.
    DoNotRetry,
    /// Move the message to dead-letter handling.
    DeadLetter,
}

/// Error classification consumed by the standard retry policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryErrorClass {
    /// A later attempt may succeed.
    Retryable,
    /// Retrying cannot make this error succeed.
    Permanent,
    /// The message should move directly to dead-letter handling.
    DeadLetter,
    /// The remote effect is unknown and retry is unsafe by default.
    OutcomeUnknown,
}

/// Allows an error to describe its retry semantics without depending on a worker engine.
pub trait ClassifyRetry {
    /// Returns the retry class of this error.
    fn retry_class(&self) -> RetryErrorClass;
}

/// Broker-neutral retry policy.
pub trait RetryPolicy<E>: Send + Sync {
    /// Decides what to do after an attempt fails.
    fn decide(&self, attempt: u32, error: &E) -> RetryDecision;

    /// Decides with elapsed-time information when the caller tracks a retry budget.
    fn decide_with_elapsed(&self, attempt: u32, _elapsed: Duration, error: &E) -> RetryDecision {
        self.decide(attempt, error)
    }
}

/// Action used when retry limits are exhausted.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RetryExhaustedAction {
    /// Stop retrying without dead-letter routing.
    DoNotRetry,
    /// Route to dead-letter handling.
    #[default]
    DeadLetter,
}

/// Deterministic bounded jitter configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JitterConfig {
    /// Maximum percentage subtracted from the nominal delay.
    pub percent: u8,
    /// Seed used to vary delay sequences between policy instances.
    pub seed: u64,
}

/// Configuration for exponential retry delay calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExponentialBackoffConfig {
    /// Delay after the first failed attempt.
    pub initial_delay: Duration,
    /// Upper bound for one retry delay.
    pub max_delay: Duration,
    /// Integer multiplier applied for each subsequent attempt.
    pub multiplier: u32,
    /// Maximum total delivery attempts, including the first attempt.
    pub max_attempts: u32,
    /// Optional elapsed-time retry budget.
    pub max_elapsed: Option<Duration>,
    /// Optional deterministic jitter.
    pub jitter: Option<JitterConfig>,
    /// Explicit opt-in to retry when a remote effect is unknown.
    pub retry_unknown_outcome: bool,
    /// Action used when attempt or elapsed-time limits are exhausted.
    pub exhausted_action: RetryExhaustedAction,
}

impl Default for ExponentialBackoffConfig {
    fn default() -> Self {
        Self {
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            multiplier: 2,
            max_attempts: 5,
            max_elapsed: None,
            jitter: None,
            retry_unknown_outcome: false,
            exhausted_action: RetryExhaustedAction::DeadLetter,
        }
    }
}

/// Invalid exponential backoff configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackoffConfigError {
    /// Initial delay must be greater than zero.
    ZeroInitialDelay,
    /// Maximum delay must be greater than zero.
    ZeroMaxDelay,
    /// Initial delay exceeds maximum delay.
    InitialDelayExceedsMaximum,
    /// Multiplier must be at least one.
    InvalidMultiplier,
    /// Maximum attempts must be at least one.
    ZeroMaxAttempts,
    /// Jitter percentage exceeds one hundred.
    InvalidJitterPercent,
}

impl Display for BackoffConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::ZeroInitialDelay => "initial retry delay must be greater than zero",
            Self::ZeroMaxDelay => "maximum retry delay must be greater than zero",
            Self::InitialDelayExceedsMaximum => "initial retry delay must not exceed maximum delay",
            Self::InvalidMultiplier => "retry multiplier must be at least one",
            Self::ZeroMaxAttempts => "maximum retry attempts must be at least one",
            Self::InvalidJitterPercent => "retry jitter percentage must not exceed one hundred",
        };
        formatter.write_str(message)
    }
}

impl Error for BackoffConfigError {}

/// Exponential backoff policy with bounded optional jitter and elapsed-time limits.
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    config: ExponentialBackoffConfig,
}

impl ExponentialBackoff {
    /// Validates configuration and creates a retry policy.
    ///
    /// # Errors
    ///
    /// Returns an error when delays, multiplier, attempts, or jitter are outside valid bounds.
    pub fn new(config: ExponentialBackoffConfig) -> Result<Self, BackoffConfigError> {
        validate_config(config)?;
        Ok(Self { config })
    }

    /// Returns the validated policy configuration.
    #[must_use]
    pub const fn config(&self) -> ExponentialBackoffConfig {
        self.config
    }

    /// Returns the retry delay for an attempt before elapsed-time checks.
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let attempt = attempt.max(1);
        let exponent = attempt.saturating_sub(1);
        let mut delay = self.config.initial_delay;

        if self.config.multiplier == 1 {
            return apply_jitter(delay, attempt, self.config.jitter);
        }

        for _ in 0..exponent {
            delay = match delay.checked_mul(self.config.multiplier) {
                Some(next) if next < self.config.max_delay => next,
                _ => self.config.max_delay,
            };
            if delay == self.config.max_delay {
                break;
            }
        }

        apply_jitter(delay, attempt, self.config.jitter)
    }

    const fn exhausted_decision(&self) -> RetryDecision {
        match self.config.exhausted_action {
            RetryExhaustedAction::DoNotRetry => RetryDecision::DoNotRetry,
            RetryExhaustedAction::DeadLetter => RetryDecision::DeadLetter,
        }
    }

    fn decide_class(
        &self,
        attempt: u32,
        elapsed: Duration,
        error_class: RetryErrorClass,
    ) -> RetryDecision {
        match error_class {
            RetryErrorClass::Permanent => return RetryDecision::DoNotRetry,
            RetryErrorClass::DeadLetter => return RetryDecision::DeadLetter,
            RetryErrorClass::OutcomeUnknown if !self.config.retry_unknown_outcome => {
                return RetryDecision::DoNotRetry;
            }
            RetryErrorClass::Retryable | RetryErrorClass::OutcomeUnknown => {}
        }

        let attempt = attempt.max(1);
        if attempt >= self.config.max_attempts {
            return self.exhausted_decision();
        }

        let delay = self.delay_for_attempt(attempt);
        if let Some(max_elapsed) = self.config.max_elapsed {
            if elapsed >= max_elapsed {
                return self.exhausted_decision();
            }
            match elapsed.checked_add(delay) {
                Some(projected) if projected <= max_elapsed => {}
                _ => return self.exhausted_decision(),
            }
        }

        RetryDecision::RetryAfter(delay)
    }
}

impl<E> RetryPolicy<E> for ExponentialBackoff
where
    E: ClassifyRetry,
{
    fn decide(&self, attempt: u32, error: &E) -> RetryDecision {
        self.decide_class(attempt, Duration::ZERO, error.retry_class())
    }

    fn decide_with_elapsed(&self, attempt: u32, elapsed: Duration, error: &E) -> RetryDecision {
        self.decide_class(attempt, elapsed, error.retry_class())
    }
}

fn validate_config(config: ExponentialBackoffConfig) -> Result<(), BackoffConfigError> {
    if config.initial_delay.is_zero() {
        return Err(BackoffConfigError::ZeroInitialDelay);
    }
    if config.max_delay.is_zero() {
        return Err(BackoffConfigError::ZeroMaxDelay);
    }
    if config.initial_delay > config.max_delay {
        return Err(BackoffConfigError::InitialDelayExceedsMaximum);
    }
    if config.multiplier == 0 {
        return Err(BackoffConfigError::InvalidMultiplier);
    }
    if config.max_attempts == 0 {
        return Err(BackoffConfigError::ZeroMaxAttempts);
    }
    if config.jitter.is_some_and(|jitter| jitter.percent > 100) {
        return Err(BackoffConfigError::InvalidJitterPercent);
    }
    Ok(())
}

fn apply_jitter(delay: Duration, attempt: u32, jitter: Option<JitterConfig>) -> Duration {
    let Some(jitter) = jitter else {
        return delay;
    };
    if jitter.percent == 0 {
        return delay;
    }

    let nominal = delay.as_nanos();
    let spread = nominal.saturating_mul(u128::from(jitter.percent)) / 100;
    let lower_bound = nominal.saturating_sub(spread);
    let sample = u128::from(mix(jitter.seed ^ u64::from(attempt)));
    let offset = sample % spread.saturating_add(1);
    duration_from_nanos(lower_bound.saturating_add(offset))
}

const fn mix(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    const NANOS_PER_SECOND: u128 = 1_000_000_000;
    let seconds = nanos / NANOS_PER_SECOND;
    let subsecond_nanos = nanos % NANOS_PER_SECOND;

    match (u64::try_from(seconds), u32::try_from(subsecond_nanos)) {
        (Ok(seconds), Ok(subsecond_nanos)) => Duration::new(seconds, subsecond_nanos),
        _ => Duration::MAX,
    }
}
