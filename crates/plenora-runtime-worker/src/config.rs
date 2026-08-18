use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    time::Duration,
};

use plenora_runtime_messaging::RetryDecision;

/// Default bounded grace after task-local cancellation is signalled.
pub const DEFAULT_TASK_CANCELLATION_GRACE_PERIOD: Duration = Duration::from_secs(1);

/// Upper bound for concurrently executing worker handlers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerConcurrency {
    /// Maximum number of handler invocations that may be in flight.
    pub max_in_flight: usize,
}

impl WorkerConcurrency {
    /// Creates a validated concurrency bound.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_in_flight` is zero.
    pub const fn new(max_in_flight: usize) -> Result<Self, WorkerConfigError> {
        if max_in_flight == 0 {
            Err(WorkerConfigError::ZeroMaxInFlight)
        } else {
            Ok(Self { max_in_flight })
        }
    }
}

impl Default for WorkerConcurrency {
    fn default() -> Self {
        Self { max_in_flight: 1 }
    }
}

/// Engine-neutral worker execution configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerConfig {
    /// Explicit concurrency bound for handler invocations.
    pub concurrency: WorkerConcurrency,
    /// Maximum time to wait for in-flight handlers during drain.
    pub shutdown_grace_period: Duration,
    /// Optional deadline for one admitted handler execution.
    pub execution_timeout: Option<Duration>,
    /// Bounded cooperative cleanup period after task-local cancellation.
    pub task_cancellation_grace_period: Duration,
    /// Retry disposition returned when execution reaches its deadline.
    pub timeout_retry_decision: RetryDecision,
    /// Optional cadence for generic task lifecycle heartbeats.
    pub lifecycle_heartbeat_interval: Option<Duration>,
}

impl WorkerConfig {
    /// Creates a worker configuration.
    #[must_use]
    pub const fn new(concurrency: WorkerConcurrency, shutdown_grace_period: Duration) -> Self {
        Self {
            concurrency,
            shutdown_grace_period,
            execution_timeout: None,
            task_cancellation_grace_period: DEFAULT_TASK_CANCELLATION_GRACE_PERIOD,
            timeout_retry_decision: RetryDecision::DoNotRetry,
            lifecycle_heartbeat_interval: None,
        }
    }

    /// Configures an execution deadline, cooperative cleanup grace, and timeout disposition.
    ///
    /// # Errors
    ///
    /// Returns an error when the deadline or cancellation grace is zero.
    pub fn with_execution_timeout(
        mut self,
        timeout: Duration,
        cancellation_grace_period: Duration,
        retry_decision: RetryDecision,
    ) -> Result<Self, WorkerConfigError> {
        self.execution_timeout = Some(timeout);
        self.task_cancellation_grace_period = cancellation_grace_period;
        self.timeout_retry_decision = retry_decision;
        self.validate()?;
        Ok(self)
    }

    /// Enables generic lifecycle heartbeat observations.
    ///
    /// # Errors
    ///
    /// Returns an error when `interval` is zero.
    pub fn with_lifecycle_heartbeat(
        mut self,
        interval: Duration,
    ) -> Result<Self, WorkerConfigError> {
        self.lifecycle_heartbeat_interval = Some(interval);
        self.validate()?;
        Ok(self)
    }

    /// Validates all worker configuration bounds.
    ///
    /// Public fields intentionally keep configuration easy to deserialize in adapters. Callers
    /// should validate after construction; `WorkerExecutor` always validates before use.
    ///
    /// # Errors
    ///
    /// Returns an error when concurrency or the shutdown grace period is zero.
    pub const fn validate(&self) -> Result<(), WorkerConfigError> {
        if self.concurrency.max_in_flight == 0 {
            return Err(WorkerConfigError::ZeroMaxInFlight);
        }
        if self.shutdown_grace_period.is_zero() {
            return Err(WorkerConfigError::ZeroShutdownGracePeriod);
        }
        if let Some(timeout) = self.execution_timeout
            && timeout.is_zero()
        {
            return Err(WorkerConfigError::ZeroExecutionTimeout);
        }
        if self.task_cancellation_grace_period.is_zero() {
            return Err(WorkerConfigError::ZeroTaskCancellationGracePeriod);
        }
        if let Some(interval) = self.lifecycle_heartbeat_interval
            && interval.is_zero()
        {
            return Err(WorkerConfigError::ZeroLifecycleHeartbeatInterval);
        }
        Ok(())
    }
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: WorkerConcurrency::default(),
            shutdown_grace_period: Duration::from_secs(30),
            execution_timeout: None,
            task_cancellation_grace_period: DEFAULT_TASK_CANCELLATION_GRACE_PERIOD,
            timeout_retry_decision: RetryDecision::DoNotRetry,
            lifecycle_heartbeat_interval: None,
        }
    }
}

/// Invalid worker configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerConfigError {
    /// At least one handler invocation must be allowed.
    ZeroMaxInFlight,
    /// Drain must have a finite, non-zero grace period.
    ZeroShutdownGracePeriod,
    /// An enabled execution deadline must be non-zero.
    ZeroExecutionTimeout,
    /// Cooperative task cancellation requires a non-zero bounded grace period.
    ZeroTaskCancellationGracePeriod,
    /// An enabled lifecycle heartbeat cadence must be non-zero.
    ZeroLifecycleHeartbeatInterval,
}

impl Display for WorkerConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMaxInFlight => {
                formatter.write_str("worker max_in_flight must be greater than zero")
            }
            Self::ZeroShutdownGracePeriod => {
                formatter.write_str("worker shutdown grace period must be greater than zero")
            }
            Self::ZeroExecutionTimeout => {
                formatter.write_str("worker execution timeout must be greater than zero")
            }
            Self::ZeroTaskCancellationGracePeriod => formatter
                .write_str("worker task cancellation grace period must be greater than zero"),
            Self::ZeroLifecycleHeartbeatInterval => {
                formatter.write_str("worker lifecycle heartbeat interval must be greater than zero")
            }
        }
    }
}

impl Error for WorkerConfigError {}
