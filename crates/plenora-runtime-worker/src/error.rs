use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    time::Duration,
};

use plenora_runtime_messaging::RetryDecision;

use crate::TaskCancellationReason;

/// Stable category for an engine-neutral worker execution error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerErrorCategory {
    /// Work was rejected because shutdown or drain had begun.
    Shutdown,
    /// The application handler returned an error.
    Handler,
    /// The configured execution deadline elapsed.
    Timeout,
    /// Task-local cooperative cancellation was requested.
    Cancelled,
    /// A bounded internal admission capacity was unavailable.
    Capacity,
}

/// Worker phase in which execution failed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerExecutionPhase {
    /// The job was waiting to be admitted under the concurrency bound.
    Admission,
    /// The application handler was executing.
    Handling,
}

/// Certainty about effects performed by a failed worker invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerRemoteEffect {
    /// The handler was not invoked.
    NotStarted,
    /// The handler failed and its external effects cannot be inferred by the runtime.
    Unknown,
}

/// Reason a job was rejected before handler invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerAdmissionReason {
    /// The worker executor has entered its drain phase.
    Draining,
    /// The runtime shutdown signal was already observed.
    ShutdownRequested,
    /// The active-task control registry could not mirror an acquired execution permit.
    ControlCapacityUnavailable,
    /// A reversible external admission gate paused new work before handler invocation.
    Paused,
}

enum WorkerExecutionErrorKind<E> {
    Admission(WorkerAdmissionReason),
    Handler {
        source: E,
        retry_decision: RetryDecision,
    },
    TimedOut {
        timeout: Duration,
        retry_decision: RetryDecision,
    },
    Cancelled(TaskCancellationReason),
}

/// Structured worker execution error that preserves handler source and retry disposition.
pub struct WorkerExecutionError<E> {
    kind: WorkerExecutionErrorKind<E>,
}

impl<E> WorkerExecutionError<E> {
    pub(crate) const fn admission(reason: WorkerAdmissionReason) -> Self {
        Self {
            kind: WorkerExecutionErrorKind::Admission(reason),
        }
    }

    pub(crate) const fn handler(source: E, retry_decision: RetryDecision) -> Self {
        Self {
            kind: WorkerExecutionErrorKind::Handler {
                source,
                retry_decision,
            },
        }
    }

    pub(crate) const fn timed_out(timeout: Duration, retry_decision: RetryDecision) -> Self {
        Self {
            kind: WorkerExecutionErrorKind::TimedOut {
                timeout,
                retry_decision,
            },
        }
    }

    pub(crate) const fn cancelled(reason: TaskCancellationReason) -> Self {
        Self {
            kind: WorkerExecutionErrorKind::Cancelled(reason),
        }
    }

    /// Returns the broad error category.
    #[must_use]
    pub const fn category(&self) -> WorkerErrorCategory {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(
                WorkerAdmissionReason::Draining | WorkerAdmissionReason::ShutdownRequested,
            ) => WorkerErrorCategory::Shutdown,
            WorkerExecutionErrorKind::Admission(
                WorkerAdmissionReason::ControlCapacityUnavailable | WorkerAdmissionReason::Paused,
            ) => WorkerErrorCategory::Capacity,
            WorkerExecutionErrorKind::Handler { .. } => WorkerErrorCategory::Handler,
            WorkerExecutionErrorKind::TimedOut { .. } => WorkerErrorCategory::Timeout,
            WorkerExecutionErrorKind::Cancelled(_) => WorkerErrorCategory::Cancelled,
        }
    }

    /// Returns the execution phase in which the failure occurred.
    #[must_use]
    pub const fn phase(&self) -> WorkerExecutionPhase {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(_) => WorkerExecutionPhase::Admission,
            WorkerExecutionErrorKind::Handler { .. }
            | WorkerExecutionErrorKind::TimedOut { .. }
            | WorkerExecutionErrorKind::Cancelled(_) => WorkerExecutionPhase::Handling,
        }
    }

    /// Returns the runtime's certainty about external effects.
    #[must_use]
    pub const fn remote_effect(&self) -> WorkerRemoteEffect {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(_) => WorkerRemoteEffect::NotStarted,
            WorkerExecutionErrorKind::Handler { .. }
            | WorkerExecutionErrorKind::TimedOut { .. }
            | WorkerExecutionErrorKind::Cancelled(_) => WorkerRemoteEffect::Unknown,
        }
    }

    /// Returns the injected retry policy decision for a handler failure.
    #[must_use]
    pub const fn retry_decision(&self) -> Option<RetryDecision> {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(_) | WorkerExecutionErrorKind::Cancelled(_) => None,
            WorkerExecutionErrorKind::Handler { retry_decision, .. }
            | WorkerExecutionErrorKind::TimedOut { retry_decision, .. } => Some(*retry_decision),
        }
    }

    /// Returns why the job was not admitted, when applicable.
    #[must_use]
    pub const fn admission_reason(&self) -> Option<WorkerAdmissionReason> {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(reason) => Some(*reason),
            WorkerExecutionErrorKind::Handler { .. }
            | WorkerExecutionErrorKind::TimedOut { .. }
            | WorkerExecutionErrorKind::Cancelled(_) => None,
        }
    }

    /// Returns the task-local cancellation reason, when applicable.
    #[must_use]
    pub const fn cancellation_reason(&self) -> Option<TaskCancellationReason> {
        match &self.kind {
            WorkerExecutionErrorKind::Cancelled(reason) => Some(*reason),
            WorkerExecutionErrorKind::Admission(_)
            | WorkerExecutionErrorKind::Handler { .. }
            | WorkerExecutionErrorKind::TimedOut { .. } => None,
        }
    }

    /// Returns the configured deadline that elapsed, when applicable.
    #[must_use]
    pub const fn execution_timeout(&self) -> Option<Duration> {
        match &self.kind {
            WorkerExecutionErrorKind::TimedOut { timeout, .. } => Some(*timeout),
            WorkerExecutionErrorKind::Admission(_)
            | WorkerExecutionErrorKind::Handler { .. }
            | WorkerExecutionErrorKind::Cancelled(_) => None,
        }
    }

    /// Returns the original handler error, when handler execution began.
    #[must_use]
    pub const fn source_error(&self) -> Option<&E> {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(_) => None,
            WorkerExecutionErrorKind::Handler { source, .. } => Some(source),
            WorkerExecutionErrorKind::TimedOut { .. } | WorkerExecutionErrorKind::Cancelled(_) => {
                None
            }
        }
    }

    /// Consumes the wrapper and returns the original handler error, when present.
    #[must_use]
    pub fn into_source(self) -> Option<E> {
        match self.kind {
            WorkerExecutionErrorKind::Admission(_) => None,
            WorkerExecutionErrorKind::Handler { source, .. } => Some(source),
            WorkerExecutionErrorKind::TimedOut { .. } | WorkerExecutionErrorKind::Cancelled(_) => {
                None
            }
        }
    }
}

impl<E> Display for WorkerExecutionError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            WorkerExecutionErrorKind::Admission(WorkerAdmissionReason::Draining) => {
                formatter.write_str("worker is draining and no longer accepts jobs")
            }
            WorkerExecutionErrorKind::Admission(WorkerAdmissionReason::ShutdownRequested) => {
                formatter.write_str("runtime shutdown was requested before handler admission")
            }
            WorkerExecutionErrorKind::Admission(
                WorkerAdmissionReason::ControlCapacityUnavailable,
            ) => formatter.write_str("worker task-control capacity is unavailable"),
            WorkerExecutionErrorKind::Admission(WorkerAdmissionReason::Paused) => {
                formatter.write_str("worker admission is temporarily paused")
            }
            WorkerExecutionErrorKind::Handler { .. } => {
                formatter.write_str("worker handler failed")
            }
            WorkerExecutionErrorKind::TimedOut { .. } => {
                formatter.write_str("worker handler execution timed out")
            }
            WorkerExecutionErrorKind::Cancelled(_) => {
                formatter.write_str("worker handler execution was cancelled")
            }
        }
    }
}

impl<E> Debug for WorkerExecutionError<E> {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerExecutionError")
            .field("category", &self.category())
            .field("phase", &self.phase())
            .field("remote_effect", &self.remote_effect())
            .field("retry_decision", &self.retry_decision())
            .field("admission_reason", &self.admission_reason())
            .field("cancellation_reason", &self.cancellation_reason())
            .field("execution_timeout", &self.execution_timeout())
            .field(
                "source",
                &self.source_error().map(|_| "<preserved; redacted>"),
            )
            .finish()
    }
}

impl<E> Error for WorkerExecutionError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source_error()
            .map(|source| source as &(dyn Error + 'static))
    }
}
