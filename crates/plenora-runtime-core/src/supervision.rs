use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use tokio::sync::oneshot;

use crate::RuntimePhase;

/// Determines how runtime health reacts to a supervised task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskCriticality {
    /// Failure makes the runtime unhealthy and starts coordinated shutdown.
    Critical,
    /// Failure makes the runtime degraded and not ready without forcing termination.
    Required,
    /// Failure follows the configured optional-task policy.
    Optional,
}

/// Stable metadata for a supervised task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSpec {
    /// Stable task name used by reports and health components.
    pub name: Arc<str>,
    /// Operational importance of the task.
    pub criticality: TaskCriticality,
}

impl TaskSpec {
    /// Creates a task specification.
    #[must_use]
    pub fn new(name: impl Into<Arc<str>>, criticality: TaskCriticality) -> Self {
        Self {
            name: name.into(),
            criticality,
        }
    }
}

/// Classification of a supervised task failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskFailureKind {
    /// The task returned an error.
    Error,
    /// The task panicked.
    Panicked,
    /// The asynchronous runtime cancelled the task.
    Cancelled,
}

/// Failure information captured by task supervision.
#[derive(Clone)]
pub struct TaskFailure {
    kind: TaskFailureKind,
    message: Arc<str>,
    source: Option<Arc<dyn Error + Send + Sync + 'static>>,
}

impl TaskFailure {
    /// Returns the failure classification.
    #[must_use]
    pub const fn kind(&self) -> TaskFailureKind {
        self.kind
    }

    /// Returns a stable redaction-safe failure description.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the original task error when one is available.
    ///
    /// This is an explicit diagnostic escape hatch: the source may contain application-sensitive
    /// text and must not be logged without a caller-owned redaction policy.
    #[must_use]
    pub fn source_error(&self) -> Option<&(dyn Error + Send + Sync + 'static)> {
        self.source.as_deref()
    }

    pub(crate) fn from_error<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind: TaskFailureKind::Error,
            message: Arc::from("supervised task returned an error"),
            source: Some(Arc::new(error)),
        }
    }

    pub(crate) fn from_panic(_payload: &(dyn std::any::Any + Send + 'static)) -> Self {
        Self {
            kind: TaskFailureKind::Panicked,
            message: Arc::from("supervised task panicked"),
            source: None,
        }
    }

    pub(crate) fn cancelled(_message: impl Into<Arc<str>>) -> Self {
        Self {
            kind: TaskFailureKind::Cancelled,
            message: Arc::from("supervised task was cancelled"),
            source: None,
        }
    }
}

impl Debug for TaskFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskFailure")
            .field("kind", &self.kind)
            .field("message", &self.message)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl Display for TaskFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TaskFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Final outcome of a supervised task.
#[derive(Clone, Debug)]
pub enum TaskOutcome {
    /// The task returned successfully.
    Completed,
    /// The task returned an error, panicked, or was cancelled.
    Failed(TaskFailure),
}

impl TaskOutcome {
    /// Returns whether the task completed without failure.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        matches!(self, Self::Completed)
    }

    /// Returns captured failure information, if any.
    #[must_use]
    pub const fn failure(&self) -> Option<&TaskFailure> {
        match self {
            Self::Completed => None,
            Self::Failed(failure) => Some(failure),
        }
    }
}

/// Immutable completion report for one supervised task.
#[derive(Clone, Debug)]
pub struct TaskReport {
    /// Task metadata supplied at spawn time.
    pub spec: TaskSpec,
    /// Captured task outcome.
    pub outcome: TaskOutcome,
}

/// Policy applied when an optional supervised task fails.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OptionalTaskFailurePolicy {
    /// Record the failure without changing aggregate health.
    Ignore,
    /// Mark aggregate health as degraded while preserving readiness.
    #[default]
    MarkDegraded,
    /// Mark the runtime unhealthy and start coordinated shutdown.
    Shutdown,
}

/// A completion receiver returned for a successfully spawned task.
#[derive(Debug)]
pub struct TaskCompletion {
    receiver: oneshot::Receiver<TaskReport>,
}

impl TaskCompletion {
    pub(crate) const fn new(receiver: oneshot::Receiver<TaskReport>) -> Self {
        Self { receiver }
    }

    /// Waits for the supervision report.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime that owns the supervisor is dropped before reporting.
    pub async fn wait(self) -> Result<TaskReport, TaskCompletionError> {
        self.receiver.await.map_err(|_| TaskCompletionError)
    }
}

/// Error returned when a task supervisor disappears before reporting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskCompletionError;

impl Display for TaskCompletionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("task supervisor closed before producing a report")
    }
}

impl Error for TaskCompletionError {}

/// Error returned when a supervised task cannot be started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpawnError {
    /// No asynchronous runtime is active on the current thread.
    NoRuntime,
    /// Runtime draining or termination has already started.
    RuntimeNotRunning(RuntimePhase),
    /// The configured supervised-task concurrency bound was reached.
    TaskCapacityExceeded {
        /// Configured maximum number of concurrently active tasks.
        limit: usize,
    },
    /// The deterministic internal task identifier space was exhausted.
    TaskIdentifierExhausted,
}

impl Display for SpawnError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoRuntime => formatter.write_str("no Tokio runtime is active"),
            Self::RuntimeNotRunning(phase) => {
                write!(
                    formatter,
                    "runtime is not accepting tasks in phase {phase:?}"
                )
            }
            Self::TaskCapacityExceeded { limit } => {
                write!(formatter, "supervised task capacity {limit} was reached")
            }
            Self::TaskIdentifierExhausted => {
                formatter.write_str("supervised task identifier space was exhausted")
            }
        }
    }
}

impl Error for SpawnError {}
