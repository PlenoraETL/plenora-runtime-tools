use std::{
    fmt::{self, Debug, Formatter},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use tokio::sync::Notify;

/// Stable reason for cancelling one worker task independently of process shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum TaskCancellationReason {
    /// The configured execution deadline elapsed.
    Timeout = 1,
    /// Broker ownership could no longer be renewed safely.
    LeaseLost = 2,
    /// An owner explicitly requested cancellation.
    Requested = 3,
    /// The future driving the task was dropped before a terminal result.
    ExecutionDropped = 4,
    /// Coordinated runtime shutdown prevented task admission.
    RuntimeShutdown = 5,
    /// Worker drain prevented task admission.
    WorkerDraining = 6,
    /// The bounded active-task control registry could not admit the invocation.
    ControlCapacityUnavailable = 7,
    /// A reversible external admission gate paused new work.
    AdmissionPaused = 8,
}

impl TaskCancellationReason {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Timeout),
            2 => Some(Self::LeaseLost),
            3 => Some(Self::Requested),
            4 => Some(Self::ExecutionDropped),
            5 => Some(Self::RuntimeShutdown),
            6 => Some(Self::WorkerDraining),
            7 => Some(Self::ControlCapacityUnavailable),
            8 => Some(Self::AdmissionPaused),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct CancellationState {
    reason: AtomicU8,
    changed: Notify,
}

/// Cloneable cooperative cancellation token scoped to one worker task.
#[derive(Clone)]
pub struct TaskCancellationToken {
    state: Arc<CancellationState>,
}

impl TaskCancellationToken {
    /// Creates an active task token.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(CancellationState {
                reason: AtomicU8::new(0),
                changed: Notify::new(),
            }),
        }
    }

    /// Requests cancellation once, preserving the first observed reason.
    ///
    /// Returns true only for the caller that performs the state transition.
    #[must_use]
    pub fn cancel(&self, reason: TaskCancellationReason) -> bool {
        if self
            .state
            .reason
            .compare_exchange(0, reason as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.state.changed.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Returns the first cancellation reason, when cancellation was requested.
    #[must_use]
    pub fn reason(&self) -> Option<TaskCancellationReason> {
        TaskCancellationReason::from_code(self.state.reason.load(Ordering::Acquire))
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.reason().is_some()
    }

    /// Waits until task cancellation is requested and returns its stable reason.
    pub async fn cancelled(&self) -> TaskCancellationReason {
        loop {
            let changed = self.state.changed.notified();
            if let Some(reason) = self.reason() {
                return reason;
            }
            changed.await;
        }
    }
}

impl Default for TaskCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for TaskCancellationToken {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TaskCancellationToken")
            .field("reason", &self.reason())
            .finish()
    }
}

/// Cancellation observed through either the process or task-local signal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerCancellationReason {
    /// Coordinated runtime shutdown began.
    RuntimeShutdown,
    /// The task-local token was cancelled.
    Task(TaskCancellationReason),
}
