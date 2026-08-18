use std::{
    collections::BTreeMap,
    fmt::{self, Debug, Display, Formatter},
    num::ParseIntError,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard},
    time::SystemTime,
};

use plenora_runtime_messaging::{CorrelationId, MessageId};

use crate::{TaskCancellationReason, TaskCancellationToken};

/// Executor-local identity of one admitted handler invocation.
///
/// The identifier is stable only for the lifetime of its worker executor. External control planes
/// should pair it with the worker-instance identity exposed by worker heartbeats.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkerTaskId(u64);

impl Display for WorkerTaskId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl FromStr for WorkerTaskId {
    type Err = ParseIntError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}

/// Payload-free snapshot of one admitted worker task.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActiveWorkerTask {
    /// Executor-local invocation identity.
    pub task_id: WorkerTaskId,
    /// Stable broker message identity.
    pub message_id: MessageId,
    /// Correlated operation identity.
    pub correlation_id: CorrelationId,
    /// One-based broker delivery attempt.
    pub attempt: u32,
    /// Time at which the handler acquired executor capacity.
    pub started_at: SystemTime,
    /// First cancellation reason already requested, when present.
    pub cancellation_reason: Option<TaskCancellationReason>,
}

/// Result of requesting cancellation for one executor-local task identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerTaskCancellationOutcome {
    /// The task was active and this call requested cancellation.
    Requested,
    /// The task was active but cancellation had already been requested.
    AlreadyRequested(TaskCancellationReason),
    /// No active task had this executor-local identity.
    NotFound,
}

/// Bounded aggregate returned when cancelling every active attempt for one message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerMessageCancellationReport {
    /// Active attempts matching the message identity.
    pub matched: usize,
    /// Matching attempts newly transitioned to requested cancellation.
    pub requested: usize,
    /// Matching attempts for which cancellation was already active.
    pub already_requested: usize,
}

#[derive(Clone)]
struct ActiveTaskRecord {
    snapshot: ActiveWorkerTask,
    cancellation: TaskCancellationToken,
}

struct ActiveTaskRegistryState {
    next_task_id: u64,
    tasks: BTreeMap<WorkerTaskId, ActiveTaskRecord>,
}

pub(crate) struct ActiveTaskRegistry {
    capacity: usize,
    state: Mutex<ActiveTaskRegistryState>,
}

impl ActiveTaskRegistry {
    pub(crate) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            state: Mutex::new(ActiveTaskRegistryState {
                next_task_id: 1,
                tasks: BTreeMap::new(),
            }),
        })
    }

    pub(crate) fn register(
        self: &Arc<Self>,
        message_id: MessageId,
        correlation_id: CorrelationId,
        attempt: u32,
        started_at: SystemTime,
        cancellation: TaskCancellationToken,
    ) -> Result<ActiveTaskGuard, ActiveTaskRegistrationError> {
        let mut state = self.lock();
        if state.tasks.len() >= self.capacity {
            return Err(ActiveTaskRegistrationError::CapacityExhausted);
        }
        let Some(task_id) = allocate_task_id(&mut state) else {
            return Err(ActiveTaskRegistrationError::IdentifierSpaceExhausted);
        };
        let snapshot = ActiveWorkerTask {
            task_id,
            message_id,
            correlation_id,
            attempt,
            started_at,
            cancellation_reason: None,
        };
        let _previous = state.tasks.insert(
            task_id,
            ActiveTaskRecord {
                snapshot,
                cancellation,
            },
        );
        Ok(ActiveTaskGuard {
            registry: Arc::clone(self),
            task_id,
        })
    }

    fn remove(&self, task_id: WorkerTaskId) {
        let _removed = self.lock().tasks.remove(&task_id);
    }

    fn lock(&self) -> MutexGuard<'_, ActiveTaskRegistryState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn allocate_task_id(state: &mut ActiveTaskRegistryState) -> Option<WorkerTaskId> {
    for _candidate_index in 0..=state.tasks.len() {
        let candidate = WorkerTaskId(state.next_task_id);
        state.next_task_id = state.next_task_id.wrapping_add(1);
        if state.next_task_id == 0 {
            state.next_task_id = 1;
        }
        if !state.tasks.contains_key(&candidate) {
            return Some(candidate);
        }
    }
    None
}

pub(crate) struct ActiveTaskGuard {
    registry: Arc<ActiveTaskRegistry>,
    task_id: WorkerTaskId,
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        self.registry.remove(self.task_id);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ActiveTaskRegistrationError {
    CapacityExhausted,
    IdentifierSpaceExhausted,
}

/// Cloneable control-plane handle for one worker executor's active tasks.
#[derive(Clone)]
pub struct WorkerTaskControl {
    registry: Arc<ActiveTaskRegistry>,
}

impl WorkerTaskControl {
    pub(crate) fn new(registry: Arc<ActiveTaskRegistry>) -> Self {
        Self { registry }
    }

    pub(crate) fn register(
        &self,
        message_id: MessageId,
        correlation_id: CorrelationId,
        attempt: u32,
        started_at: SystemTime,
        cancellation: TaskCancellationToken,
    ) -> Result<ActiveTaskGuard, ActiveTaskRegistrationError> {
        self.registry.register(
            message_id,
            correlation_id,
            attempt,
            started_at,
            cancellation,
        )
    }

    /// Returns the maximum number of snapshots this handle can expose.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.registry.capacity
    }

    /// Returns a bounded, task-ID-ordered snapshot of active handler invocations.
    #[must_use]
    pub fn active_tasks(&self) -> Vec<ActiveWorkerTask> {
        self.registry
            .lock()
            .tasks
            .values()
            .map(|record| {
                let mut snapshot = record.snapshot;
                snapshot.cancellation_reason = record.cancellation.reason();
                snapshot
            })
            .collect()
    }

    /// Requests cooperative cancellation for one active executor-local task.
    #[must_use]
    pub fn request_cancellation(&self, task_id: WorkerTaskId) -> WorkerTaskCancellationOutcome {
        let state = self.registry.lock();
        let Some(task) = state.tasks.get(&task_id) else {
            return WorkerTaskCancellationOutcome::NotFound;
        };
        if task.cancellation.cancel(TaskCancellationReason::Requested) {
            WorkerTaskCancellationOutcome::Requested
        } else {
            WorkerTaskCancellationOutcome::AlreadyRequested(
                task.cancellation
                    .reason()
                    .map_or(TaskCancellationReason::Requested, std::convert::identity),
            )
        }
    }

    /// Requests cooperative cancellation for every active attempt of one broker message.
    #[must_use]
    pub fn request_message_cancellation(
        &self,
        message_id: MessageId,
    ) -> WorkerMessageCancellationReport {
        let state = self.registry.lock();
        let mut report = WorkerMessageCancellationReport {
            matched: 0,
            requested: 0,
            already_requested: 0,
        };
        for task in state
            .tasks
            .values()
            .filter(|task| task.snapshot.message_id == message_id)
        {
            report.matched = report.matched.saturating_add(1);
            if task.cancellation.cancel(TaskCancellationReason::Requested) {
                report.requested = report.requested.saturating_add(1);
            } else {
                report.already_requested = report.already_requested.saturating_add(1);
            }
        }
        report
    }
}

impl Debug for WorkerTaskControl {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerTaskControl")
            .field("capacity", &self.capacity())
            .field("active_tasks", &self.registry.lock().tasks.len())
            .finish()
    }
}
