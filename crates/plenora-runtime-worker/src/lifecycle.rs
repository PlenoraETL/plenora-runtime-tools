use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    num::NonZeroU64,
    sync::{Arc, Mutex, MutexGuard},
    time::SystemTime,
};

use plenora_runtime_core::{Clock, SystemClock};
use plenora_runtime_messaging::{CorrelationId, MessageId};

use crate::TaskCancellationReason;

/// Stable worker task state emitted by the lifecycle observer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    /// Execution was submitted and may be waiting for capacity.
    Queued,
    /// The handler owns an execution permit and is running.
    Running,
    /// The handler completed successfully.
    Succeeded,
    /// The handler returned an application error.
    Failed,
    /// Processing ended because task cancellation was requested.
    Cancelled(TaskCancellationReason),
    /// The configured execution deadline elapsed.
    TimedOut,
}

impl TaskState {
    const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled(_) | Self::TimedOut
        )
    }
}

/// Bounded numeric task progress without payload or arbitrary labels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskProgress {
    completed_units: u64,
    total_units: Option<NonZeroU64>,
}

impl TaskProgress {
    /// Creates validated progress counters.
    ///
    /// # Errors
    ///
    /// Returns an error when completed units exceed a supplied total.
    pub fn new(
        completed_units: u64,
        total_units: Option<NonZeroU64>,
    ) -> Result<Self, TaskProgressError> {
        if total_units.is_some_and(|total| completed_units > total.get()) {
            return Err(TaskProgressError::CompletedExceedsTotal);
        }
        Ok(Self {
            completed_units,
            total_units,
        })
    }

    /// Returns completed work units.
    #[must_use]
    pub const fn completed_units(self) -> u64 {
        self.completed_units
    }

    /// Returns the optional non-zero total work units.
    #[must_use]
    pub const fn total_units(self) -> Option<NonZeroU64> {
        self.total_units
    }
}

/// Lifecycle event payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskLifecycleEventKind {
    /// The task moved to a new lifecycle state.
    StateChanged(TaskState),
    /// The handler reported updated numeric progress.
    Progress(TaskProgress),
    /// The runtime confirmed liveness, including the latest progress when available.
    Heartbeat(Option<TaskProgress>),
}

/// Ordered, payload-free lifecycle observation for one delivery attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaskLifecycleEvent {
    /// Stable message identity.
    pub message_id: MessageId,
    /// Correlated operation identity.
    pub correlation_id: CorrelationId,
    /// One-based broker delivery attempt.
    pub attempt: u32,
    /// Monotonic per-attempt event sequence.
    pub sequence: u64,
    /// Observer timestamp supplied by an injected clock.
    pub observed_at: SystemTime,
    /// Typed event payload.
    pub kind: TaskLifecycleEventKind,
}

/// Synchronous non-blocking task lifecycle observation boundary.
///
/// Implementations must not perform blocking I/O. Persistence adapters should enqueue into an
/// explicitly bounded channel and expose saturation through their own health boundary. These
/// events are observational and never control broker settlement.
pub trait TaskLifecycleObserver: Send + Sync {
    /// Records one ordered task lifecycle event.
    fn record(&self, event: TaskLifecycleEvent);
}

/// Observer used when lifecycle reporting is not configured.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopTaskLifecycleObserver;

impl TaskLifecycleObserver for NoopTaskLifecycleObserver {
    fn record(&self, _event: TaskLifecycleEvent) {}
}

#[derive(Debug)]
struct LifecycleState {
    current: TaskState,
    sequence: u64,
    latest_progress: Option<TaskProgress>,
}

struct ReporterState {
    message_id: MessageId,
    correlation_id: CorrelationId,
    attempt: u32,
    observer: Arc<dyn TaskLifecycleObserver>,
    clock: Arc<dyn Clock>,
    lifecycle: Mutex<LifecycleState>,
}

/// Cloneable progress and heartbeat reporter scoped to one task attempt.
#[derive(Clone)]
pub struct TaskProgressReporter {
    state: Arc<ReporterState>,
}

impl TaskProgressReporter {
    pub(crate) fn start(
        message_id: MessageId,
        correlation_id: CorrelationId,
        attempt: u32,
        observer: Arc<dyn TaskLifecycleObserver>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let reporter = Self {
            state: Arc::new(ReporterState {
                message_id,
                correlation_id,
                attempt,
                observer,
                clock,
                lifecycle: Mutex::new(LifecycleState {
                    current: TaskState::Queued,
                    sequence: 0,
                    latest_progress: None,
                }),
            }),
        };
        reporter.record_initial();
        reporter
    }

    pub(crate) fn noop(message_id: MessageId, correlation_id: CorrelationId, attempt: u32) -> Self {
        Self::start(
            message_id,
            correlation_id,
            attempt,
            Arc::new(NoopTaskLifecycleObserver),
            Arc::new(SystemClock),
        )
    }

    /// Reports updated numeric progress.
    ///
    /// # Errors
    ///
    /// Returns an error after a terminal state or if the sequence space is exhausted.
    pub fn report(&self, progress: TaskProgress) -> Result<(), TaskProgressError> {
        let mut state = self.lock();
        ensure_active(&state)?;
        state.latest_progress = Some(progress);
        self.record_locked(&mut state, TaskLifecycleEventKind::Progress(progress))
    }

    /// Emits a liveness observation with the latest progress snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error after a terminal state or if the sequence space is exhausted.
    pub fn heartbeat(&self) -> Result<(), TaskProgressError> {
        let mut state = self.lock();
        ensure_active(&state)?;
        let progress = state.latest_progress;
        self.record_locked(&mut state, TaskLifecycleEventKind::Heartbeat(progress))
    }

    /// Returns the latest progress snapshot.
    #[must_use]
    pub fn latest(&self) -> Option<TaskProgress> {
        self.lock().latest_progress
    }

    pub(crate) fn transition(&self, next: TaskState) {
        let mut state = self.lock();
        if state.current.is_terminal() {
            return;
        }
        state.current = next;
        let _recorded = self.record_locked(&mut state, TaskLifecycleEventKind::StateChanged(next));
    }

    fn record_initial(&self) {
        let mut state = self.lock();
        let _recorded = self.record_locked(
            &mut state,
            TaskLifecycleEventKind::StateChanged(TaskState::Queued),
        );
    }

    fn record_locked(
        &self,
        state: &mut LifecycleState,
        kind: TaskLifecycleEventKind,
    ) -> Result<(), TaskProgressError> {
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(TaskProgressError::SequenceExhausted)?;
        state.sequence = sequence;
        self.state.observer.record(TaskLifecycleEvent {
            message_id: self.state.message_id,
            correlation_id: self.state.correlation_id,
            attempt: self.state.attempt,
            sequence,
            observed_at: self.state.clock.now(),
            kind,
        });
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, LifecycleState> {
        match self.state.lifecycle.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Debug for TaskProgressReporter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let state = self.lock();
        formatter
            .debug_struct("TaskProgressReporter")
            .field("message_id", &self.state.message_id)
            .field("correlation_id", &self.state.correlation_id)
            .field("attempt", &self.state.attempt)
            .field("current", &state.current)
            .field("sequence", &state.sequence)
            .field("latest_progress", &state.latest_progress)
            .finish()
    }
}

fn ensure_active(state: &LifecycleState) -> Result<(), TaskProgressError> {
    if state.current.is_terminal() {
        Err(TaskProgressError::TaskAlreadyTerminal)
    } else {
        Ok(())
    }
}

/// Invalid task progress operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskProgressError {
    /// Completed units must not exceed a known total.
    CompletedExceedsTotal,
    /// Progress and heartbeats cannot be emitted after a terminal event.
    TaskAlreadyTerminal,
    /// The per-attempt sequence identifier cannot be incremented further.
    SequenceExhausted,
}

impl Display for TaskProgressError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::CompletedExceedsTotal => {
                formatter.write_str("task progress completed units exceed total units")
            }
            Self::TaskAlreadyTerminal => {
                formatter.write_str("task progress cannot be reported after terminal state")
            }
            Self::SequenceExhausted => {
                formatter.write_str("task lifecycle sequence space is exhausted")
            }
        }
    }
}

impl Error for TaskProgressError {}
