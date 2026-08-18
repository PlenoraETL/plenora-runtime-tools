use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, SystemTime},
};

use plenora_runtime_core::{Clock, ServiceMetadata};

/// Default cadence for worker-instance heartbeats.
pub const DEFAULT_WORKER_INSTANCE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// Worker-instance heartbeat cadence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerInstanceHeartbeatConfig {
    interval: Duration,
}

impl WorkerInstanceHeartbeatConfig {
    /// Creates a validated heartbeat cadence.
    ///
    /// # Errors
    ///
    /// Returns an error when `interval` is zero.
    pub const fn new(interval: Duration) -> Result<Self, WorkerInstanceHeartbeatConfigError> {
        if interval.is_zero() {
            return Err(WorkerInstanceHeartbeatConfigError::ZeroInterval);
        }
        Ok(Self { interval })
    }

    /// Returns the delay between periodic worker-instance snapshots.
    #[must_use]
    pub const fn interval(self) -> Duration {
        self.interval
    }
}

impl Default for WorkerInstanceHeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: DEFAULT_WORKER_INSTANCE_HEARTBEAT_INTERVAL,
        }
    }
}

/// Invalid worker-instance heartbeat configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerInstanceHeartbeatConfigError {
    /// A zero interval would create a busy loop.
    ZeroInterval,
}

impl Display for WorkerInstanceHeartbeatConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("worker-instance heartbeat interval must be nonzero")
    }
}

impl Error for WorkerInstanceHeartbeatConfigError {}

/// Stable identity attached to every heartbeat from one worker instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerInstanceIdentity {
    /// Logical service name.
    pub service_name: Arc<str>,
    /// Service build or release version.
    pub service_version: Arc<str>,
    /// Unique process instance identifier.
    pub instance_id: Arc<str>,
    /// Optional deployment environment.
    pub environment: Option<Arc<str>>,
    /// Stable worker name within the service instance.
    pub worker_name: Arc<str>,
}

impl WorkerInstanceIdentity {
    /// Creates worker identity from runtime-owned process metadata.
    #[must_use]
    pub fn new(metadata: &ServiceMetadata, worker_name: impl Into<Arc<str>>) -> Self {
        Self {
            service_name: Arc::clone(&metadata.service_name),
            service_version: Arc::clone(&metadata.service_version),
            instance_id: Arc::clone(&metadata.instance_id),
            environment: metadata.environment.clone(),
            worker_name: worker_name.into(),
        }
    }
}

/// Current lifecycle state of one worker instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerInstanceStatus {
    /// The runner is being constructed and has not started polling.
    Starting,
    /// The runner accepts work up to its configured capacity.
    Ready,
    /// Admission is closed while active work drains.
    Draining,
    /// The runner has stopped.
    Stopped,
}

/// Payload-free point-in-time view of one worker instance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerInstanceHeartbeat {
    /// Stable worker and process identity.
    pub identity: WorkerInstanceIdentity,
    /// Monotonic sequence scoped to this reporter instance.
    pub sequence: u64,
    /// Observer clock time for this snapshot.
    pub observed_at: SystemTime,
    /// Current worker lifecycle state.
    pub status: WorkerInstanceStatus,
    /// Maximum concurrent handlers configured for this worker.
    pub max_in_flight: usize,
    /// Handler invocations active at observation time.
    pub in_flight: usize,
    /// Capacity currently available for new handlers.
    pub available_slots: usize,
}

/// Non-blocking observational boundary for worker-instance heartbeats.
///
/// Implementations must return quickly and must not perform network or database I/O directly.
/// Persistence adapters should hand off through an explicitly bounded queue.
pub trait WorkerInstanceHeartbeatObserver: Send + Sync {
    /// Records one payload-free worker snapshot.
    fn record(&self, heartbeat: WorkerInstanceHeartbeat);
}

/// Observer used when worker-instance monitoring is disabled.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopWorkerInstanceHeartbeatObserver;

impl WorkerInstanceHeartbeatObserver for NoopWorkerInstanceHeartbeatObserver {
    fn record(&self, _heartbeat: WorkerInstanceHeartbeat) {}
}

#[derive(Debug)]
struct ReporterState {
    status: WorkerInstanceStatus,
    sequence: u64,
}

struct ReporterInner {
    identity: WorkerInstanceIdentity,
    max_in_flight: usize,
    sample_in_flight: Arc<dyn Fn() -> usize + Send + Sync>,
    observer: Arc<dyn WorkerInstanceHeartbeatObserver>,
    clock: Arc<dyn Clock>,
    state: Mutex<ReporterState>,
}

/// Cloneable reporter tied to one executor's live capacity counters.
#[derive(Clone)]
pub struct WorkerInstanceHeartbeatReporter {
    inner: Arc<ReporterInner>,
}

impl WorkerInstanceHeartbeatReporter {
    pub(crate) fn new(
        identity: WorkerInstanceIdentity,
        max_in_flight: usize,
        sample_in_flight: Arc<dyn Fn() -> usize + Send + Sync>,
        observer: Arc<dyn WorkerInstanceHeartbeatObserver>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner: Arc::new(ReporterInner {
                identity,
                max_in_flight,
                sample_in_flight,
                observer,
                clock,
                state: Mutex::new(ReporterState {
                    status: WorkerInstanceStatus::Starting,
                    sequence: 0,
                }),
            }),
        }
    }

    /// Returns the current lifecycle state without emitting a heartbeat.
    #[must_use]
    pub fn status(&self) -> WorkerInstanceStatus {
        self.state().status
    }

    /// Emits a snapshot using the current state and live capacity counters.
    ///
    /// # Errors
    ///
    /// Returns an error if the per-reporter sequence is exhausted.
    pub fn heartbeat(&self) -> Result<WorkerInstanceHeartbeat, WorkerInstanceHeartbeatError> {
        self.record(None)
    }

    /// Marks the worker ready and emits the transition snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid reverse transition or exhausted sequence.
    pub fn mark_ready(&self) -> Result<WorkerInstanceHeartbeat, WorkerInstanceHeartbeatError> {
        self.record(Some(WorkerInstanceStatus::Ready))
    }

    /// Marks the worker draining and emits the transition snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid reverse transition or exhausted sequence.
    pub fn mark_draining(&self) -> Result<WorkerInstanceHeartbeat, WorkerInstanceHeartbeatError> {
        self.record(Some(WorkerInstanceStatus::Draining))
    }

    /// Marks the worker stopped and emits the terminal snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid reverse transition or exhausted sequence.
    pub fn mark_stopped(&self) -> Result<WorkerInstanceHeartbeat, WorkerInstanceHeartbeatError> {
        self.record(Some(WorkerInstanceStatus::Stopped))
    }

    fn record(
        &self,
        transition: Option<WorkerInstanceStatus>,
    ) -> Result<WorkerInstanceHeartbeat, WorkerInstanceHeartbeatError> {
        let mut state = self.state();
        if let Some(next) = transition {
            if status_rank(next) < status_rank(state.status) {
                return Err(WorkerInstanceHeartbeatError::InvalidTransition {
                    from: state.status,
                    to: next,
                });
            }
            state.status = next;
        }
        let sequence = state
            .sequence
            .checked_add(1)
            .ok_or(WorkerInstanceHeartbeatError::SequenceExhausted)?;
        state.sequence = sequence;
        let status = state.status;
        drop(state);

        let in_flight = (self.inner.sample_in_flight)();
        let heartbeat = WorkerInstanceHeartbeat {
            identity: self.inner.identity.clone(),
            sequence,
            observed_at: self.inner.clock.now(),
            status,
            max_in_flight: self.inner.max_in_flight,
            in_flight,
            available_slots: self.inner.max_in_flight.saturating_sub(in_flight),
        };
        self.inner.observer.record(heartbeat.clone());
        Ok(heartbeat)
    }

    fn state(&self) -> MutexGuard<'_, ReporterState> {
        match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl Debug for WorkerInstanceHeartbeatReporter {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerInstanceHeartbeatReporter")
            .field("identity", &self.inner.identity)
            .field("max_in_flight", &self.inner.max_in_flight)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

/// Worker-instance reporter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerInstanceHeartbeatError {
    /// The monotonic per-reporter sequence cannot advance further.
    SequenceExhausted,
    /// A lifecycle transition attempted to move backwards.
    InvalidTransition {
        /// Current state.
        from: WorkerInstanceStatus,
        /// Rejected target state.
        to: WorkerInstanceStatus,
    },
}

impl Display for WorkerInstanceHeartbeatError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::SequenceExhausted => {
                formatter.write_str("worker-instance heartbeat sequence is exhausted")
            }
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "worker-instance transition {from:?} -> {to:?} is invalid"
                )
            }
        }
    }
}

impl Error for WorkerInstanceHeartbeatError {}

const fn status_rank(status: WorkerInstanceStatus) -> u8 {
    match status {
        WorkerInstanceStatus::Starting => 0,
        WorkerInstanceStatus::Ready => 1,
        WorkerInstanceStatus::Draining => 2,
        WorkerInstanceStatus::Stopped => 3,
    }
}
