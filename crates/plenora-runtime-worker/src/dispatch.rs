use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};

use plenora_runtime_core::{
    ComponentHealth, ComponentReadiness, HealthRegistry, HealthStatus, ReadinessStatus,
};

use crate::{
    TaskLifecycleEvent, TaskLifecycleObserver, WorkerInstanceHeartbeat,
    WorkerInstanceHeartbeatObserver,
};

/// Default number of lifecycle observations retained in memory before backpressure is reported.
pub const DEFAULT_WORKER_LIFECYCLE_CHANNEL_CAPACITY: usize = 1_024;
/// Hard upper bound for the in-memory lifecycle observation queue.
pub const MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY: usize = 65_536;

/// Validated memory bound for worker lifecycle observation handoff.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerLifecycleChannelConfig {
    capacity: usize,
}

impl WorkerLifecycleChannelConfig {
    /// Creates a validated lifecycle channel bound.
    ///
    /// # Errors
    ///
    /// Returns an error for zero capacity or a capacity above the hard memory bound.
    pub const fn new(capacity: usize) -> Result<Self, WorkerLifecycleChannelConfigError> {
        if capacity == 0 {
            return Err(WorkerLifecycleChannelConfigError::ZeroCapacity);
        }
        if capacity > MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY {
            return Err(WorkerLifecycleChannelConfigError::CapacityTooLarge {
                capacity,
                maximum: MAX_WORKER_LIFECYCLE_CHANNEL_CAPACITY,
            });
        }
        Ok(Self { capacity })
    }

    /// Returns the maximum number of queued observations.
    #[must_use]
    pub const fn capacity(self) -> usize {
        self.capacity
    }
}

impl Default for WorkerLifecycleChannelConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_WORKER_LIFECYCLE_CHANNEL_CAPACITY,
        }
    }
}

/// Invalid lifecycle observation channel configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerLifecycleChannelConfigError {
    /// A zero-capacity channel cannot provide a non-blocking handoff.
    ZeroCapacity,
    /// The requested queue could retain more observations than the hard memory bound.
    CapacityTooLarge {
        /// Rejected capacity.
        capacity: usize,
        /// Maximum supported capacity.
        maximum: usize,
    },
}

impl Display for WorkerLifecycleChannelConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => {
                formatter.write_str("worker lifecycle channel capacity must be nonzero")
            }
            Self::CapacityTooLarge { .. } => {
                formatter.write_str("worker lifecycle channel capacity exceeds its hard bound")
            }
        }
    }
}

impl Error for WorkerLifecycleChannelConfigError {}

/// Payload-free observation accepted by the shared lifecycle dispatcher.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerLifecycleObservation {
    /// One task attempt changed state, progress, or liveness.
    Task(TaskLifecycleEvent),
    /// One worker instance changed state or reported current capacity.
    Instance(WorkerInstanceHeartbeat),
}

/// Current availability of a lifecycle dispatcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerLifecycleDispatchState {
    /// The receiver is open and at least one queue slot is available.
    Open,
    /// The receiver is open but every bounded queue slot is occupied.
    Saturated,
    /// The receiver was closed or dropped.
    Closed,
}

/// Whether lifecycle observation is optional or required for service readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerLifecycleHealthCriticality {
    /// Observation failures degrade health but do not stop admission.
    Optional,
    /// Saturation or receiver loss removes readiness until the handoff recovers.
    Required,
}

/// Observable bounded queue state and monotonic handoff counters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerLifecycleDispatchSnapshot {
    /// Configured queue capacity.
    pub capacity: usize,
    /// Observations currently waiting for the receiver.
    pub queued: usize,
    /// Observations successfully accepted into the bounded queue.
    pub accepted: u64,
    /// Observations obtained by the receiver.
    pub delivered: u64,
    /// New observations dropped because the queue was full.
    pub dropped_full: u64,
    /// New observations dropped because the receiver was closed.
    pub dropped_closed: u64,
    /// Current dispatcher availability.
    pub state: WorkerLifecycleDispatchState,
}

/// Projects lifecycle dispatcher snapshots into the shared runtime health registry.
///
/// Call [`Self::refresh`] from an application-owned supervised monitor. The dispatcher itself
/// remains non-blocking and performs no health-registry I/O inside observer callbacks.
#[derive(Clone, Debug)]
pub struct WorkerLifecycleHealthReporter {
    registry: HealthRegistry,
    component: Arc<str>,
    criticality: WorkerLifecycleHealthCriticality,
}

impl WorkerLifecycleHealthReporter {
    /// Creates a reporter for one stable lifecycle handoff component.
    #[must_use]
    pub fn new(
        registry: HealthRegistry,
        component: impl Into<Arc<str>>,
        criticality: WorkerLifecycleHealthCriticality,
    ) -> Self {
        Self {
            registry,
            component: component.into(),
            criticality,
        }
    }

    /// Publishes current lifecycle queue health and readiness.
    pub fn refresh(&self, snapshot: WorkerLifecycleDispatchSnapshot) {
        let (health, readiness, message) = match (snapshot.state, self.criticality) {
            (WorkerLifecycleDispatchState::Open, _) => {
                (HealthStatus::Healthy, ReadinessStatus::Ready, None)
            }
            (
                WorkerLifecycleDispatchState::Saturated,
                WorkerLifecycleHealthCriticality::Optional,
            ) => (
                HealthStatus::Degraded,
                ReadinessStatus::Ready,
                Some("worker lifecycle handoff is saturated"),
            ),
            (WorkerLifecycleDispatchState::Closed, WorkerLifecycleHealthCriticality::Optional) => (
                HealthStatus::Degraded,
                ReadinessStatus::Ready,
                Some("worker lifecycle receiver is closed"),
            ),
            (
                WorkerLifecycleDispatchState::Saturated,
                WorkerLifecycleHealthCriticality::Required,
            ) => (
                HealthStatus::Degraded,
                ReadinessStatus::NotReady,
                Some("required worker lifecycle handoff is saturated"),
            ),
            (WorkerLifecycleDispatchState::Closed, WorkerLifecycleHealthCriticality::Required) => (
                HealthStatus::Unhealthy,
                ReadinessStatus::NotReady,
                Some("required worker lifecycle receiver is closed"),
            ),
        };
        let message = message.map(Arc::from);
        self.registry.set_health(ComponentHealth {
            component: Arc::clone(&self.component),
            status: health,
            message: message.clone(),
        });
        self.registry.set_readiness(ComponentReadiness {
            component: Arc::clone(&self.component),
            status: readiness,
            message,
        });
    }

    /// Removes this component from both aggregate health views.
    pub fn remove(&self) {
        let _health = self.registry.remove_health(&self.component);
        let _readiness = self.registry.remove_readiness(&self.component);
    }
}

#[derive(Debug, Default)]
struct DispatchCounters {
    accepted: AtomicU64,
    delivered: AtomicU64,
    dropped_full: AtomicU64,
    dropped_closed: AtomicU64,
}

/// Cloneable, non-blocking observer that hands lifecycle events to a bounded receiver.
///
/// Saturation drops the newest observation and increments an explicit counter. Task execution and
/// broker settlement never wait for lifecycle persistence.
#[derive(Clone)]
pub struct WorkerLifecycleDispatcher {
    sender: Sender<WorkerLifecycleObservation>,
    counters: Arc<DispatchCounters>,
}

impl WorkerLifecycleDispatcher {
    /// Creates a bounded dispatcher and its single async receiver.
    #[must_use]
    pub fn channel(config: WorkerLifecycleChannelConfig) -> (Self, WorkerLifecycleReceiver) {
        let capacity = config.capacity();
        let (sender, receiver) = mpsc::channel(capacity);
        let counters = Arc::new(DispatchCounters::default());
        (
            Self {
                sender,
                counters: Arc::clone(&counters),
            },
            WorkerLifecycleReceiver {
                receiver,
                counters,
                capacity,
            },
        )
    }

    /// Returns current queue occupancy and monotonic saturation counters.
    #[must_use]
    pub fn snapshot(&self) -> WorkerLifecycleDispatchSnapshot {
        let available = self.sender.capacity();
        let capacity = self.sender.max_capacity();
        let state = if self.sender.is_closed() {
            WorkerLifecycleDispatchState::Closed
        } else if available == 0 {
            WorkerLifecycleDispatchState::Saturated
        } else {
            WorkerLifecycleDispatchState::Open
        };
        WorkerLifecycleDispatchSnapshot {
            capacity,
            queued: capacity.saturating_sub(available),
            accepted: self.counters.accepted.load(Ordering::Acquire),
            delivered: self.counters.delivered.load(Ordering::Acquire),
            dropped_full: self.counters.dropped_full.load(Ordering::Acquire),
            dropped_closed: self.counters.dropped_closed.load(Ordering::Acquire),
            state,
        }
    }

    fn dispatch(&self, observation: WorkerLifecycleObservation) {
        match self.sender.try_send(observation) {
            Ok(()) => increment(&self.counters.accepted),
            Err(TrySendError::Full(_observation)) => increment(&self.counters.dropped_full),
            Err(TrySendError::Closed(_observation)) => increment(&self.counters.dropped_closed),
        }
    }
}

impl TaskLifecycleObserver for WorkerLifecycleDispatcher {
    fn record(&self, event: TaskLifecycleEvent) {
        self.dispatch(WorkerLifecycleObservation::Task(event));
    }
}

impl WorkerInstanceHeartbeatObserver for WorkerLifecycleDispatcher {
    fn record(&self, heartbeat: WorkerInstanceHeartbeat) {
        self.dispatch(WorkerLifecycleObservation::Instance(heartbeat));
    }
}

impl Debug for WorkerLifecycleDispatcher {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerLifecycleDispatcher")
            .field("snapshot", &self.snapshot())
            .finish()
    }
}

/// Single-consumer async side of a bounded worker lifecycle dispatcher.
pub struct WorkerLifecycleReceiver {
    receiver: Receiver<WorkerLifecycleObservation>,
    counters: Arc<DispatchCounters>,
    capacity: usize,
}

impl WorkerLifecycleReceiver {
    /// Receives the next observation, or `None` after every dispatcher is dropped or the receiver
    /// has been closed and drained.
    pub async fn recv(&mut self) -> Option<WorkerLifecycleObservation> {
        let observation = self.receiver.recv().await;
        if observation.is_some() {
            increment(&self.counters.delivered);
        }
        observation
    }

    /// Prevents new observations while allowing already queued observations to be drained.
    pub fn close(&mut self) {
        self.receiver.close();
    }

    /// Returns the configured queue capacity.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Debug for WorkerLifecycleReceiver {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerLifecycleReceiver")
            .field("capacity", &self.capacity)
            .field("queued", &self.receiver.len())
            .field("closed", &self.receiver.is_closed())
            .finish_non_exhaustive()
    }
}

fn increment(counter: &AtomicU64) {
    let _previous = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(1))
    });
}
