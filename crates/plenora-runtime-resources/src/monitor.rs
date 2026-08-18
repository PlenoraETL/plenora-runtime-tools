use std::sync::{Arc, Mutex, MutexGuard};

use plenora_runtime_core::{
    ComponentHealth, ComponentReadiness, HealthRegistry, HealthStatus, ReadinessStatus,
    ShutdownSignal,
};
use plenora_runtime_worker::WorkerAdmissionControl;
use tokio::time::{MissedTickBehavior, interval};

use crate::{MemoryPressureConfig, MemorySampleError, MemorySampler};

const MEMORY_COMPONENT: &str = "runtime.memory";

/// Current process-memory pressure classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryPressureState {
    /// No successful sample has been observed yet.
    Initializing,
    /// Resident memory is below the configured soft limit.
    Normal,
    /// The soft limit was confirmed and new work is paused.
    Pressured,
    /// The hard limit was reached and process health is unhealthy.
    Critical,
    /// Sampling failed; admission remains fail-closed until a successful recovery sample.
    Unavailable,
}

/// Immutable current memory-pressure view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPressureSnapshot {
    /// Monotonic observation sequence.
    pub sequence: u64,
    /// Current pressure classification.
    pub state: MemoryPressureState,
    /// Most recent successfully sampled resident bytes.
    pub resident_bytes: Option<u64>,
}

/// One emitted state observation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPressureObservation {
    /// State after applying the sample.
    pub snapshot: MemoryPressureSnapshot,
    /// Whether this sample changed the pressure classification.
    pub changed: bool,
    /// Whether the sample itself succeeded.
    pub sample_succeeded: bool,
}

/// Non-blocking observation boundary for metrics and operational persistence.
pub trait MemoryPressureObserver: Send + Sync {
    /// Records one redaction-safe pressure observation.
    fn record(&self, observation: MemoryPressureObservation);
}

/// Observer that intentionally discards pressure observations.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMemoryPressureObserver;

impl MemoryPressureObserver for NoopMemoryPressureObserver {
    fn record(&self, _observation: MemoryPressureObservation) {}
}

/// Bounded monitor-loop completion report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MemoryPressureRunReport {
    /// Total sampling attempts made by this run.
    pub samples: u64,
    /// Sampling attempts that failed.
    pub failures: u64,
    /// Final pressure state when shutdown was observed.
    pub final_state: MemoryPressureState,
}

#[derive(Debug)]
struct MonitorState {
    snapshot: MemoryPressureSnapshot,
    pressure_streak: u32,
    recovery_streak: u32,
}

/// Generic fail-closed memory monitor with hysteresis and reversible worker admission.
pub struct MemoryPressureMonitor<S, A, O = NoopMemoryPressureObserver> {
    config: MemoryPressureConfig,
    sampler: Arc<S>,
    admission: Arc<A>,
    observer: Arc<O>,
    health: HealthRegistry,
    state: Mutex<MonitorState>,
}

impl<S, A> MemoryPressureMonitor<S, A, NoopMemoryPressureObserver>
where
    S: MemorySampler,
    A: WorkerAdmissionControl,
{
    /// Creates a fail-closed monitor with a no-op observer.
    #[must_use]
    pub fn new(
        config: MemoryPressureConfig,
        sampler: Arc<S>,
        admission: Arc<A>,
        health: HealthRegistry,
    ) -> Self {
        Self::with_observer(
            config,
            sampler,
            admission,
            Arc::new(NoopMemoryPressureObserver),
            health,
        )
    }
}

impl<S, A, O> MemoryPressureMonitor<S, A, O>
where
    S: MemorySampler,
    A: WorkerAdmissionControl,
    O: MemoryPressureObserver,
{
    /// Creates a fail-closed monitor with an explicit non-blocking observer.
    #[must_use]
    pub fn with_observer(
        config: MemoryPressureConfig,
        sampler: Arc<S>,
        admission: Arc<A>,
        observer: Arc<O>,
        health: HealthRegistry,
    ) -> Self {
        let _paused = admission.pause_admission();
        set_health(&health, MemoryPressureState::Initializing);
        Self {
            config,
            sampler,
            admission,
            observer,
            health,
            state: Mutex::new(MonitorState {
                snapshot: MemoryPressureSnapshot {
                    sequence: 0,
                    state: MemoryPressureState::Initializing,
                    resident_bytes: None,
                },
                pressure_streak: 0,
                recovery_streak: 0,
            }),
        }
    }

    /// Returns the validated monitor configuration.
    #[must_use]
    pub const fn config(&self) -> MemoryPressureConfig {
        self.config
    }

    /// Returns the current immutable pressure view.
    #[must_use]
    pub fn snapshot(&self) -> MemoryPressureSnapshot {
        self.lock_state().snapshot
    }

    /// Samples and applies memory pressure once.
    ///
    /// Sampling failure is fail-closed: readiness becomes false and admission remains paused.
    ///
    /// # Errors
    ///
    /// Returns the source-preserving sampler failure after updating fail-closed state.
    pub fn sample_once(&self) -> Result<MemoryPressureObservation, MemorySampleError> {
        match self.sampler.sample() {
            Ok(sample) => Ok(self.apply_sample(sample.resident_bytes)),
            Err(error) => {
                self.apply_unavailable();
                Err(error)
            }
        }
    }

    /// Runs bounded periodic sampling until process shutdown is observed.
    pub async fn run(&self, shutdown: ShutdownSignal) -> MemoryPressureRunReport {
        let mut ticker = interval(self.config.sample_interval());
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut samples = 0_u64;
        let mut failures = 0_u64;

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                _instant = ticker.tick() => {
                    samples = samples.saturating_add(1);
                    if self.sample_once().is_err() {
                        failures = failures.saturating_add(1);
                    }
                }
            }
        }

        MemoryPressureRunReport {
            samples,
            failures,
            final_state: self.snapshot().state,
        }
    }

    fn apply_sample(&self, resident_bytes: u64) -> MemoryPressureObservation {
        let (observation, next_state) = {
            let mut state = self.lock_state();
            let previous = state.snapshot.state;
            let next = next_state(self.config, &mut state, resident_bytes);
            state.snapshot = MemoryPressureSnapshot {
                sequence: state.snapshot.sequence.saturating_add(1),
                state: next,
                resident_bytes: Some(resident_bytes),
            };
            (
                MemoryPressureObservation {
                    snapshot: state.snapshot,
                    changed: previous != next,
                    sample_succeeded: true,
                },
                next,
            )
        };
        self.apply_external_state(next_state);
        self.observer.record(observation);
        observation
    }

    fn apply_unavailable(&self) {
        let observation = {
            let mut state = self.lock_state();
            let changed = state.snapshot.state != MemoryPressureState::Unavailable;
            state.pressure_streak = 0;
            state.recovery_streak = 0;
            state.snapshot = MemoryPressureSnapshot {
                sequence: state.snapshot.sequence.saturating_add(1),
                state: MemoryPressureState::Unavailable,
                resident_bytes: state.snapshot.resident_bytes,
            };
            MemoryPressureObservation {
                snapshot: state.snapshot,
                changed,
                sample_succeeded: false,
            }
        };
        self.apply_external_state(MemoryPressureState::Unavailable);
        self.observer.record(observation);
    }

    fn apply_external_state(&self, state: MemoryPressureState) {
        if state == MemoryPressureState::Normal {
            let _resumed = self.admission.resume_admission();
        } else {
            let _paused = self.admission.pause_admission();
        }
        set_health(&self.health, state);
    }

    fn lock_state(&self) -> MutexGuard<'_, MonitorState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

fn next_state(
    config: MemoryPressureConfig,
    state: &mut MonitorState,
    resident_bytes: u64,
) -> MemoryPressureState {
    if resident_bytes >= config.hard_limit_bytes() {
        state.pressure_streak = 0;
        state.recovery_streak = 0;
        return MemoryPressureState::Critical;
    }

    if resident_bytes >= config.soft_limit_bytes() {
        state.recovery_streak = 0;
        state.pressure_streak = state.pressure_streak.saturating_add(1);
        if state.pressure_streak >= config.pressure_confirmation_samples() {
            return MemoryPressureState::Pressured;
        }
        return state.snapshot.state;
    }

    state.pressure_streak = 0;
    if resident_bytes <= config.resume_below_bytes() {
        if matches!(
            state.snapshot.state,
            MemoryPressureState::Pressured
                | MemoryPressureState::Critical
                | MemoryPressureState::Unavailable
        ) {
            state.recovery_streak = state.recovery_streak.saturating_add(1);
            if state.recovery_streak < config.recovery_confirmation_samples() {
                return state.snapshot.state;
            }
        }
        state.recovery_streak = 0;
        return MemoryPressureState::Normal;
    }

    state.recovery_streak = 0;
    match state.snapshot.state {
        MemoryPressureState::Initializing => MemoryPressureState::Normal,
        current => current,
    }
}

fn set_health(registry: &HealthRegistry, state: MemoryPressureState) {
    let (health, readiness, message) = match state {
        MemoryPressureState::Initializing => (
            HealthStatus::Degraded,
            ReadinessStatus::NotReady,
            "memory monitor is initializing",
        ),
        MemoryPressureState::Normal => (
            HealthStatus::Healthy,
            ReadinessStatus::Ready,
            "memory usage is within configured bounds",
        ),
        MemoryPressureState::Pressured => (
            HealthStatus::Degraded,
            ReadinessStatus::NotReady,
            "memory soft limit is active",
        ),
        MemoryPressureState::Critical => (
            HealthStatus::Unhealthy,
            ReadinessStatus::NotReady,
            "memory hard limit is active",
        ),
        MemoryPressureState::Unavailable => (
            HealthStatus::Degraded,
            ReadinessStatus::NotReady,
            "memory sampling is unavailable",
        ),
    };
    registry.set_health(ComponentHealth {
        component: Arc::from(MEMORY_COMPONENT),
        status: health,
        message: Some(Arc::from(message)),
    });
    registry.set_readiness(ComponentReadiness {
        component: Arc::from(MEMORY_COMPONENT),
        status: readiness,
        message: Some(Arc::from(message)),
    });
}
