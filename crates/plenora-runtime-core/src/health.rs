use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

/// Liveness status for a runtime component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthStatus {
    /// The component is operating normally.
    Healthy,
    /// The component is impaired but the process can continue operating.
    Degraded,
    /// The component is not operating safely.
    Unhealthy,
}

/// Readiness status for a runtime component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessStatus {
    /// The component can accept new work.
    Ready,
    /// The component must not accept new work.
    NotReady,
}

/// Health information reported by one named component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentHealth {
    /// Stable component name.
    pub component: Arc<str>,
    /// Current liveness status.
    pub status: HealthStatus,
    /// Optional operator-facing detail.
    pub message: Option<Arc<str>>,
}

/// Readiness information reported by one named component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentReadiness {
    /// Stable component name.
    pub component: Arc<str>,
    /// Current readiness status.
    pub status: ReadinessStatus,
    /// Optional operator-facing detail.
    pub message: Option<Arc<str>>,
}

/// An immutable aggregate health view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    /// Worst status across all registered components.
    pub status: HealthStatus,
    /// Component details ordered by component name.
    pub components: Vec<ComponentHealth>,
}

/// An immutable aggregate readiness view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadinessSnapshot {
    /// Not ready when at least one registered component is not ready.
    pub status: ReadinessStatus,
    /// Component details ordered by component name.
    pub components: Vec<ComponentReadiness>,
}

#[derive(Debug, Default)]
struct RegistryState {
    health: BTreeMap<Arc<str>, ComponentHealth>,
    readiness: BTreeMap<Arc<str>, ComponentReadiness>,
}

/// Thread-safe registry that keeps health and readiness as independent signals.
#[derive(Clone, Debug, Default)]
pub struct HealthRegistry {
    state: Arc<RwLock<RegistryState>>,
}

impl HealthRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or replaces the health report for a component.
    pub fn set_health(&self, health: ComponentHealth) {
        self.write_state()
            .health
            .insert(Arc::clone(&health.component), health);
    }

    /// Inserts or replaces the readiness report for a component.
    pub fn set_readiness(&self, readiness: ComponentReadiness) {
        self.write_state()
            .readiness
            .insert(Arc::clone(&readiness.component), readiness);
    }

    /// Removes a component health report.
    #[must_use]
    pub fn remove_health(&self, component: &str) -> Option<ComponentHealth> {
        self.write_state().health.remove(component)
    }

    /// Removes a component readiness report.
    #[must_use]
    pub fn remove_readiness(&self, component: &str) -> Option<ComponentReadiness> {
        self.write_state().readiness.remove(component)
    }

    /// Returns the current aggregate health view.
    #[must_use]
    pub fn health(&self) -> HealthSnapshot {
        let state = self.read_state();
        let status = state
            .health
            .values()
            .fold(HealthStatus::Healthy, |aggregate, component| {
                aggregate_health(aggregate, component.status)
            });

        HealthSnapshot {
            status,
            components: state.health.values().cloned().collect(),
        }
    }

    /// Returns the current aggregate readiness view.
    #[must_use]
    pub fn readiness(&self) -> ReadinessSnapshot {
        let state = self.read_state();
        let status = state
            .readiness
            .values()
            .fold(ReadinessStatus::Ready, |aggregate, component| {
                aggregate_readiness(aggregate, component.status)
            });

        ReadinessSnapshot {
            status,
            components: state.readiness.values().cloned().collect(),
        }
    }

    fn read_state(&self) -> RwLockReadGuard<'_, RegistryState> {
        match self.state.read() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, RegistryState> {
        match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

const fn aggregate_health(aggregate: HealthStatus, candidate: HealthStatus) -> HealthStatus {
    match (aggregate, candidate) {
        (HealthStatus::Unhealthy, _) | (_, HealthStatus::Unhealthy) => HealthStatus::Unhealthy,
        (HealthStatus::Degraded, _) | (_, HealthStatus::Degraded) => HealthStatus::Degraded,
        (HealthStatus::Healthy, HealthStatus::Healthy) => HealthStatus::Healthy,
    }
}

const fn aggregate_readiness(
    aggregate: ReadinessStatus,
    candidate: ReadinessStatus,
) -> ReadinessStatus {
    match (aggregate, candidate) {
        (ReadinessStatus::NotReady, _) | (_, ReadinessStatus::NotReady) => {
            ReadinessStatus::NotReady
        }
        (ReadinessStatus::Ready, ReadinessStatus::Ready) => ReadinessStatus::Ready,
    }
}
