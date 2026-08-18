use std::sync::Arc;

use plenora_runtime_core::{
    ComponentHealth, ComponentReadiness, HealthRegistry, HealthStatus, ReadinessStatus,
};

#[derive(Clone, Debug)]
pub(crate) struct NatsHealthReporter {
    registry: HealthRegistry,
    component: Arc<str>,
}

impl NatsHealthReporter {
    pub(crate) fn new(registry: HealthRegistry, component: Arc<str>) -> Self {
        let reporter = Self {
            registry,
            component,
        };
        reporter.not_ready(HealthStatus::Degraded, "NATS connection is initializing");
        reporter
    }

    pub(crate) fn ready(&self) {
        self.registry.set_health(ComponentHealth {
            component: Arc::clone(&self.component),
            status: HealthStatus::Healthy,
            message: None,
        });
        self.registry.set_readiness(ComponentReadiness {
            component: Arc::clone(&self.component),
            status: ReadinessStatus::Ready,
            message: None,
        });
    }

    pub(crate) fn degraded(&self, message: &'static str) {
        self.not_ready(HealthStatus::Degraded, message);
    }

    pub(crate) fn unhealthy(&self, message: &'static str) {
        self.not_ready(HealthStatus::Unhealthy, message);
    }

    fn not_ready(&self, status: HealthStatus, message: &'static str) {
        self.registry.set_health(ComponentHealth {
            component: Arc::clone(&self.component),
            status,
            message: Some(Arc::from(message)),
        });
        self.registry.set_readiness(ComponentReadiness {
            component: Arc::clone(&self.component),
            status: ReadinessStatus::NotReady,
            message: Some(Arc::from(message)),
        });
    }
}
