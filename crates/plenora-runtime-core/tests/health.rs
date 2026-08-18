//! Tests for independent health and readiness aggregation.

use std::sync::Arc;

use plenora_runtime_core::{
    ComponentHealth, ComponentReadiness, HealthRegistry, HealthStatus, ReadinessStatus,
};

#[test]
fn empty_registry_is_healthy_and_ready() {
    let registry = HealthRegistry::new();

    assert_eq!(registry.health().status, HealthStatus::Healthy);
    assert_eq!(registry.readiness().status, ReadinessStatus::Ready);
}

#[test]
fn health_and_readiness_are_aggregated_independently() {
    let registry = HealthRegistry::new();
    registry.set_health(ComponentHealth {
        component: Arc::from("optional-cache"),
        status: HealthStatus::Degraded,
        message: Some(Arc::from("cache bypass active")),
    });
    registry.set_readiness(ComponentReadiness {
        component: Arc::from("request-listener"),
        status: ReadinessStatus::Ready,
        message: None,
    });

    assert_eq!(registry.health().status, HealthStatus::Degraded);
    assert_eq!(registry.readiness().status, ReadinessStatus::Ready);
}

#[test]
fn worst_component_status_wins_and_components_are_ordered() {
    let registry = HealthRegistry::new();
    registry.set_health(ComponentHealth {
        component: Arc::from("zeta"),
        status: HealthStatus::Degraded,
        message: None,
    });
    registry.set_health(ComponentHealth {
        component: Arc::from("alpha"),
        status: HealthStatus::Unhealthy,
        message: Some(Arc::from("offline")),
    });

    let snapshot = registry.health();

    assert_eq!(snapshot.status, HealthStatus::Unhealthy);
    assert_eq!(snapshot.components[0].component.as_ref(), "alpha");
    assert_eq!(snapshot.components[1].component.as_ref(), "zeta");
    assert!(registry.remove_health("alpha").is_some());
    assert_eq!(registry.health().status, HealthStatus::Degraded);
}
