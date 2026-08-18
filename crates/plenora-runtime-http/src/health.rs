use axum::{
    Extension, Json, Router,
    http::{StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::get,
};
use plenora_runtime_core::{HealthRegistry, HealthStatus, ReadinessStatus};
use serde::Serialize;

#[derive(Serialize)]
struct StatusBody {
    status: &'static str,
}

pub(crate) fn router(registry: HealthRegistry) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(readiness))
        .layer(Extension(registry))
}

async fn health(Extension(registry): Extension<HealthRegistry>) -> Response {
    let snapshot = registry.health();
    let (status_code, status) = match snapshot.status {
        HealthStatus::Healthy => (StatusCode::OK, "healthy"),
        HealthStatus::Degraded => (StatusCode::OK, "degraded"),
        HealthStatus::Unhealthy => (StatusCode::SERVICE_UNAVAILABLE, "unhealthy"),
    };
    status_response(status_code, status)
}

async fn readiness(Extension(registry): Extension<HealthRegistry>) -> Response {
    let snapshot = registry.readiness();
    let (status_code, status) = match snapshot.status {
        ReadinessStatus::Ready => (StatusCode::OK, "ready"),
        ReadinessStatus::NotReady => (StatusCode::SERVICE_UNAVAILABLE, "not_ready"),
    };
    status_response(status_code, status)
}

fn status_response(status_code: StatusCode, status: &'static str) -> Response {
    (
        status_code,
        [(CACHE_CONTROL, "no-store")],
        Json(StatusBody { status }),
    )
        .into_response()
}
