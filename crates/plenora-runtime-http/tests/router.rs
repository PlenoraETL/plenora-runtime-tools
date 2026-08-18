//! Router, middleware, health, and public error contract tests.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    net::SocketAddr,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::Extension,
    http::{
        HeaderValue, Request, StatusCode,
        header::{AUTHORIZATION, CONTENT_LENGTH, COOKIE, SET_COOKIE},
    },
    response::IntoResponse,
    routing::{get, post},
};
use plenora_runtime_core::{
    ComponentHealth, ComponentReadiness, HealthStatus, ReadinessStatus, RuntimeHandle,
    ServiceMetadata,
};
use plenora_runtime_http::{
    CORRELATION_ID_HEADER, DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_REQUEST_BODY_BYTES,
    HttpBootstrap, HttpError, HttpRequestContext, HttpServerConfig, HttpServerConfigError,
    REQUEST_ID_HEADER, RequestId,
};
use plenora_runtime_messaging::{CorrelationId, MessageId};
use serde_json::Value;
use tokio::{
    sync::{Notify, Semaphore},
    time::timeout,
};
use tower::ServiceExt;

#[derive(Clone, Copy, Debug)]
struct TestError(&'static str);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RequestHeadersSensitive(bool);

impl Display for TestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestError {}

#[test]
fn configuration_has_safe_defaults_and_rejects_zero_bounds() -> Result<(), Box<dyn Error>> {
    let bind_address = SocketAddr::from(([127, 0, 0, 1], 0));
    assert_eq!(
        HttpServerConfig::new(bind_address, Duration::ZERO),
        Err(HttpServerConfigError::ZeroShutdownGracePeriod)
    );
    let grace_period = Duration::from_secs(1);
    let config = HttpServerConfig::new(bind_address, grace_period)?;
    assert_eq!(config.bind_address(), bind_address);
    assert_eq!(config.shutdown_grace_period(), grace_period);
    assert_eq!(
        config.max_request_body_bytes(),
        DEFAULT_MAX_REQUEST_BODY_BYTES
    );
    assert_eq!(
        config.max_in_flight_requests(),
        DEFAULT_MAX_IN_FLIGHT_REQUESTS
    );
    assert_eq!(
        config.with_max_request_body_bytes(0),
        Err(HttpServerConfigError::ZeroMaxRequestBodyBytes)
    );
    assert_eq!(
        config.with_max_in_flight_requests(0),
        Err(HttpServerConfigError::ZeroMaxInFlightRequests)
    );
    let configured = config
        .with_max_request_body_bytes(8_192)?
        .with_max_in_flight_requests(17)?;
    assert_eq!(configured.max_request_body_bytes(), 8_192);
    assert_eq!(configured.max_in_flight_requests(), 17);
    configured.validate()?;

    assert_eq!(
        HttpServerConfigError::ZeroShutdownGracePeriod.to_string(),
        "HTTP shutdown grace period must be greater than zero"
    );
    assert_eq!(
        HttpServerConfigError::ZeroMaxRequestBodyBytes.to_string(),
        "maximum HTTP request body bytes must be greater than zero"
    );
    assert_eq!(
        HttpServerConfigError::ZeroMaxInFlightRequests.to_string(),
        "maximum in-flight HTTP requests must be greater than zero"
    );
    Ok(())
}

#[test]
fn request_identifiers_and_context_round_trip_without_private_state() -> Result<(), Box<dyn Error>>
{
    let message_id = MessageId::random();
    let request_id = RequestId::from_message_id(message_id);
    assert_eq!(request_id.as_message_id(), &message_id);
    assert_eq!(RequestId::from(message_id), request_id);
    assert_eq!(MessageId::from(request_id), message_id);
    assert_eq!(RequestId::from_str(&request_id.to_string())?, request_id);

    let parse_error = RequestId::from_str("private-invalid-id")
        .err()
        .ok_or(TestError(
            "malformed request identifier unexpectedly parsed",
        ))?;
    assert_eq!(parse_error.to_string(), "invalid HTTP request identifier");
    assert!(!format!("{parse_error:?}").contains("private-invalid-id"));

    let correlation_id = CorrelationId::random();
    let context = HttpRequestContext::new(request_id, correlation_id);
    assert_eq!(context.request_id(), request_id);
    assert_eq!(context.correlation_id(), correlation_id);
    Ok(())
}

#[tokio::test]
async fn public_error_taxonomy_has_stable_redacted_json_responses() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let bootstrap = bootstrap(&runtime)?;
    let cases = [
        (
            HttpError::InvalidRequestId,
            "invalid_request_id",
            StatusCode::BAD_REQUEST,
        ),
        (
            HttpError::InvalidCorrelationId,
            "invalid_correlation_id",
            StatusCode::BAD_REQUEST,
        ),
        (
            HttpError::BadRequest,
            "bad_request",
            StatusCode::BAD_REQUEST,
        ),
        (HttpError::NotFound, "not_found", StatusCode::NOT_FOUND),
        (HttpError::Conflict, "conflict", StatusCode::CONFLICT),
        (
            HttpError::PayloadTooLarge,
            "payload_too_large",
            StatusCode::PAYLOAD_TOO_LARGE,
        ),
        (
            HttpError::ServiceUnavailable,
            "service_unavailable",
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            HttpError::Internal,
            "internal_error",
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
    ];

    for (error, code, status) in cases {
        assert_eq!(error.code(), code);
        assert_eq!(error.status(), status);
        assert_eq!(error.to_string(), code);
        let response = bootstrap.error_response(error);
        assert_eq!(response.status(), status);
        assert_eq!(response_header(&response, "cache-control")?, "no-store");
        let body = to_bytes(response.into_body(), 4_096).await?;
        assert_eq!(
            serde_json::from_slice::<Value>(&body)?,
            serde_json::json!({"error": {"code": code}})
        );
    }
    Ok(())
}

#[test]
fn bootstrap_exposes_shared_runtime_state_and_redacted_debug() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let bootstrap = bootstrap(&runtime)?;
    assert_eq!(
        bootstrap.config().bind_address(),
        SocketAddr::from(([127, 0, 0, 1], 0))
    );
    assert!(!bootstrap.shutdown_signal().is_cancelled());
    assert_eq!(
        bootstrap.health_registry().health().status,
        HealthStatus::Healthy
    );

    let debug = format!("{bootstrap:?}");
    assert!(debug.contains("HttpBootstrap"));
    assert!(debug.contains("<redacted>"));
    Ok(())
}

#[derive(Clone)]
struct ConcurrencyState {
    entered: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Semaphore>,
}

#[tokio::test]
async fn concurrent_request_bound_backpressures_and_cancels_safely() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let config = HttpServerConfig::new(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Duration::from_millis(100),
    )?
    .with_max_in_flight_requests(1)?;
    let state = ConcurrencyState {
        entered: Arc::new(AtomicUsize::new(0)),
        started: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(0)),
    };
    let application = Router::new()
        .route("/hold", get(concurrency_handler))
        .layer(Extension(state.clone()));
    let router = HttpBootstrap::new(&runtime, config)?.build_router(application);

    let first = router
        .clone()
        .oneshot(Request::get("/hold").body(Body::empty())?);
    let coordinate = async {
        state.started.notified().await;
        let second = router
            .clone()
            .oneshot(Request::get("/hold").body(Body::empty())?);
        let mut second = Box::pin(second);

        assert!(
            timeout(Duration::from_millis(25), second.as_mut())
                .await
                .is_err()
        );
        assert_eq!(state.entered.load(Ordering::SeqCst), 1);
        drop(second);
        state.release.add_permits(2);
        Ok::<(), Box<dyn Error>>(())
    };

    let (first_response, coordinate_result) = timeout(Duration::from_secs(2), async {
        tokio::join!(first, coordinate)
    })
    .await?;
    coordinate_result?;
    assert_eq!(first_response?.status(), StatusCode::NO_CONTENT);
    assert_eq!(state.entered.load(Ordering::SeqCst), 1);

    let third_response = timeout(
        Duration::from_secs(2),
        router.oneshot(Request::get("/hold").body(Body::empty())?),
    )
    .await??;
    assert_eq!(third_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(state.entered.load(Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test]
async fn middleware_generates_context_and_propagates_response_headers() -> Result<(), Box<dyn Error>>
{
    let runtime = runtime();
    let router =
        bootstrap(&runtime)?.build_router(Router::new().route("/context", get(context_handler)));

    let response = router
        .oneshot(Request::get("/context").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let context = match response.extensions().get::<HttpRequestContext>() {
        Some(context) => *context,
        None => return Err(Box::new(TestError("missing response context")) as Box<dyn Error>),
    };
    assert_eq!(
        response_header(&response, REQUEST_ID_HEADER)?,
        context.request_id().to_string()
    );
    assert_eq!(
        response_header(&response, CORRELATION_ID_HEADER)?,
        context.correlation_id().to_string()
    );

    Ok(())
}

#[tokio::test]
async fn middleware_preserves_valid_supplied_identifiers() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let request_id = RequestId::random();
    let correlation_id = CorrelationId::random();
    let router =
        bootstrap(&runtime)?.build_router(Router::new().route("/context", get(context_handler)));
    let request = Request::get("/context")
        .header(REQUEST_ID_HEADER, request_id.to_string())
        .header(CORRELATION_ID_HEADER, correlation_id.to_string())
        .body(Body::empty())?;

    let response = router.oneshot(request).await?;

    assert_eq!(
        response_header(&response, REQUEST_ID_HEADER)?,
        request_id.to_string()
    );
    assert_eq!(
        response_header(&response, CORRELATION_ID_HEADER)?,
        correlation_id.to_string()
    );
    assert_eq!(
        response.extensions().get::<HttpRequestContext>().copied(),
        Some(HttpRequestContext::new(request_id, correlation_id))
    );

    Ok(())
}

#[tokio::test]
async fn malformed_identifier_is_rejected_without_echoing_input() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let router = bootstrap(&runtime)?.build_router(Router::new());
    let request = Request::get("/")
        .header(REQUEST_ID_HEADER, "private-invalid-value")
        .body(Body::empty())?;

    let response = router.oneshot(request).await?;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let generated = response_header(&response, REQUEST_ID_HEADER)?;
    assert_ne!(generated, "private-invalid-value");
    let _validated = RequestId::from_str(generated)?;
    let body = to_bytes(response.into_body(), 4_096).await?;
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body.pointer("/error/code").and_then(Value::as_str),
        Some("invalid_request_id")
    );
    assert!(!body.to_string().contains("private-invalid-value"));

    Ok(())
}

#[tokio::test]
async fn oversized_payload_is_rejected_with_redacted_common_error() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let config = HttpServerConfig::new(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Duration::from_millis(100),
    )?
    .with_max_request_body_bytes(4)?;
    let router = HttpBootstrap::new(&runtime, config)?.build_router(Router::new().route(
        "/upload",
        post(|_body: Bytes| async { StatusCode::NO_CONTENT }),
    ));
    let private_payload = "private-payload";
    let request = Request::post("/upload")
        .header(CONTENT_LENGTH, private_payload.len().to_string())
        .body(Body::from(private_payload))?;

    let response = router.clone().oneshot(request).await?;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let _request_id = RequestId::from_str(response_header(&response, REQUEST_ID_HEADER)?)?;
    let body = to_bytes(response.into_body(), 4_096).await?;
    let body: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        body.pointer("/error/code").and_then(Value::as_str),
        Some("payload_too_large")
    );
    assert!(!body.to_string().contains(private_payload));

    let streamed_response = router
        .oneshot(Request::post("/upload").body(Body::from(private_payload))?)
        .await?;
    assert_eq!(streamed_response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let streamed_body = to_bytes(streamed_response.into_body(), 4_096).await?;
    let streamed_body: Value = serde_json::from_slice(&streamed_body)?;
    assert_eq!(
        streamed_body.pointer("/error/code").and_then(Value::as_str),
        Some("payload_too_large")
    );
    assert!(!streamed_body.to_string().contains(private_payload));
    Ok(())
}

#[tokio::test]
async fn health_routes_expose_only_aggregate_redacted_status() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let registry = runtime.health_registry();
    registry.set_health(ComponentHealth {
        component: Arc::from("database.internal"),
        status: HealthStatus::Degraded,
        message: Some(Arc::from("password=private")),
    });
    registry.set_readiness(ComponentReadiness {
        component: Arc::from("broker.internal"),
        status: ReadinessStatus::NotReady,
        message: Some(Arc::from("credential=private")),
    });
    let router = bootstrap(&runtime)?.build_router(Router::new());

    let health = router
        .clone()
        .oneshot(Request::get("/health").body(Body::empty())?)
        .await?;
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(response_header(&health, "cache-control")?, "no-store");
    let health_body = to_bytes(health.into_body(), 4_096).await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&health_body)?,
        serde_json::json!({"status": "degraded"})
    );

    let readiness = router
        .oneshot(Request::get("/ready").body(Body::empty())?)
        .await?;
    assert_eq!(readiness.status(), StatusCode::SERVICE_UNAVAILABLE);
    let readiness_body = to_bytes(readiness.into_body(), 4_096).await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&readiness_body)?,
        serde_json::json!({"status": "not_ready"})
    );

    Ok(())
}

#[tokio::test]
async fn custom_error_hook_receives_only_public_category() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let bootstrap = bootstrap(&runtime)?.with_error_response_hook(|error: HttpError| {
        (StatusCode::IM_A_TEAPOT, format!("safe:{}", error.code())).into_response()
    });
    let request = Request::get("/")
        .header(CORRELATION_ID_HEADER, "private-invalid-value")
        .body(Body::empty())?;

    let response = bootstrap
        .build_router(Router::new())
        .oneshot(request)
        .await?;

    assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
    let body = to_bytes(response.into_body(), 4_096).await?;
    assert_eq!(std::str::from_utf8(&body)?, "safe:invalid_correlation_id");

    Ok(())
}

#[tokio::test]
async fn runtime_health_route_takes_precedence_without_duplicate_route_panic()
-> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let application = Router::new().route("/health", get(|| async { StatusCode::IM_A_TEAPOT }));
    let response = bootstrap(&runtime)?
        .build_router(application)
        .oneshot(Request::get("/health").body(Body::empty())?)
        .await?;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 4_096).await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&body)?,
        serde_json::json!({"status": "healthy"})
    );
    Ok(())
}

#[tokio::test]
async fn sensitive_http_headers_are_marked_around_tracing() -> Result<(), Box<dyn Error>> {
    let runtime = runtime();
    let application = Router::new().route("/sensitive", get(sensitive_headers_handler));
    let request = Request::get("/sensitive")
        .header(AUTHORIZATION, "Bearer private")
        .header(COOKIE, "session=private")
        .body(Body::empty())?;

    let response = bootstrap(&runtime)?
        .build_router(application)
        .oneshot(request)
        .await?;

    assert_eq!(
        response
            .extensions()
            .get::<RequestHeadersSensitive>()
            .copied(),
        Some(RequestHeadersSensitive(true))
    );
    assert!(
        response
            .headers()
            .get(SET_COOKIE)
            .is_some_and(HeaderValue::is_sensitive)
    );
    Ok(())
}

async fn context_handler(Extension(context): Extension<HttpRequestContext>) -> String {
    format!("{}:{}", context.request_id(), context.correlation_id())
}

async fn sensitive_headers_handler(request: axum::extract::Request) -> axum::response::Response {
    let request_headers_sensitive = [AUTHORIZATION, COOKIE].into_iter().all(|name| {
        request
            .headers()
            .get(name)
            .is_some_and(HeaderValue::is_sensitive)
    });
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .extensions_mut()
        .insert(RequestHeadersSensitive(request_headers_sensitive));
    response
        .headers_mut()
        .insert(SET_COOKIE, HeaderValue::from_static("session=private"));
    response
}

async fn concurrency_handler(Extension(state): Extension<ConcurrencyState>) -> StatusCode {
    state.entered.fetch_add(1, Ordering::SeqCst);
    state.started.notify_one();
    if let Ok(permit) = state.release.acquire().await {
        permit.forget();
    }
    StatusCode::NO_CONTENT
}

fn response_header<'a>(
    response: &'a axum::response::Response,
    name: &'static str,
) -> Result<&'a str, Box<dyn Error>> {
    let value = response
        .headers()
        .get(name)
        .ok_or(TestError("missing response header"))?;
    Ok(value.to_str()?)
}

fn bootstrap(runtime: &RuntimeHandle) -> Result<HttpBootstrap, Box<dyn Error>> {
    let config = HttpServerConfig::new(
        SocketAddr::from(([127, 0, 0, 1], 0)),
        Duration::from_millis(100),
    )?;
    Ok(HttpBootstrap::new(runtime, config)?)
}

fn runtime() -> RuntimeHandle {
    RuntimeHandle::new(ServiceMetadata::new("http-adapter-test", "0.1.0", "test-1"))
}
