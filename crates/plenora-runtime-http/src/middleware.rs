use std::{str::FromStr, sync::Arc};

use axum::{
    Router,
    body::Body,
    extract::{DefaultBodyLimit, Request, State},
    http::{
        HeaderMap, HeaderValue,
        header::{AUTHORIZATION, COOKIE, SET_COOKIE},
    },
    middleware::{self, Next},
    response::Response,
};
use plenora_runtime_messaging::CorrelationId;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::{
    limit::RequestBodyLimitLayer,
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
    trace::TraceLayer,
};
use tracing::{Span, field};

use crate::{HttpError, HttpErrorResponseHook, HttpRequestContext, RequestId};

/// Header carrying the unique HTTP request identifier.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Header carrying the cross-operation correlation identifier.
pub const CORRELATION_ID_HEADER: &str = "x-correlation-id";

#[derive(Clone)]
struct MiddlewareState {
    error_hook: Arc<dyn HttpErrorResponseHook>,
}

pub(crate) fn apply(
    router: Router,
    error_hook: Arc<dyn HttpErrorResponseHook>,
    max_request_body_bytes: usize,
    max_in_flight_requests: usize,
) -> Router {
    let state = MiddlewareState { error_hook };

    router
        .layer(DefaultBodyLimit::max(max_request_body_bytes))
        .layer(RequestBodyLimitLayer::new(max_request_body_bytes))
        .layer(SetSensitiveResponseHeadersLayer::new([SET_COOKIE]))
        .layer(GlobalConcurrencyLimitLayer::new(max_in_flight_requests))
        .layer(TraceLayer::new_for_http().make_span_with(make_span))
        .layer(SetSensitiveRequestHeadersLayer::new([
            AUTHORIZATION,
            COOKIE,
        ]))
        .layer(middleware::from_fn_with_state(
            state,
            request_context_middleware,
        ))
}

async fn request_context_middleware(
    State(state): State<MiddlewareState>,
    mut request: Request,
    next: Next,
) -> Response {
    let (request_id, request_id_invalid) = resolve_request_id(request.headers());
    let (correlation_id, correlation_id_invalid) = resolve_correlation_id(request.headers());
    let context = HttpRequestContext::new(request_id, correlation_id);
    request.extensions_mut().insert(context);

    if request_id_invalid || correlation_id_invalid {
        let error = if request_id_invalid {
            HttpError::InvalidRequestId
        } else {
            HttpError::InvalidCorrelationId
        };
        let span = make_span(&request);
        return span.in_scope(|| {
            tracing::warn!(http_error = error.code(), "HTTP request rejected");
            attach_context(state.error_hook.response(error), context)
        });
    }

    let response = next.run(request).await;
    let response = if response.status() == axum::http::StatusCode::PAYLOAD_TOO_LARGE {
        state.error_hook.response(HttpError::PayloadTooLarge)
    } else {
        response
    };
    attach_context(response, context)
}

fn resolve_request_id(headers: &HeaderMap) -> (RequestId, bool) {
    let Some(value) = headers.get(REQUEST_ID_HEADER) else {
        return (RequestId::random(), false);
    };
    let Ok(value) = value.to_str() else {
        return (RequestId::random(), true);
    };
    match RequestId::from_str(value) {
        Ok(request_id) => (request_id, false),
        Err(_error) => (RequestId::random(), true),
    }
}

fn resolve_correlation_id(headers: &HeaderMap) -> (CorrelationId, bool) {
    let Some(value) = headers.get(CORRELATION_ID_HEADER) else {
        return (CorrelationId::random(), false);
    };
    let Ok(value) = value.to_str() else {
        return (CorrelationId::random(), true);
    };
    match CorrelationId::from_str(value) {
        Ok(correlation_id) => (correlation_id, false),
        Err(_error) => (CorrelationId::random(), true),
    }
}

fn attach_context(mut response: Response, context: HttpRequestContext) -> Response {
    response.extensions_mut().insert(context);
    if let Some(set_cookie) = response.headers_mut().get_mut(SET_COOKIE) {
        set_cookie.set_sensitive(true);
    }
    let request_id = context.request_id().to_string();
    let correlation_id = context.correlation_id().to_string();
    insert_identifier_header(response.headers_mut(), REQUEST_ID_HEADER, &request_id);
    insert_identifier_header(
        response.headers_mut(),
        CORRELATION_ID_HEADER,
        &correlation_id,
    );
    response
}

fn insert_identifier_header(headers: &mut HeaderMap, name: &'static str, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

fn make_span(request: &axum::http::Request<Body>) -> Span {
    if let Some(context) = request.extensions().get::<HttpRequestContext>() {
        tracing::info_span!(
            "http.request",
            http_method = %request.method(),
            http_version = ?request.version(),
            request_id = %context.request_id(),
            correlation_id = %context.correlation_id(),
        )
    } else {
        tracing::info_span!(
            "http.request",
            http_method = %request.method(),
            http_version = ?request.version(),
            request_id = field::Empty,
            correlation_id = field::Empty,
        )
    }
}
