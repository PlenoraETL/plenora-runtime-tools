use std::{error::Error, fmt};

use axum::{
    Json,
    http::{StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use serde::Serialize;

/// Stable public HTTP failure categories accepted by error-response hooks.
///
/// The type deliberately carries no source error, payload, or free-form message.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum HttpError {
    /// A request identifier header was malformed.
    InvalidRequestId,
    /// A correlation identifier header was malformed.
    InvalidCorrelationId,
    /// The public request was invalid.
    BadRequest,
    /// The requested resource was not found.
    NotFound,
    /// The request conflicts with current public state.
    Conflict,
    /// The request payload exceeds the configured public bound.
    PayloadTooLarge,
    /// The service is temporarily unavailable.
    ServiceUnavailable,
    /// An internal failure occurred.
    Internal,
}

impl HttpError {
    /// Returns the stable machine-readable public error code.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidRequestId => "invalid_request_id",
            Self::InvalidCorrelationId => "invalid_correlation_id",
            Self::BadRequest => "bad_request",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::PayloadTooLarge => "payload_too_large",
            Self::ServiceUnavailable => "service_unavailable",
            Self::Internal => "internal_error",
        }
    }

    /// Returns the HTTP status associated with this public category.
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::InvalidRequestId | Self::InvalidCorrelationId | Self::BadRequest => {
                StatusCode::BAD_REQUEST
            }
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::CONFLICT,
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for HttpError {}

/// Synchronous hook that maps a redacted public category to an HTTP response.
pub trait HttpErrorResponseHook: Send + Sync + 'static {
    /// Builds a response without receiving any private source or diagnostic detail.
    fn response(&self, error: HttpError) -> Response;
}

impl<F> HttpErrorResponseHook for F
where
    F: Fn(HttpError) -> Response + Send + Sync + 'static,
{
    fn response(&self, error: HttpError) -> Response {
        self(error)
    }
}

/// Default JSON error-response hook.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonErrorResponseHook;

impl HttpErrorResponseHook for JsonErrorResponseHook {
    fn response(&self, error: HttpError) -> Response {
        default_error_response(error)
    }
}

#[derive(Serialize)]
struct ErrorEnvelope {
    error: ErrorBody,
}

#[derive(Serialize)]
struct ErrorBody {
    code: &'static str,
}

/// Builds the default redaction-safe JSON response for a public HTTP error.
#[must_use]
pub fn default_error_response(error: HttpError) -> Response {
    (
        error.status(),
        [(CACHE_CONTROL, "no-store")],
        Json(ErrorEnvelope {
            error: ErrorBody { code: error.code() },
        }),
    )
        .into_response()
}
