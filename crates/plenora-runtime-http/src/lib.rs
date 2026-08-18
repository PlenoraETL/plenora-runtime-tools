//! HTTP runtime adapter contracts and bootstrap support.

#![forbid(unsafe_code)]

mod bootstrap;
mod config;
mod context;
mod error;
mod health;
mod middleware;

pub use bootstrap::{HttpBootstrap, HttpServeError, HttpServeOutcome, HttpServePhase};
pub use config::{
    DEFAULT_MAX_IN_FLIGHT_REQUESTS, DEFAULT_MAX_REQUEST_BODY_BYTES, HttpServerConfig,
    HttpServerConfigError,
};
pub use context::{HttpRequestContext, RequestId, RequestIdParseError};
pub use error::{HttpError, HttpErrorResponseHook, JsonErrorResponseHook, default_error_response};
pub use middleware::{CORRELATION_ID_HEADER, REQUEST_ID_HEADER};
