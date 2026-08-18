use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    future::IntoFuture,
    io,
    sync::Arc,
    time::Duration,
};

use axum::{Router, response::Response};
use plenora_runtime_core::{HealthRegistry, RuntimeHandle, ShutdownSignal};
use tokio::{net::TcpListener, time::timeout};

use crate::{
    HttpError, HttpErrorResponseHook, HttpServerConfig, HttpServerConfigError,
    JsonErrorResponseHook, health, middleware,
};

/// Result of running the HTTP server through coordinated shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpServeOutcome {
    /// The server and its active connections completed after shutdown.
    GracefulShutdown,
    /// The HTTP grace period elapsed before all connections completed.
    ShutdownTimedOut {
        /// Configured bound that elapsed.
        grace_period: Duration,
    },
}

/// HTTP server phase that produced an I/O failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpServePhase {
    /// Binding the configured TCP listener failed.
    Bind,
    /// Serving an already-bound listener failed.
    Serve,
}

/// HTTP bootstrap or serving failure that preserves its I/O source.
pub struct HttpServeError {
    phase: HttpServePhase,
    source: io::Error,
}

impl HttpServeError {
    fn new(phase: HttpServePhase, source: io::Error) -> Self {
        Self { phase, source }
    }

    /// Returns the phase in which the failure occurred.
    #[must_use]
    pub const fn phase(&self) -> HttpServePhase {
        self.phase
    }

    /// Returns the preserved I/O source.
    #[must_use]
    pub const fn source_error(&self) -> &io::Error {
        &self.source
    }
}

impl Display for HttpServeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self.phase {
            HttpServePhase::Bind => formatter.write_str("failed to bind HTTP listener"),
            HttpServePhase::Serve => formatter.write_str("HTTP server failed"),
        }
    }
}

impl Debug for HttpServeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpServeError")
            .field("phase", &self.phase)
            .field("source_kind", &self.source.kind())
            .finish()
    }
}

impl Error for HttpServeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

/// Composable Axum bootstrap linked to Plenora lifecycle and health contracts.
#[derive(Clone)]
pub struct HttpBootstrap {
    config: HttpServerConfig,
    health_registry: HealthRegistry,
    shutdown: ShutdownSignal,
    error_hook: Arc<dyn HttpErrorResponseHook>,
}

impl HttpBootstrap {
    /// Creates a bootstrap from a runtime and validated HTTP configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP configuration is invalid.
    pub fn new(
        runtime: &RuntimeHandle,
        config: HttpServerConfig,
    ) -> Result<Self, HttpServerConfigError> {
        config.validate()?;
        Ok(Self {
            config,
            health_registry: runtime.health_registry(),
            shutdown: runtime.shutdown_signal(),
            error_hook: Arc::new(JsonErrorResponseHook),
        })
    }

    /// Returns validated HTTP server configuration.
    #[must_use]
    pub const fn config(&self) -> HttpServerConfig {
        self.config
    }

    /// Returns the shared health and readiness registry.
    #[must_use]
    pub fn health_registry(&self) -> HealthRegistry {
        self.health_registry.clone()
    }

    /// Returns the runtime shutdown signal observed by the HTTP server.
    #[must_use]
    pub fn shutdown_signal(&self) -> ShutdownSignal {
        self.shutdown.clone()
    }

    /// Replaces the default JSON error-response hook.
    #[must_use]
    pub fn with_error_response_hook<H>(mut self, hook: H) -> Self
    where
        H: HttpErrorResponseHook,
    {
        self.error_hook = Arc::new(hook);
        self
    }

    /// Uses the configured hook to build a redaction-safe common error response.
    #[must_use]
    pub fn error_response(&self, error: HttpError) -> Response {
        self.error_hook.response(error)
    }

    /// Adds reserved health routes and runtime middleware to an application router.
    ///
    /// Runtime `/health` and `/ready` routes take precedence over application routes
    /// at those paths without using a potentially panicking router merge.
    #[must_use = "the composed router must be served"]
    pub fn build_router(&self, application: Router) -> Router {
        let router = health::router(self.health_registry.clone()).fallback_service(application);
        middleware::apply(
            router,
            Arc::clone(&self.error_hook),
            self.config.max_request_body_bytes(),
            self.config.max_in_flight_requests(),
        )
    }

    /// Binds the configured address and serves a composed application router.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving error when listener binding or serving fails.
    pub async fn serve(&self, application: Router) -> Result<HttpServeOutcome, HttpServeError> {
        if self.shutdown.is_cancelled() {
            return Ok(HttpServeOutcome::GracefulShutdown);
        }
        let listener = TcpListener::bind(self.config.bind_address())
            .await
            .map_err(|error| HttpServeError::new(HttpServePhase::Bind, error))?;
        self.serve_listener(listener, application).await
    }

    /// Serves a composed application router on an already-bound listener.
    ///
    /// After runtime shutdown, active connections receive the configured grace period. The server
    /// future is dropped and a timeout outcome is returned when that bound expires.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving error when serving fails.
    pub async fn serve_listener(
        &self,
        listener: TcpListener,
        application: Router,
    ) -> Result<HttpServeOutcome, HttpServeError> {
        if self.shutdown.is_cancelled() {
            return Ok(HttpServeOutcome::GracefulShutdown);
        }
        let router = self.build_router(application);
        let shutdown = self.shutdown.clone();
        let graceful_signal = shutdown.clone();
        let server = axum::serve(listener, router).with_graceful_shutdown(async move {
            graceful_signal.cancelled().await;
        });
        let mut server = Box::pin(server.into_future());

        tokio::select! {
            biased;
            () = shutdown.cancelled() => {
                match timeout(self.config.shutdown_grace_period(), &mut server).await {
                    Ok(result) => complete_server(result),
                    Err(_elapsed) => Ok(HttpServeOutcome::ShutdownTimedOut {
                        grace_period: self.config.shutdown_grace_period(),
                    }),
                }
            }
            result = &mut server => complete_server(result),
        }
    }
}

impl Debug for HttpBootstrap {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpBootstrap")
            .field("config", &self.config)
            .field("shutdown_cancelled", &self.shutdown.is_cancelled())
            .field("error_hook", &"<redacted>")
            .finish_non_exhaustive()
    }
}

fn complete_server(result: io::Result<()>) -> Result<HttpServeOutcome, HttpServeError> {
    result
        .map(|()| HttpServeOutcome::GracefulShutdown)
        .map_err(|error| HttpServeError::new(HttpServePhase::Serve, error))
}
