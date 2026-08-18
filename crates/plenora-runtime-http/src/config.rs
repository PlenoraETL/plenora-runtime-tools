use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    net::SocketAddr,
    time::Duration,
};

/// Default maximum request payload accepted by the HTTP adapter: one mebibyte.
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

/// Default maximum number of requests admitted concurrently by the HTTP adapter.
pub const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;

/// Validated network and shutdown configuration for the HTTP adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpServerConfig {
    bind_address: SocketAddr,
    shutdown_grace_period: Duration,
    max_request_body_bytes: usize,
    max_in_flight_requests: usize,
}

impl HttpServerConfig {
    /// Creates validated server configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when the shutdown grace period is zero.
    pub fn new(
        bind_address: SocketAddr,
        shutdown_grace_period: Duration,
    ) -> Result<Self, HttpServerConfigError> {
        let config = Self {
            bind_address,
            shutdown_grace_period,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
        };
        config.validate()?;
        Ok(config)
    }

    /// Returns the address used when the bootstrap binds its listener.
    #[must_use]
    pub const fn bind_address(self) -> SocketAddr {
        self.bind_address
    }

    /// Returns the maximum time allowed for HTTP connections to drain.
    #[must_use]
    pub const fn shutdown_grace_period(self) -> Duration {
        self.shutdown_grace_period
    }

    /// Returns the maximum accepted HTTP request payload size.
    #[must_use]
    pub const fn max_request_body_bytes(self) -> usize {
        self.max_request_body_bytes
    }

    /// Returns the maximum number of requests admitted concurrently.
    #[must_use]
    pub const fn max_in_flight_requests(self) -> usize {
        self.max_in_flight_requests
    }

    /// Replaces the request payload bound.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_request_body_bytes` is zero.
    pub fn with_max_request_body_bytes(
        mut self,
        max_request_body_bytes: usize,
    ) -> Result<Self, HttpServerConfigError> {
        self.max_request_body_bytes = max_request_body_bytes;
        self.validate()?;
        Ok(self)
    }

    /// Replaces the concurrent request bound.
    ///
    /// # Errors
    ///
    /// Returns an error when `max_in_flight_requests` is zero.
    pub fn with_max_in_flight_requests(
        mut self,
        max_in_flight_requests: usize,
    ) -> Result<Self, HttpServerConfigError> {
        self.max_in_flight_requests = max_in_flight_requests;
        self.validate()?;
        Ok(self)
    }

    /// Validates the server bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when the shutdown grace period, request body bound, or concurrent request
    /// bound is zero.
    pub fn validate(self) -> Result<(), HttpServerConfigError> {
        if self.shutdown_grace_period.is_zero() {
            return Err(HttpServerConfigError::ZeroShutdownGracePeriod);
        }
        if self.max_request_body_bytes == 0 {
            return Err(HttpServerConfigError::ZeroMaxRequestBodyBytes);
        }
        if self.max_in_flight_requests == 0 {
            return Err(HttpServerConfigError::ZeroMaxInFlightRequests);
        }
        Ok(())
    }
}

/// Invalid HTTP server configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpServerConfigError {
    /// A zero grace period would not permit cooperative HTTP drain.
    ZeroShutdownGracePeriod,
    /// A zero body limit would reject every non-empty request.
    ZeroMaxRequestBodyBytes,
    /// A zero concurrency limit would prevent all requests from being admitted.
    ZeroMaxInFlightRequests,
}

impl Display for HttpServerConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroShutdownGracePeriod => {
                formatter.write_str("HTTP shutdown grace period must be greater than zero")
            }
            Self::ZeroMaxRequestBodyBytes => {
                formatter.write_str("maximum HTTP request body bytes must be greater than zero")
            }
            Self::ZeroMaxInFlightRequests => {
                formatter.write_str("maximum in-flight HTTP requests must be greater than zero")
            }
        }
    }
}

impl Error for HttpServerConfigError {}
