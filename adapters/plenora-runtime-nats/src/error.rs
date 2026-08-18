use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

/// Stable category for a NATS adapter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NatsErrorCategory {
    /// Local configuration is invalid.
    Configuration,
    /// The NATS connection is unavailable.
    Connection,
    /// A `JetStream` resource is unavailable or incompatible.
    Infrastructure,
    /// A message could not be encoded or decoded.
    Protocol,
    /// A broker operation failed with a known outcome.
    Broker,
}

/// Operation being performed when an adapter failure occurred.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NatsOperation {
    /// Client connection.
    Connect,
    /// Forced client reconnection.
    Reconnect,
    /// Graceful client drain.
    Drain,
    /// Health probe.
    Probe,
    /// Stream or consumer binding.
    Bind,
    /// Message publication.
    Publish,
    /// Delivery polling.
    Receive,
    /// Metadata conversion.
    Metadata,
}

/// Redaction-safe NATS adapter failure.
#[derive(Clone)]
pub struct NatsAdapterError {
    category: NatsErrorCategory,
    operation: NatsOperation,
    message: Arc<str>,
    source: Option<Arc<dyn Error + Send + Sync + 'static>>,
}

impl NatsAdapterError {
    /// Creates an error without an underlying source.
    #[must_use]
    pub fn new(
        category: NatsErrorCategory,
        operation: NatsOperation,
        message: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            category,
            operation,
            message: message.into(),
            source: None,
        }
    }

    /// Creates an error while preserving its source.
    #[must_use]
    pub fn with_source<E>(
        category: NatsErrorCategory,
        operation: NatsOperation,
        message: impl Into<Arc<str>>,
        source: E,
    ) -> Self
    where
        E: Into<Box<dyn Error + Send + Sync + 'static>>,
    {
        Self {
            category,
            operation,
            message: message.into(),
            source: Some(Arc::from(source.into())),
        }
    }

    /// Returns the stable category.
    #[must_use]
    pub const fn category(&self) -> NatsErrorCategory {
        self.category
    }

    /// Returns the failed operation.
    #[must_use]
    pub const fn operation(&self) -> NatsOperation {
        self.operation
    }

    /// Returns the redaction-safe operator message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Debug for NatsAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NatsAdapterError")
            .field("category", &self.category)
            .field("operation", &self.operation)
            .field("message", &self.message)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl Display for NatsAdapterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for NatsAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}
