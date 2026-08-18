use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

use async_trait::async_trait;
use plenora_runtime_messaging::{ClassifyRetry, RetryErrorClass};
use plenora_runtime_worker::{WorkerContext, WorkerHandler};

use crate::{
    CapabilityFailure, CapabilityId, CapabilityRegistry, CapabilityRemoteEffect, CapabilityRequest,
    OperationName,
};

/// Hard upper bound for opaque input passed to one capability adapter: 64 MiB.
pub const MAX_CAPABILITY_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Payload admission bound applied before any concrete library is invoked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityDispatcherConfig {
    /// Maximum encoded input bytes accepted by one invocation.
    pub max_payload_bytes: usize,
}

impl CapabilityDispatcherConfig {
    /// Creates validated dispatcher bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload bound is zero or above
    /// [`MAX_CAPABILITY_PAYLOAD_BYTES`].
    pub const fn new(max_payload_bytes: usize) -> Result<Self, CapabilityDispatcherConfigError> {
        if max_payload_bytes == 0 {
            Err(CapabilityDispatcherConfigError::ZeroPayloadBytes)
        } else if max_payload_bytes > MAX_CAPABILITY_PAYLOAD_BYTES {
            Err(CapabilityDispatcherConfigError::PayloadBytesAboveMaximum {
                requested: max_payload_bytes,
                maximum: MAX_CAPABILITY_PAYLOAD_BYTES,
            })
        } else {
            Ok(Self { max_payload_bytes })
        }
    }
}

impl Default for CapabilityDispatcherConfig {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1024 * 1024,
        }
    }
}

/// Invalid capability dispatcher bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityDispatcherConfigError {
    /// Zero bytes would reject every non-empty request.
    ZeroPayloadBytes,
    /// The requested bound exceeds the hard maximum.
    PayloadBytesAboveMaximum {
        /// Rejected requested bound.
        requested: usize,
        /// Hard upper bound.
        maximum: usize,
    },
}

impl Display for CapabilityDispatcherConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPayloadBytes => {
                formatter.write_str("capability payload bound must be positive")
            }
            Self::PayloadBytesAboveMaximum { .. } => {
                formatter.write_str("capability payload bound exceeds the hard maximum")
            }
        }
    }
}

impl Error for CapabilityDispatcherConfigError {}

/// Cloneable worker handler that routes requests through a frozen capability registry.
#[derive(Clone, Debug)]
pub struct CapabilityDispatcher {
    registry: CapabilityRegistry,
    config: CapabilityDispatcherConfig,
}

impl CapabilityDispatcher {
    /// Creates a dispatcher from an immutable registry and validated payload bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload bound is invalid.
    pub fn new(
        registry: CapabilityRegistry,
        config: CapabilityDispatcherConfig,
    ) -> Result<Self, CapabilityDispatcherConfigError> {
        let config = CapabilityDispatcherConfig::new(config.max_payload_bytes)?;
        Ok(Self { registry, config })
    }

    /// Returns the frozen registry shared by worker clones.
    #[must_use]
    pub const fn registry(&self) -> &CapabilityRegistry {
        &self.registry
    }

    /// Returns the validated dispatch bounds.
    #[must_use]
    pub const fn config(&self) -> CapabilityDispatcherConfig {
        self.config
    }
}

#[async_trait]
impl WorkerHandler<CapabilityRequest> for CapabilityDispatcher {
    type Error = CapabilityDispatchError;

    async fn handle(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<(), Self::Error> {
        let capability = request.capability().clone();
        let operation = request.operation().clone();
        if request.input().len() > self.config.max_payload_bytes {
            return Err(CapabilityDispatchError::PayloadTooLarge {
                capability,
                operation,
                actual: request.input().len(),
                limit: self.config.max_payload_bytes,
            });
        }
        let handler = self.registry.handler(&capability).ok_or_else(|| {
            CapabilityDispatchError::UnknownCapability {
                capability: capability.clone(),
                operation: operation.clone(),
            }
        })?;
        handler
            .invoke(context, request)
            .await
            .map_err(|failure| CapabilityDispatchError::Handler {
                capability,
                operation,
                failure,
            })
    }
}

/// Stable category exposed by a generic dispatch failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityDispatchErrorCategory {
    /// No adapter was registered for the versioned identity.
    UnknownCapability,
    /// Opaque input exceeded the configured bound.
    PayloadTooLarge,
    /// The selected concrete adapter failed.
    Handler,
}

/// Structured dispatch failure that never exposes input or concrete error text.
pub enum CapabilityDispatchError {
    /// No adapter was registered; the concrete library was never invoked.
    UnknownCapability {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
    },
    /// Input was rejected before adapter invocation.
    PayloadTooLarge {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Observed encoded bytes.
        actual: usize,
        /// Configured maximum encoded bytes.
        limit: usize,
    },
    /// The selected adapter returned an explicit failure.
    Handler {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Source-preserving adapter failure.
        failure: CapabilityFailure,
    },
}

impl CapabilityDispatchError {
    /// Returns the stable failure category.
    #[must_use]
    pub const fn category(&self) -> CapabilityDispatchErrorCategory {
        match self {
            Self::UnknownCapability { .. } => CapabilityDispatchErrorCategory::UnknownCapability,
            Self::PayloadTooLarge { .. } => CapabilityDispatchErrorCategory::PayloadTooLarge,
            Self::Handler { .. } => CapabilityDispatchErrorCategory::Handler,
        }
    }

    /// Returns the requested capability identity.
    #[must_use]
    pub const fn capability(&self) -> &CapabilityId {
        match self {
            Self::UnknownCapability { capability, .. }
            | Self::PayloadTooLarge { capability, .. }
            | Self::Handler { capability, .. } => capability,
        }
    }

    /// Returns the requested operation.
    #[must_use]
    pub const fn operation(&self) -> &OperationName {
        match self {
            Self::UnknownCapability { operation, .. }
            | Self::PayloadTooLarge { operation, .. }
            | Self::Handler { operation, .. } => operation,
        }
    }

    /// Returns remote-effect certainty for this failed invocation.
    #[must_use]
    pub const fn remote_effect(&self) -> CapabilityRemoteEffect {
        match self {
            Self::UnknownCapability { .. } | Self::PayloadTooLarge { .. } => {
                CapabilityRemoteEffect::NotStarted
            }
            Self::Handler { failure, .. } => failure.remote_effect(),
        }
    }

    /// Returns a preserved concrete adapter failure when invocation began.
    #[must_use]
    pub const fn handler_failure(&self) -> Option<&CapabilityFailure> {
        match self {
            Self::Handler { failure, .. } => Some(failure),
            Self::UnknownCapability { .. } | Self::PayloadTooLarge { .. } => None,
        }
    }
}

impl ClassifyRetry for CapabilityDispatchError {
    fn retry_class(&self) -> RetryErrorClass {
        match self {
            Self::UnknownCapability { .. } | Self::PayloadTooLarge { .. } => {
                RetryErrorClass::DeadLetter
            }
            Self::Handler { failure, .. } => failure.retry_classification(),
        }
    }
}

impl Display for CapabilityDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownCapability { .. } => {
                formatter.write_str("requested capability is not registered")
            }
            Self::PayloadTooLarge { .. } => {
                formatter.write_str("capability input exceeds the configured bound")
            }
            Self::Handler { .. } => formatter.write_str("capability adapter failed"),
        }
    }
}

impl Debug for CapabilityDispatchError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let mut debug = formatter.debug_struct("CapabilityDispatchError");
        debug
            .field("category", &self.category())
            .field("capability", self.capability())
            .field("operation", self.operation())
            .field("remote_effect", &self.remote_effect());
        if let Self::PayloadTooLarge { actual, limit, .. } = self {
            debug.field("actual", actual).field("limit", limit);
        }
        if self.handler_failure().is_some() {
            debug.field("source", &"<preserved; redacted>");
        }
        debug.finish()
    }
}

impl Error for CapabilityDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.handler_failure()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}
