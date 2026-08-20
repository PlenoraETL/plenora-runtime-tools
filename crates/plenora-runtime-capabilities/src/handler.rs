use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use plenora_runtime_messaging::{ClassifyRetry, RetryErrorClass};
use plenora_runtime_worker::WorkerContext;

use crate::{
    CapabilityRequest, CapabilityResponse, PlenoraError, PlenoraErrorRemoteEffect,
    PlenoraErrorRetry,
};

/// Certainty about external effects when a capability adapter fails.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityRemoteEffect {
    /// The concrete library was not invoked.
    NotStarted,
    /// The adapter cannot prove whether the concrete library applied an effect.
    Unknown,
}

/// Source-preserving, redaction-safe failure returned through the object-safe capability boundary.
#[derive(Clone)]
pub struct CapabilityFailure {
    retry_class: RetryErrorClass,
    remote_effect: CapabilityRemoteEffect,
    public_error: Option<PlenoraError>,
    source: Arc<dyn Error + Send + Sync>,
}

impl CapabilityFailure {
    /// Wraps a concrete adapter error with explicit retry and remote-effect semantics.
    #[must_use]
    pub fn new<E>(
        retry_class: RetryErrorClass,
        remote_effect: CapabilityRemoteEffect,
        source: E,
    ) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        let remote_effect = if retry_class == RetryErrorClass::OutcomeUnknown {
            CapabilityRemoteEffect::Unknown
        } else {
            remote_effect
        };
        Self {
            retry_class,
            remote_effect,
            public_error: None,
            source: Arc::new(source),
        }
    }

    /// Wraps an adapter failure with an explicit bounded `plenora-error-v1` mapping.
    #[must_use]
    pub fn with_public_error<E>(public_error: PlenoraError, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        let retry_class = match public_error.retry() {
            PlenoraErrorRetry::Never => RetryErrorClass::Permanent,
            PlenoraErrorRetry::Quarantine => RetryErrorClass::DeadLetter,
            PlenoraErrorRetry::Safe
            | PlenoraErrorRetry::RequiresIdempotencyKey
            | PlenoraErrorRetry::After { .. } => RetryErrorClass::Retryable,
            PlenoraErrorRetry::RequiresRecovery => RetryErrorClass::OutcomeUnknown,
        };
        let remote_effect = match public_error.remote_effect() {
            PlenoraErrorRemoteEffect::None => CapabilityRemoteEffect::NotStarted,
            PlenoraErrorRemoteEffect::RolledBack
            | PlenoraErrorRemoteEffect::Partial
            | PlenoraErrorRemoteEffect::Committed
            | PlenoraErrorRemoteEffect::Unknown => CapabilityRemoteEffect::Unknown,
        };
        Self {
            retry_class,
            remote_effect,
            public_error: Some(public_error),
            source: Arc::new(source),
        }
    }

    /// Returns the adapter's retry classification.
    #[must_use]
    pub const fn retry_classification(&self) -> RetryErrorClass {
        self.retry_class
    }

    /// Returns whether invocation effects may have occurred.
    #[must_use]
    pub const fn remote_effect(&self) -> CapabilityRemoteEffect {
        self.remote_effect
    }

    /// Returns the explicit public error mapping, when supplied by the adapter.
    #[must_use]
    pub const fn public_error(&self) -> Option<&PlenoraError> {
        self.public_error.as_ref()
    }

    /// Returns the preserved concrete adapter error.
    #[must_use]
    pub fn source_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.source.as_ref()
    }
}

impl ClassifyRetry for CapabilityFailure {
    fn retry_class(&self) -> RetryErrorClass {
        self.retry_class
    }
}

impl Display for CapabilityFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability adapter invocation failed")
    }
}

impl Debug for CapabilityFailure {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityFailure")
            .field("retry_class", &self.retry_class)
            .field("remote_effect", &self.remote_effect)
            .field("has_public_error", &self.public_error.is_some())
            .field("source", &"<preserved; redacted>")
            .finish()
    }
}

impl Error for CapabilityFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Object-safe adapter implemented by an embedding application for each Rust library.
///
/// The adapter owns translation between the portable request and concrete library types. It must
/// forward `context.cancellation` and progress into long-running library operations
/// when those operations support cooperative control.
#[async_trait]
pub trait CapabilityHandler: Send + Sync {
    /// Invokes one capability operation.
    ///
    /// # Errors
    ///
    /// Returns an explicit, source-preserving failure. The worker retry policy consumes its
    /// [`RetryErrorClass`] instead of inferring retry safety from strings.
    async fn invoke(
        &self,
        context: WorkerContext,
        request: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure>;
}
