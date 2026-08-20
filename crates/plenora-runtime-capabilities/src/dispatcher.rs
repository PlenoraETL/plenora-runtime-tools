use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
    time::SystemTime,
};

use async_trait::async_trait;
use chrono::DateTime;
use plenora_runtime_messaging::{ClassifyRetry, CorrelationId, PublishOutcome, RetryErrorClass};
use plenora_runtime_worker::{WorkerContext, WorkerHandler};

use crate::{
    CapabilityFailure, CapabilityId, CapabilityPayload, CapabilityRegistry, CapabilityRemoteEffect,
    CapabilityRequest, CapabilityRequestRejection, CapabilityResponse, CapabilityResult,
    CapabilityResultBuildError, CapabilityResultPublishError, CapabilityResultSink,
    EXECUTION_DEADLINE_METADATA_KEY, OperationName, OperationVersion, PlenoraError,
    PlenoraErrorCategory, PlenoraErrorPhase, PlenoraErrorRemoteEffect, PlenoraErrorRetry,
    PlenoraErrorValidationError,
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
#[derive(Clone)]
pub struct CapabilityDispatcher {
    registry: CapabilityRegistry,
    config: CapabilityDispatcherConfig,
    result_sink: Option<Arc<dyn CapabilityResultSink>>,
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
        Ok(Self {
            registry,
            config,
            result_sink: None,
        })
    }

    /// Creates a dispatcher that publishes public results through a broker-neutral sink.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload bound is invalid.
    pub fn with_result_sink<S>(
        registry: CapabilityRegistry,
        config: CapabilityDispatcherConfig,
        result_sink: S,
    ) -> Result<Self, CapabilityDispatcherConfigError>
    where
        S: CapabilityResultSink + 'static,
    {
        Self::with_shared_result_sink(registry, config, Arc::new(result_sink))
    }

    /// Creates a dispatcher using a shared type-erased public result sink.
    ///
    /// # Errors
    ///
    /// Returns an error when the payload bound is invalid.
    pub fn with_shared_result_sink(
        registry: CapabilityRegistry,
        config: CapabilityDispatcherConfig,
        result_sink: Arc<dyn CapabilityResultSink>,
    ) -> Result<Self, CapabilityDispatcherConfigError> {
        let config = CapabilityDispatcherConfig::new(config.max_payload_bytes)?;
        Ok(Self {
            registry,
            config,
            result_sink: Some(result_sink),
        })
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

    async fn complete_response(
        &self,
        capability: CapabilityId,
        operation: OperationName,
        operation_version: OperationVersion,
        correlation_id: CorrelationId,
        expected: Option<CapabilityPayload>,
        response: CapabilityResponse,
    ) -> Result<(), CapabilityDispatchError> {
        let Some((output_contract, output)) = response.into_parts() else {
            return if expected.is_some() {
                Err(CapabilityDispatchError::IncompatibleResponse {
                    capability,
                    operation,
                    reason: CapabilityResponseRejection::MissingOutput,
                })
            } else {
                Ok(())
            };
        };
        if output.len() > self.config.max_payload_bytes {
            return Err(CapabilityDispatchError::IncompatibleResponse {
                capability,
                operation,
                reason: CapabilityResponseRejection::OutputTooLarge,
            });
        }
        if let Some(expected) = expected {
            if expected.contract() != &output_contract {
                return Err(CapabilityDispatchError::IncompatibleResponse {
                    capability,
                    operation,
                    reason: CapabilityResponseRejection::OutputContractMismatch,
                });
            }
            if !expected.supports_content_type(output.content_type.as_ref()) {
                return Err(CapabilityDispatchError::IncompatibleResponse {
                    capability,
                    operation,
                    reason: CapabilityResponseRejection::OutputContentTypeMismatch,
                });
            }
        }
        let result = CapabilityResult::new(
            operation.clone(),
            operation_version,
            output_contract,
            correlation_id,
            output,
        )
        .map_err(|failure| CapabilityDispatchError::ResultMetadata {
            capability: capability.clone(),
            operation: operation.clone(),
            failure,
        })?;
        let sink = self.result_sink.as_ref().ok_or_else(|| {
            CapabilityDispatchError::ResultSinkUnavailable {
                capability: capability.clone(),
                operation: operation.clone(),
            }
        })?;
        match sink.publish_result(result).await {
            Ok(PublishOutcome::Confirmed) => Ok(()),
            Ok(PublishOutcome::OutcomeUnknown) => {
                Err(CapabilityDispatchError::ResultPublicationUnknown {
                    capability,
                    operation,
                })
            }
            Err(failure) => Err(CapabilityDispatchError::ResultPublication {
                capability,
                operation,
                failure,
            }),
        }
    }
}

impl Debug for CapabilityDispatcher {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityDispatcher")
            .field("registry", &self.registry)
            .field("config", &self.config)
            .field(
                "result_sink",
                &self.result_sink.as_ref().map(|_| "<configured>"),
            )
            .finish()
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
        let operation_version = request.operation_version();
        let correlation_id = context.correlation_id;
        if request.input().len() > self.config.max_payload_bytes {
            return Err(CapabilityDispatchError::PayloadTooLarge {
                capability,
                operation,
                actual: request.input().len(),
                limit: self.config.max_payload_bytes,
            });
        }
        let registration = self.registry.registration(&capability).ok_or_else(|| {
            CapabilityDispatchError::UnknownCapability {
                capability: capability.clone(),
                operation: operation.clone(),
            }
        })?;
        if let Some(discovery) = registration.discovery() {
            discovery.validate_request(&request).map_err(|reason| {
                CapabilityDispatchError::IncompatibleRequest {
                    capability: capability.clone(),
                    operation: operation.clone(),
                    reason,
                }
            })?;
        }
        let expected_output = registration
            .discovery()
            .and_then(|discovery| discovery.operation(&operation))
            .map(|discovered| discovered.output().clone());
        let public_error_required = registration.discovery().is_some();
        let deadline = request_deadline(&request).map_err(|reason| {
            CapabilityDispatchError::IncompatibleRequest {
                capability: capability.clone(),
                operation: operation.clone(),
                reason,
            }
        })?;
        let remaining = match deadline {
            Some(deadline) => Some(deadline.duration_since(SystemTime::now()).map_err(
                |_elapsed| CapabilityDispatchError::DeadlineExceeded {
                    capability: capability.clone(),
                    operation: operation.clone(),
                    invocation_started: false,
                },
            )?),
            None => None,
        };
        let cancellation = context.cancellation.clone();
        let invocation = registration.handler().invoke(context, request);
        let result = if let Some(remaining) = remaining {
            match tokio::time::timeout(remaining, invocation).await {
                Ok(result) => result,
                Err(_elapsed) => {
                    let _cancelled = cancellation
                        .cancel(plenora_runtime_worker::TaskCancellationReason::Timeout);
                    return Err(CapabilityDispatchError::DeadlineExceeded {
                        capability,
                        operation,
                        invocation_started: true,
                    });
                }
            }
        } else {
            invocation.await
        };
        let response = result.map_err(|failure| CapabilityDispatchError::Handler {
            capability: capability.clone(),
            operation: operation.clone(),
            public_mapping_missing: public_error_required && failure.public_error().is_none(),
            failure,
        })?;
        self.complete_response(
            capability,
            operation,
            operation_version,
            correlation_id,
            expected_output,
            response,
        )
        .await
    }
}

/// Stable category exposed by a generic dispatch failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityDispatchErrorCategory {
    /// No adapter was registered for the versioned identity.
    UnknownCapability,
    /// Opaque input exceeded the configured bound.
    PayloadTooLarge,
    /// The request conflicts with immutable capability discovery metadata.
    IncompatibleRequest,
    /// The absolute request deadline elapsed.
    DeadlineExceeded,
    /// Adapter output is incompatible with the advertised public result.
    IncompatibleResponse,
    /// Canonical result publication was not confirmed.
    ResultPublication,
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
    /// Discovery metadata rejected the request before adapter invocation.
    IncompatibleRequest {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Stable compatibility rejection.
        reason: CapabilityRequestRejection,
    },
    /// The absolute deadline elapsed before or during adapter invocation.
    DeadlineExceeded {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Whether invocation may have begun, making remote effects unknown.
        invocation_started: bool,
    },
    /// The adapter returned an invalid or missing public output.
    IncompatibleResponse {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Stable response rejection.
        reason: CapabilityResponseRejection,
    },
    /// Canonical result metadata could not be constructed.
    ResultMetadata {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Source-preserving metadata failure.
        failure: CapabilityResultBuildError,
    },
    /// A result was produced but no result sink was configured.
    ResultSinkUnavailable {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
    },
    /// Result publication returned an explicit failure.
    ResultPublication {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Source-preserving sink failure.
        failure: CapabilityResultPublishError,
    },
    /// Result publication effect could not be confirmed.
    ResultPublicationUnknown {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
    },
    /// The selected adapter returned an explicit failure.
    Handler {
        /// Requested versioned capability.
        capability: CapabilityId,
        /// Requested operation.
        operation: OperationName,
        /// Whether a discovered adapter omitted its required public error mapping.
        public_mapping_missing: bool,
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
            Self::IncompatibleRequest { .. } => {
                CapabilityDispatchErrorCategory::IncompatibleRequest
            }
            Self::DeadlineExceeded { .. } => CapabilityDispatchErrorCategory::DeadlineExceeded,
            Self::IncompatibleResponse { .. } => {
                CapabilityDispatchErrorCategory::IncompatibleResponse
            }
            Self::ResultMetadata { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. } => {
                CapabilityDispatchErrorCategory::ResultPublication
            }
            Self::Handler { .. } => CapabilityDispatchErrorCategory::Handler,
        }
    }

    /// Returns the requested capability identity.
    #[must_use]
    pub const fn capability(&self) -> &CapabilityId {
        match self {
            Self::UnknownCapability { capability, .. }
            | Self::PayloadTooLarge { capability, .. }
            | Self::IncompatibleRequest { capability, .. }
            | Self::DeadlineExceeded { capability, .. }
            | Self::IncompatibleResponse { capability, .. }
            | Self::ResultMetadata { capability, .. }
            | Self::ResultSinkUnavailable { capability, .. }
            | Self::ResultPublication { capability, .. }
            | Self::ResultPublicationUnknown { capability, .. }
            | Self::Handler { capability, .. } => capability,
        }
    }

    /// Returns the requested operation.
    #[must_use]
    pub const fn operation(&self) -> &OperationName {
        match self {
            Self::UnknownCapability { operation, .. }
            | Self::PayloadTooLarge { operation, .. }
            | Self::IncompatibleRequest { operation, .. }
            | Self::DeadlineExceeded { operation, .. }
            | Self::IncompatibleResponse { operation, .. }
            | Self::ResultMetadata { operation, .. }
            | Self::ResultSinkUnavailable { operation, .. }
            | Self::ResultPublication { operation, .. }
            | Self::ResultPublicationUnknown { operation, .. }
            | Self::Handler { operation, .. } => operation,
        }
    }

    /// Returns remote-effect certainty for this failed invocation.
    #[must_use]
    pub const fn remote_effect(&self) -> CapabilityRemoteEffect {
        match self {
            Self::UnknownCapability { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IncompatibleRequest { .. }
            | Self::DeadlineExceeded {
                invocation_started: false,
                ..
            } => CapabilityRemoteEffect::NotStarted,
            Self::DeadlineExceeded {
                invocation_started: true,
                ..
            }
            | Self::IncompatibleResponse { .. }
            | Self::ResultMetadata { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. } => CapabilityRemoteEffect::Unknown,
            Self::Handler { failure, .. } => failure.remote_effect(),
        }
    }

    /// Returns a preserved concrete adapter failure when invocation began.
    #[must_use]
    pub const fn handler_failure(&self) -> Option<&CapabilityFailure> {
        match self {
            Self::Handler { failure, .. } => Some(failure),
            Self::UnknownCapability { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IncompatibleRequest { .. }
            | Self::DeadlineExceeded { .. }
            | Self::IncompatibleResponse { .. }
            | Self::ResultMetadata { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. } => None,
        }
    }

    /// Returns the discovery compatibility rejection, when admission failed before invocation.
    #[must_use]
    pub const fn request_rejection(&self) -> Option<CapabilityRequestRejection> {
        match self {
            Self::IncompatibleRequest { reason, .. } => Some(*reason),
            Self::UnknownCapability { .. }
            | Self::PayloadTooLarge { .. }
            | Self::DeadlineExceeded { .. }
            | Self::IncompatibleResponse { .. }
            | Self::ResultMetadata { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. }
            | Self::Handler { .. } => None,
        }
    }

    /// Returns the public response rejection, when adapter output was incompatible.
    #[must_use]
    pub const fn response_rejection(&self) -> Option<CapabilityResponseRejection> {
        match self {
            Self::IncompatibleResponse { reason, .. } => Some(*reason),
            Self::UnknownCapability { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IncompatibleRequest { .. }
            | Self::DeadlineExceeded { .. }
            | Self::ResultMetadata { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. }
            | Self::Handler { .. } => None,
        }
    }

    /// Maps this failure to the four required `plenora-error-v1` axes.
    ///
    /// Adapter mappings are preserved exactly. Runtime admission, deadline, response, and result
    /// failures use stable runtime-owned mappings. A discovered adapter that omits its mapping is
    /// exposed as a protocol failure requiring recovery.
    ///
    /// # Errors
    ///
    /// Returns an error only if a runtime-owned bounded static mapping violates the public error
    /// constructor, which indicates an internal invariant failure.
    pub fn public_error(&self) -> Result<PlenoraError, PlenoraErrorValidationError> {
        if let Self::Handler {
            failure,
            public_mapping_missing: false,
            ..
        } = self
            && let Some(public_error) = failure.public_error()
        {
            return Ok(public_error.clone());
        }

        let (category, phase, effect, retry, message) = match self {
            Self::UnknownCapability { .. } => (
                PlenoraErrorCategory::Unsupported,
                PlenoraErrorPhase::Validate,
                PlenoraErrorRemoteEffect::None,
                PlenoraErrorRetry::Quarantine,
                "Requested runtime capability is not registered.",
            ),
            Self::PayloadTooLarge { .. } => (
                PlenoraErrorCategory::ResourceLimit,
                PlenoraErrorPhase::Validate,
                PlenoraErrorRemoteEffect::None,
                PlenoraErrorRetry::Quarantine,
                "Capability input exceeds the configured runtime bound.",
            ),
            Self::IncompatibleRequest { reason, .. } => (
                request_error_category(*reason),
                PlenoraErrorPhase::Validate,
                PlenoraErrorRemoteEffect::None,
                PlenoraErrorRetry::Quarantine,
                "Capability request is incompatible with discovery metadata.",
            ),
            Self::DeadlineExceeded {
                invocation_started: false,
                ..
            } => (
                PlenoraErrorCategory::Timeout,
                PlenoraErrorPhase::Prepare,
                PlenoraErrorRemoteEffect::None,
                PlenoraErrorRetry::Never,
                "Capability deadline elapsed before invocation.",
            ),
            Self::DeadlineExceeded {
                invocation_started: true,
                ..
            } => (
                PlenoraErrorCategory::Timeout,
                PlenoraErrorPhase::Prepare,
                PlenoraErrorRemoteEffect::Unknown,
                PlenoraErrorRetry::RequiresRecovery,
                "Capability deadline elapsed after invocation may have begun.",
            ),
            Self::IncompatibleResponse { .. } | Self::ResultMetadata { .. } => (
                PlenoraErrorCategory::Protocol,
                PlenoraErrorPhase::Finalize,
                PlenoraErrorRemoteEffect::Unknown,
                PlenoraErrorRetry::RequiresRecovery,
                "Capability result is incompatible with the runtime contract.",
            ),
            Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. } => (
                PlenoraErrorCategory::Io,
                PlenoraErrorPhase::Finalize,
                PlenoraErrorRemoteEffect::Unknown,
                PlenoraErrorRetry::RequiresRecovery,
                "Capability result publication was not confirmed.",
            ),
            Self::Handler {
                failure,
                public_mapping_missing: false,
                ..
            } => (
                PlenoraErrorCategory::Internal,
                PlenoraErrorPhase::Finalize,
                public_remote_effect(failure.remote_effect()),
                public_retry(failure),
                "Legacy capability adapter failed without a public error mapping.",
            ),
            Self::Handler {
                public_mapping_missing: true,
                ..
            } => (
                PlenoraErrorCategory::Protocol,
                PlenoraErrorPhase::Finalize,
                PlenoraErrorRemoteEffect::Unknown,
                PlenoraErrorRetry::RequiresRecovery,
                "Capability adapter omitted its required public error mapping.",
            ),
        };
        PlenoraError::new(category, phase, effect, retry, message)
    }
}

impl ClassifyRetry for CapabilityDispatchError {
    fn retry_class(&self) -> RetryErrorClass {
        match self {
            Self::UnknownCapability { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IncompatibleRequest { .. }
            | Self::DeadlineExceeded {
                invocation_started: false,
                ..
            } => RetryErrorClass::DeadLetter,
            Self::DeadlineExceeded {
                invocation_started: true,
                ..
            }
            | Self::IncompatibleResponse { .. }
            | Self::ResultMetadata { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublication { .. }
            | Self::ResultPublicationUnknown { .. } => RetryErrorClass::OutcomeUnknown,
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
            Self::IncompatibleRequest { .. } => {
                formatter.write_str("capability request is incompatible with discovery metadata")
            }
            Self::DeadlineExceeded { .. } => {
                formatter.write_str("capability execution deadline elapsed")
            }
            Self::IncompatibleResponse { .. } => {
                formatter.write_str("capability output is incompatible with discovery metadata")
            }
            Self::ResultMetadata { .. } => {
                formatter.write_str("capability result metadata could not be constructed")
            }
            Self::ResultSinkUnavailable { .. } => {
                formatter.write_str("capability result sink is not configured")
            }
            Self::ResultPublication { .. } | Self::ResultPublicationUnknown { .. } => {
                formatter.write_str("capability result publication was not confirmed")
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
        if let Some(reason) = self.request_rejection() {
            debug.field("reason", &reason);
        }
        if let Some(reason) = self.response_rejection() {
            debug.field("reason", &reason);
        }
        if let Self::DeadlineExceeded {
            invocation_started, ..
        } = self
        {
            debug.field("invocation_started", invocation_started);
        }
        if self.handler_failure().is_some()
            || matches!(
                self,
                Self::ResultMetadata { .. } | Self::ResultPublication { .. }
            )
        {
            debug.field("source", &"<preserved; redacted>");
        }
        if let Self::Handler {
            public_mapping_missing,
            ..
        } = self
        {
            debug.field("public_mapping_missing", public_mapping_missing);
        }
        debug.finish()
    }
}

impl Error for CapabilityDispatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Handler { failure, .. } => Some(failure),
            Self::ResultMetadata { failure, .. } => Some(failure),
            Self::ResultPublication { failure, .. } => Some(failure),
            Self::UnknownCapability { .. }
            | Self::PayloadTooLarge { .. }
            | Self::IncompatibleRequest { .. }
            | Self::DeadlineExceeded { .. }
            | Self::IncompatibleResponse { .. }
            | Self::ResultSinkUnavailable { .. }
            | Self::ResultPublicationUnknown { .. } => None,
        }
    }
}

/// Stable reason an adapter response is incompatible with its discovery descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityResponseRejection {
    /// A discovered result-producing operation returned only an acknowledgement.
    MissingOutput,
    /// The output contract differs from discovery metadata.
    OutputContractMismatch,
    /// The output content type differs from discovery metadata.
    OutputContentTypeMismatch,
    /// The encoded output exceeds the dispatcher payload bound.
    OutputTooLarge,
}

fn request_deadline(
    request: &CapabilityRequest,
) -> Result<Option<SystemTime>, CapabilityRequestRejection> {
    let value = request
        .input()
        .headers
        .get_text(EXECUTION_DEADLINE_METADATA_KEY)
        .map_err(|_error| CapabilityRequestRejection::InvalidDeadline)?;
    let Some(value) = value else {
        return Ok(None);
    };
    let deadline = DateTime::parse_from_rfc3339(value)
        .map_err(|_error| CapabilityRequestRejection::InvalidDeadline)?;
    if deadline.offset().local_minus_utc() != 0 {
        return Err(CapabilityRequestRejection::InvalidDeadline);
    }
    let deadline: SystemTime = deadline.into();
    Ok(Some(deadline))
}

const fn request_error_category(reason: CapabilityRequestRejection) -> PlenoraErrorCategory {
    match reason {
        CapabilityRequestRejection::UnknownOperation
        | CapabilityRequestRejection::OperationUnavailable
        | CapabilityRequestRejection::RuntimeSurfaceUnsupported
        | CapabilityRequestRejection::DeadlineUnsupported
        | CapabilityRequestRejection::IdempotencyKeyUnsupported => {
            PlenoraErrorCategory::Unsupported
        }
        CapabilityRequestRejection::OperationVersionMismatch
        | CapabilityRequestRejection::InputContractMismatch
        | CapabilityRequestRejection::InputContentTypeMismatch
        | CapabilityRequestRejection::InvalidDeadline => PlenoraErrorCategory::Protocol,
    }
}

const fn public_remote_effect(effect: CapabilityRemoteEffect) -> PlenoraErrorRemoteEffect {
    match effect {
        CapabilityRemoteEffect::NotStarted => PlenoraErrorRemoteEffect::None,
        CapabilityRemoteEffect::Unknown => PlenoraErrorRemoteEffect::Unknown,
    }
}

fn public_retry(failure: &CapabilityFailure) -> PlenoraErrorRetry {
    match (failure.remote_effect(), failure.retry_classification()) {
        (CapabilityRemoteEffect::Unknown, _) | (_, RetryErrorClass::OutcomeUnknown) => {
            PlenoraErrorRetry::RequiresRecovery
        }
        (_, RetryErrorClass::Retryable) => PlenoraErrorRetry::Safe,
        (_, RetryErrorClass::Permanent) => PlenoraErrorRetry::Never,
        (_, RetryErrorClass::DeadLetter) => PlenoraErrorRetry::Quarantine,
    }
}
