use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use async_trait::async_trait;
use plenora_runtime_messaging::{
    CorrelationId, MessageProducer, MetadataKeyError, PublishOutcome, SerializedMessage,
};

use crate::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, ContractId, INPUT_CONTRACT_METADATA_KEY,
    OPERATION_VERSION_METADATA_KEY, OUTPUT_CONTRACT_METADATA_KEY, OperationName, OperationVersion,
    TRACE_CORRELATION_ID_METADATA_KEY,
};

/// Output returned by an application-owned capability adapter.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityResponse {
    output_contract: Option<ContractId>,
    output: Option<SerializedMessage>,
}

impl CapabilityResponse {
    /// Creates a public serialized operation result.
    #[must_use]
    pub const fn new(output_contract: ContractId, output: SerializedMessage) -> Self {
        Self {
            output_contract: Some(output_contract),
            output: Some(output),
        }
    }

    /// Creates an empty acknowledgement for legacy operations whose contract explicitly permits
    /// no public result.
    #[must_use]
    pub const fn acknowledged() -> Self {
        Self {
            output_contract: None,
            output: None,
        }
    }

    /// Returns the adapter-declared output contract, when a result is present.
    #[must_use]
    pub const fn output_contract(&self) -> Option<&ContractId> {
        self.output_contract.as_ref()
    }

    /// Returns the serialized output, when a result is present.
    #[must_use]
    pub const fn output(&self) -> Option<&SerializedMessage> {
        self.output.as_ref()
    }

    pub(crate) fn into_parts(self) -> Option<(ContractId, SerializedMessage)> {
        self.output_contract.zip(self.output)
    }
}

impl Debug for CapabilityResponse {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityResponse")
            .field("output_contract", &self.output_contract)
            .field("output", &self.output)
            .finish()
    }
}

/// Runtime Binding 1.0 success result with canonical routing metadata.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityResult {
    operation: OperationName,
    operation_version: OperationVersion,
    output_contract: ContractId,
    correlation_id: CorrelationId,
    message: SerializedMessage,
}

impl CapabilityResult {
    pub(crate) fn new(
        operation: OperationName,
        operation_version: OperationVersion,
        output_contract: ContractId,
        correlation_id: CorrelationId,
        mut message: SerializedMessage,
    ) -> Result<Self, CapabilityResultBuildError> {
        for key in [
            CAPABILITY_NAME_METADATA_KEY,
            CAPABILITY_VERSION_METADATA_KEY,
            CAPABILITY_OPERATION_METADATA_KEY,
            INPUT_CONTRACT_METADATA_KEY,
            OPERATION_VERSION_METADATA_KEY,
            OUTPUT_CONTRACT_METADATA_KEY,
            TRACE_CORRELATION_ID_METADATA_KEY,
        ] {
            let _removed = message.headers.remove(key);
        }
        message
            .headers
            .insert_text(CAPABILITY_OPERATION_METADATA_KEY, operation.as_str())
            .map_err(CapabilityResultBuildError::Metadata)?;
        message
            .headers
            .insert_text(
                OPERATION_VERSION_METADATA_KEY,
                operation_version.to_string(),
            )
            .map_err(CapabilityResultBuildError::Metadata)?;
        message
            .headers
            .insert_text(OUTPUT_CONTRACT_METADATA_KEY, output_contract.as_str())
            .map_err(CapabilityResultBuildError::Metadata)?;
        message
            .headers
            .insert_text(
                TRACE_CORRELATION_ID_METADATA_KEY,
                correlation_id.to_string(),
            )
            .map_err(CapabilityResultBuildError::Metadata)?;
        Ok(Self {
            operation,
            operation_version,
            output_contract,
            correlation_id,
            message,
        })
    }

    /// Returns the public operation identifier.
    #[must_use]
    pub const fn operation(&self) -> &OperationName {
        &self.operation
    }

    /// Returns the public operation contract version.
    #[must_use]
    pub const fn operation_version(&self) -> OperationVersion {
        self.operation_version
    }

    /// Returns the public output contract identifier.
    #[must_use]
    pub const fn output_contract(&self) -> &ContractId {
        &self.output_contract
    }

    /// Returns the originating correlation identity.
    #[must_use]
    pub const fn correlation_id(&self) -> CorrelationId {
        self.correlation_id
    }

    /// Returns the serialized result envelope.
    #[must_use]
    pub const fn message(&self) -> &SerializedMessage {
        &self.message
    }

    /// Consumes the result and returns the serialized envelope.
    #[must_use]
    pub fn into_message(self) -> SerializedMessage {
        self.message
    }
}

impl Debug for CapabilityResult {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityResult")
            .field("operation", &self.operation)
            .field("operation_version", &self.operation_version)
            .field("output_contract", &self.output_contract)
            .field("correlation_id", &self.correlation_id)
            .field("message", &self.message)
            .finish()
    }
}

/// Failure while constructing canonical Runtime Binding 1.0 result metadata.
#[derive(Debug)]
pub enum CapabilityResultBuildError {
    /// Portable message metadata bounds were exceeded.
    Metadata(MetadataKeyError),
}

impl Display for CapabilityResultBuildError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability result metadata is invalid")
    }
}

impl Error for CapabilityResultBuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Metadata(source) => Some(source),
        }
    }
}

/// Source-preserving, payload-redacted result publication failure.
#[derive(Clone)]
pub struct CapabilityResultPublishError {
    source: Arc<dyn Error + Send + Sync>,
}

impl CapabilityResultPublishError {
    /// Wraps a concrete result-sink failure.
    #[must_use]
    pub fn with_source<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            source: Arc::new(source),
        }
    }

    /// Returns the preserved concrete sink error.
    #[must_use]
    pub fn source_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.source.as_ref()
    }
}

impl Debug for CapabilityResultPublishError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityResultPublishError")
            .field("source", &"<preserved; redacted>")
            .finish()
    }
}

impl Display for CapabilityResultPublishError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("capability result publication failed")
    }
}

impl Error for CapabilityResultPublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Broker-neutral destination for public capability results.
#[async_trait]
pub trait CapabilityResultSink: Send + Sync {
    /// Publishes one canonical Runtime Binding 1.0 result.
    ///
    /// # Errors
    ///
    /// Returns a source-preserving, payload-redacted sink failure.
    async fn publish_result(
        &self,
        result: CapabilityResult,
    ) -> Result<PublishOutcome, CapabilityResultPublishError>;
}

#[async_trait]
impl<P> CapabilityResultSink for P
where
    P: MessageProducer + ?Sized,
{
    async fn publish_result(
        &self,
        result: CapabilityResult,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        self.publish(result.into_message())
            .await
            .map_err(CapabilityResultPublishError::with_source)
    }
}
