use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

// P2.1b: only outbound candidate construction lives in this generic crate.
// Canonical decoding and the only registered dispatcher live in runtime-boundary.
// Constructing or encoding a candidate is not evidence of admission.

use plenora_runtime_messaging::{MetadataKeyError, SerializedMessage};

use crate::{
    CapabilityId, CapabilityIdentifierError, ContractId, ContractIdentifierError, OperationName,
    OperationVersion,
};

/// Metadata key carrying the canonical capability namespace.
pub const CAPABILITY_NAME_METADATA_KEY: &str = "plenora.capability.name";
/// Metadata key carrying the positive capability wire version.
pub const CAPABILITY_VERSION_METADATA_KEY: &str = "plenora.capability.version";
/// Metadata key carrying the capability-local operation name.
pub const CAPABILITY_OPERATION_METADATA_KEY: &str = "plenora.capability.operation";
/// Metadata key carrying the positive public operation-contract version.
pub const OPERATION_VERSION_METADATA_KEY: &str = "plenora.operation.version";
/// Metadata key carrying the immutable public input contract identifier.
pub const INPUT_CONTRACT_METADATA_KEY: &str = "plenora.input.contract";
/// Metadata key carrying the immutable public output contract identifier.
pub const OUTPUT_CONTRACT_METADATA_KEY: &str = "plenora.output.contract";
/// Metadata key carrying the originating public correlation identity.
pub const TRACE_CORRELATION_ID_METADATA_KEY: &str = "plenora.trace.correlation_id";
/// Metadata key carrying an absolute RFC 3339 UTC execution deadline.
pub const EXECUTION_DEADLINE_METADATA_KEY: &str = "plenora.execution.deadline";
/// Metadata key carrying a bounded opaque idempotency key.
pub const IDEMPOTENCY_KEY_METADATA_KEY: &str = "plenora.execution.idempotency_key";

/// Unadmitted outbound candidate or input inspected by an adapter.
/// Constructing this DTO never authorizes runtime dispatch.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityRequest {
    capability: CapabilityId,
    operation: OperationName,
    operation_version: OperationVersion,
    input_contract: ContractId,
    input: SerializedMessage,
}

impl CapabilityRequest {
    /// Creates a request from validated routing and opaque serialized input.
    ///
    /// The five Runtime Binding 1.0 routing and input-contract metadata keys are reserved.
    /// Encoding replaces them with canonical values, and decoding removes them before adapter
    /// invocation.
    #[must_use]
    pub const fn new(
        capability: CapabilityId,
        operation: OperationName,
        operation_version: OperationVersion,
        input_contract: ContractId,
        input: SerializedMessage,
    ) -> Self {
        Self {
            capability,
            operation,
            operation_version,
            input_contract,
            input,
        }
    }

    /// Returns the capability selected by this request.
    #[must_use]
    pub const fn capability(&self) -> &CapabilityId {
        &self.capability
    }

    /// Returns the complete public operation selected within the capability.
    #[must_use]
    pub const fn operation(&self) -> &OperationName {
        &self.operation
    }

    /// Returns the selected public operation-contract version.
    #[must_use]
    pub const fn operation_version(&self) -> OperationVersion {
        self.operation_version
    }

    /// Returns the immutable public input contract identifier.
    #[must_use]
    pub const fn input_contract(&self) -> &ContractId {
        &self.input_contract
    }

    /// Returns the opaque content-type-aware input.
    #[must_use]
    pub const fn input(&self) -> &SerializedMessage {
        &self.input
    }

    /// Consumes the request and returns its serialized input.
    #[must_use]
    pub fn into_input(self) -> SerializedMessage {
        self.input
    }
}

impl Debug for CapabilityRequest {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityRequest")
            .field("capability", &self.capability)
            .field("operation", &self.operation)
            .field("operation_version", &self.operation_version)
            .field("input_contract", &self.input_contract)
            .field("input", &self.input)
            .finish()
    }
}

/// Outbound encoder storing routing in namespaced message metadata.
/// Decoding is exclusively owned by the runtime boundary.
#[derive(Clone, Copy, Debug, Default)]
pub struct CapabilityMessageCodec;

impl CapabilityMessageCodec {
    /// Encode an outbound candidate; this does not admit it for execution.
    /// # Errors
    /// Rejects metadata exceeding the portable limits.
    pub fn encode(
        &self,
        request: &CapabilityRequest,
    ) -> Result<SerializedMessage, CapabilityMessageCodecError> {
        let mut message = request.input.clone();
        let _previous = message
            .headers
            .insert_text(CAPABILITY_NAME_METADATA_KEY, request.capability.name())
            .map_err(CapabilityMessageCodecError::Metadata)?;
        let _previous = message
            .headers
            .insert_text(
                CAPABILITY_VERSION_METADATA_KEY,
                request.capability.version().to_string(),
            )
            .map_err(CapabilityMessageCodecError::Metadata)?;
        let _previous = message
            .headers
            .insert_text(
                CAPABILITY_OPERATION_METADATA_KEY,
                request.operation.as_str(),
            )
            .map_err(CapabilityMessageCodecError::Metadata)?;
        let _previous = message
            .headers
            .insert_text(
                OPERATION_VERSION_METADATA_KEY,
                request.operation_version.to_string(),
            )
            .map_err(CapabilityMessageCodecError::Metadata)?;
        let _previous = message
            .headers
            .insert_text(INPUT_CONTRACT_METADATA_KEY, request.input_contract.as_str())
            .map_err(CapabilityMessageCodecError::Metadata)?;
        Ok(message)
    }
}

/// Stable failure while encoding or decoding capability routing metadata.
pub enum CapabilityMessageCodecError {
    /// A required routing key is absent.
    Missing(&'static str),
    /// A routing value is not UTF-8.
    InvalidEncoding(&'static str),
    /// The version is not a positive `u16` integer.
    InvalidVersion(&'static str),
    /// A decoded routing identifier is invalid.
    Identifier(CapabilityIdentifierError),
    /// A decoded public contract identifier is invalid.
    Contract(ContractIdentifierError),
    /// Encoding exceeded portable message metadata bounds.
    Metadata(MetadataKeyError),
}

impl CapabilityMessageCodecError {
    /// Returns the stable routing field category.
    #[must_use]
    pub const fn field(&self) -> &'static str {
        match self {
            Self::Missing(key) | Self::InvalidEncoding(key) | Self::InvalidVersion(key) => key,
            Self::Identifier(error) => match error.field() {
                crate::CapabilityIdentifierField::Capability => CAPABILITY_NAME_METADATA_KEY,
                crate::CapabilityIdentifierField::Version => CAPABILITY_VERSION_METADATA_KEY,
                crate::CapabilityIdentifierField::Operation => CAPABILITY_OPERATION_METADATA_KEY,
            },
            Self::Contract(_) => INPUT_CONTRACT_METADATA_KEY,
            Self::Metadata(_) => "metadata",
        }
    }
}

impl Debug for CapabilityMessageCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityMessageCodecError")
            .field("field", &self.field())
            .field(
                "source",
                &matches!(
                    self,
                    Self::Identifier(_) | Self::Contract(_) | Self::Metadata(_)
                )
                .then_some("<preserved; redacted>"),
            )
            .finish()
    }
}

impl Display for CapabilityMessageCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "capability routing metadata field '{}' is invalid",
            self.field()
        )
    }
}

impl Error for CapabilityMessageCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Identifier(error) => Some(error),
            Self::Contract(error) => Some(error),
            Self::Metadata(error) => Some(error),
            Self::Missing(_) | Self::InvalidEncoding(_) | Self::InvalidVersion(_) => None,
        }
    }
}
