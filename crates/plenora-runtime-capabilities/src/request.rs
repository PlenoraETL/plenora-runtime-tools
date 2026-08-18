use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
};

use plenora_runtime_messaging::{MessageCodec, MetadataKeyError, SerializedMessage};

use crate::{CapabilityId, CapabilityIdentifierError, OperationName};

/// Metadata key carrying the canonical capability namespace.
pub const CAPABILITY_NAME_METADATA_KEY: &str = "plenora.capability.name";
/// Metadata key carrying the positive capability wire version.
pub const CAPABILITY_VERSION_METADATA_KEY: &str = "plenora.capability.version";
/// Metadata key carrying the capability-local operation name.
pub const CAPABILITY_OPERATION_METADATA_KEY: &str = "plenora.capability.operation";

/// Transport-neutral request routed to one registered capability.
#[derive(Clone, Eq, PartialEq)]
pub struct CapabilityRequest {
    capability: CapabilityId,
    operation: OperationName,
    input: SerializedMessage,
}

impl CapabilityRequest {
    /// Creates a request from validated routing and opaque serialized input.
    ///
    /// The three `plenora.capability.*` metadata keys are reserved. Encoding replaces them with
    /// canonical routing values, and decoding removes them before adapter invocation.
    #[must_use]
    pub const fn new(
        capability: CapabilityId,
        operation: OperationName,
        input: SerializedMessage,
    ) -> Self {
        Self {
            capability,
            operation,
            input,
        }
    }

    /// Returns the capability selected by this request.
    #[must_use]
    pub const fn capability(&self) -> &CapabilityId {
        &self.capability
    }

    /// Returns the operation selected within the capability.
    #[must_use]
    pub const fn operation(&self) -> &OperationName {
        &self.operation
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
            .field("input", &self.input)
            .finish()
    }
}

/// Portable codec storing capability routing in namespaced message metadata.
#[derive(Clone, Copy, Debug, Default)]
pub struct CapabilityMessageCodec;

impl MessageCodec<CapabilityRequest> for CapabilityMessageCodec {
    type Error = CapabilityMessageCodecError;

    fn encode(&self, request: &CapabilityRequest) -> Result<SerializedMessage, Self::Error> {
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
        Ok(message)
    }

    fn decode(&self, message: &SerializedMessage) -> Result<CapabilityRequest, Self::Error> {
        let name = required_text(message, CAPABILITY_NAME_METADATA_KEY)?;
        let version = required_text(message, CAPABILITY_VERSION_METADATA_KEY)?
            .parse::<u16>()
            .map_err(|_error| {
                CapabilityMessageCodecError::InvalidVersion(CAPABILITY_VERSION_METADATA_KEY)
            })?;
        let operation = required_text(message, CAPABILITY_OPERATION_METADATA_KEY)?;
        let capability =
            CapabilityId::new(name, version).map_err(CapabilityMessageCodecError::Identifier)?;
        let operation =
            OperationName::new(operation).map_err(CapabilityMessageCodecError::Identifier)?;
        let mut input = message.clone();
        let _name = input.headers.remove(CAPABILITY_NAME_METADATA_KEY);
        let _version = input.headers.remove(CAPABILITY_VERSION_METADATA_KEY);
        let _operation = input.headers.remove(CAPABILITY_OPERATION_METADATA_KEY);
        Ok(CapabilityRequest::new(capability, operation, input))
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
                &matches!(self, Self::Identifier(_) | Self::Metadata(_))
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
            Self::Metadata(error) => Some(error),
            Self::Missing(_) | Self::InvalidEncoding(_) | Self::InvalidVersion(_) => None,
        }
    }
}

fn required_text<'a>(
    message: &'a SerializedMessage,
    key: &'static str,
) -> Result<&'a str, CapabilityMessageCodecError> {
    message
        .headers
        .get_text(key)
        .map_err(|_error| CapabilityMessageCodecError::InvalidEncoding(key))?
        .ok_or(CapabilityMessageCodecError::Missing(key))
}
