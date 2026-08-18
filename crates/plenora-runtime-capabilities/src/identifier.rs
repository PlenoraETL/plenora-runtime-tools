use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

/// Maximum UTF-8 byte length of a namespaced capability name.
pub const MAX_CAPABILITY_NAME_BYTES: usize = 128;
/// Maximum UTF-8 byte length of an operation name.
pub const MAX_OPERATION_NAME_BYTES: usize = 128;

/// Stable, versioned identity of one integration capability.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CapabilityId {
    name: Arc<str>,
    version: u16,
}

impl CapabilityId {
    /// Creates a validated capability identity.
    ///
    /// Names are lowercase ASCII namespaces such as `plenora.data-tools`. Version zero is
    /// reserved and rejected.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, oversized, non-namespaced, non-portable, or zero-version
    /// identity.
    pub fn new(name: impl Into<Arc<str>>, version: u16) -> Result<Self, CapabilityIdentifierError> {
        let name = name.into();
        validate_name(
            &name,
            CapabilityIdentifierField::Capability,
            MAX_CAPABILITY_NAME_BYTES,
            true,
        )?;
        if version == 0 {
            return Err(CapabilityIdentifierError::new(
                CapabilityIdentifierField::Version,
                CapabilityIdentifierErrorKind::ZeroVersion,
            ));
        }
        Ok(Self { name, version })
    }

    /// Returns the canonical namespaced capability name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the non-zero wire contract version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }
}

impl Display for CapabilityId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}@v{}", self.name, self.version)
    }
}

/// Validated operation name within a capability.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationName(Arc<str>);

impl OperationName {
    /// Creates a portable operation name such as `convert` or `dataset.export`.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, oversized, or non-portable name.
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, CapabilityIdentifierError> {
        let value = value.into();
        validate_name(
            &value,
            CapabilityIdentifierField::Operation,
            MAX_OPERATION_NAME_BYTES,
            false,
        )?;
        Ok(Self(value))
    }

    /// Returns the validated operation text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for OperationName {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Identifier field rejected during validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityIdentifierField {
    /// Namespaced capability name.
    Capability,
    /// Capability contract version.
    Version,
    /// Operation routed within a capability.
    Operation,
}

/// Stable reason an identifier was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityIdentifierErrorKind {
    /// Text is empty.
    Empty,
    /// Text exceeds its explicit byte bound.
    TooLong,
    /// A capability name does not contain at least two namespace segments.
    MissingNamespace,
    /// A namespace segment is empty.
    EmptySegment,
    /// Text contains characters outside the portable lowercase ASCII vocabulary.
    InvalidCharacter,
    /// A segment does not begin and end with an ASCII letter or digit.
    InvalidSegmentBoundary,
    /// Version zero is reserved.
    ZeroVersion,
}

/// Redaction-safe identifier validation error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityIdentifierError {
    field: CapabilityIdentifierField,
    kind: CapabilityIdentifierErrorKind,
}

impl CapabilityIdentifierError {
    const fn new(field: CapabilityIdentifierField, kind: CapabilityIdentifierErrorKind) -> Self {
        Self { field, kind }
    }

    /// Returns the rejected identifier field.
    #[must_use]
    pub const fn field(self) -> CapabilityIdentifierField {
        self.field
    }

    /// Returns the stable validation reason.
    #[must_use]
    pub const fn kind(self) -> CapabilityIdentifierErrorKind {
        self.kind
    }
}

impl Display for CapabilityIdentifierError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "capability identifier field {:?} is invalid: {:?}",
            self.field, self.kind
        )
    }
}

impl Error for CapabilityIdentifierError {}

fn validate_name(
    value: &str,
    field: CapabilityIdentifierField,
    max_bytes: usize,
    namespace_required: bool,
) -> Result<(), CapabilityIdentifierError> {
    if value.is_empty() {
        return Err(CapabilityIdentifierError::new(
            field,
            CapabilityIdentifierErrorKind::Empty,
        ));
    }
    if value.len() > max_bytes {
        return Err(CapabilityIdentifierError::new(
            field,
            CapabilityIdentifierErrorKind::TooLong,
        ));
    }
    if namespace_required && !value.contains('.') {
        return Err(CapabilityIdentifierError::new(
            field,
            CapabilityIdentifierErrorKind::MissingNamespace,
        ));
    }
    for segment in value.split('.') {
        if segment.is_empty() {
            return Err(CapabilityIdentifierError::new(
                field,
                CapabilityIdentifierErrorKind::EmptySegment,
            ));
        }
        if !segment.bytes().all(is_portable_byte) {
            return Err(CapabilityIdentifierError::new(
                field,
                CapabilityIdentifierErrorKind::InvalidCharacter,
            ));
        }
        if !segment
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
            || !segment
                .as_bytes()
                .last()
                .is_some_and(u8::is_ascii_alphanumeric)
        {
            return Err(CapabilityIdentifierError::new(
                field,
                CapabilityIdentifierErrorKind::InvalidSegmentBoundary,
            ));
        }
    }
    Ok(())
}

fn is_portable_byte(value: u8) -> bool {
    value.is_ascii_lowercase() || value.is_ascii_digit() || matches!(value, b'-' | b'_')
}
