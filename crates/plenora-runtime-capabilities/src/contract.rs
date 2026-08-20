use std::{
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    num::NonZeroU16,
    sync::Arc,
};

/// Maximum UTF-8 bytes admitted for a public contract identifier.
pub const MAX_CONTRACT_ID_BYTES: usize = 128;

/// Immutable public contract identifier such as `plenora-data-run-input-v1`.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContractId(Arc<str>);

impl ContractId {
    /// Validates and owns one immutable Plenora contract identifier.
    ///
    /// # Errors
    ///
    /// Returns a stable category when the value is empty, oversized, not a
    /// lowercase Plenora identifier, or lacks a positive version suffix.
    pub fn new(value: impl AsRef<str>) -> Result<Self, ContractIdentifierError> {
        let value = value.as_ref();
        validate_contract_id(value)?;
        Ok(Self(Arc::from(value)))
    }

    /// Returns the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for ContractId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ContractId").field(&self.0).finish()
    }
}

impl Display for ContractId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable reason why a public contract identifier was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractIdentifierErrorKind {
    /// The identifier is empty.
    Empty,
    /// The identifier exceeds [`MAX_CONTRACT_ID_BYTES`].
    TooLong,
    /// The identifier does not start with `plenora-`.
    InvalidPrefix,
    /// The identifier contains a non-lowercase-ASCII contract character.
    InvalidCharacter,
    /// The identifier has no `-v<positive integer>` suffix.
    MissingVersion,
    /// The version suffix is not a positive base-10 integer.
    InvalidVersion,
}

/// Redaction-safe public contract identifier validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContractIdentifierError {
    kind: ContractIdentifierErrorKind,
}

impl ContractIdentifierError {
    const fn new(kind: ContractIdentifierErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection reason.
    #[must_use]
    pub const fn kind(&self) -> ContractIdentifierErrorKind {
        self.kind
    }
}

impl Display for ContractIdentifierError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("public contract identifier is invalid")
    }
}

impl Error for ContractIdentifierError {}

/// Positive public operation-contract version carried independently from the runtime binding.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationVersion(NonZeroU16);

impl OperationVersion {
    /// Creates a positive operation-contract version.
    ///
    /// # Errors
    ///
    /// Returns [`OperationVersionError`] when `value` is zero.
    pub const fn new(value: u16) -> Result<Self, OperationVersionError> {
        match NonZeroU16::new(value) {
            Some(value) => Ok(Self(value)),
            None => Err(OperationVersionError),
        }
    }

    /// Returns the positive wire value.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

impl Display for OperationVersion {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.get(), formatter)
    }
}

/// A zero operation-contract version was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationVersionError;

impl Display for OperationVersionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("operation contract version must be positive")
    }
}

impl Error for OperationVersionError {}

fn validate_contract_id(value: &str) -> Result<(), ContractIdentifierError> {
    if value.is_empty() {
        return Err(ContractIdentifierError::new(
            ContractIdentifierErrorKind::Empty,
        ));
    }
    if value.len() > MAX_CONTRACT_ID_BYTES {
        return Err(ContractIdentifierError::new(
            ContractIdentifierErrorKind::TooLong,
        ));
    }
    let Some(unversioned) = value.strip_prefix("plenora-") else {
        return Err(ContractIdentifierError::new(
            ContractIdentifierErrorKind::InvalidPrefix,
        ));
    };
    let Some((name, version)) = unversioned.rsplit_once("-v") else {
        return Err(ContractIdentifierError::new(
            ContractIdentifierErrorKind::MissingVersion,
        ));
    };
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(ContractIdentifierError::new(
            ContractIdentifierErrorKind::InvalidCharacter,
        ));
    }
    let mut digits = version.bytes();
    if !matches!(digits.next(), Some(b'1'..=b'9')) || !digits.all(|byte| byte.is_ascii_digit()) {
        return Err(ContractIdentifierError::new(
            ContractIdentifierErrorKind::InvalidVersion,
        ));
    }
    Ok(())
}
