use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

/// Maximum accepted bytes for a runtime control component identity.
pub const MAX_CONTROL_COMPONENT_ID_BYTES: usize = 96;

/// Stable, bounded identifier for one registered runtime component.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ControlComponentId(Arc<str>);

impl ControlComponentId {
    /// Validates a component identifier.
    ///
    /// Identifiers accept lowercase ASCII letters, digits, `.`, `_`, and `-`.
    ///
    /// # Errors
    ///
    /// Returns a stable validation category for empty, oversized, or invalid input.
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, ControlComponentIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ControlComponentIdError::Empty);
        }
        if value.len() > MAX_CONTROL_COMPONENT_ID_BYTES {
            return Err(ControlComponentIdError::TooLong);
        }
        if !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        }) {
            return Err(ControlComponentIdError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    /// Returns the validated identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ControlComponentId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Component identifier validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlComponentIdError {
    /// Identifier is empty.
    Empty,
    /// Identifier exceeds its defensive byte bound.
    TooLong,
    /// Identifier contains unsupported characters.
    InvalidCharacter,
}

impl Display for ControlComponentIdError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "control component identifier must not be empty",
            Self::TooLong => "control component identifier exceeds the byte bound",
            Self::InvalidCharacter => "control component identifier contains invalid characters",
        })
    }
}

impl Error for ControlComponentIdError {}

/// Stable component category exposed during discovery.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ControlComponentKind {
    /// Bounded worker executor.
    Worker,
    /// Scheduler registry.
    Scheduler,
    /// Process memory-pressure monitor.
    Memory,
    /// Child-process supervisor.
    Subprocess,
}

/// One payload-free discovery record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlComponent {
    /// Validated component identity.
    pub id: ControlComponentId,
    /// Runtime component category.
    pub kind: ControlComponentKind,
}
