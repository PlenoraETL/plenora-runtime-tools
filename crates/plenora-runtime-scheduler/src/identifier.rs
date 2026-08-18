use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
    time::SystemTime,
};

/// Maximum UTF-8 byte length of a stable schedule identifier.
pub const MAX_SCHEDULE_ID_BYTES: usize = 128;

/// Validated stable schedule identifier.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ScheduleId(Arc<str>);

impl ScheduleId {
    /// Creates a lowercase namespaced identifier.
    ///
    /// # Errors
    ///
    /// Returns an error for empty, oversized, or non-portable values.
    pub fn new(value: impl Into<Arc<str>>) -> Result<Self, ScheduleIdError> {
        let value = value.into();
        if value.is_empty() {
            return Err(ScheduleIdError::Empty);
        }
        if value.len() > MAX_SCHEDULE_ID_BYTES {
            return Err(ScheduleIdError::TooLong);
        }
        if !value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        }) {
            return Err(ScheduleIdError::InvalidCharacter);
        }
        Ok(Self(value))
    }

    /// Returns the portable identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for ScheduleId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Invalid schedule identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScheduleIdError {
    /// The identifier was empty.
    Empty,
    /// The identifier exceeded the portable bound.
    TooLong,
    /// The identifier contained a non-portable character.
    InvalidCharacter,
}

impl Display for ScheduleIdError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Empty => "schedule identifier must not be empty",
            Self::TooLong => "schedule identifier is too long",
            Self::InvalidCharacter => "schedule identifier contains an invalid character",
        })
    }
}

impl Error for ScheduleIdError {}

/// Deterministic identity of one scheduled occurrence.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ScheduleOccurrenceId {
    /// Stable schedule identity.
    pub schedule_id: ScheduleId,
    /// Logical due instant. Retries preserve this value across process restarts.
    pub due_at: SystemTime,
}
