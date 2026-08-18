use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    str::Utf8Error,
    sync::Arc,
};

use bytes::Bytes;

/// Maximum number of portable metadata entries retained on one message.
pub const MAX_METADATA_ENTRIES: usize = 64;
/// Maximum UTF-8 byte length of one portable metadata key.
pub const MAX_METADATA_KEY_BYTES: usize = 128;
/// Maximum byte length of one portable metadata value.
pub const MAX_METADATA_VALUE_BYTES: usize = 8_192;
/// Maximum combined key and value bytes retained on one message.
pub const MAX_METADATA_TOTAL_BYTES: usize = 32_768;

/// Reason a metadata key was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataKeyErrorKind {
    /// The key does not contain a namespace separator.
    MissingNamespace,
    /// The key contains an empty namespace segment.
    EmptySegment,
    /// The key contains a character outside the portable metadata alphabet.
    InvalidCharacter,
    /// The key exceeds the portable per-key byte limit.
    KeyTooLong,
    /// The binary value exceeds the portable per-value byte limit.
    ValueTooLarge,
    /// A new key would exceed the portable entry-count limit.
    EntryCapacityExceeded,
    /// The resulting map would exceed the portable total-byte limit.
    TotalBytesExceeded,
}

/// Validation error returned for a non-namespaced metadata key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataKeyError {
    key: Arc<str>,
    kind: MetadataKeyErrorKind,
}

impl MetadataKeyError {
    /// Returns the rejected key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the validation failure kind.
    #[must_use]
    pub const fn kind(&self) -> MetadataKeyErrorKind {
        self.kind
    }
}

impl Display for MetadataKeyError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "metadata key '{}' is invalid: {:?}",
            self.key, self.kind
        )
    }
}

impl Error for MetadataKeyError {}

/// Namespaced, binary-safe metadata attached to a message.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct MessageMetadata {
    entries: BTreeMap<Arc<str>, Bytes>,
}

impl MessageMetadata {
    /// Creates empty message metadata.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a binary metadata value after validating its key.
    ///
    /// # Errors
    ///
    /// Returns an error when key syntax or a per-entry/aggregate metadata bound is violated.
    pub fn insert(
        &mut self,
        key: impl Into<Arc<str>>,
        value: impl Into<Bytes>,
    ) -> Result<Option<Bytes>, MetadataKeyError> {
        let key = key.into();
        validate_key(&key)?;
        let value = value.into();
        validate_value(&key, &value)?;
        if !self.entries.contains_key(key.as_ref()) && self.entries.len() >= MAX_METADATA_ENTRIES {
            return Err(metadata_error(
                &key,
                MetadataKeyErrorKind::EntryCapacityExceeded,
            ));
        }

        let current_bytes =
            self.entries
                .iter()
                .fold(0_usize, |total, (stored_key, stored_value)| {
                    total
                        .saturating_add(stored_key.len())
                        .saturating_add(stored_value.len())
                });
        let replaced_bytes = self.entries.get(key.as_ref()).map_or(0, |stored_value| {
            key.len().saturating_add(stored_value.len())
        });
        let resulting_bytes = current_bytes
            .saturating_sub(replaced_bytes)
            .saturating_add(key.len())
            .saturating_add(value.len());
        if resulting_bytes > MAX_METADATA_TOTAL_BYTES {
            return Err(metadata_error(
                &key,
                MetadataKeyErrorKind::TotalBytesExceeded,
            ));
        }

        Ok(self.entries.insert(key, value))
    }

    /// Inserts a UTF-8 metadata value.
    ///
    /// # Errors
    ///
    /// Returns the same validation errors as the binary insertion method.
    pub fn insert_text(
        &mut self,
        key: impl Into<Arc<str>>,
        value: impl Into<String>,
    ) -> Result<Option<Bytes>, MetadataKeyError> {
        self.insert(key, Bytes::from(value.into()))
    }

    /// Returns a binary metadata value.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.entries.get(key)
    }

    /// Returns a metadata value interpreted as UTF-8.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored bytes are not valid UTF-8.
    pub fn get_text(&self, key: &str) -> Result<Option<&str>, Utf8Error> {
        self.get(key)
            .map(|value| std::str::from_utf8(value))
            .transpose()
    }

    /// Returns whether the metadata contains a key.
    #[must_use]
    pub fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    /// Removes a metadata entry.
    #[must_use]
    pub fn remove(&mut self, key: &str) -> Option<Bytes> {
        self.entries.remove(key)
    }

    /// Returns the number of metadata entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether the metadata is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over metadata in key order.
    #[must_use]
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &Bytes)> {
        self.entries
            .iter()
            .map(|(key, value)| (key.as_ref(), value))
    }
}

impl Debug for MessageMetadata {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let keys: Vec<&str> = self.entries.keys().map(AsRef::as_ref).collect();
        formatter
            .debug_struct("MessageMetadata")
            .field("entry_count", &self.entries.len())
            .field("keys", &keys)
            .finish()
    }
}

fn validate_key(key: &str) -> Result<(), MetadataKeyError> {
    let error = |kind| metadata_error(key, kind);

    if key.len() > MAX_METADATA_KEY_BYTES {
        return Err(error(MetadataKeyErrorKind::KeyTooLong));
    }
    if !key.contains('.') {
        return Err(error(MetadataKeyErrorKind::MissingNamespace));
    }
    if key.split('.').any(str::is_empty) {
        return Err(error(MetadataKeyErrorKind::EmptySegment));
    }
    if !key.bytes().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, b'.' | b'-' | b'_')
    }) {
        return Err(error(MetadataKeyErrorKind::InvalidCharacter));
    }

    Ok(())
}

fn validate_value(key: &str, value: &Bytes) -> Result<(), MetadataKeyError> {
    if value.len() > MAX_METADATA_VALUE_BYTES {
        Err(metadata_error(key, MetadataKeyErrorKind::ValueTooLarge))
    } else {
        Ok(())
    }
}

fn metadata_error(key: &str, kind: MetadataKeyErrorKind) -> MetadataKeyError {
    MetadataKeyError {
        key: Arc::from(key),
        kind,
    }
}
