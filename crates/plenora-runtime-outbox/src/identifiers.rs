use std::{
    fmt::{self, Debug, Formatter},
    sync::Arc,
};

use uuid::Uuid;

/// Opaque identity of one persisted outbox record.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutboxId(Uuid);

impl OutboxId {
    /// Generates a random version 4 identifier.
    #[must_use]
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps an existing universally unique identifier.
    #[must_use]
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the wrapped universally unique identifier.
    #[must_use]
    pub const fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Consumes the wrapper and returns its universally unique identifier.
    #[must_use]
    pub const fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl From<Uuid> for OutboxId {
    fn from(value: Uuid) -> Self {
        Self::from_uuid(value)
    }
}

impl From<OutboxId> for Uuid {
    fn from(value: OutboxId) -> Self {
        value.into_uuid()
    }
}

/// Opaque caller-supplied key for one idempotent operation.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct IdempotencyKey(Arc<str>);

impl IdempotencyKey {
    /// Creates an idempotency key.
    #[must_use]
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// Returns the key value for persistence adapters.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Debug for IdempotencyKey {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("IdempotencyKey(<redacted>)")
    }
}

/// Opaque digest used to detect reuse of an idempotency key with different input.
#[derive(Clone, Eq, Hash, PartialEq)]
pub struct RequestFingerprint(Arc<[u8]>);

impl RequestFingerprint {
    /// Creates a fingerprint from caller-computed bytes.
    #[must_use]
    pub fn new(value: impl Into<Arc<[u8]>>) -> Self {
        Self(value.into())
    }

    /// Returns the digest bytes for persistence adapters.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the fingerprint length.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether the fingerprint is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Debug for RequestFingerprint {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RequestFingerprint")
            .field("byte_len", &self.len())
            .finish_non_exhaustive()
    }
}
