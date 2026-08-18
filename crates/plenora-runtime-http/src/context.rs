use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use plenora_runtime_messaging::{CorrelationId, MessageId};

/// Unique identity assigned to one HTTP request.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RequestId(MessageId);

impl RequestId {
    /// Generates a random request identifier.
    #[must_use]
    pub fn random() -> Self {
        Self(MessageId::random())
    }

    /// Wraps an existing messaging identifier without changing its value.
    #[must_use]
    pub const fn from_message_id(value: MessageId) -> Self {
        Self(value)
    }

    /// Returns the underlying portable identifier.
    #[must_use]
    pub const fn as_message_id(&self) -> &MessageId {
        &self.0
    }

    /// Consumes the wrapper and returns its portable identifier.
    #[must_use]
    pub const fn into_message_id(self) -> MessageId {
        self.0
    }
}

impl Display for RequestId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl From<MessageId> for RequestId {
    fn from(value: MessageId) -> Self {
        Self::from_message_id(value)
    }
}

impl From<RequestId> for MessageId {
    fn from(value: RequestId) -> Self {
        value.into_message_id()
    }
}

impl FromStr for RequestId {
    type Err = RequestIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<MessageId>()
            .map(Self::from_message_id)
            .map_err(|_error| RequestIdParseError)
    }
}

/// A supplied HTTP request identifier was not a valid UUID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdParseError;

impl Display for RequestIdParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid HTTP request identifier")
    }
}

impl Error for RequestIdParseError {}

/// Request-scoped identifiers inserted by the HTTP middleware.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HttpRequestContext {
    request_id: RequestId,
    correlation_id: CorrelationId,
}

impl HttpRequestContext {
    /// Creates a request context from validated identifiers.
    #[must_use]
    pub const fn new(request_id: RequestId, correlation_id: CorrelationId) -> Self {
        Self {
            request_id,
            correlation_id,
        }
    }

    /// Returns the unique request identifier.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Returns the cross-operation correlation identifier.
    #[must_use]
    pub const fn correlation_id(self) -> CorrelationId {
        self.correlation_id
    }
}
