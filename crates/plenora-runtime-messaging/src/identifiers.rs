use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use uuid::Uuid;

macro_rules! message_identifier {
    ($(#[$metadata:meta])* $name:ident) => {
        $(#[$metadata])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Uuid);

        impl $name {
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

        impl Display for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                Display::fmt(&self.0, formatter)
            }
        }

        impl From<Uuid> for $name {
            fn from(value: Uuid) -> Self {
                Self::from_uuid(value)
            }
        }

        impl From<$name> for Uuid {
            fn from(value: $name) -> Self {
                value.into_uuid()
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self::from_uuid)
            }
        }
    };
}

message_identifier!(
    /// Globally unique identity of a message.
    MessageId
);

message_identifier!(
    /// Identity shared by messages that belong to one correlated operation.
    CorrelationId
);

message_identifier!(
    /// Identity of the message that directly caused another message.
    CausationId
);

impl From<MessageId> for CausationId {
    fn from(value: MessageId) -> Self {
        Self::from_uuid(value.into_uuid())
    }
}
