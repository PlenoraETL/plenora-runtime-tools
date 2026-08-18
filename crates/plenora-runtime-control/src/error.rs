use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use crate::{ControlComponentId, ControlComponentKind};

/// Runtime control lookup failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlPlaneError {
    /// No component of the requested category has this identity.
    UnknownComponent {
        /// Requested category.
        kind: ControlComponentKind,
        /// Requested validated identity.
        id: ControlComponentId,
    },
}

impl Display for ControlPlaneError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("runtime control component is not registered")
    }
}

impl Error for ControlPlaneError {}

/// Bounded control-plane registration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlRegistrationError {
    /// A category already contains this identity.
    Duplicate {
        /// Duplicated category.
        kind: ControlComponentKind,
        /// Duplicated identity.
        id: ControlComponentId,
    },
    /// A category exhausted its configured registration capacity.
    CapacityExceeded {
        /// Exhausted category.
        kind: ControlComponentKind,
        /// Configured category capacity.
        limit: usize,
    },
}

impl Display for ControlRegistrationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Duplicate { .. } => "runtime control component is already registered",
            Self::CapacityExceeded { .. } => "runtime control component capacity is exhausted",
        })
    }
}

impl Error for ControlRegistrationError {}
