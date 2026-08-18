use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use crate::{CapabilityHandler, CapabilityId};

/// Hard upper bound for one process capability registry.
pub const MAX_REGISTERED_CAPABILITIES: usize = 4_096;

/// Explicit capacity of a capability registry assembled during startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityRegistryConfig {
    /// Maximum distinct versioned capability handlers.
    pub max_capabilities: usize,
}

impl CapabilityRegistryConfig {
    /// Creates validated registry bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when capacity is zero or above [`MAX_REGISTERED_CAPABILITIES`].
    pub const fn new(max_capabilities: usize) -> Result<Self, CapabilityRegistryError> {
        if max_capabilities == 0 {
            Err(CapabilityRegistryError::ZeroCapacity)
        } else if max_capabilities > MAX_REGISTERED_CAPABILITIES {
            Err(CapabilityRegistryError::CapacityAboveMaximum {
                requested: max_capabilities,
                maximum: MAX_REGISTERED_CAPABILITIES,
            })
        } else {
            Ok(Self { max_capabilities })
        }
    }
}

impl Default for CapabilityRegistryConfig {
    fn default() -> Self {
        Self {
            max_capabilities: 64,
        }
    }
}

/// Mutable startup-only builder that freezes into [`CapabilityRegistry`].
#[derive(Default)]
pub struct CapabilityRegistryBuilder {
    config: CapabilityRegistryConfig,
    handlers: BTreeMap<CapabilityId, Arc<dyn CapabilityHandler>>,
}

impl CapabilityRegistryBuilder {
    /// Creates an empty builder with validated capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured capacity is invalid.
    pub fn new(config: CapabilityRegistryConfig) -> Result<Self, CapabilityRegistryError> {
        let config = CapabilityRegistryConfig::new(config.max_capabilities)?;
        Ok(Self {
            config,
            handlers: BTreeMap::new(),
        })
    }

    /// Registers one concrete application adapter.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate identity or when the explicit capacity is full.
    pub fn register<H>(
        &mut self,
        capability: CapabilityId,
        handler: H,
    ) -> Result<(), CapabilityRegistryError>
    where
        H: CapabilityHandler + 'static,
    {
        self.register_shared(capability, Arc::new(handler))
    }

    /// Registers one shared type-erased application adapter.
    ///
    /// # Errors
    ///
    /// Returns an error for a duplicate identity or when the explicit capacity is full.
    pub fn register_shared(
        &mut self,
        capability: CapabilityId,
        handler: Arc<dyn CapabilityHandler>,
    ) -> Result<(), CapabilityRegistryError> {
        if self.handlers.contains_key(&capability) {
            return Err(CapabilityRegistryError::Duplicate(capability));
        }
        if self.handlers.len() >= self.config.max_capabilities {
            return Err(CapabilityRegistryError::CapacityReached {
                limit: self.config.max_capabilities,
            });
        }
        self.handlers.insert(capability, handler);
        Ok(())
    }

    /// Freezes all registered handlers into an immutable cloneable registry.
    #[must_use]
    pub fn build(self) -> CapabilityRegistry {
        CapabilityRegistry {
            handlers: Arc::new(self.handlers),
        }
    }
}

impl Debug for CapabilityRegistryBuilder {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityRegistryBuilder")
            .field("config", &self.config)
            .field("registered", &self.handlers.len())
            .field("handlers", &"<type-erased>")
            .finish()
    }
}

/// Immutable capability table shared by all dynamically admitted worker tasks.
#[derive(Clone)]
pub struct CapabilityRegistry {
    handlers: Arc<BTreeMap<CapabilityId, Arc<dyn CapabilityHandler>>>,
}

impl CapabilityRegistry {
    /// Returns the number of registered versioned capabilities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Returns whether no capability is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    /// Returns a bounded, sorted snapshot of registered identities without exposing handlers.
    #[must_use]
    pub fn capabilities(&self) -> Vec<CapabilityId> {
        self.handlers.keys().cloned().collect()
    }

    pub(crate) fn handler(&self, capability: &CapabilityId) -> Option<&dyn CapabilityHandler> {
        self.handlers.get(capability).map(Arc::as_ref)
    }
}

impl Debug for CapabilityRegistry {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityRegistry")
            .field("capabilities", &self.capabilities())
            .field("handlers", &"<type-erased>")
            .finish()
    }
}

/// Failure while configuring or populating a registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityRegistryError {
    /// Zero capacity would reject every adapter.
    ZeroCapacity,
    /// Configured capacity exceeds the hard process bound.
    CapacityAboveMaximum {
        /// Rejected requested capacity.
        requested: usize,
        /// Hard upper bound.
        maximum: usize,
    },
    /// The versioned identity is already registered.
    Duplicate(CapabilityId),
    /// The configured registry bound has been reached.
    CapacityReached {
        /// Configured maximum number of capabilities.
        limit: usize,
    },
}

impl Display for CapabilityRegistryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => {
                formatter.write_str("capability registry capacity must be positive")
            }
            Self::CapacityAboveMaximum { .. } => {
                formatter.write_str("capability registry capacity exceeds the hard maximum")
            }
            Self::Duplicate(_) => formatter.write_str("capability identity is already registered"),
            Self::CapacityReached { .. } => {
                formatter.write_str("capability registry capacity has been reached")
            }
        }
    }
}

impl Error for CapabilityRegistryError {}
