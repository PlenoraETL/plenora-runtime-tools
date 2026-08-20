use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Debug, Display, Formatter},
    sync::Arc,
};

use crate::{
    CapabilityDiscovery, CapabilityDiscoveryError, CapabilityHandler, CapabilityId, REST_COMPONENT,
    REST_RUNTIME_CAPABILITY, RestCapabilityProfile, RestProfileError,
};

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
    registrations: BTreeMap<CapabilityId, CapabilityRegistration>,
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
            registrations: BTreeMap::new(),
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
        if capability.name() == REST_RUNTIME_CAPABILITY {
            return Err(CapabilityRegistryError::DiscoveryRequired(capability));
        }
        self.insert(capability, CapabilityRegistration::new(handler, None))
    }

    /// Registers an adapter together with its validated Capability Discovery 2.0 document.
    ///
    /// The runtime capability identity is resolved exclusively from the document's runtime
    /// interface. The required REST profile is enforced automatically for `plenora-rest-tools`.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible discovery document, duplicate identity, or exhausted
    /// registry capacity.
    pub fn register_discovered<H>(
        &mut self,
        discovery: CapabilityDiscovery,
        handler: H,
    ) -> Result<CapabilityId, CapabilityRegistryError>
    where
        H: CapabilityHandler + 'static,
    {
        self.register_discovered_shared(discovery, Arc::new(handler))
    }

    /// Registers a shared adapter with an immutable Capability Discovery 2.0 document.
    ///
    /// # Errors
    ///
    /// Returns an error for an incompatible discovery document, duplicate identity, or exhausted
    /// registry capacity.
    pub fn register_discovered_shared(
        &mut self,
        discovery: CapabilityDiscovery,
        handler: Arc<dyn CapabilityHandler>,
    ) -> Result<CapabilityId, CapabilityRegistryError> {
        let capability = discovery
            .runtime_capability()
            .map_err(CapabilityRegistryError::Discovery)?;
        if discovery.component() == REST_COMPONENT || capability.name() == REST_RUNTIME_CAPABILITY {
            RestCapabilityProfile::validate(&discovery)
                .map_err(CapabilityRegistryError::RestProfile)?;
        }
        self.insert(
            capability.clone(),
            CapabilityRegistration::new(handler, Some(discovery)),
        )?;
        Ok(capability)
    }

    fn insert(
        &mut self,
        capability: CapabilityId,
        registration: CapabilityRegistration,
    ) -> Result<(), CapabilityRegistryError> {
        if self.registrations.contains_key(&capability) {
            return Err(CapabilityRegistryError::Duplicate(capability));
        }
        if self.registrations.len() >= self.config.max_capabilities {
            return Err(CapabilityRegistryError::CapacityReached {
                limit: self.config.max_capabilities,
            });
        }
        self.registrations.insert(capability, registration);
        Ok(())
    }

    /// Freezes all registered handlers into an immutable cloneable registry.
    #[must_use]
    pub fn build(self) -> CapabilityRegistry {
        CapabilityRegistry {
            registrations: Arc::new(self.registrations),
        }
    }
}

impl Debug for CapabilityRegistryBuilder {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapabilityRegistryBuilder")
            .field("config", &self.config)
            .field("registered", &self.registrations.len())
            .field("handlers", &"<type-erased>")
            .finish()
    }
}

/// Immutable capability table shared by all dynamically admitted worker tasks.
#[derive(Clone)]
pub struct CapabilityRegistry {
    registrations: Arc<BTreeMap<CapabilityId, CapabilityRegistration>>,
}

impl CapabilityRegistry {
    /// Returns the number of registered versioned capabilities.
    #[must_use]
    pub fn len(&self) -> usize {
        self.registrations.len()
    }

    /// Returns whether no capability is registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.registrations.is_empty()
    }

    /// Returns a bounded, sorted snapshot of registered identities without exposing handlers.
    #[must_use]
    pub fn capabilities(&self) -> Vec<CapabilityId> {
        self.registrations.keys().cloned().collect()
    }

    /// Returns the immutable discovery document registered for a capability, when present.
    #[must_use]
    pub fn discovery(&self, capability: &CapabilityId) -> Option<&CapabilityDiscovery> {
        self.registrations
            .get(capability)
            .and_then(CapabilityRegistration::discovery)
    }

    pub(crate) fn registration(
        &self,
        capability: &CapabilityId,
    ) -> Option<&CapabilityRegistration> {
        self.registrations.get(capability)
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
    /// The REST black box cannot be registered without discovery metadata.
    DiscoveryRequired(CapabilityId),
    /// Capability Discovery 2.0 metadata is incompatible with the runtime binding.
    Discovery(CapabilityDiscoveryError),
    /// The REST component does not implement its complete required public profile.
    RestProfile(RestProfileError),
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
            Self::DiscoveryRequired(_) => {
                formatter.write_str("capability discovery is required for this public profile")
            }
            Self::Discovery(_) => {
                formatter.write_str("capability discovery is incompatible with the runtime")
            }
            Self::RestProfile(_) => formatter
                .write_str("REST capability discovery does not satisfy the required profile"),
            Self::CapacityReached { .. } => {
                formatter.write_str("capability registry capacity has been reached")
            }
        }
    }
}

impl Error for CapabilityRegistryError {}

pub(crate) struct CapabilityRegistration {
    handler: Arc<dyn CapabilityHandler>,
    discovery: Option<CapabilityDiscovery>,
}

impl CapabilityRegistration {
    fn new(handler: Arc<dyn CapabilityHandler>, discovery: Option<CapabilityDiscovery>) -> Self {
        Self { handler, discovery }
    }

    pub(crate) fn handler(&self) -> &dyn CapabilityHandler {
        self.handler.as_ref()
    }

    pub(crate) const fn discovery(&self) -> Option<&CapabilityDiscovery> {
        self.discovery.as_ref()
    }
}
