//! Generic, bounded integration point for application-owned Rust libraries.
//!
//! A consumer registers capability adapters during startup, freezes the registry, and passes a
//! [`CapabilityDispatcher`] to the worker runtime. Concrete libraries do not depend on this crate:
//! the embedding application owns the small adapter implementing [`CapabilityHandler`].

#![forbid(unsafe_code)]

mod dispatcher;
mod handler;
mod identifier;
mod registry;
mod request;

pub use dispatcher::{
    CapabilityDispatchError, CapabilityDispatchErrorCategory, CapabilityDispatcher,
    CapabilityDispatcherConfig, CapabilityDispatcherConfigError, MAX_CAPABILITY_PAYLOAD_BYTES,
};
pub use handler::{CapabilityFailure, CapabilityHandler, CapabilityRemoteEffect};
pub use identifier::{
    CapabilityId, CapabilityIdentifierError, CapabilityIdentifierErrorKind,
    CapabilityIdentifierField, MAX_CAPABILITY_NAME_BYTES, MAX_OPERATION_NAME_BYTES, OperationName,
};
pub use registry::{
    CapabilityRegistry, CapabilityRegistryBuilder, CapabilityRegistryConfig,
    CapabilityRegistryError, MAX_REGISTERED_CAPABILITIES,
};
pub use request::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, CapabilityMessageCodec, CapabilityMessageCodecError,
    CapabilityRequest,
};
