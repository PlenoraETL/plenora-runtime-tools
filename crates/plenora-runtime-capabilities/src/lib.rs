//! Generic, bounded integration point for application-owned Rust libraries.
//!
//! A consumer registers capability adapters during startup, freezes the registry, and passes a
//! [`CapabilityDispatcher`] to the worker runtime. Concrete libraries do not depend on this crate:
//! the embedding application owns the small adapter implementing [`CapabilityHandler`].

#![forbid(unsafe_code)]

mod contract;
mod discovery;
mod dispatcher;
mod handler;
mod identifier;
mod public_error;
mod registry;
mod request;
mod rest_profile;
mod result;

pub use contract::{
    ContractId, ContractIdentifierError, ContractIdentifierErrorKind, MAX_CONTRACT_ID_BYTES,
    OperationVersion, OperationVersionError,
};
pub use discovery::{
    CapabilityAttributes, CapabilityControls, CapabilityDiscovery, CapabilityDiscoveryError,
    CapabilityDiscoveryErrorKind, CapabilityInterface, CapabilityOperation, CapabilityPayload,
    CapabilityRequestRejection, CapabilitySideEffect, CapabilityStatus, CapabilitySurface,
    MAX_CAPABILITY_ATTRIBUTES_BYTES, MAX_CAPABILITY_DISCOVERY_BYTES, MAX_DISCOVERED_INTERFACES,
    MAX_DISCOVERED_OPERATIONS,
};
pub use dispatcher::{
    CapabilityDispatchError, CapabilityDispatchErrorCategory, CapabilityDispatcher,
    CapabilityDispatcherConfig, CapabilityDispatcherConfigError, CapabilityResponseRejection,
    MAX_CAPABILITY_PAYLOAD_BYTES,
};
pub use handler::{CapabilityFailure, CapabilityHandler, CapabilityRemoteEffect};
pub use identifier::{
    CapabilityId, CapabilityIdentifierError, CapabilityIdentifierErrorKind,
    CapabilityIdentifierField, MAX_CAPABILITY_NAME_BYTES, MAX_OPERATION_NAME_BYTES, OperationName,
};
pub use public_error::{
    PLENORA_ERROR_CONTENT_TYPE, PLENORA_ERROR_CONTRACT, PlenoraError, PlenoraErrorCategory,
    PlenoraErrorPhase, PlenoraErrorRemoteEffect, PlenoraErrorRetry, PlenoraErrorValidationError,
    PlenoraErrorValidationErrorKind,
};
pub use registry::{
    CapabilityRegistry, CapabilityRegistryBuilder, CapabilityRegistryConfig,
    CapabilityRegistryError, MAX_REGISTERED_CAPABILITIES,
};
pub use request::{
    CAPABILITY_NAME_METADATA_KEY, CAPABILITY_OPERATION_METADATA_KEY,
    CAPABILITY_VERSION_METADATA_KEY, CapabilityMessageCodec, CapabilityMessageCodecError,
    CapabilityRequest, EXECUTION_DEADLINE_METADATA_KEY, IDEMPOTENCY_KEY_METADATA_KEY,
    INPUT_CONTRACT_METADATA_KEY, OPERATION_VERSION_METADATA_KEY, OUTPUT_CONTRACT_METADATA_KEY,
    TRACE_CORRELATION_ID_METADATA_KEY,
};
pub use rest_profile::{
    REST_ATTRIBUTES_CONTRACT, REST_COMPONENT, REST_DOWNLOAD_OPERATION, REST_ENRICH_OPERATION,
    REST_EXECUTION_REQUEST_CONTRACT, REST_EXECUTION_RESULT_CONTRACT,
    REST_FILE_TRANSFER_INPUT_CONTRACT, REST_FILE_TRANSFER_RESULT_CONTRACT, REST_GENERATE_OPERATION,
    REST_RUNTIME_CAPABILITY, REST_RUNTIME_VERSION, REST_TEST_OPERATION, REST_UPLOAD_OPERATION,
    RestCapabilityProfile, RestProfileError, RestProfileErrorKind,
};
pub use result::{
    CapabilityResponse, CapabilityResult, CapabilityResultBuildError, CapabilityResultPublishError,
    CapabilityResultSink,
};
