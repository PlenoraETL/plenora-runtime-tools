use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

use crate::{CapabilityDiscovery, CapabilitySideEffect, CapabilityStatus, CapabilitySurface};

/// Capability Discovery component identifier for the REST black box.
pub const REST_COMPONENT: &str = "plenora-rest-tools";
/// Runtime capability artifact for the REST black box.
pub const REST_RUNTIME_CAPABILITY: &str = "plenora.rest-tools";
/// Required runtime binding version for REST operations.
pub const REST_RUNTIME_VERSION: u16 = 1;
/// Public `rest.test` operation.
pub const REST_TEST_OPERATION: &str = "rest.test";
/// Public `rest.generate` operation.
pub const REST_GENERATE_OPERATION: &str = "rest.generate";
/// Public `rest.enrich` operation.
pub const REST_ENRICH_OPERATION: &str = "rest.enrich";
/// Public `rest.download` operation.
pub const REST_DOWNLOAD_OPERATION: &str = "rest.download";
/// Public `rest.upload` operation.
pub const REST_UPLOAD_OPERATION: &str = "rest.upload";
/// Request contract shared by REST execution operations.
pub const REST_EXECUTION_REQUEST_CONTRACT: &str = "plenora-rest-execution-request-v1";
/// Result contract shared by REST execution operations.
pub const REST_EXECUTION_RESULT_CONTRACT: &str = "plenora-rest-execution-result-v1";
/// Input contract shared by REST download and upload.
pub const REST_FILE_TRANSFER_INPUT_CONTRACT: &str = "plenora-rest-file-transfer-input-v1";
/// Result contract shared by REST download and upload.
pub const REST_FILE_TRANSFER_RESULT_CONTRACT: &str = "plenora-rest-file-transfer-result-v1";
/// Component-owned REST capability attributes contract.
pub const REST_ATTRIBUTES_CONTRACT: &str = "plenora-rest-capability-attributes-v1";

const JSON_CONTENT_TYPE: &str = "application/json";

/// Validator for the required `plenora-rest-tools-profile-v1` runtime surface.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RestCapabilityProfile;

impl RestCapabilityProfile {
    /// Validates the complete required REST runtime profile without inspecting implementation
    /// details or performing HTTP work.
    ///
    /// # Errors
    ///
    /// Returns a stable incompatibility category when identity, operation availability, public
    /// contracts, JSON envelopes, side effects, controls, or component-owned attributes differ.
    pub fn validate(discovery: &CapabilityDiscovery) -> Result<(), RestProfileError> {
        if discovery.component() != REST_COMPONENT {
            return Err(RestProfileError::new(
                RestProfileErrorKind::ComponentMismatch,
                None,
            ));
        }
        let identity = discovery.runtime_capability().map_err(|_error| {
            RestProfileError::new(RestProfileErrorKind::RuntimeBindingMismatch, None)
        })?;
        if identity.name() != REST_RUNTIME_CAPABILITY || identity.version() != REST_RUNTIME_VERSION
        {
            return Err(RestProfileError::new(
                RestProfileErrorKind::RuntimeBindingMismatch,
                None,
            ));
        }

        for expected in EXPECTED_OPERATIONS {
            validate_operation(discovery, expected)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct ExpectedOperation {
    name: &'static str,
    input_contract: &'static str,
    output_contract: &'static str,
    direction: Option<&'static str>,
}

const EXPECTED_OPERATIONS: &[ExpectedOperation] = &[
    ExpectedOperation {
        name: REST_TEST_OPERATION,
        input_contract: REST_EXECUTION_REQUEST_CONTRACT,
        output_contract: REST_EXECUTION_RESULT_CONTRACT,
        direction: None,
    },
    ExpectedOperation {
        name: REST_GENERATE_OPERATION,
        input_contract: REST_EXECUTION_REQUEST_CONTRACT,
        output_contract: REST_EXECUTION_RESULT_CONTRACT,
        direction: None,
    },
    ExpectedOperation {
        name: REST_ENRICH_OPERATION,
        input_contract: REST_EXECUTION_REQUEST_CONTRACT,
        output_contract: REST_EXECUTION_RESULT_CONTRACT,
        direction: None,
    },
    ExpectedOperation {
        name: REST_DOWNLOAD_OPERATION,
        input_contract: REST_FILE_TRANSFER_INPUT_CONTRACT,
        output_contract: REST_FILE_TRANSFER_RESULT_CONTRACT,
        direction: Some("download"),
    },
    ExpectedOperation {
        name: REST_UPLOAD_OPERATION,
        input_contract: REST_FILE_TRANSFER_INPUT_CONTRACT,
        output_contract: REST_FILE_TRANSFER_RESULT_CONTRACT,
        direction: Some("upload"),
    },
];

fn validate_operation(
    discovery: &CapabilityDiscovery,
    expected: &ExpectedOperation,
) -> Result<(), RestProfileError> {
    let operation = discovery.operation_named(expected.name).ok_or_else(|| {
        RestProfileError::new(
            RestProfileErrorKind::RequiredOperationMissing,
            Some(expected.name),
        )
    })?;
    if operation.version().get() != 1 {
        return Err(RestProfileError::new(
            RestProfileErrorKind::OperationVersionMismatch,
            Some(expected.name),
        ));
    }
    if operation.status() != CapabilityStatus::Available {
        return Err(RestProfileError::new(
            RestProfileErrorKind::OperationUnavailable,
            Some(expected.name),
        ));
    }
    if !operation.supports_surface(CapabilitySurface::Runtime) {
        return Err(RestProfileError::new(
            RestProfileErrorKind::RuntimeSurfaceMissing,
            Some(expected.name),
        ));
    }
    if operation.input().contract().as_str() != expected.input_contract
        || operation.output().contract().as_str() != expected.output_contract
    {
        return Err(RestProfileError::new(
            RestProfileErrorKind::PayloadContractMismatch,
            Some(expected.name),
        ));
    }
    if operation.input().content_type_count() != 1
        || operation.output().content_type_count() != 1
        || !operation.input().supports_content_type(JSON_CONTENT_TYPE)
        || !operation.output().supports_content_type(JSON_CONTENT_TYPE)
    {
        return Err(RestProfileError::new(
            RestProfileErrorKind::EnvelopeContentTypeMismatch,
            Some(expected.name),
        ));
    }
    if operation.side_effect() != CapabilitySideEffect::Remote {
        return Err(RestProfileError::new(
            RestProfileErrorKind::SideEffectMismatch,
            Some(expected.name),
        ));
    }
    let controls = operation.controls();
    if !controls.cancellation || !controls.deadline || controls.idempotency_key {
        return Err(RestProfileError::new(
            RestProfileErrorKind::ControlsMismatch,
            Some(expected.name),
        ));
    }
    let attributes = operation.attributes();
    if attributes.string("contract") != Some(REST_ATTRIBUTES_CONTRACT) {
        return Err(RestProfileError::new(
            RestProfileErrorKind::AttributesContractMismatch,
            Some(expected.name),
        ));
    }
    if let Some(direction) = expected.direction
        && (attributes.string("direction") != Some(direction)
            || attributes.string("integrity") != Some("sha256"))
    {
        return Err(RestProfileError::new(
            RestProfileErrorKind::TransferAttributesMismatch,
            Some(expected.name),
        ));
    }
    Ok(())
}

/// Stable reason a REST discovery document does not implement the required public profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestProfileErrorKind {
    /// The component is not `plenora-rest-tools`.
    ComponentMismatch,
    /// Runtime Binding 1.0 identity or version differs.
    RuntimeBindingMismatch,
    /// A required REST operation is absent.
    RequiredOperationMissing,
    /// A REST operation version differs from v1.
    OperationVersionMismatch,
    /// A required REST operation is not available.
    OperationUnavailable,
    /// A required REST operation omits the runtime surface.
    RuntimeSurfaceMissing,
    /// Input or output contract differs from the ratified profile.
    PayloadContractMismatch,
    /// The public runtime envelope is not JSON-only.
    EnvelopeContentTypeMismatch,
    /// The conservative side-effect class is not remote.
    SideEffectMismatch,
    /// Cancellation, deadline, or idempotency controls differ.
    ControlsMismatch,
    /// The REST attributes contract identifier is absent or incompatible.
    AttributesContractMismatch,
    /// Download or upload direction/integrity attributes differ.
    TransferAttributesMismatch,
}

/// Redaction-safe REST public-profile validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestProfileError {
    kind: RestProfileErrorKind,
    operation: Option<&'static str>,
}

impl RestProfileError {
    const fn new(kind: RestProfileErrorKind, operation: Option<&'static str>) -> Self {
        Self { kind, operation }
    }

    /// Returns the stable incompatibility category.
    #[must_use]
    pub const fn kind(self) -> RestProfileErrorKind {
        self.kind
    }

    /// Returns the affected required operation, when applicable.
    #[must_use]
    pub const fn operation(self) -> Option<&'static str> {
        self.operation
    }
}

impl Display for RestProfileError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("REST capability discovery is incompatible with the required profile")
    }
}

impl Error for RestProfileError {}
