use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display, Formatter},
    sync::Arc,
};

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    CapabilityId, CapabilityRequest, ContractId, EXECUTION_DEADLINE_METADATA_KEY,
    IDEMPOTENCY_KEY_METADATA_KEY, OperationName, OperationVersion,
};

/// Maximum encoded size accepted for one Capability Discovery 2.0 document.
pub const MAX_CAPABILITY_DISCOVERY_BYTES: usize = 256 * 1024;
/// Maximum interfaces admitted from one discovery document.
pub const MAX_DISCOVERED_INTERFACES: usize = 16;
/// Maximum operations admitted from one discovery document.
pub const MAX_DISCOVERED_OPERATIONS: usize = 256;
/// Maximum encoded attributes size admitted for one operation.
pub const MAX_CAPABILITY_ATTRIBUTES_BYTES: usize = 16 * 1024;

const MAX_COMPONENT_BYTES: usize = 71;
const MAX_COMPONENT_VERSION_BYTES: usize = 128;
const MAX_ARTIFACT_BYTES: usize = 128;
const MAX_REASON_BYTES: usize = 512;
const MAX_CONTENT_TYPES: usize = 16;
const MAX_CONTENT_TYPE_BYTES: usize = 128;
const MAX_ATTRIBUTE_DEPTH: usize = 8;
const MAX_ATTRIBUTE_NODES: usize = 1_024;
const MAX_ATTRIBUTE_COLLECTION_ITEMS: usize = 64;
const MAX_ATTRIBUTE_KEY_BYTES: usize = 64;
const MAX_ATTRIBUTE_STRING_BYTES: usize = 512;
const RUNTIME_BINDING_CONTRACT: &str = "plenora-runtime-binding-v1";

/// Public surface advertised by Capability Discovery 2.0.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySurface {
    /// In-process Rust API.
    Rust,
    /// Command-line interface.
    Cli,
    /// Python SDK.
    PythonSdk,
    /// Serialized runtime binding.
    Runtime,
}

/// Availability state of a discovered operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityStatus {
    /// Stable and callable.
    Available,
    /// Advertised but not callable.
    Unavailable,
    /// Available only as an experimental surface.
    Experimental,
    /// Retained for compatibility but deprecated.
    Deprecated,
}

/// Conservative maximum side-effect class of an operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CapabilitySideEffect {
    /// No mutation is produced.
    None,
    /// Mutations are restricted to local state.
    Local,
    /// Remote mutation is possible; local effects may also occur.
    Remote,
}

/// Execution controls supported by a discovered operation.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CapabilityControls {
    /// Whether cooperative cancellation is supported.
    pub cancellation: bool,
    /// Whether absolute execution deadlines are supported.
    pub deadline: bool,
    /// Whether an idempotency key is supported.
    pub idempotency_key: bool,
}

/// One public interface advertised by a component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityInterface {
    kind: CapabilitySurface,
    contract: ContractId,
    version: u16,
    artifact: Option<Arc<str>>,
}

impl CapabilityInterface {
    /// Returns the public surface kind.
    #[must_use]
    pub const fn kind(&self) -> CapabilitySurface {
        self.kind
    }

    /// Returns the interface contract identifier.
    #[must_use]
    pub const fn contract(&self) -> &ContractId {
        &self.contract
    }

    /// Returns the positive interface version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    /// Returns the optional public artifact identifier.
    #[must_use]
    pub fn artifact(&self) -> Option<&str> {
        self.artifact.as_deref()
    }
}

/// Contract and content types for one operation input or output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityPayload {
    contract: ContractId,
    content_types: Vec<Arc<str>>,
}

impl CapabilityPayload {
    /// Returns the immutable payload contract.
    #[must_use]
    pub const fn contract(&self) -> &ContractId {
        &self.contract
    }

    /// Returns whether the exact public envelope content type is supported.
    #[must_use]
    pub fn supports_content_type(&self, content_type: &str) -> bool {
        self.content_types
            .iter()
            .any(|candidate| candidate.as_ref() == content_type)
    }

    /// Returns the number of distinct advertised content types.
    #[must_use]
    pub fn content_type_count(&self) -> usize {
        self.content_types.len()
    }
}

/// Bounded component-owned attributes attached to an operation.
#[derive(Clone, Debug, PartialEq)]
pub struct CapabilityAttributes(Map<String, Value>);

impl CapabilityAttributes {
    /// Returns a string attribute without interpreting component-private data.
    #[must_use]
    pub fn string(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(Value::as_str)
    }

    /// Returns the number of top-level attributes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns whether no attributes were advertised.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// One immutable operation descriptor from Capability Discovery 2.0.
#[derive(Clone, Debug, PartialEq)]
pub struct CapabilityOperation {
    name: OperationName,
    version: OperationVersion,
    status: CapabilityStatus,
    reason: Option<Arc<str>>,
    surfaces: BTreeSet<CapabilitySurface>,
    input: CapabilityPayload,
    output: CapabilityPayload,
    side_effect: CapabilitySideEffect,
    controls: CapabilityControls,
    attributes: CapabilityAttributes,
}

impl CapabilityOperation {
    /// Returns the complete namespaced operation name.
    #[must_use]
    pub const fn name(&self) -> &OperationName {
        &self.name
    }

    /// Returns the operation contract version.
    #[must_use]
    pub const fn version(&self) -> OperationVersion {
        self.version
    }

    /// Returns the advertised operation status.
    #[must_use]
    pub const fn status(&self) -> CapabilityStatus {
        self.status
    }

    /// Returns the bounded reason attached to an unavailable operation.
    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Returns whether the operation supports a public surface.
    #[must_use]
    pub fn supports_surface(&self, surface: CapabilitySurface) -> bool {
        self.surfaces.contains(&surface)
    }

    /// Returns the input envelope descriptor.
    #[must_use]
    pub const fn input(&self) -> &CapabilityPayload {
        &self.input
    }

    /// Returns the output envelope descriptor.
    #[must_use]
    pub const fn output(&self) -> &CapabilityPayload {
        &self.output
    }

    /// Returns the conservative maximum side-effect class.
    #[must_use]
    pub const fn side_effect(&self) -> CapabilitySideEffect {
        self.side_effect
    }

    /// Returns the execution controls supported by the component.
    #[must_use]
    pub const fn controls(&self) -> CapabilityControls {
        self.controls
    }

    /// Returns the bounded component-owned attributes.
    #[must_use]
    pub const fn attributes(&self) -> &CapabilityAttributes {
        &self.attributes
    }
}

/// Validated immutable Capability Discovery 2.0 document.
#[derive(Clone, Debug, PartialEq)]
pub struct CapabilityDiscovery {
    component: Arc<str>,
    component_version: Arc<str>,
    interfaces: Vec<CapabilityInterface>,
    operations: BTreeMap<OperationName, CapabilityOperation>,
}

impl CapabilityDiscovery {
    /// Parses and validates a bounded Capability Discovery 2.0 JSON document.
    ///
    /// # Errors
    ///
    /// Returns a redaction-safe error category for malformed, unsupported, unbounded, or
    /// internally inconsistent documents.
    pub fn from_json(bytes: &[u8]) -> Result<Self, CapabilityDiscoveryError> {
        if bytes.len() > MAX_CAPABILITY_DISCOVERY_BYTES {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::DocumentTooLarge,
            ));
        }
        let raw: RawDiscovery = serde_json::from_slice(bytes).map_err(|_error| {
            CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidJson)
        })?;
        Self::try_from_raw(raw)
    }

    /// Returns the component identifier from the discovery document.
    #[must_use]
    pub fn component(&self) -> &str {
        &self.component
    }

    /// Returns the component semantic version text.
    #[must_use]
    pub fn component_version(&self) -> &str {
        &self.component_version
    }

    /// Returns all advertised public interfaces.
    #[must_use]
    pub fn interfaces(&self) -> &[CapabilityInterface] {
        &self.interfaces
    }

    /// Returns all operations in stable name order.
    #[must_use]
    pub fn operations(&self) -> impl ExactSizeIterator<Item = &CapabilityOperation> {
        self.operations.values()
    }

    /// Looks up a complete namespaced operation.
    #[must_use]
    pub fn operation(&self, operation: &OperationName) -> Option<&CapabilityOperation> {
        self.operations.get(operation)
    }

    /// Looks up an operation by its already-known public name.
    #[must_use]
    pub fn operation_named(&self, operation: &str) -> Option<&CapabilityOperation> {
        self.operations
            .values()
            .find(|candidate| candidate.name().as_str() == operation)
    }

    /// Resolves the single compatible runtime interface to a versioned capability identity.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime interface is absent, duplicated, uses another binding,
    /// omits its artifact, or advertises an invalid runtime capability identity.
    pub fn runtime_capability(&self) -> Result<CapabilityId, CapabilityDiscoveryError> {
        let mut runtime = self
            .interfaces
            .iter()
            .filter(|interface| interface.kind == CapabilitySurface::Runtime);
        let interface = runtime.next().ok_or_else(|| {
            CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::MissingRuntimeInterface)
        })?;
        if runtime.next().is_some() {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::DuplicateRuntimeInterface,
            ));
        }
        if interface.contract.as_str() != RUNTIME_BINDING_CONTRACT {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::UnsupportedRuntimeBinding,
            ));
        }
        let artifact = interface.artifact().ok_or_else(|| {
            CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::MissingRuntimeArtifact)
        })?;
        CapabilityId::new(artifact, interface.version).map_err(|_error| {
            CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidRuntimeCapability)
        })
    }

    /// Checks the request against the immutable discovered runtime descriptor.
    ///
    /// # Errors
    ///
    /// Returns a stable admission reason before the concrete component is invoked.
    pub fn validate_request(
        &self,
        request: &CapabilityRequest,
    ) -> Result<(), CapabilityRequestRejection> {
        let operation = self
            .operation(request.operation())
            .ok_or(CapabilityRequestRejection::UnknownOperation)?;
        if operation.version != request.operation_version() {
            return Err(CapabilityRequestRejection::OperationVersionMismatch);
        }
        if operation.status != CapabilityStatus::Available {
            return Err(CapabilityRequestRejection::OperationUnavailable);
        }
        if !operation.supports_surface(CapabilitySurface::Runtime) {
            return Err(CapabilityRequestRejection::RuntimeSurfaceUnsupported);
        }
        if operation.input.contract() != request.input_contract() {
            return Err(CapabilityRequestRejection::InputContractMismatch);
        }
        if !operation
            .input
            .supports_content_type(request.input().content_type.as_ref())
        {
            return Err(CapabilityRequestRejection::InputContentTypeMismatch);
        }
        if request
            .input()
            .headers
            .contains_key(EXECUTION_DEADLINE_METADATA_KEY)
            && !operation.controls.deadline
        {
            return Err(CapabilityRequestRejection::DeadlineUnsupported);
        }
        if request
            .input()
            .headers
            .contains_key(IDEMPOTENCY_KEY_METADATA_KEY)
            && !operation.controls.idempotency_key
        {
            return Err(CapabilityRequestRejection::IdempotencyKeyUnsupported);
        }
        Ok(())
    }

    fn try_from_raw(raw: RawDiscovery) -> Result<Self, CapabilityDiscoveryError> {
        if raw.schema_version != 2 {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::UnsupportedSchemaVersion,
            ));
        }
        if !is_component(&raw.component) {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidComponent,
            ));
        }
        if !is_component_version(&raw.component_version) {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidComponentVersion,
            ));
        }
        if raw.interfaces.is_empty() {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::MissingInterfaces,
            ));
        }
        if raw.interfaces.len() > MAX_DISCOVERED_INTERFACES {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::TooManyInterfaces,
            ));
        }
        if raw.operations.len() > MAX_DISCOVERED_OPERATIONS {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::TooManyOperations,
            ));
        }

        let mut interfaces = Vec::with_capacity(raw.interfaces.len());
        for interface in raw.interfaces {
            let interface = CapabilityInterface::try_from(interface)?;
            if interfaces.contains(&interface) {
                return Err(CapabilityDiscoveryError::new(
                    CapabilityDiscoveryErrorKind::DuplicateInterface,
                ));
            }
            interfaces.push(interface);
        }

        let mut operations = BTreeMap::new();
        for operation in raw.operations {
            let operation = CapabilityOperation::try_from(operation)?;
            if operations
                .insert(operation.name.clone(), operation)
                .is_some()
            {
                return Err(CapabilityDiscoveryError::new(
                    CapabilityDiscoveryErrorKind::DuplicateOperation,
                ));
            }
        }
        Ok(Self {
            component: Arc::from(raw.component),
            component_version: Arc::from(raw.component_version),
            interfaces,
            operations,
        })
    }
}

/// Stable reason why a discovery document was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityDiscoveryErrorKind {
    /// Encoded JSON exceeded the hard bound.
    DocumentTooLarge,
    /// JSON syntax or shape is invalid.
    InvalidJson,
    /// The schema version is not Capability Discovery 2.0.
    UnsupportedSchemaVersion,
    /// The component identifier is invalid.
    InvalidComponent,
    /// The component version is not a bounded semantic version.
    InvalidComponentVersion,
    /// At least one public interface is required.
    MissingInterfaces,
    /// The interface count exceeds the runtime bound.
    TooManyInterfaces,
    /// An interface descriptor is invalid.
    InvalidInterface,
    /// An identical interface was repeated.
    DuplicateInterface,
    /// The operation count exceeds the runtime bound.
    TooManyOperations,
    /// An operation descriptor is invalid.
    InvalidOperation,
    /// An operation identifier was repeated.
    DuplicateOperation,
    /// Component-owned attributes exceed structural bounds.
    InvalidAttributes,
    /// No runtime interface was advertised.
    MissingRuntimeInterface,
    /// More than one runtime interface was advertised.
    DuplicateRuntimeInterface,
    /// The runtime interface does not use Runtime Binding 1.0.
    UnsupportedRuntimeBinding,
    /// The runtime interface omitted its artifact identifier.
    MissingRuntimeArtifact,
    /// The runtime artifact is not a valid capability identity.
    InvalidRuntimeCapability,
}

/// Redaction-safe Capability Discovery 2.0 validation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityDiscoveryError {
    kind: CapabilityDiscoveryErrorKind,
}

impl CapabilityDiscoveryError {
    const fn new(kind: CapabilityDiscoveryErrorKind) -> Self {
        Self { kind }
    }

    /// Returns the stable rejection category.
    #[must_use]
    pub const fn kind(self) -> CapabilityDiscoveryErrorKind {
        self.kind
    }
}

impl Display for CapabilityDiscoveryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "capability discovery document is invalid: {:?}",
            self.kind
        )
    }
}

impl Error for CapabilityDiscoveryError {}

/// Stable reason a request is incompatible with discovered capability metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityRequestRejection {
    /// The operation is not advertised.
    UnknownOperation,
    /// The requested operation contract version differs.
    OperationVersionMismatch,
    /// The operation is not stably available.
    OperationUnavailable,
    /// The operation does not advertise the runtime surface.
    RuntimeSurfaceUnsupported,
    /// The input contract identifier differs.
    InputContractMismatch,
    /// The public envelope content type is not supported.
    InputContentTypeMismatch,
    /// The request carries a deadline that the operation does not advertise.
    DeadlineUnsupported,
    /// The request carries an idempotency key that the operation does not advertise.
    IdempotencyKeyUnsupported,
    /// The deadline is not a valid absolute RFC 3339 UTC timestamp.
    InvalidDeadline,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDiscovery {
    schema_version: u16,
    component: String,
    component_version: String,
    interfaces: Vec<RawInterface>,
    operations: Vec<RawOperation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawInterface {
    kind: CapabilitySurface,
    contract: String,
    version: u64,
    artifact: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPayload {
    contract: String,
    content_types: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOperation {
    id: String,
    version: u64,
    status: CapabilityStatus,
    reason: Option<String>,
    surfaces: Vec<CapabilitySurface>,
    input: RawPayload,
    output: RawPayload,
    side_effect: CapabilitySideEffect,
    controls: CapabilityControls,
    #[serde(default)]
    attributes: Map<String, Value>,
}

impl TryFrom<RawInterface> for CapabilityInterface {
    type Error = CapabilityDiscoveryError;

    fn try_from(raw: RawInterface) -> Result<Self, Self::Error> {
        let version = u16::try_from(raw.version).ok().filter(|value| *value > 0);
        let contract = ContractId::new(&raw.contract).ok();
        let artifact = raw.artifact.map(Arc::<str>::from);
        if version.is_none()
            || contract.is_none()
            || artifact
                .as_ref()
                .is_some_and(|value| value.is_empty() || value.len() > MAX_ARTIFACT_BYTES)
        {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidInterface,
            ));
        }
        Ok(Self {
            kind: raw.kind,
            contract: contract.ok_or_else(|| {
                CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidInterface)
            })?,
            version: version.ok_or_else(|| {
                CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidInterface)
            })?,
            artifact,
        })
    }
}

impl TryFrom<RawPayload> for CapabilityPayload {
    type Error = CapabilityDiscoveryError;

    fn try_from(raw: RawPayload) -> Result<Self, Self::Error> {
        if raw.content_types.is_empty() || raw.content_types.len() > MAX_CONTENT_TYPES {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidOperation,
            ));
        }
        let contract = ContractId::new(raw.contract).map_err(|_error| {
            CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidOperation)
        })?;
        let mut content_types = Vec::with_capacity(raw.content_types.len());
        for content_type in raw.content_types {
            if !is_content_type(&content_type)
                || content_types
                    .iter()
                    .any(|candidate: &Arc<str>| candidate.as_ref() == content_type)
            {
                return Err(CapabilityDiscoveryError::new(
                    CapabilityDiscoveryErrorKind::InvalidOperation,
                ));
            }
            content_types.push(Arc::from(content_type));
        }
        Ok(Self {
            contract,
            content_types,
        })
    }
}

impl TryFrom<RawOperation> for CapabilityOperation {
    type Error = CapabilityDiscoveryError;

    fn try_from(raw: RawOperation) -> Result<Self, Self::Error> {
        let name = OperationName::new(raw.id).map_err(|_error| {
            CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidOperation)
        })?;
        let version = u16::try_from(raw.version)
            .ok()
            .and_then(|value| OperationVersion::new(value).ok())
            .ok_or_else(|| {
                CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidOperation)
            })?;
        if raw.surfaces.is_empty() || raw.surfaces.len() > 4 {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidOperation,
            ));
        }
        let surfaces: BTreeSet<_> = raw.surfaces.iter().copied().collect();
        if surfaces.len() != raw.surfaces.len()
            || raw
                .reason
                .as_ref()
                .is_some_and(|reason| reason.is_empty() || reason.len() > MAX_REASON_BYTES)
            || (raw.status == CapabilityStatus::Unavailable && raw.reason.is_none())
        {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidOperation,
            ));
        }
        validate_attributes(&raw.attributes)?;
        Ok(Self {
            name,
            version,
            status: raw.status,
            reason: raw.reason.map(Arc::from),
            surfaces,
            input: CapabilityPayload::try_from(raw.input)?,
            output: CapabilityPayload::try_from(raw.output)?,
            side_effect: raw.side_effect,
            controls: raw.controls,
            attributes: CapabilityAttributes(raw.attributes),
        })
    }
}

fn is_component(value: &str) -> bool {
    let Some(name) = value.strip_prefix("plenora-") else {
        return false;
    };
    value.len() <= MAX_COMPONENT_BYTES
        && (2..=63).contains(&name.len())
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name
            .bytes()
            .skip(1)
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn is_component_version(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_COMPONENT_VERSION_BYTES {
        return false;
    }
    let suffix_index = value.find(['-', '+']);
    let (core, suffix) = suffix_index.map_or((value, None), |index| {
        (&value[..index], Some(&value[index + 1..]))
    });
    if suffix.is_some_and(|suffix| {
        suffix.is_empty()
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    }) {
        return false;
    }
    let parts: Vec<_> = core.split('.').collect();
    parts.len() == 3
        && parts.iter().all(|part| {
            !part.is_empty()
                && part.bytes().all(|byte| byte.is_ascii_digit())
                && (part == &"0" || !part.starts_with('0'))
        })
}

fn is_content_type(value: &str) -> bool {
    if value.is_empty() || value.len() > MAX_CONTENT_TYPE_BYTES {
        return false;
    }
    let Some((kind, subtype)) = value.split_once('/') else {
        return false;
    };
    !kind.is_empty()
        && !subtype.is_empty()
        && !subtype.contains('/')
        && kind.bytes().all(is_content_type_byte)
        && subtype.bytes().all(is_content_type_byte)
}

fn is_content_type_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#' | b'$' | b'&' | b'^' | b'_' | b'.' | b'+' | b'-'
        )
}

fn validate_attributes(attributes: &Map<String, Value>) -> Result<(), CapabilityDiscoveryError> {
    let encoded = serde_json::to_vec(attributes).map_err(|_error| {
        CapabilityDiscoveryError::new(CapabilityDiscoveryErrorKind::InvalidAttributes)
    })?;
    if encoded.len() > MAX_CAPABILITY_ATTRIBUTES_BYTES
        || attributes.len() > MAX_ATTRIBUTE_COLLECTION_ITEMS
    {
        return Err(CapabilityDiscoveryError::new(
            CapabilityDiscoveryErrorKind::InvalidAttributes,
        ));
    }
    let mut nodes = 0;
    validate_attribute_value(&Value::Object(attributes.clone()), 0, &mut nodes)
}

fn validate_attribute_value(
    value: &Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), CapabilityDiscoveryError> {
    *nodes += 1;
    if depth > MAX_ATTRIBUTE_DEPTH || *nodes > MAX_ATTRIBUTE_NODES {
        return Err(CapabilityDiscoveryError::new(
            CapabilityDiscoveryErrorKind::InvalidAttributes,
        ));
    }
    match value {
        Value::Object(values) => {
            if values.len() > MAX_ATTRIBUTE_COLLECTION_ITEMS
                || values.keys().any(|key| key.len() > MAX_ATTRIBUTE_KEY_BYTES)
            {
                return Err(CapabilityDiscoveryError::new(
                    CapabilityDiscoveryErrorKind::InvalidAttributes,
                ));
            }
            for child in values.values() {
                validate_attribute_value(child, depth + 1, nodes)?;
            }
        }
        Value::Array(values) => {
            if values.len() > MAX_ATTRIBUTE_COLLECTION_ITEMS {
                return Err(CapabilityDiscoveryError::new(
                    CapabilityDiscoveryErrorKind::InvalidAttributes,
                ));
            }
            for child in values {
                validate_attribute_value(child, depth + 1, nodes)?;
            }
        }
        Value::String(value) if value.len() > MAX_ATTRIBUTE_STRING_BYTES => {
            return Err(CapabilityDiscoveryError::new(
                CapabilityDiscoveryErrorKind::InvalidAttributes,
            ));
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}
