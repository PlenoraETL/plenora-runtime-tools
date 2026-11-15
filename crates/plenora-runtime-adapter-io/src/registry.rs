//! Registry restricted to the real compiled discovery and the host's Arrow profile.
use crate::{Error, digest, encode};
use plenora_runtime_chains::contracts::{
    Component, Document, Port, Registry, RegistryBoundariesItem, RegistryOperationsItem,
    parse_registry,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Host-declared Arrow schemas, independently checked at both data boundaries.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ports {
    /// Expected read schema digest; None permits a measured dynamic schema.
    pub read_schema: Option<String>,
    /// Expected write schema digest; None permits a measured dynamic schema.
    pub write_schema: Option<String>,
    /// Whether read output has a resolved Primary Geometry.
    pub primary_geometry: bool,
}
impl Ports {
    pub(crate) fn read(&self) -> Port {
        port(self.read_schema.clone(), self.primary_geometry)
    }
    pub(crate) fn write(&self) -> Port {
        port(self.write_schema.clone(), self.primary_geometry)
    }
}
fn port(schema_digest: Option<String>, primary_geometry: bool) -> Port {
    Port {
        contract: "plenora-arrow-interchange-v1".into(),
        content_type: "application/vnd.apache.arrow.stream".into(),
        cardinality: "one".into(),
        required: true,
        schema_digest,
        primary_geometry,
    }
}
/// Discovery emitted by the pinned library, not the target catalog.
/// # Errors
/// Returns a data-free serialization/size failure.
pub fn discovery() -> Result<Vec<u8>, Error> {
    encode(&plenora_io_tools::operazioni::capabilities())
}
/// Real linked format catalog, including unavailable optional native backends.
#[must_use]
pub fn catalog() -> serde_json::Value {
    plenora_io_tools::operazioni::catalog()
}
/// Registry resolved from actual read/write descriptors, restricted to the host's ports.
/// # Errors
/// Refuses a changed contract, unavailable operation, or incompatible port type.
pub fn registry(artifact_sha256: &str, ports: &Ports) -> Result<Registry, Error> {
    let capabilities = plenora_io_tools::operazioni::capabilities();
    let mut operations = Vec::new();
    for name in ["io.read", "io.write"] {
        let op = capabilities["operations"]
            .as_array()
            .ok_or(Error::Unsupported)?
            .iter()
            .find(|op| op["id"] == name && op["version"] == 2 && op["status"] == "available")
            .ok_or(Error::Unsupported)?;
        let read = name == "io.read";
        let port = if read { ports.read() } else { ports.write() };
        let descriptor = if read { &op["output"] } else { &op["input"] };
        if !descriptor["content_types"]
            .as_array()
            .ok_or(Error::Unsupported)?
            .iter()
            .any(|v| v.as_str() == Some(&port.content_type))
        {
            return Err(Error::Unsupported);
        }
        operations.push(RegistryOperationsItem {
            component: "io".into(),
            operation: name.into(),
            version: 2,
            input_contract: op["input"]["contract"]
                .as_str()
                .ok_or(Error::Unsupported)?
                .into(),
            output_contract: op["output"]["contract"]
                .as_str()
                .ok_or(Error::Unsupported)?
                .into(),
            inputs: if read {
                BTreeMap::new()
            } else {
                BTreeMap::from([("data".into(), port.clone())])
            },
            outputs: if read {
                BTreeMap::from([("data".into(), port)])
            } else {
                BTreeMap::new()
            },
        });
    }
    let registry = Registry {
        contract: "plenora-chain-reference-context-v2".into(),
        schema_version: 2,
        components: BTreeMap::from([(
            "io".into(),
            Component {
                identity: capabilities["component"]
                    .as_str()
                    .ok_or(Error::Unsupported)?
                    .into(),
                package_version: capabilities["component_version"]
                    .as_str()
                    .ok_or(Error::Unsupported)?
                    .into(),
                artifact_sha256: artifact_sha256.into(),
                discovery_sha256: digest(&discovery()?),
                catalog: Document {
                    contract: "plenora-io-catalog-v2".into(),
                    artifact_ref: "ref:io-catalog".into(),
                    sha256: digest(&encode(&catalog())?),
                },
                contracts_revision: "38d68b402389855edbcecb7ce57c31e91282cb4e".into(),
                claims: vec![
                    "plenora-arrow-interchange-v1".into(),
                    "plenora-arrow-primary-geometry-v1".into(),
                ],
            },
        )]),
        operations,
        boundaries: vec![RegistryBoundariesItem {
            id: "io-read-write".into(),
            source_operation: "io.read".into(),
            source_version: 2,
            target_operation: "io.write".into(),
            target_version: 2,
            source_contract: "plenora-arrow-interchange-v1".into(),
            target_contract: "plenora-arrow-interchange-v1".into(),
            content_type: "application/vnd.apache.arrow.stream".into(),
            mode: "direct".into(),
            adapter: None,
            status: "draft".into(),
        }],
        adapters: vec![],
    };
    parse_registry(&encode(&registry)?).map_err(Into::into)
}
