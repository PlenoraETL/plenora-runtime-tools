//! Registry restricted to the real compiled discovery and the host's Arrow profile.
use crate::{CATALOG, Error, digest, encode};
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
    encode(&plenora_database_engine::runtime_binding::runtime_capabilities())
}
/// Resolve read@1/write@2 from actual discovery and restrict to the implemented profile.
/// # Errors
/// Refuses changed versions/contracts and malformed deployment identity or ports.
pub fn registry(artifact_sha256: &str, ports: &Ports) -> Result<Registry, Error> {
    if ports.primary_geometry {
        return Err(Error::Unsupported);
    }
    let capabilities = plenora_database_engine::runtime_binding::runtime_capabilities();
    let mut operations = Vec::new();
    for (name, version) in [("database.read", 1), ("database.write", 2)] {
        let op = capabilities
            .operations
            .iter()
            .find(|op| op.id == name && op.version == version)
            .ok_or(Error::Unsupported)?;
        let read = name == "database.read";
        operations.push(RegistryOperationsItem {
            component: "database".into(),
            operation: op.id.clone(),
            version: u64::from(op.version),
            input_contract: op.input.contract.clone(),
            output_contract: op.output.contract.clone(),
            inputs: if read {
                BTreeMap::new()
            } else {
                BTreeMap::from([("data".into(), ports.write())])
            },
            outputs: if read {
                BTreeMap::from([("data".into(), ports.read())])
            } else {
                BTreeMap::new()
            },
        });
    }
    let registry = Registry {
        contract: "plenora-chain-reference-context-v2".into(),
        schema_version: 2,
        components: BTreeMap::from([(
            "database".into(),
            Component {
                identity: capabilities.component,
                package_version: capabilities.component_version,
                artifact_sha256: artifact_sha256.into(),
                discovery_sha256: digest(&discovery()?),
                catalog: Document {
                    contract: "plenora-public-catalog-v2".into(),
                    artifact_ref: "ref:database-catalog".into(),
                    sha256: digest(CATALOG),
                },
                contracts_revision: "3aec9f724f5a0ad21366f293895b4d0dbcd0aad1".into(),
                claims: vec!["plenora-arrow-interchange-v1".into()],
            },
        )]),
        operations,
        boundaries: vec![RegistryBoundariesItem {
            id: "database-read-write".into(),
            source_operation: "database.read".into(),
            source_version: 1,
            target_operation: "database.write".into(),
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
