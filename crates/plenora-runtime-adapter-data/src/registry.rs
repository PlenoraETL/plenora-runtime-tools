//! Discovery from the candidate Rust runtime binding, with explicit host ports.
use crate::{Error, digest, encode};
use plenora_runtime_chains::contracts::{
    Component, Document, Port, Registry, RegistryBoundariesItem, RegistryOperationsItem,
    parse_registry,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
/// Exact schema expectation for a named Arrow stream port.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowPort {
    /// Optional canonical schema digest, checked against actual Arrow.
    pub schema_digest: Option<String>,
    /// Whether a resolved Primary Geometry is required.
    pub primary_geometry: bool,
}
impl ArrowPort {
    /// Concrete chain port, always an exact Arrow stream handoff.
    #[must_use]
    pub fn port(&self) -> Port {
        Port {
            contract: "plenora-arrow-interchange-v1".into(),
            content_type: "application/vnd.apache.arrow.stream".into(),
            cardinality: "one".into(),
            required: true,
            schema_digest: self.schema_digest.clone(),
            primary_geometry: self.primary_geometry,
        }
    }
}
/// Named Arrow ports of this bounded host profile (at most eight per side).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ports {
    /// Inputs matched exactly to data.run request input names.
    pub inputs: BTreeMap<String, ArrowPort>,
    /// Outputs matched exactly to data.run request output names.
    pub outputs: BTreeMap<String, ArrowPort>,
}
/// Actual runtime discovery, not a target catalog.
/// # Errors
/// Fails if the bounded document cannot be encoded.
pub fn discovery() -> Result<Vec<u8>, Error> {
    encode(&plenora_cli::runtime_binding::runtime_capabilities())
}
/// Native kernel catalog. Availability is not a qualification claim.
#[must_use]
pub fn catalog() -> serde_json::Value {
    plenora_cli::api::catalogo()
}
/// Resolve data.run@3 and artifact media types from linked capabilities.
/// # Errors
/// Rejects unsupported versions, ports or malformed deployment identity.
pub fn registry(artifact_sha256: &str, ports: &Ports) -> Result<Registry, Error> {
    let caps = plenora_cli::runtime_binding::runtime_capabilities();
    let op = caps["operations"]
        .as_array()
        .ok_or(Error::Unsupported)?
        .iter()
        .find(|v| v["id"] == "data.run" && v["version"] == 3 && v["status"] == "available")
        .ok_or(Error::Unsupported)?;
    for (side, map) in [("source", &ports.inputs), ("sink", &ports.outputs)] {
        if map.is_empty() || map.len() > 8 {
            return Err(Error::Unsupported);
        }
        for spec in map.values() {
            let port = spec.port();
            if port.contract != "plenora-arrow-interchange-v1"
                || port.content_type != "application/vnd.apache.arrow.stream"
                || port.cardinality != "one"
                || !port.required
                || !op["attributes"]["artifact_content_types"][side]
                    .as_array()
                    .ok_or(Error::Unsupported)?
                    .iter()
                    .any(|v| v == &port.content_type)
            {
                return Err(Error::Unsupported);
            }
        }
    }
    let registry = Registry {
        contract: "plenora-chain-reference-context-v2".into(),
        schema_version: 2,
        components: BTreeMap::from([(
            "data".into(),
            Component {
                identity: caps["component"].as_str().ok_or(Error::Unsupported)?.into(),
                package_version: caps["component_version"]
                    .as_str()
                    .ok_or(Error::Unsupported)?
                    .into(),
                artifact_sha256: artifact_sha256.into(),
                discovery_sha256: digest(&discovery()?),
                catalog: Document {
                    contract: caps["operations"]
                        .as_array()
                        .ok_or(Error::Unsupported)?
                        .iter()
                        .find(|op| op["id"] == "data.catalog" && op["status"] == "available")
                        .and_then(|op| op["output"]["contract"].as_str())
                        .ok_or(Error::Unsupported)?
                        .into(),
                    artifact_ref: "ref:data-catalog".into(),
                    sha256: digest(&encode(&catalog())?),
                },
                contracts_revision: "38d68b402389855edbcecb7ce57c31e91282cb4e".into(),
                claims: vec![
                    "plenora-arrow-interchange-v1".into(),
                    "plenora-arrow-primary-geometry-v1".into(),
                ],
            },
        )]),
        operations: vec![RegistryOperationsItem {
            component: "data".into(),
            operation: "data.run".into(),
            version: 3,
            input_contract: op["input"]["contract"]
                .as_str()
                .ok_or(Error::Unsupported)?
                .into(),
            output_contract: op["output"]["contract"]
                .as_str()
                .ok_or(Error::Unsupported)?
                .into(),
            inputs: ports
                .inputs
                .iter()
                .map(|(n, p)| (n.clone(), p.port()))
                .collect(),
            outputs: ports
                .outputs
                .iter()
                .map(|(n, p)| (n.clone(), p.port()))
                .collect(),
        }],
        boundaries: vec![RegistryBoundariesItem {
            id: "data-run-run".into(),
            source_operation: "data.run".into(),
            source_version: 3,
            target_operation: "data.run".into(),
            target_version: 3,
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
