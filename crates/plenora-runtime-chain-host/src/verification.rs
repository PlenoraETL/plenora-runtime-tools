//! Immutable, explicitly fake domain resolver and Arrow/fidelity oracle.
//! JSON fixture envelopes are NOT Arrow IPC or implementations of library domain schemas.
use super::ScriptedAdapter;
use plenora_runtime_chains::{
    Error, Invocation,
    contracts::{Document, Port, Report, StepResult},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn object_digest(value: &impl Serialize) -> Result<String, Error> {
    // Value sorts every object, including nested metadata; no schema information is dropped.
    let value = serde_json::to_value(value).map_err(|_| Error::Invalid)?;
    Ok(digest(
        &serde_json::to_vec(&value).map_err(|_| Error::Invalid)?,
    ))
}
// The fake envelope is canonical JSON, unlike real library payloads: comparison before
// typed decoding rejects duplicate members at every depth as well as alternate encodings.
fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
    if serde_json::to_vec(&value).map_err(|_| Error::Invalid)? != bytes {
        return Err(Error::Invalid);
    }
    serde_json::from_value(value).map_err(|_| Error::Invalid)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureDocument {
    contract: String,
    fixture: String,
    body: Body,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Body {
    Catalog,
    Parameters {
        operation: String,
        version: u64,
    },
    Upstream,
    Fidelity {
        step: String,
        operation: String,
        version: u64,
        parameters_sha256: String,
        destination: Option<String>,
        inputs: BTreeMap<String, (String, String)>,
        outputs: BTreeMap<String, (String, String)>,
        verdict: Verdict,
    },
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Verdict {
    Exact,
    DatabaseLoss,
    ForbiddenLoss,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureArrow {
    fixture: String,
    schema: Schema,
    rows: Vec<Vec<serde_json::Value>>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Schema {
    fields: Vec<Field>,
    metadata: BTreeMap<String, String>,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Field {
    id: String,
    name: String,
    data_type: String,
    nullable: bool,
    geometry: bool,
    metadata: BTreeMap<String, String>,
}
impl Schema {
    fn primary(&self) -> Result<bool, Error> {
        let mut ids = BTreeSet::new();
        let mut names = BTreeSet::new();
        for field in &self.fields {
            if field.id.is_empty()
                || field.id == "none"
                || field.name.is_empty()
                || !ids.insert(&field.id)
                || !names.insert(&field.name)
                || !matches!(field.data_type.as_str(), "int64" | "utf8" | "binary")
                || (field.geometry && field.data_type != "binary")
            {
                return Err(Error::Schema);
            }
        }
        let geometries: Vec<_> = self.fields.iter().filter(|f| f.geometry).collect();
        match self
            .metadata
            .get("plenora.primary_geometry")
            .map(String::as_str)
        {
            Some("none") => Ok(false),
            Some(id) if geometries.iter().any(|f| f.id == id) => Ok(true),
            Some(_) => Err(Error::Schema),
            None => Ok(geometries.len() == 1),
        }
    }
}
impl ScriptedAdapter {
    fn resource(&self, reference: &str, sha256: &str) -> Result<&[u8], Error> {
        let bytes = self.artifacts.get(reference).ok_or(Error::NotFound)?;
        if bytes.len() > 1024 * 1024 {
            return Err(Error::ResourceLimit);
        }
        if digest(bytes) != sha256 {
            return Err(Error::Invalid);
        }
        Ok(bytes)
    }
    fn document(&self, d: &Document) -> Result<FixtureDocument, Error> {
        let document: FixtureDocument =
            decode(self.resource(&d.artifact_ref, &d.sha256)?).map_err(|_| Error::Invalid)?;
        if document.contract != d.contract || document.fixture != "scripted-v1" {
            return Err(Error::Invalid);
        }
        Ok(document)
    }
    fn arrow(
        &self,
        reference: &str,
        sha256: &str,
        bytes: u64,
        port: &Port,
    ) -> Result<(usize, usize), Error> {
        let raw = self.resource(reference, sha256)?;
        if u64::try_from(raw.len()).map_err(|_| Error::ResourceLimit)? != bytes
            || port.contract != "plenora-arrow-interchange-v1"
            || !matches!(
                port.content_type.as_str(),
                "application/vnd.apache.arrow.stream" | "application/vnd.apache.arrow.file"
            )
        {
            return Err(Error::Invalid);
        }
        let arrow: FixtureArrow = decode(raw).map_err(|_| Error::Schema)?;
        let primary = arrow.schema.primary()?;
        for row in &arrow.rows {
            if row.len() != arrow.schema.fields.len() {
                return Err(Error::Schema);
            }
            for (value, field) in row.iter().zip(&arrow.schema.fields) {
                let valid = if value.is_null() {
                    field.nullable
                } else {
                    match field.data_type.as_str() {
                        "int64" => value.as_i64().is_some(),
                        "utf8" => value.is_string(),
                        "binary" => value.as_array().is_some_and(|bytes| {
                            bytes.iter().all(|b| b.as_u64().is_some_and(|n| n <= 255))
                        }),
                        _ => false,
                    }
                };
                if !valid {
                    return Err(Error::Schema);
                }
            }
        }
        if arrow.fixture != "scripted-arrow-v1"
            || (port.primary_geometry && !primary)
            || port
                .schema_digest
                .as_ref()
                .is_some_and(|d| object_digest(&arrow.schema).as_ref() != Ok(d))
        {
            return Err(Error::Schema);
        }
        Ok((arrow.rows.len(), arrow.schema.fields.len()))
    }
    fn reports(&self, reports: &[Report]) -> Result<(), Error> {
        for report in reports {
            if report.availability != "available" || report.document.is_none() {
                return Err(Error::Invalid);
            }
            if let Some(d) = &report.document {
                self.document(d)?;
            }
        }
        Ok(())
    }
    // The trusted profile is immutable for the lifetime of this adapter. Every preparation,
    // including dispatcher re-entry, rechecks it; a restarted host receives no cached approval.
    pub(super) fn verify(&self, i: &Invocation, result: &StepResult) -> Result<(), Error> {
        if i.tenant != self.tenant {
            return Err(Error::Invalid);
        }
        let plan = self.authorized.get(&i.plan_digest).ok_or(Error::Invalid)?;
        if !plan.steps.iter().any(|s| s == &i.step) {
            return Err(Error::Invalid);
        }
        for component in plan.components.values() {
            if !matches!(self.document(&component.catalog)?.body, Body::Catalog) {
                return Err(Error::Invalid);
            }
        }
        let parameters =
            self.resource(&i.step.parameters.artifact_ref, &i.step.parameters.sha256)?;
        if u64::try_from(parameters.len()).map_err(|_| Error::ResourceLimit)?
            > i.reservation.limits.request_bytes
            || result.counters.request_bytes < parameters.len() as u64
        {
            return Err(Error::ResourceLimit);
        }
        match self.document(&i.step.parameters)?.body {
            Body::Parameters { operation, version }
                if operation == i.step.operation && version == i.step.operation_version => {}
            _ => return Err(Error::Invalid),
        }
        for input in plan.inputs.values() {
            self.arrow(&input.artifact_ref, &input.sha256, input.bytes, &input.port)?;
        }
        if result.state == "succeeded" && !result.outputs.keys().eq(i.step.outputs.keys()) {
            return Err(Error::Invalid);
        }
        for error in result.error.iter().chain(&result.other_errors) {
            self.reports(&error.reports)?;
            if let Some(d) = &error.upstream_error {
                self.document(d)?;
            }
        }
        for (name, input) in &i.inputs {
            self.arrow(&input.artifact_ref, &input.sha256, input.bytes, &input.port)?;
            let expected = &i.step.inputs.get(name).ok_or(Error::Invalid)?.expected;
            // Dynamic schemas are checked against the consumer without changing the manifest.
            self.arrow(&input.artifact_ref, &input.sha256, input.bytes, expected)?;
        }
        for (name, output) in &result.outputs {
            let (rows, columns) = self.arrow(
                &output.artifact_ref,
                &output.sha256,
                output.bytes,
                &output.port,
            )?;
            self.arrow(
                &output.artifact_ref,
                &output.sha256,
                output.bytes,
                i.step.outputs.get(name).ok_or(Error::Invalid)?,
            )?;
            if output.rows.is_some_and(|n| n != rows as u64)
                || output.columns.is_some_and(|n| n != columns as u64)
            {
                return Err(Error::Schema);
            }
            self.reports(&output.reports)?;
        }
        if let Some(d) = &result.upstream_result
            && (!matches!(self.document(d)?.body, Body::Upstream)
                || d.contract != i.step.output_contract)
        {
            return Err(Error::Invalid);
        }
        self.reports(&result.reports)?;
        self.fidelity(i, result, &plan.requirements.fidelity)
    }
    fn fidelity(
        &self,
        i: &Invocation,
        result: &StepResult,
        requirement: &str,
    ) -> Result<(), Error> {
        let reports: Vec<_> = result
            .reports
            .iter()
            .filter(|r| r.kind == "fidelity")
            .collect();
        if reports.len() != 1 {
            return Err(Error::Invalid);
        }
        let d = reports[0].document.as_ref().ok_or(Error::Invalid)?;
        if reports[0].availability != "available" || d.contract != "plenora-fixture-fidelity-v1" {
            return Err(Error::Invalid);
        }
        if result
            .reports
            .iter()
            .any(|r| r.kind == "loss_report" && r.document.as_ref() != Some(d))
        {
            return Err(Error::Invalid);
        }
        let Body::Fidelity {
            step,
            operation,
            version,
            parameters_sha256,
            destination,
            inputs,
            outputs,
            verdict,
        } = self.document(d)?.body
        else {
            return Err(Error::Invalid);
        };
        let actual_inputs = i
            .inputs
            .iter()
            .map(|(name, a)| (name.clone(), (a.artifact_ref.clone(), a.sha256.clone())))
            .collect();
        let actual_outputs: BTreeMap<_, _> = result
            .outputs
            .iter()
            .map(|(name, a)| (name.clone(), (a.artifact_ref.clone(), a.sha256.clone())))
            .collect();
        if step != i.step.id
            || operation != i.step.operation
            || version != i.step.operation_version
            || parameters_sha256 != i.step.parameters.sha256
            || destination != i.step.destination_ref
            || inputs != actual_inputs
            || actual_outputs
                .iter()
                .any(|(name, a)| outputs.get(name) != Some(a))
            || (result.state == "succeeded" && outputs != actual_outputs)
        {
            return Err(Error::Invalid);
        }
        match verdict {
            Verdict::Exact => Ok(()),
            Verdict::DatabaseLoss
                if requirement == "reported"
                    && i.step.operation == "database.write"
                    && i.step.operation_version == 2
                    && i.step.loss_consent == Some(true)
                    && result
                        .reports
                        .iter()
                        .any(|r| r.kind == "loss_report" && r.availability == "available") =>
            {
                Ok(())
            }
            _ => Err(Error::Invalid),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn canonical_fake_envelopes_reject_duplicate_members_at_every_depth() {
        for bytes in [
            br#"{"a":1,"a":1}"#.as_slice(),
            br#"{"a":{"b":1,"b":2}}"#,
            br#"{"a":[{"b":1,"b":1}]}"#,
            br#"{"a":1 }"#,
            br#"{"a":"\u0061"}"#,
        ] {
            assert!(decode::<serde_json::Value>(bytes).is_err());
        }
    }
    #[test]
    fn primary_geometry_rules_and_full_schema_digest_are_explicit() -> Result<(), Error> {
        let mut value = json!({"fields":[],"metadata":{}});
        let read = |v: &serde_json::Value| {
            serde_json::from_value::<Schema>(v.clone()).map_err(|_| Error::Invalid)
        };
        assert!(!read(&value)?.primary()?);
        let field = json!({"id":"g1","name":"a","data_type":"binary","nullable":true,"geometry":true,"metadata":{"crs":"fixture"}});
        value["fields"] = json!([field]);
        assert!(read(&value)?.primary()?);
        let original = object_digest(&read(&value)?)?;
        value["metadata"]["plenora.primary_geometry"] = json!("none");
        assert!(!read(&value)?.primary()?);
        assert_ne!(original, object_digest(&read(&value)?)?);
        value["fields"].as_array_mut().ok_or(Error::Invalid)?.push(json!({"id":"g2","name":"b","data_type":"binary","nullable":true,"geometry":true,"metadata":{}}));
        value["metadata"] = json!({});
        assert!(!read(&value)?.primary()?); // Multiple geometries are transportable without a primary.
        value["metadata"]["plenora.primary_geometry"] = json!("g2");
        assert!(read(&value)?.primary()?);
        value["metadata"]["plenora.primary_geometry"] = json!("missing");
        assert_eq!(read(&value)?.primary(), Err(Error::Schema));
        Ok(())
    }
}
