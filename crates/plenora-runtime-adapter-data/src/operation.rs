//! In-process runtime binding with a closed, bounded, private artifact resolver.
use crate::{Error, Invocation, MAX_BLOB, digest, ipc};
use plenora_cli::{
    api::{Destinazione, PubblicazioneFallita, RifiutoDestinazioni, RisolutoreArtefatti},
    runtime_binding::{RuntimeBinding, RuntimeInvocation},
};
use plenora_pipeline::{Interruzione, Pipeline};
use plenora_runtime_chains::Store;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    sync::{Arc, Mutex, atomic::AtomicBool},
};
pub(crate) struct Prepared {
    request: Vec<u8>,
    inputs: BTreeMap<String, Vec<u8>>,
    outputs: BTreeMap<String, String>,
    pub request_bytes: u64,
    input_rows: u64,
    plan: Pipeline,
    schemas: BTreeMap<String, Arc<arrow_schema::Schema>>,
}
pub(crate) struct Failure {
    pub category: String,
    pub phase: String,
    pub code: String,
    pub effect: String,
    pub retry: Value,
}
pub(crate) struct Output {
    pub bytes: Vec<u8>,
    pub rows: u64,
    pub columns: u64,
}
pub(crate) struct Outcome {
    pub upstream: Value,
    pub upstream_error: bool,
    pub effects: Option<Value>,
    pub complete: bool,
    pub outputs: BTreeMap<String, Output>,
    pub rows: u64,
    pub produced: u64,
    pub failure: Option<Failure>,
    pub effect: String,
}
fn invalid<T>() -> Result<T, Error> {
    Err(Error::Invalid)
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep exact authorization and cumulative input checks together"
)]
pub(crate) async fn prepare<S: Store>(
    raw: &[u8],
    i: &Invocation,
    artifacts: &BTreeMap<String, Vec<u8>>,
    store: &Arc<S>,
) -> Result<Prepared, Error> {
    if i.step.operation != "data.run"
        || i.step.operation_version != 3
        || i.step.loss_consent.is_some()
        || i.step.destination_ref.is_some()
    {
        return invalid();
    }
    let text = std::str::from_utf8(raw).map_err(|_| Error::Invalid)?;
    plenora_core::json::ensure_no_duplicate_keys(text).map_err(|_| Error::Invalid)?;
    plenora_core::json::ensure_numbers_exact(text).map_err(|_| Error::Invalid)?;
    let request: Value = serde_json::from_str(text).map_err(|_| Error::Invalid)?;
    let plan = Pipeline::from_json(&request["plan"].to_string()).map_err(|_| Error::Invalid)?;
    let limits = plan
        .limits
        .as_ref()
        .ok_or(Error::ResourceLimit)?
        .applica()
        .map_err(|_| Error::Invalid)?;
    if limits.rows.max_input_rows > i.reservation.limits.rows_read
        || limits.rows.max_output_rows > i.reservation.limits.rows_produced
        || limits.rows.max_rows_per_edge > i.reservation.limits.rows_produced
        || limits.max_governed_memory_bytes > 16 * MAX_BLOB as u64
        || plan.steps.len() > 32
    {
        return Err(Error::ResourceLimit);
    }
    let source = request["inputs"].as_object().ok_or(Error::Invalid)?;
    let target = request["outputs"].as_object().ok_or(Error::Invalid)?;
    if source.len() != i.inputs.len()
        || target.len() != i.step.outputs.len()
        || source.is_empty()
        || source.len() > 8
        || target.is_empty()
        || target.len() > 8
    {
        return invalid();
    }
    let mut inputs = BTreeMap::new();
    let mut schemas = BTreeMap::new();
    let mut outputs = BTreeMap::new();
    let mut input_rows = 0_u64;
    let mut request_bytes = raw.len() as u64;
    for (name, manifest) in &i.inputs {
        let descriptor = source.get(name).ok_or(Error::Invalid)?;
        let reference = format!("ref:input-{name}");
        if descriptor["reference"] != reference
            || descriptor["content_type"] != "application/vnd.apache.arrow.stream"
        {
            return invalid();
        }
        let bytes = if let Some(bytes) = artifacts.get(&manifest.artifact_ref) {
            bytes.clone()
        } else {
            store
                .read_resource(&i.tenant, &i.run_id, &manifest.artifact_ref, MAX_BLOB)
                .await?
        };
        if bytes.len() as u64 != manifest.bytes || digest(&bytes) != manifest.sha256 {
            return invalid();
        }
        let (rows, _) = ipc::check(&bytes, &manifest.port, i, i.reservation.limits.rows_read)?;
        ipc::check(
            &bytes,
            &i.step.inputs.get(name).ok_or(Error::Invalid)?.expected,
            i,
            i.reservation.limits.rows_read,
        )?;
        input_rows = input_rows.checked_add(rows).ok_or(Error::ResourceLimit)?;
        request_bytes = request_bytes
            .checked_add(bytes.len() as u64)
            .ok_or(Error::ResourceLimit)?;
        schemas.insert(
            name.clone(),
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
                .map_err(|_| Error::Schema)?
                .schema(),
        );
        inputs.insert(reference, bytes);
    }
    for name in i.step.outputs.keys() {
        let descriptor = target.get(name).ok_or(Error::Invalid)?;
        let reference = format!("ref:output-{name}");
        if descriptor["reference"] != reference
            || descriptor["content_type"] != "application/vnd.apache.arrow.stream"
            || descriptor["overwrite"] != false
        {
            return invalid();
        }
        outputs.insert(reference, name.clone());
    }
    if input_rows > i.reservation.limits.rows_read
        || request_bytes > i.reservation.limits.request_bytes
        || i.reservation.limits.result_bytes < crate::receipt::RECEIPT_RESERVATION
    {
        return Err(Error::ResourceLimit);
    }
    Ok(Prepared {
        request: raw.to_vec(),
        inputs,
        outputs,
        request_bytes,
        input_rows,
        plan,
        schemas,
    })
}
struct Resolver {
    inputs: BTreeMap<String, Vec<u8>>,
    expected: BTreeMap<String, String>,
    staged: Mutex<BTreeMap<String, Vec<u8>>>,
}
impl RisolutoreArtefatti for Resolver {
    fn leggi(&self, reference: &str, out: &mut dyn Write) -> std::io::Result<()> {
        out.write_all(
            self.inputs
                .get(reference)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?,
        )
    }
    fn prepara(&self, destinations: &[Destinazione<'_>]) -> Result<(), RifiutoDestinazioni> {
        let names: std::collections::BTreeSet<_> =
            destinations.iter().map(|d| d.riferimento).collect();
        if destinations.len() != self.expected.len()
            || names.len() != destinations.len()
            || destinations.iter().any(|d| {
                self.expected.get(d.riferimento).map(String::as_str) != Some(d.nome)
                    || d.sovrascrivi
                    || d.tipo != "application/vnd.apache.arrow.stream"
            })
        {
            return Err(RifiutoDestinazioni::NonAutorizzata);
        }
        Ok(())
    }
    fn pubblica(
        &self,
        d: &Destinazione<'_>,
        content: &mut dyn Read,
        size: u64,
    ) -> Result<(), PubblicazioneFallita> {
        let fail = || {
            PubblicazioneFallita::NienteScritto(std::io::Error::other("private artifact rejected"))
        };
        if self.expected.get(d.riferimento).map(String::as_str) != Some(d.nome)
            || size > MAX_BLOB as u64
        {
            return Err(fail());
        }
        let mut bytes = Vec::new();
        content
            .take(MAX_BLOB as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(PubblicazioneFallita::NienteScritto)?;
        if bytes.len() as u64 != size {
            return Err(fail());
        }
        let mut staged = self.staged.lock().map_err(|_| fail())?;
        if staged.contains_key(d.nome) {
            return Err(fail());
        }
        staged.insert(d.nome.into(), bytes);
        Ok(())
    }
}
fn failure(category: &str, phase: &str) -> Failure {
    Failure {
        category: category.into(),
        phase: phase.into(),
        code: "CHAIN_UPSTREAM".into(),
        effect: "none".into(),
        retry: json!({"kind":"never"}),
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "Keep evidence collection before every terminal validation path visible"
)]
pub(crate) fn execute(p: Prepared, i: &Invocation, token: &Arc<AtomicBool>) -> Outcome {
    let resolver = Resolver {
        inputs: p.inputs,
        expected: p.outputs,
        staged: Mutex::new(BTreeMap::new()),
    };
    let invocation = RuntimeInvocation {
        content_type: "application/json".into(),
        payload: p.request,
        metadata: BTreeMap::from([
            ("plenora.message.id".into(), json!(i.message_id)),
            ("plenora.trace.correlation_id".into(), json!(i.run_id)),
            (
                "plenora.capability.name".into(),
                json!("plenora.data-tools"),
            ),
            ("plenora.capability.version".into(), json!("1")),
            ("plenora.capability.operation".into(), json!("data.run")),
            ("plenora.operation.version".into(), json!("3")),
            (
                "plenora.input.contract".into(),
                json!(i.step.input_contract),
            ),
        ]),
    };
    // No wall clock deadline substitution: the coordinator drives the cooperative token and drains it.
    let result = RuntimeBinding {
        resolver: &resolver,
    }
    .invoke(
        &invocation,
        &Interruzione {
            scadenza: None,
            annullamento: Some(token.clone()),
        },
    );
    let is_error = result.content_type == "application/vnd.plenora.error+json";
    let mut outcome = Outcome {
        upstream: Value::Null,
        upstream_error: is_error,
        effects: None,
        complete: false,
        outputs: BTreeMap::new(),
        rows: p.input_rows,
        produced: 0,
        failure: None,
        effect: "none".into(),
    };
    // Collect bounded evidence first. A later barrier failure must not erase it.
    if result.payload.len() <= MAX_BLOB {
        outcome.upstream = serde_json::from_slice(&result.payload).unwrap_or(Value::Null);
    }
    if let Some(raw) = result.metadata.get("plenora.data.localized_effects")
        && raw.len() <= MAX_BLOB
        && let Ok(value) = serde_json::from_str::<Value>(raw)
        && value["contract"] == "plenora-data-localized-effects-v1"
    {
        outcome.effects = Some(value);
    }
    let checked = (|| -> Result<(), Error> {
        if outcome.upstream.is_null() {
            return Err(Error::ResourceLimit);
        }
        if is_error {
            let native = &outcome.upstream;
            let category = native["category"].as_str().ok_or(Error::Recovery)?;
            let phase = native["phase"].as_str().ok_or(Error::Recovery)?;
            outcome.failure = Some(failure(category, phase));
            outcome.upstream = json!({"category":category,"phase":phase,"remote_effect":native["remote_effect"],"retry":native["retry"],"message":"DATA operation rejected or incomplete."});
            return Ok(());
        }
        if result.content_type != "application/json"
            || result.metadata.get("plenora.output.contract") != Some(&i.step.output_contract)
        {
            return Err(Error::Recovery);
        }
        let manifests = outcome.upstream["outputs"]
            .as_array()
            .ok_or(Error::Recovery)?;
        // Native observed production remains visible even when the output barrier rejects it.
        for m in manifests {
            outcome.produced = outcome
                .produced
                .checked_add(m["rows"].as_u64().ok_or(Error::Recovery)?)
                .ok_or(Error::ResourceLimit)?;
        }
        let effects = outcome.effects.as_ref().ok_or(Error::Recovery)?;
        let steps = effects["steps"].as_array().ok_or(Error::Recovery)?;
        let native_steps = outcome.upstream["steps"]
            .as_array()
            .ok_or(Error::Recovery)?;
        if steps.len() != p.plan.steps.len() || native_steps.len() != steps.len() {
            return Err(Error::Recovery);
        }
        let mut lossy = false;
        for ((effect_step, native_step), planned) in
            steps.iter().zip(native_steps).zip(&p.plan.steps)
        {
            if effect_step["op"] != planned.op
                || effect_step["out"] != planned.out
                || native_step["op"] != planned.op
                || native_step["out"] != planned.out
            {
                return Err(Error::Recovery);
            }
            if native_step["division_by_zero_rows"]
                .as_u64()
                .ok_or(Error::Recovery)?
                != 0
            {
                lossy = true;
            }
            for effect in effect_step["effects"].as_array().ok_or(Error::Recovery)? {
                match effect["action"].as_str() {
                    Some("removed" | "metadata_invalidated") => lossy = true,
                    Some("derived" | "metadata_reconstructed") => {}
                    _ => return Err(Error::Recovery),
                }
            }
        }
        let staged = resolver.staged.into_inner().map_err(|_| Error::Recovery)?;
        if staged.len() != i.step.outputs.len() || manifests.len() != staged.len() {
            return Err(Error::Recovery);
        }
        for (name, bytes) in staged {
            let port = i.step.outputs.get(&name).ok_or(Error::Recovery)?;
            let (rows, columns) = ipc::check(&bytes, port, i, i.reservation.limits.rows_produced)?;
            let m = manifests
                .iter()
                .find(|m| m["name"] == name)
                .ok_or(Error::Recovery)?;
            if m["reference"] != format!("ref:output-{name}")
                || m["artifact"]["content_type"] != port.content_type
                || m["artifact"]["sha256"] != digest(&bytes)
                || m["artifact"]["size"] != bytes.len()
                || m["rows"] != rows
                || m["columns"] != columns
            {
                return Err(Error::Recovery);
            }
            let schema =
                arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&bytes), None)
                    .map_err(|_| Error::Schema)?
                    .schema();
            let ancestors = output_ancestors(&p.plan, &name)?;
            let schemas = ancestors
                .iter()
                .map(|n| p.schemas.get(n).cloned().ok_or(Error::Recovery))
                .collect::<Result<Vec<_>, _>>()?;
            metadata_preserved(&schemas, &schema)?;
            outcome.outputs.insert(
                name,
                Output {
                    bytes,
                    rows,
                    columns,
                },
            );
        }
        if outcome.produced > i.reservation.limits.rows_produced {
            return Err(Error::ResourceLimit);
        }
        outcome.complete = true;
        // `reported` is evidence selection, not a DATA loss grant. No private consent alias.
        if lossy {
            outcome.failure = Some(failure("data_mapping", "finalize"));
            outcome.outputs.clear();
        }
        Ok(())
    })();
    if let Err(error) = checked {
        outcome.complete = false;
        outcome.outputs.clear();
        outcome.failure = Some(failure(
            match error {
                Error::ResourceLimit => "resource_limit",
                Error::Schema => "schema",
                _ => "internal",
            },
            "finalize",
        ));
        if is_error {
            outcome.upstream = Value::Null;
        }
    }
    // Native committed/partial concerns only private staging; the journal publishes outputs.
    outcome
}
// Conservative guard for metadata emission AFTER C-01 was computed upstream.
// New fields are allowed; surviving field identities and schema metadata cannot silently change.
fn metadata_preserved(
    inputs: &[Arc<arrow_schema::Schema>],
    output: &arrow_schema::Schema,
) -> Result<(), Error> {
    if !inputs.iter().all(|input| {
        input
            .metadata()
            .iter()
            .all(|(k, v)| output.metadata().get(k) == Some(v))
    }) {
        return Err(Error::Schema);
    }
    for field in output.fields() {
        let id = field
            .metadata()
            .get("plenora.field_id")
            .ok_or(Error::Schema)?;
        let ancestors: Vec<_> = inputs
            .iter()
            .flat_map(|s| s.fields())
            .filter(|f| f.metadata().get("plenora.field_id") == Some(id))
            .collect();
        if !ancestors.is_empty()
            && !ancestors.iter().all(|old| {
                old.metadata()
                    .iter()
                    .all(|(k, v)| field.metadata().get(k) == Some(v))
            })
        {
            return Err(Error::Schema);
        }
    }
    Ok(())
}

// SSA lineage selects only the source tables that can contribute to this output.
fn output_ancestors(
    plan: &Pipeline,
    output: &str,
) -> Result<std::collections::BTreeSet<String>, Error> {
    let mut lineage: BTreeMap<String, std::collections::BTreeSet<String>> = BTreeMap::new();
    for name in &plan.inputs {
        if lineage
            .insert(
                name.clone(),
                std::collections::BTreeSet::from([name.clone()]),
            )
            .is_some()
        {
            return Err(Error::Invalid);
        }
    }
    for step in &plan.steps {
        let mut ancestors = std::collections::BTreeSet::new();
        for name in &step.inputs {
            ancestors.extend(lineage.get(name).ok_or(Error::Invalid)?.iter().cloned());
        }
        if lineage.insert(step.out.clone(), ancestors).is_some() {
            return Err(Error::Invalid);
        }
    }
    lineage.remove(output).ok_or(Error::Invalid)
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    #[test]
    fn metadata_of_unrelated_input_cannot_mask_loss() -> Result<(), Box<dyn std::error::Error>> {
        let a = Arc::new(Schema::new_with_metadata(
            Vec::<Field>::new(),
            BTreeMap::from([("plenora.primary_geometry".into(), "none".into())]),
        ));
        let b = Arc::new(Schema::empty());
        assert_eq!(
            metadata_preserved(&[a, b], &Schema::empty()),
            Err(Error::Schema)
        );
        let pipeline =
            Pipeline::from_json(r#"{"version":1,"inputs":["a","b"],"steps":[],"outputs":["a"]}"#)?;
        assert_eq!(
            output_ancestors(&pipeline, "a")?,
            std::collections::BTreeSet::from(["a".into()])
        );
        let field = |value: &str| {
            Field::new("x", DataType::Utf8, true).with_metadata(BTreeMap::from([
                ("plenora.field_id".into(), "1".into()),
                ("opaque".into(), value.into()),
            ]))
        };
        let a = Arc::new(Schema::new(vec![field("a")]));
        let b = Arc::new(Schema::new(vec![field("b")]));
        assert_eq!(metadata_preserved(&[a.clone(), b], &a), Err(Error::Schema));
        Ok(())
    }
}
