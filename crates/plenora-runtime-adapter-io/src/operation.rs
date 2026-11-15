//! Real IO operations in isolated staging, then a separately confirmed bundle publication.
use crate::{Destination, Error, Invocation, MAX_BLOB, Source, ipc};
use plenora_io_model::{
    CancellationToken,
    budget::{PipelineBudget, PipelineLimits},
};
use plenora_io_tools::operazioni::{self, Richiesta};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::{Path, PathBuf},
};

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Budgets {
    memory_bytes: Option<u64>,
    max_rows: Option<u64>,
    max_columns: Option<u64>,
    max_input_bytes: Option<u64>,
    max_input_entries: Option<u64>,
    max_output_bytes: Option<u64>,
    max_vertices: Option<u64>,
    max_wkb_cell_bytes: Option<u64>,
    max_wkb_components: Option<u64>,
    max_wkb_depth: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Parameters {
    pub source: String,
    pub destination: Option<String>,
    format: Option<String>,
    layer: Option<u32>,
    limit: Option<u64>,
    assume_crs: Option<String>,
    #[serde(default)]
    input_options: BTreeMap<String, String>,
    #[serde(default)]
    output_options: BTreeMap<String, String>,
    #[serde(default)]
    durable: bool,
    deadline_ms: Option<u64>,
    #[serde(default)]
    budgets: Budgets,
}
impl Parameters {
    pub fn validate(&self, i: &Invocation) -> Result<(), Error> {
        if i.step.operation_version != 2
            || !matches!(i.step.operation.as_str(), "io.read" | "io.write")
        {
            return Err(Error::Unsupported);
        }
        if self.limit.is_some() || self.deadline_ms.is_some() || i.step.loss_consent.is_some() {
            return Err(Error::Invalid);
        }
        if i.step.operation == "io.read" {
            if self.destination.is_some() || self.format.is_some() || self.durable {
                return Err(Error::Invalid);
            }
            if self
                .output_options
                .get("serialization")
                .is_some_and(|v| v != "stream")
            {
                return Err(Error::Unsupported);
            }
        } else {
            if self.destination != i.step.destination_ref || self.destination.is_none() {
                return Err(Error::Invalid);
            }
            let format = self.format.as_ref().ok_or(Error::Invalid)?;
            if format == "ipc"
                && self
                    .output_options
                    .get("serialization")
                    .is_some_and(|v| v != "stream")
            {
                return Err(Error::Unsupported);
            }
            let catalog = crate::catalog();
            let descriptor = catalog["drivers"]
                .as_array()
                .ok_or(Error::Unsupported)?
                .iter()
                .find(|d| d["id"].as_str() == Some(format))
                .ok_or(Error::Unsupported)?;
            if descriptor["available"] != true || descriptor["write_mode"].is_null() {
                return Err(Error::Unsupported);
            }
        }
        Ok(())
    }
}
pub(crate) struct Prepared {
    pub parameters: Parameters,
    pub source: Option<Source>,
    pub input: Option<Vec<u8>>,
    pub input_rows: u64,
    pub destination: Option<Destination>,
    pub staging: String,
    pub request_bytes: u64,
}
pub(crate) fn relative(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && path.split('/').all(|s| {
            let stem = s.split('.').next().unwrap_or("").to_ascii_uppercase();
            let device = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
                || (stem.len() == 4
                    && (stem.starts_with("COM") || stem.starts_with("LPT"))
                    && matches!(stem.as_bytes()[3], b'1'..=b'9'));
            !s.is_empty()
                && !s.ends_with('.')
                && !device
                && s.bytes()
                    .all(|b| b.is_ascii_graphic() && !b"\\:*?\"<>|".contains(&b))
        })
}
pub(crate) fn validate_source(source: &Source) -> Result<(), Error> {
    if !relative(&source.entry)
        || source.files.is_empty()
        || source.files.len() > 256
        || source.files.keys().any(|k| !relative(k))
        || !source
            .files
            .keys()
            .any(|k| k == &source.entry || k.starts_with(&(source.entry.clone() + "/")))
    {
        return Err(Error::Invalid);
    }
    let mut names = std::collections::BTreeMap::new();
    for name in source.files.keys() {
        let mut prefix = String::new();
        for component in name.split('/') {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(component);
            if names
                .insert(prefix.to_ascii_lowercase(), prefix.clone())
                .is_some_and(|old| old != prefix)
            {
                return Err(Error::Invalid);
            }
        }
    }
    if source.files.keys().any(|name| {
        source
            .files
            .keys()
            .any(|other| other.starts_with(&(name.clone() + "/")))
    }) {
        return Err(Error::Invalid);
    }
    let size = source.files.values().try_fold(0_usize, |n, b| {
        n.checked_add(b.len()).ok_or(Error::ResourceLimit)
    })?;
    if size > MAX_BLOB {
        return Err(Error::ResourceLimit);
    }
    Ok(())
}
fn cap(requested: Option<u64>, bound: u64) -> Result<u64, Error> {
    let value = requested.unwrap_or(bound);
    if value > bound {
        return Err(Error::ResourceLimit);
    }
    Ok(value)
}
pub(crate) fn limits(i: &Invocation, p: &Parameters) -> Result<PipelineLimits, Error> {
    let b = &i.reservation.limits;
    let r = &p.budgets;
    let limits = PipelineLimits::default()
        .with_memory_bytes(cap(
            r.memory_bytes,
            (b.memory_bytes / 4).min(8 * MAX_BLOB as u64),
        )?)
        .with_spill_bytes((b.spool_bytes / 4).min(8 * MAX_BLOB as u64))
        .with_duration_ms(b.elapsed_ms)
        .with_max_rows(cap(
            r.max_rows,
            if i.step.operation == "io.read" {
                b.rows_read.min(b.rows_produced)
            } else {
                b.rows_read
            },
        )?)
        .with_max_columns(cap(r.max_columns, 32)?)
        .with_max_input_bytes(cap(r.max_input_bytes, MAX_BLOB as u64)?)
        .with_max_input_entries(cap(r.max_input_entries, 256)?)
        .with_max_output_bytes(cap(
            r.max_output_bytes,
            b.artifact_bytes.min(MAX_BLOB as u64),
        )?)
        .with_max_vertices(
            usize::try_from(cap(r.max_vertices, 100_000)?).map_err(|_| Error::ResourceLimit)?,
        )
        .with_max_wkb_cell_bytes(
            usize::try_from(cap(r.max_wkb_cell_bytes, MAX_BLOB as u64)?)
                .map_err(|_| Error::ResourceLimit)?,
        )
        .with_max_wkb_components(
            usize::try_from(cap(r.max_wkb_components, 100_000)?)
                .map_err(|_| Error::ResourceLimit)?,
        )
        .with_max_wkb_depth(
            usize::try_from(cap(r.max_wkb_depth, 32)?).map_err(|_| Error::ResourceLimit)?,
        );
    limits.validate().map_err(|_| Error::ResourceLimit)?;
    Ok(limits)
}
#[derive(Clone)]
pub(crate) struct Failure {
    pub category: String,
    pub phase: String,
    pub code: String,
    pub effect: String,
    pub retry: Value,
}
pub(crate) struct Outcome {
    pub upstream: Value,
    pub upstream_error: bool,
    pub data: Option<Vec<u8>>,
    pub rows: u64,
    pub columns: Option<u64>,
    pub failure: Option<Failure>,
    pub other: Vec<Failure>,
    pub complete: bool,
    pub effect: String,
}
impl Outcome {
    pub fn fail(&mut self, category: &str, phase: &str, code: &str, effect: &str) {
        let failure = Failure {
            category: category.into(),
            phase: phase.into(),
            code: code.into(),
            effect: effect.into(),
            retry: json!({"kind":if effect=="unknown" {"requires_recovery"} else {"never"}}),
        };
        if self.failure.is_none() {
            self.failure = Some(failure);
        } else {
            self.other.push(failure);
        }
        self.effect = effect.into();
    }
    fn from_upstream(result: operazioni::Esito) -> Self {
        match result {
            Ok(upstream) => Self {
                upstream,
                upstream_error: false,
                data: None,
                rows: 0,
                columns: None,
                failure: None,
                other: vec![],
                complete: false,
                effect: "none".into(),
            },
            Err(mut envelope) => {
                let mut upstream = envelope
                    .get_mut("error")
                    .map(Value::take)
                    .unwrap_or(envelope);
                upstream["message"] = json!("IO operation failed.");
                let failure = Failure {
                    category: upstream["category"].as_str().unwrap_or("internal").into(),
                    phase: upstream["phase"].as_str().unwrap_or("finalize").into(),
                    code: "CHAIN_UPSTREAM".into(),
                    effect: "none".into(),
                    retry: json!({"kind":"never"}),
                };
                Self {
                    upstream,
                    upstream_error: true,
                    data: None,
                    rows: 0,
                    columns: None,
                    failure: Some(failure),
                    other: vec![],
                    complete: false,
                    effect: "none".into(),
                }
            }
        }
    }
}
fn category(error: Error) -> &'static str {
    match error {
        Error::Cancelled => "cancelled",
        Error::ResourceLimit => "resource_limit",
        Error::Storage => "io",
        Error::Unsupported => "unsupported",
        _ => "schema",
    }
}
fn clean(temp: tempfile::TempDir, outcome: &mut Outcome) {
    if temp.close().is_err() {
        outcome.fail("io", "finalize", "CHAIN_UPSTREAM", &outcome.effect.clone());
    }
}
fn snapshot(source: &Source, directory: &Path) -> Result<PathBuf, Error> {
    for (name, bytes) in &source.files {
        let path = directory.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|_| Error::Storage)?;
        }
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .and_then(|mut f| f.write_all(bytes))
            .map_err(|_| Error::Storage)?;
    }
    Ok(directory.join(&source.entry))
}
fn bounded_file(path: &Path) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|_| Error::Storage)?
        .take(MAX_BLOB as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Storage)?;
    if bytes.len() > MAX_BLOB {
        return Err(Error::ResourceLimit);
    }
    Ok(bytes)
}
fn bundle_size(root: &Path, token: &CancellationToken) -> Result<u64, Error> {
    let mut paths = vec![root.to_path_buf()];
    let mut entries = 0;
    let mut bytes = 0_u64;
    while let Some(path) = paths.pop() {
        if token.is_cancelled() {
            return Err(Error::Cancelled);
        }
        let metadata = std::fs::symlink_metadata(&path).map_err(|_| Error::Storage)?;
        entries += 1;
        if entries > 256 || metadata.file_type().is_symlink() {
            return Err(Error::ResourceLimit);
        }
        if metadata.is_dir() {
            for entry in std::fs::read_dir(&path).map_err(|_| Error::Storage)? {
                paths.push(entry.map_err(|_| Error::Storage)?.path());
            }
        } else if metadata.is_file() {
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or(Error::ResourceLimit)?;
        } else {
            return Err(Error::Unsupported);
        }
        if bytes > MAX_BLOB as u64 {
            return Err(Error::ResourceLimit);
        }
    }
    Ok(bytes)
}
fn evidence(outcome: &mut Outcome, read: bool) {
    let pairs = if read {
        vec![("fidelity", "loss")]
    } else {
        vec![
            ("input_fidelity", "input_loss"),
            ("write_fidelity", "write_loss"),
        ]
    };
    outcome.complete = pairs.iter().all(|(f, l)| {
        outcome.upstream[*f]["troncato"] == false && outcome.upstream[*l]["troncato"] == false
    });
    let exact = pairs.iter().all(|(f, l)| {
        outcome.upstream[*f]["level"] == "lossless" && outcome.upstream[*l]["lossless"] == true
    });
    // Reported fidelity is NOT generic loss consent (CHAIN-PLAN 2.0); only DB has a grant.
    if !outcome.complete || !exact {
        outcome.fail("data_mapping", "validate", "CHAIN_BOUNDARY", "none");
    }
}
/// All upstream writes are private until the independent fidelity and bundle gates pass.
#[allow(clippy::too_many_lines)]
pub(crate) fn execute(
    prepared: Prepared,
    i: &Invocation,
    token: &CancellationToken,
) -> Result<Outcome, Error> {
    let limits = limits(i, &prepared.parameters)?;
    let context = PipelineBudget::builder()
        .limits(limits)
        .cancellation(token.clone())
        .build()
        .map_err(|_| Error::ResourceLimit)?;
    let source_dir = tempfile::tempdir_in(&prepared.staging).map_err(|_| Error::Storage)?;
    let read = i.step.operation == "io.read";
    let source = if let Some(source) = &prepared.source {
        snapshot(source, source_dir.path())
    } else {
        let path = source_dir.path().join("input.arrow");
        std::fs::write(&path, prepared.input.as_ref().ok_or(Error::Invalid)?)
            .map(|()| path)
            .map_err(|_| Error::Storage)
    };
    let source = match source {
        Ok(path) => path,
        Err(e) => {
            source_dir.close().map_err(|_| Error::Storage)?;
            return Err(e);
        }
    };
    let original_schema = prepared
        .source
        .as_ref()
        .and_then(|s| s.files.get(&s.entry))
        .or(prepared.input.as_ref())
        .map(|bytes| ipc::stream_schema(bytes, i));
    let output_dir = if let Some(destination) = &prepared.destination {
        plenora_io_core::publish::create_staged_dir(Path::new(&destination.directory))
            .map_err(|_| Error::Storage)
    } else {
        tempfile::tempdir_in(&prepared.staging).map_err(|_| Error::Storage)
    };
    let output_dir = match output_dir {
        Ok(dir) => dir,
        Err(e) => {
            source_dir.close().map_err(|_| Error::Storage)?;
            return Err(e);
        }
    };
    let entry = prepared
        .destination
        .as_ref()
        .map_or("data.arrow", |d| d.entry.as_str());
    let output = output_dir.path().join(entry);
    if let Some(parent) = output.parent()
        && std::fs::create_dir_all(parent).is_err()
    {
        let a = source_dir.close();
        let b = output_dir.close();
        return Err(if a.is_err() || b.is_err() {
            Error::Recovery
        } else {
            Error::Storage
        });
    }
    let mut request = Richiesta::sulla_sorgente(source)
        .con_destinazione(&output)
        .con_tetti(limits)
        .con_cancellazione(token.clone());
    request.layer = prepared.parameters.layer;
    request.assume_crs = prepared.parameters.assume_crs;
    request.input_options = prepared.parameters.input_options;
    request.output_options = prepared.parameters.output_options;
    request.target_format = prepared.parameters.format;
    // Only the final bundle publication is requested durable. Upstream publication is private.
    if read || request.target_format.as_deref() == Some("ipc") {
        request
            .output_options
            .insert("serialization".into(), "stream".into());
    }
    let mut outcome = Outcome::from_upstream(if read {
        operazioni::read(request)
    } else {
        operazioni::write(request)
    });
    outcome.rows = prepared.input_rows;
    if outcome.failure.is_none() {
        if let Some(rows) =
            outcome.upstream[if read { "rows_read" } else { "rows_written" }].as_u64()
        {
            if read {
                outcome.rows = rows;
            } else if rows != prepared.input_rows {
                outcome.fail("protocol", "finalize", "CHAIN_RESULT", "none");
            }
        } else {
            outcome.fail("protocol", "finalize", "CHAIN_RESULT", "none");
        }
        evidence(&mut outcome, read);
        if read && outcome.upstream["truncated"] != false {
            outcome.complete = false;
            outcome.fail("protocol", "finalize", "CHAIN_RESULT", "none");
        }
    }
    if crate::encode(&outcome.upstream).is_err() {
        outcome.upstream = Value::Null;
        outcome.complete = false;
        outcome.fail("resource_limit", "finalize", "CHAIN_LIMIT", "none");
    }
    if outcome.failure.is_none() && read {
        match bounded_file(&output).and_then(|bytes| {
            let (rows, columns) =
                ipc::check(&bytes, i.step.outputs.get("data").ok_or(Error::Invalid)?, i)?;
            if outcome.upstream["format"] == "ipc" {
                let schema = original_schema
                    .as_ref()
                    .ok_or(Error::Schema)?
                    .as_ref()
                    .map_err(|_| Error::Unsupported)?;
                if ipc::stream_schema(&bytes, i)?.as_ref() != schema.as_ref() {
                    return Err(Error::Schema);
                }
            }
            if rows != outcome.rows || rows > i.reservation.limits.rows_produced {
                return Err(Error::Schema);
            }
            Ok((bytes, columns))
        }) {
            Ok((bytes, columns)) => {
                outcome.data = Some(bytes);
                outcome.columns = Some(columns);
            }
            Err(e) => {
                outcome.complete = false;
                outcome.fail(category(e), "finalize", "CHAIN_SCHEMA", "none");
            }
        }
    }
    if !read && outcome.failure.is_none() && outcome.upstream["format"] == "ipc" {
        let matches = bounded_file(&output)
            .and_then(|bytes| ipc::stream_schema(&bytes, i))
            .is_ok_and(|schema| {
                original_schema.as_ref().and_then(|s| s.as_ref().ok()) == Some(&schema)
            });
        if !matches {
            outcome.complete = false;
            outcome.fail("schema", "finalize", "CHAIN_SCHEMA", "none");
        }
    }
    clean(source_dir, &mut outcome);
    if !read && outcome.failure.is_none() {
        let Some(destination) = prepared.destination.as_ref() else {
            outcome.fail("internal", "finalize", "CHAIN_REFERENCE", "none");
            clean(output_dir, &mut outcome);
            return Ok(outcome);
        };
        if let Err(e) = bundle_size(output_dir.path(), token) {
            outcome.fail(category(e), "finalize", "CHAIN_LIMIT", "none");
        } else {
            match plenora_io_core::publish::publish_dir_atomic(
                output_dir.path(),
                Path::new(&destination.directory),
                prepared.parameters.durable,
                context.context(),
            ) {
                Ok(plenora_io_core::publish::PublishOutcome::Published) => {
                    outcome.effect = "committed".into();
                    let _old = output_dir.keep();
                    return Ok(outcome);
                }
                Ok(plenora_io_core::publish::PublishOutcome::PublishedButDurabilityUnconfirmed) => {
                    outcome.fail("io", "finalize", "CHAIN_RECOVERY", "unknown");
                    let _old = output_dir.keep();
                    return Ok(outcome);
                }
                Err(error) => {
                    let effect = serde_json::to_value(error.remote_effect).ok();
                    let category = serde_json::to_value(error.category).ok();
                    outcome.fail(
                        category
                            .as_ref()
                            .and_then(Value::as_str)
                            .unwrap_or("internal"),
                        "finalize",
                        "CHAIN_UPSTREAM",
                        effect.as_ref().and_then(Value::as_str).unwrap_or("unknown"),
                    );
                }
            }
        }
    }
    clean(output_dir, &mut outcome);
    Ok(outcome)
}
