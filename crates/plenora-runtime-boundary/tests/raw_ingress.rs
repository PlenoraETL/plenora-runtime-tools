//! Raw-ingress regressions; upstream probes are executed, not merely deserialized.
use async_trait::async_trait;
use plenora_runtime_boundary::{AdmissionPolicy, BoundaryResultSink, Rejection, RuntimeBoundary};
use plenora_runtime_capabilities::*;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::{
    CAUSATION_ID_METADATA_KEY as CAUSE, CORRELATION_ID_METADATA_KEY as CORRELATION,
    MESSAGE_ID_METADATA_KEY as ID, MessageMetadata, PublishOutcome, SerializedMessage,
};
use plenora_runtime_testkit::ManualClock;
use plenora_runtime_worker::{WorkerContext, WorkerHandler, WorkerMessageDecoder};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::SystemTime,
};

type TestResult = Result<(), Box<dyn Error>>;
type Probe = (&'static str, fn() -> TestResult);
const RULES: &[&str] = &[
    "RT-012", "RT-016", "RT-017", "RT-018", "RT-019", "RT-020", "RT-021", "RT-022", "RT-023",
];
const NOW: &str = "2029-01-01T00:00:00Z";
const SENTINEL: &str = "SECRET_SENTINEL_DO_NOT_REFLECT";

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn fixture(name: &str) -> Result<Value, Box<dyn Error>> {
    Ok(serde_json::from_slice(&fs::read(
        root().join("contracts/runtime-v1").join(name),
    )?)?)
}
fn now() -> Result<SystemTime, Box<dyn Error>> {
    Ok(chrono::DateTime::parse_from_rfc3339(NOW)?.into())
}

struct Handler(Arc<AtomicUsize>);
#[async_trait]
impl CapabilityHandler for Handler {
    async fn invoke(
        &self,
        _: WorkerContext,
        _: CapabilityRequest,
    ) -> Result<CapabilityResponse, CapabilityFailure> {
        self.0.fetch_add(1, Ordering::SeqCst);
        // The test models a resolver only inside invoke; admission cannot reach it.
        let contract = ContractId::new("plenora-test-output-v1").map_err(|e| {
            CapabilityFailure::new(
                plenora_runtime_messaging::RetryErrorClass::Permanent,
                CapabilityRemoteEffect::NotStarted,
                e,
            )
        })?;
        Ok(CapabilityResponse::new(
            contract,
            SerializedMessage::new("application/json", b"{}".as_slice()),
        ))
    }
}
struct Policy;
impl AdmissionPolicy for Policy {
    fn idempotency_key_limit(&self, capability: &str, _: &str, _: &str) -> Option<usize> {
        (capability == "plenora.rest-tools").then_some(64)
    }
    fn validate_payload(&self, request: &CapabilityRequest) -> Result<(), PlenoraErrorCategory> {
        // Test component schema owns this field; boundary never interprets arbitrary payloads.
        if request
            .input()
            .headers
            .contains_key(EXECUTION_DEADLINE_METADATA_KEY)
        {
            let value: Value = serde_json::from_slice(&request.input().bytes)
                .map_err(|_| PlenoraErrorCategory::InvalidConfiguration)?;
            if value.get("deadline").is_some() {
                return Err(PlenoraErrorCategory::InvalidConfiguration);
            }
        }
        Ok(())
    }
}
#[derive(Default)]
struct Sink(Mutex<Vec<SerializedMessage>>);
#[async_trait]
impl BoundaryResultSink for Sink {
    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, CapabilityResultPublishError> {
        self.0
            .lock()
            .map_err(|_| {
                CapabilityResultPublishError::with_source(std::io::Error::other("sink lock"))
            })?
            .push(message);
        Ok(PublishOutcome::Confirmed)
    }
}
struct Harness {
    boundary: RuntimeBoundary,
    dispatcher: CapabilityDispatcher,
    calls: Arc<AtomicUsize>,
    sink: Arc<Sink>,
    clock: Arc<ManualClock>,
}
fn harness() -> Result<Harness, Box<dyn Error>> {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut operations: BTreeMap<String, BTreeMap<String, Value>> = BTreeMap::new();
    for file in fs::read_dir(root().join("contracts/runtime-v1"))? {
        let doc: Value = serde_json::from_slice(&fs::read(file?.path())?)?;
        if doc["kind"] != "request" {
            continue;
        }
        let m = &doc["metadata"];
        let name = m[CAPABILITY_NAME_METADATA_KEY]
            .as_str()
            .ok_or("capability")?;
        let op = m[CAPABILITY_OPERATION_METADATA_KEY]
            .as_str()
            .ok_or("operation")?;
        let version = m[OPERATION_VERSION_METADATA_KEY]
            .as_str()
            .ok_or("version")?
            .parse::<u16>()?;
        operations.entry(name.into()).or_default().insert(format!("{op}@{version}"), json!({
            "id": op, "version": version, "status": "available", "surfaces": ["runtime"],
            "input": {"contract": m[INPUT_CONTRACT_METADATA_KEY], "content_types": [doc["content_type"]]},
            "output": {"contract": "plenora-test-output-v1", "content_types": ["application/json"]},
            "side_effect": "none", "controls": {"deadline": true, "idempotency_key": name == "plenora.rest-tools", "cancellation": false}
        }));
    }
    let mut builder = CapabilityRegistryBuilder::default();
    for (capability, ops) in operations {
        // REST's required complete profile is registered from the real fixture below.
        if capability == "plenora.rest-tools" {
            continue;
        }
        let doc = json!({"schema_version": 2, "component": capability.replace('.', "-"), "component_version": "1.0.0",
            "interfaces": [{"kind": "runtime", "contract": "plenora-runtime-binding-v1", "version": 1, "artifact": capability}],
            "operations": ops.into_values().collect::<Vec<_>>()});
        builder.register_discovered(
            CapabilityDiscovery::from_json(&serde_json::to_vec(&doc)?)?,
            Handler(calls.clone()),
        )?;
    }
    builder.register_discovered(
        CapabilityDiscovery::from_json(&fs::read(
            root().join("contracts/capabilities-v2/rest-tools-v1.json"),
        )?)?,
        Handler(calls.clone()),
    )?;
    let sink = Arc::new(Sink::default());
    let clock = Arc::new(ManualClock::new(now()?));
    let dispatcher =
        CapabilityDispatcher::new(builder.build(), CapabilityDispatcherConfig::default())?;
    let boundary = RuntimeBoundary::new(
        dispatcher.clone(),
        clock.clone(),
        Arc::new(Policy),
        sink.clone(),
    );
    Ok(Harness {
        boundary,
        dispatcher,
        calls,
        sink,
        clock,
    })
}
fn input(doc: &Value) -> Result<SerializedMessage, Box<dyn Error>> {
    Ok(SerializedMessage::new(
        doc["content_type"]
            .as_str()
            .ok_or("content type")?
            .to_owned(),
        serde_json::to_vec(&doc["payload"])?,
    ))
}
fn reject(
    h: &Harness,
    doc: &Value,
    category: PlenoraErrorCategory,
) -> Result<Rejection, Box<dyn Error>> {
    let error = h
        .boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(doc)?)
        .err()
        .ok_or("raw input accepted")?;
    assert_eq!(error.category(), category);
    assert_eq!(error.phase(), PlenoraErrorPhase::Validate);
    assert_eq!(error.remote_effect(), PlenoraErrorRemoteEffect::None);
    assert_eq!(error.retry(), PlenoraErrorRetry::Never);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    let message = error.to_message()?;
    assert_eq!(message.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
    assert!(!format!("{error:?} {error}").contains(SENTINEL));
    assert!(error.source().is_none());
    // Portable worker ingress must make the same decision whenever raw values are strings.
    if doc["metadata"]
        .as_object()
        .ok_or("metadata")?
        .values()
        .all(Value::is_string)
    {
        let mut serialized = input(doc)?;
        let mut representable = true;
        for (key, value) in doc["metadata"].as_object().ok_or("metadata")? {
            if serialized
                .headers
                .insert_text(key.clone(), value.as_str().ok_or("text")?)
                .is_err()
            {
                representable = false;
                break;
            }
        }
        if representable {
            let alternate = h
                .boundary
                .decode(&serialized)
                .err()
                .ok_or("worker ingress accepted")?;
            assert_eq!(alternate.category(), error.category());
            let mut a = alternate.metadata().clone();
            let mut b = error.metadata().clone();
            a.remove(ID);
            b.remove(ID);
            assert_eq!(a, b);
        }
    }
    Ok(error)
}

#[test]
fn upstream_raw_probes() -> TestResult {
    let h = harness()?;
    let mut count = 0;
    for file in fs::read_dir(root().join("contracts/runtime-probes-v1"))? {
        let probe: Value = serde_json::from_slice(&fs::read(file?.path())?)?;
        let mut doc = fixture(probe["base"].as_str().ok_or("base")?)?;
        if let Some(set) = probe["mutation"]["set"].as_object() {
            for (key, value) in set {
                doc["metadata"][key] = value.clone();
            }
        }
        if let Some(key) = probe["mutation"]["remove"].as_str() {
            doc["metadata"]
                .as_object_mut()
                .ok_or("metadata")?
                .remove(key);
        }
        let expected = &probe["expected"];
        let category = match expected["error"]["category"].as_str() {
            Some("protocol") => PlenoraErrorCategory::Protocol,
            Some("unsupported") => PlenoraErrorCategory::Unsupported,
            Some("timeout") => PlenoraErrorCategory::Timeout,
            _ => return Err("unknown probe category".into()),
        };
        let error = reject(&h, &doc, category)?;
        let mut metadata = error.metadata().clone();
        metadata.remove(ID);
        metadata.remove(CAUSE);
        assert_eq!(
            Value::Object(metadata),
            expected["metadata"],
            "{}",
            probe["base"]
        );
        count += 1;
    }
    assert_eq!(count, 21);
    Ok(())
}

fn rt012() -> TestResult {
    let h = harness()?;
    for key in [ID, CORRELATION, CAUSE] {
        for value in [
            Value::Null,
            json!(1),
            json!("018F3D84-7B2C-7F00-8000-000000000105"),
            json!("{018f3d84-7b2c-7f00-8000-000000000105}"),
            json!("018f3d847b2c7f008000000000000105"),
            json!(SENTINEL),
        ] {
            let mut doc = fixture("io-read-request.json")?;
            doc["metadata"][key] = value;
            reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
        }
    }
    for key in [ID, CORRELATION] {
        let mut doc = fixture("io-read-request.json")?;
        let removed = doc["metadata"]
            .as_object_mut()
            .ok_or("metadata")?
            .remove(key)
            .ok_or("id")?;
        doc["metadata"][key.replace('.', "_")] = removed;
        reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
    }
    Ok(())
}
fn rt016() -> TestResult {
    upstream_raw_probes()
}
fn rt017() -> TestResult {
    let h = harness()?;
    for key in [
        CAPABILITY_VERSION_METADATA_KEY,
        OPERATION_VERSION_METADATA_KEY,
    ] {
        for value in [
            json!("01"),
            json!("+1"),
            json!(" 1"),
            json!("1 "),
            json!("0"),
            json!("1.0"),
            json!("1e0"),
            json!(1),
            Value::Null,
        ] {
            let mut doc = fixture("io-read-request.json")?;
            doc["metadata"][key] = value;
            reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
        }
        for size in [5, 100, 8192] {
            let mut doc = fixture("io-read-request.json")?;
            doc["metadata"][key] = json!("9".repeat(size));
            reject(&h, &doc, PlenoraErrorCategory::Unsupported)?;
        }
        let mut doc = fixture("io-read-request.json")?;
        doc["metadata"][key] = json!("9".repeat(8193));
        reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
    }
    for raw in [
        br#"{"plenora.operation.version":"1","plenora.operation.version":"2"}"#.as_slice(),
        b"{",
        b"null",
        b"{} null",
    ] {
        let e = h
            .boundary
            .admit_raw(
                raw,
                SerializedMessage::new("application/json", b"{}".as_slice()),
            )
            .err()
            .ok_or("raw accepted")?;
        assert_eq!(e.category(), PlenoraErrorCategory::Protocol);
        e.to_message()?;
    }
    Ok(())
}
fn rt018() -> TestResult {
    let h = harness()?;
    for malformed in [false, true] {
        for unsupported in [false, true] {
            for expired in [false, true] {
                let mut doc = fixture("io-read-request.json")?;
                if malformed {
                    doc["metadata"][OPERATION_VERSION_METADATA_KEY] = json!("01");
                }
                if unsupported {
                    doc["metadata"][CAPABILITY_NAME_METADATA_KEY] = json!("plenora.other-tools");
                }
                if expired {
                    doc["metadata"][EXECUTION_DEADLINE_METADATA_KEY] = json!(NOW);
                }
                let category = if malformed {
                    Some(PlenoraErrorCategory::Protocol)
                } else if unsupported {
                    Some(PlenoraErrorCategory::Unsupported)
                } else if expired {
                    Some(PlenoraErrorCategory::Timeout)
                } else {
                    None
                };
                if let Some(c) = category {
                    reject(&h, &doc, c)?;
                } else {
                    h.boundary
                        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
                }
            }
        }
    }
    let mut doc = fixture("io-read-request.json")?;
    doc["metadata"][CAPABILITY_VERSION_METADATA_KEY] = json!("9999999999999999999999");
    doc["metadata"][EXECUTION_DEADLINE_METADATA_KEY] = json!("2030-01-01T00:00:00-00:00");
    reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
    Ok(())
}
fn rt019() -> TestResult {
    let h = harness()?;
    let mut doc = fixture("io-read-request.json")?;
    doc["metadata"][CAPABILITY_OPERATION_METADATA_KEY] = json!(SENTINEL);
    doc["metadata"][CORRELATION] = Value::Null;
    doc["metadata"][OPERATION_VERSION_METADATA_KEY] = json!("+1");
    let error = reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
    for key in [
        CAPABILITY_OPERATION_METADATA_KEY,
        CORRELATION,
        OPERATION_VERSION_METADATA_KEY,
    ] {
        assert!(!error.metadata().contains_key(key));
    }
    assert!(!String::from_utf8(error.to_message()?.bytes.to_vec())?.contains(SENTINEL));
    let error = h
        .boundary
        .admit_raw(b"{", input(&doc)?)
        .err()
        .ok_or("accepted")?;
    assert_eq!(error.metadata().len(), 2);
    Ok(())
}
fn rt020() -> TestResult {
    let h = harness()?;
    let mut doc = fixture("io-read-request.json")?;
    doc["metadata"][CAUSE] = json!("018f3d84-7b2c-7f00-8000-000000000999");
    doc["metadata"][CAPABILITY_VERSION_METADATA_KEY] = json!("2");
    let error = reject(&h, &doc, PlenoraErrorCategory::Unsupported)?;
    assert_ne!(error.metadata()[ID], doc["metadata"][ID]);
    assert_eq!(error.metadata()[CAUSE], doc["metadata"][ID]);
    assert_ne!(error.metadata()[CAUSE], doc["metadata"][CAUSE]);
    assert_eq!(error.to_message()?, error.to_message()?);
    Ok(())
}
fn rt021() -> TestResult {
    let h = harness()?;
    for deadline in ["2028-12-31T23:59:59Z", NOW] {
        let mut doc = fixture("io-read-request.json")?;
        doc["metadata"][EXECUTION_DEADLINE_METADATA_KEY] = json!(deadline);
        reject(&h, &doc, PlenoraErrorCategory::Timeout)?;
    }
    for deadline in [
        "2030-01-01T00:00:00Z",
        "2030-01-01t00:00:00z",
        "2030-01-01T00:00:00+00:00",
        "2029-01-01T00:00:00.000000001Z",
    ] {
        let mut doc = fixture("io-read-request.json")?;
        doc["metadata"][EXECUTION_DEADLINE_METADATA_KEY] = json!(deadline);
        h.boundary
            .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    }
    for deadline in [
        "2030-01-01T00:00:00-00:00",
        "2030-01-01T00:00:00+01:00",
        "2030-01-01 00:00:00Z",
        "2030-01-01T00:00:00.Z",
        "2030-01-01T00:00:00.1234567890Z",
        "2030-01-01T00:00:60Z",
        "2030-02-30T00:00:00Z",
    ] {
        let mut doc = fixture("io-read-request.json")?;
        doc["metadata"][EXECUTION_DEADLINE_METADATA_KEY] = json!(deadline);
        reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
    }
    Ok(())
}
fn rt022() -> TestResult {
    let h = harness()?;
    for value in [Value::Null, json!(""), json!("a".repeat(65))] {
        let mut doc = fixture("rest-upload-request.json")?;
        doc["metadata"][IDEMPOTENCY_KEY_METADATA_KEY] = value;
        reject(&h, &doc, PlenoraErrorCategory::Protocol)?;
    }
    let mut doc = fixture("rest-upload-request.json")?;
    doc["metadata"][IDEMPOTENCY_KEY_METADATA_KEY] = json!("a".repeat(64));
    h.boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    doc["metadata"]
        .as_object_mut()
        .ok_or("metadata")?
        .remove(IDEMPOTENCY_KEY_METADATA_KEY);
    h.boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    Ok(())
}
fn rt023() -> TestResult {
    let h = harness()?;
    for payload_deadline in [json!("2030-01-01T00:00:00Z"), json!("2031-01-01T00:00:00Z")] {
        let mut doc = fixture("io-read-request.json")?;
        doc["payload"]["deadline"] = payload_deadline;
        reject(&h, &doc, PlenoraErrorCategory::InvalidConfiguration)?;
    }
    Ok(())
}
#[test]
fn every_p2_1_row_has_an_executed_raw_probe() -> TestResult {
    let inventory: Value =
        serde_json::from_slice(&fs::read(root().join("docs/boundary-probes.json"))?)?;
    assert_eq!(inventory["rules"], json!(RULES));
    let probes: &[Probe] = &[
        ("RT-012", rt012),
        ("RT-016", rt016),
        ("RT-017", rt017),
        ("RT-018", rt018),
        ("RT-019", rt019),
        ("RT-020", rt020),
        ("RT-021", rt021),
        ("RT-022", rt022),
        ("RT-023", rt023),
    ];
    assert_eq!(probes.iter().map(|(id, _)| *id).collect::<Vec<_>>(), RULES);
    for (_, probe) in probes {
        probe()?;
    }
    Ok(())
}
#[tokio::test]
async fn real_dispatch_success_identity_and_queued_expiry() -> TestResult {
    let h = harness()?;
    let doc = fixture("io-read-request.json")?;
    let admitted = h
        .boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    let runtime = RuntimeHandle::new(ServiceMetadata::new("boundary-test", "1", "fixture"));
    let context = WorkerContext::from_identity(
        admitted.identity(),
        1,
        admitted.metadata().clone(),
        runtime.shutdown_signal(),
    );
    h.boundary.handle(context, admitted).await?;
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    let results = h.sink.0.lock().map_err(|_| "sink lock")?.clone();
    assert_eq!(results.len(), 1);
    let output = results.first().ok_or("output")?;
    assert_ne!(output.headers.get_text(ID)?, doc["metadata"][ID].as_str());
    assert_eq!(
        output.headers.get_text(CAUSE)?,
        doc["metadata"][ID].as_str()
    );
    assert_eq!(
        output.headers.get_text(CORRELATION)?,
        doc["metadata"][CORRELATION].as_str()
    );
    drop(results);
    let admitted = h
        .boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    let context = WorkerContext::from_identity(
        admitted.identity(),
        1,
        admitted.metadata().clone(),
        runtime.shutdown_signal(),
    );
    h.clock
        .set(chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")?.into());
    let error = h
        .boundary
        .handle(context, admitted)
        .await
        .err()
        .ok_or("expired queue accepted")?;
    assert_eq!(error.category(), PlenoraErrorCategory::Timeout);
    assert_eq!(error.phase(), PlenoraErrorPhase::Validate);
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

struct EntryThenDeadline {
    calls: AtomicUsize,
    entry: SystemTime,
    deadline: SystemTime,
}
impl plenora_runtime_core::Clock for EntryThenDeadline {
    fn now(&self) -> SystemTime {
        if self.calls.fetch_add(1, Ordering::SeqCst) < 2 {
            self.entry
        } else {
            self.deadline
        }
    }
}

#[tokio::test]
async fn deadline_reached_between_boundary_and_dispatcher_never_polls_handler() -> TestResult {
    let h = harness()?;
    let doc = fixture("io-read-request.json")?;
    let clock = Arc::new(EntryThenDeadline {
        calls: AtomicUsize::new(0),
        entry: now()?,
        deadline: chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")?.into(),
    });
    let boundary = RuntimeBoundary::new(
        h.dispatcher.clone(),
        clock,
        Arc::new(Policy),
        h.sink.clone(),
    );
    let admitted = boundary.admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    let runtime = RuntimeHandle::new(ServiceMetadata::new("boundary-test", "1", "fixture"));
    let context = WorkerContext::from_identity(
        admitted.identity(),
        1,
        admitted.metadata().clone(),
        runtime.shutdown_signal(),
    );
    let error = boundary
        .handle(context, admitted)
        .await
        .err()
        .ok_or("equality invoked handler")?;
    assert_eq!(error.category(), PlenoraErrorCategory::Timeout);
    assert_eq!(error.phase(), PlenoraErrorPhase::Validate);
    assert_eq!(error.remote_effect(), PlenoraErrorRemoteEffect::None);
    assert_eq!(error.retry(), PlenoraErrorRetry::Never);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    error.to_message()?;
    Ok(())
}

#[tokio::test]
async fn nanosecond_deadline_is_not_rounded_by_dispatcher() -> TestResult {
    let h = harness()?;
    let mut doc = fixture("io-read-request.json")?;
    doc["metadata"][EXECUTION_DEADLINE_METADATA_KEY] = json!("2029-01-01T00:00:00.000000001Z");
    let admitted = h
        .boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    let runtime = RuntimeHandle::new(ServiceMetadata::new("boundary-test", "1", "fixture"));
    let context = WorkerContext::from_identity(
        admitted.identity(),
        1,
        admitted.metadata().clone(),
        runtime.shutdown_signal(),
    );
    h.boundary.handle(context, admitted).await?;
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn context_cannot_bypass_admitted_metadata() -> TestResult {
    let h = harness()?;
    let doc = fixture("io-read-request.json")?;
    let admitted = h
        .boundary
        .admit_raw(&serde_json::to_vec(&doc["metadata"])?, input(&doc)?)?;
    let runtime = RuntimeHandle::new(ServiceMetadata::new("boundary-test", "1", "fixture"));
    let context = WorkerContext::from_identity(
        admitted.identity(),
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    );
    // Protocol ambiguity must also win when the queued request has expired.
    h.clock
        .set(chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")?.into());
    let error = h
        .boundary
        .handle(context, admitted)
        .await
        .err()
        .ok_or("ambiguous context accepted")?;
    assert_eq!(error.category(), PlenoraErrorCategory::Protocol);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    Ok(())
}
