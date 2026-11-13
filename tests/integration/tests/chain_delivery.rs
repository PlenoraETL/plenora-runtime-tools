//! Product gate and settlement probes. Memory storage and scripted components are test doubles.
#![allow(
    clippy::wildcard_imports,
    reason = "Contract probes use public API families"
)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "Test assertions"
)]
use async_trait::async_trait;
use plenora_runtime_boundary::*;
use plenora_runtime_capabilities::*;
use plenora_runtime_core::{RuntimeHandle, ServiceMetadata};
use plenora_runtime_messaging::*;
use plenora_runtime_testkit::ManualClock;
use plenora_runtime_worker::WorkerContext;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    error::Error,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
type TestResult = Result<(), Box<dyn Error>>;
const REQUIRES: &str = "plenora.negotiation.requires";
const DOWNGRADE: &str = "plenora.negotiation.accepts_downgrade";
#[derive(Default)]
struct Double {
    acceptance: Mutex<BTreeMap<MessageId, NegotiationContext>>,
    journals: Mutex<BTreeMap<String, Vec<u8>>>,
    unavailable: AtomicBool,
    fail_seal: AtomicBool,
    before_seal: AtomicBool,
    fail_finish: AtomicBool,
    clock: ManualClock,
}
impl Double {
    fn now(&self) -> u64 {
        use plenora_runtime_core::Clock;
        u64::try_from(
            self.clock
                .now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    }
    fn get(&self, key: &str) -> DeliveryJournal {
        DeliveryJournal::from_json(self.journals.lock().unwrap().get(key).unwrap()).unwrap()
    }
}
impl AcceptanceStore for Double {
    fn accept(
        &self,
        id: &MessageId,
        proposed: Option<NegotiationContext>,
    ) -> Result<AcceptanceRecord, NegotiationError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(NegotiationError);
        }
        let mut records = self.acceptance.lock().map_err(|_| NegotiationError)?;
        if let Some(c) = records.get(id) {
            return Ok(AcceptanceRecord::Existing(*c));
        }
        records.insert(*id, proposed.ok_or(NegotiationError)?);
        Ok(AcceptanceRecord::Created)
    }
}
#[async_trait]
impl DurableExecutionStore for Double {
    async fn open(
        &self,
        key: &str,
        binding: [u8; 32],
        plan: &DeliveryPlan,
    ) -> Result<DeliveryOpen, SettlementError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(SettlementError);
        }
        let mut rows = self.journals.lock().map_err(|_| SettlementError)?;
        if let Some(bytes) = rows.get(key) {
            let mut j = DeliveryJournal::from_json(bytes)?;
            j.maintain(self.now(), 604_800_000, 7_776_000_000)?;
            return Ok(if j.verify(binding, plan).is_ok() {
                DeliveryOpen::Opened(j, self.now())
            } else {
                DeliveryOpen::Conflict
            });
        }
        if let Some((_, id)) = key.rsplit_once(":m:")
            && rows.keys().any(|k| k.ends_with(&format!(":m:{id}")))
        {
            return Ok(DeliveryOpen::Conflict);
        }
        let j = DeliveryJournal::new(
            key,
            binding,
            plan.clone(),
            self.now(),
            604_800_000,
            7_776_000_000,
        )?;
        self.acceptance.lock().map_err(|_| SettlementError)?.insert(
            j.result_id().parse().map_err(|_| SettlementError)?,
            NegotiationContext {
                requires: RuntimeBinding::V2,
                accepts_downgrade: false,
            },
        );
        rows.insert(key.into(), j.to_json()?);
        Ok(DeliveryOpen::Opened(j, self.now()))
    }
    async fn load(&self, key: &str) -> Result<(DeliveryJournal, u64), SettlementError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(SettlementError);
        }
        let mut rows = self.journals.lock().map_err(|_| SettlementError)?;
        let mut j = DeliveryJournal::from_json(rows.get(key).ok_or(SettlementError)?)?;
        j.maintain(self.now(), 604_800_000, 7_776_000_000)?;
        rows.insert(key.into(), j.to_json()?);
        Ok((j, self.now()))
    }
    async fn change(
        &self,
        key: &str,
        revision: u64,
        c: DeliveryChange,
    ) -> Result<(DeliveryJournal, u64), SettlementError> {
        if matches!(c, DeliveryChange::Evidence(_)) && self.before_seal.load(Ordering::SeqCst) {
            return Err(SettlementError);
        }
        if matches!(c, DeliveryChange::Settle) && self.fail_finish.load(Ordering::SeqCst) {
            return Err(SettlementError);
        }
        let mut rows = self.journals.lock().map_err(|_| SettlementError)?;
        let mut j = DeliveryJournal::from_json(rows.get(key).ok_or(SettlementError)?)?;
        j.apply(revision, c, self.now())?;
        rows.insert(key.into(), j.to_json()?);
        if j.stage() == DeliveryStage::Evidence && self.fail_seal.load(Ordering::SeqCst) {
            return Err(SettlementError);
        }
        Ok((j, self.now()))
    }
    async fn pending(
        &self,
        prefix: &str,
        route: &str,
        limit: usize,
    ) -> Result<Vec<String>, SettlementError> {
        Ok(self
            .journals
            .lock()
            .map_err(|_| SettlementError)?
            .iter()
            .filter_map(|(k, b)| {
                let j = DeliveryJournal::from_json(b).ok()?;
                (k.starts_with(prefix)
                    && j.plan().route() == route
                    && !j.expired(self.now())
                    && !j.delivered()
                    && matches!(j.stage(), DeliveryStage::Evidence | DeliveryStage::Finished))
                .then(|| k.clone())
            })
            .take(limit)
            .collect())
    }
}
struct Policy;
impl AdmissionPolicy for Policy {
    fn idempotency_key_limit(&self, _: &str, _: &str, _: &str) -> Option<usize> {
        None
    }
    fn validate_payload(&self, _: &CapabilityRequest) -> Result<(), PlenoraErrorCategory> {
        Ok(())
    }
}
#[derive(Default)]
struct Producer(Mutex<Vec<SerializedMessage>>);
#[async_trait]
impl MessageProducer for Producer {
    type Error = SettlementError;
    async fn publish(&self, message: SerializedMessage) -> Result<PublishOutcome, SettlementError> {
        self.0.lock().map_err(|_| SettlementError)?.push(message);
        Ok(PublishOutcome::Confirmed)
    }
}
struct Component {
    calls: Arc<AtomicUsize>,
    bytes: usize,
    effect: PlenoraErrorRemoteEffect,
    phase: PlenoraErrorPhase,
    fail: bool,
}
#[async_trait]
impl EvidenceComponent for Component {
    async fn invoke(
        &self,
        _: WorkerContext,
        request: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> ComponentOutcome {
        assert!(!request.input().headers.contains_key(REQUIRES));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let result = if self.fail {
            PlenoraError::new(
                PlenoraErrorCategory::ResourceLimit,
                self.phase,
                self.effect,
                if self.effect == PlenoraErrorRemoteEffect::None {
                    PlenoraErrorRetry::Never
                } else {
                    PlenoraErrorRetry::RequiresRecovery
                },
                "Component quota.",
            )
            .map_or_else(|_| unreachable!("static fixture error"), Err)
        } else {
            ContractId::new("plenora-test-output-v1")
                .map(|c| {
                    CapabilityResponse::new(
                        c,
                        SerializedMessage::new("application/json", vec![b' '; self.bytes]),
                    )
                })
                .map_err(|_| unreachable!("static fixture contract"))
        };
        match result {
            Ok(response) => budget.success(response, self.phase, self.effect).unwrap(),
            Err(error) => budget.failure(error),
        }
    }
}
fn discovery(version: u16) -> Result<CapabilityDiscovery, Box<dyn Error>> {
    Ok(CapabilityDiscovery::from_json(&serde_json::to_vec(
        &json!({"schema_version":2,"component":"plenora-test-tools","component_version":"1.0.0",
        "interfaces":[{"kind":"runtime","contract":format!("plenora-runtime-binding-v{version}"),"version":version,"artifact":"plenora.test-tools"}],
        "operations":[{"id":"test.run","version":1,"status":"available","surfaces":["runtime"],"input":{"contract":"plenora-test-input-v1","content_types":["application/json"]},"output":{"contract":"plenora-test-output-v1","content_types":["application/json"]},"side_effect":"remote","controls":{"deadline":true,"idempotency_key":false,"cancellation":true}}]}),
    )?)?)
}
fn message() -> Result<SerializedMessage, Box<dyn Error>> {
    let mut m = CapabilityMessageCodec.encode(&CapabilityRequest::new(
        CapabilityId::new("plenora.test-tools", 1)?,
        OperationName::new("test.run")?,
        OperationVersion::new(1)?,
        ContractId::new("plenora-test-input-v1")?,
        SerializedMessage::new("application/json", "{}"),
    ))?;
    for (k, v) in [
        (MESSAGE_ID_METADATA_KEY, MessageId::random().to_string()),
        (
            CORRELATION_ID_METADATA_KEY,
            CorrelationId::random().to_string(),
        ),
        (REQUIRES, "plenora-runtime-binding-v2".into()),
        (DOWNGRADE, "false".into()),
    ] {
        m.headers.insert_text(k, v)?;
    }
    Ok(m)
}
fn host<S: AcceptanceStore>(
    store: Arc<S>,
    component: Component,
    limit: usize,
) -> Result<NegotiatedRuntime<S>, Box<dyn Error>> {
    let mut r = CapabilityRegistryBuilder::default();
    r.register_evidenced(discovery(2)?, component)?;
    Ok(NegotiatedRuntime::new(
        RuntimeBoundary::new(
            CapabilityDispatcher::new(r.build(), CapabilityDispatcherConfig::new(limit)?)?,
            Arc::new(ManualClock::default()),
            Arc::new(Policy),
            Arc::new(Producer::default()),
        ),
        store,
    ))
}
fn component(
    calls: Arc<AtomicUsize>,
    bytes: usize,
    effect: PlenoraErrorRemoteEffect,
    phase: PlenoraErrorPhase,
    fail: bool,
) -> Component {
    Component {
        calls,
        bytes,
        effect,
        phase,
        fail,
    }
}

#[tokio::test]
async fn enumerate_p2_3_all_21_sequences_through_product_gate() -> TestResult {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../contracts/runtime-negotiation-v1");
    let mut count = 0;
    for file in std::fs::read_dir(path)? {
        let fixture: Value = serde_json::from_slice(&std::fs::read(file?.path())?)?;
        let store = Arc::new(Double::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let host = host(
            store.clone(),
            component(
                calls.clone(),
                2,
                PlenoraErrorRemoteEffect::None,
                PlenoraErrorPhase::Finalize,
                false,
            ),
            1024,
        )?;
        let mut m = message()?;
        let mut refused = false;
        let sender = Producer::default();
        for hop in fixture["hops"].as_array().ok_or("hops")? {
            store
                .unavailable
                .store(hop["record"] == "unreadable", Ordering::SeqCst);
            let _ = m.headers.remove(REQUIRES);
            let _ = m.headers.remove(DOWNGRADE);
            if hop["carries"] == true && !hop["receives"].is_null() {
                m.headers.insert_text(
                    REQUIRES,
                    hop["receives"]["requires"].as_str().ok_or("requires")?,
                )?;
                m.headers.insert_text(
                    DOWNGRADE,
                    if hop["receives"]["accepts_downgrade"] == true {
                        "true"
                    } else {
                        "false"
                    },
                )?;
            }
            if host
                .admit(
                    &m,
                    if hop["from"] == "sender" {
                        DeliveryRoute::Ingress
                    } else {
                        DeliveryRoute::Forward
                    },
                )
                .is_err()
            {
                refused = true;
                break;
            }
        }
        if !refused {
            let destination = fixture["destination"]["interface"]["version"]
                .as_u64()
                .map(u16::try_from)
                .transpose()?
                .map(discovery)
                .transpose()?;
            let d = host.admit(&m, DeliveryRoute::Forward)?;
            refused = host.send(&d, destination.as_ref(), &sender).await.is_err();
        }
        // The product deliberately narrows the common compatibility profile: only v2/no downgrade.
        let first = &fixture["hops"][0]["receives"];
        let expected = fixture["expect"]["decision"] == "refuse"
            || first["requires"] != "plenora-runtime-binding-v2"
            || first["accepts_downgrade"] != false
            || fixture["destination"]["interface"]["version"] != 2;
        assert_eq!(refused, expected, "{}", fixture["description"]);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            sender.0.lock().map_err(|_| "lock")?.len(),
            usize::from(!refused)
        );
        count += 1;
    }
    assert_eq!(count, 21);
    Ok(())
}

fn worker_context() -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "1", "test"));
    WorkerContext::new(
        MessageId::random(),
        CorrelationId::random(),
        None,
        1,
        MessageMetadata::new(),
        runtime.shutdown_signal(),
    )
}
fn durable<S: DurableExecutionStore>(
    store: Arc<S>,
    clock: Arc<ManualClock>,
    scope: &str,
    component: impl EvidenceComponent + 'static,
    plan: DeliveryPlan,
    dispatcher: usize,
    sink: Arc<dyn BoundaryResultSink>,
) -> Result<Arc<DurableRuntime<S>>, Box<dyn Error>> {
    let mut r = CapabilityRegistryBuilder::default();
    r.register_evidenced(discovery(2)?, component)?;
    let boundary = RuntimeBoundary::new(
        CapabilityDispatcher::new(r.build(), CapabilityDispatcherConfig::new(dispatcher)?)?,
        clock,
        Arc::new(Policy),
        sink.clone(),
    );
    let routes = RouteTable::new(vec![(plan.route().into(), discovery(2)?, sink)])?;
    Ok(Arc::new(DurableRuntime::new(
        boundary, store, scope, plan, routes,
    )?))
}
fn standard_plan() -> DeliveryPlan {
    DeliveryPlan::new("nats://loopback/results", 3, 60_000, 4096, 1024).unwrap()
}
fn good(calls: Arc<AtomicUsize>) -> Component {
    component(
        calls,
        2,
        PlenoraErrorRemoteEffect::Committed,
        PlenoraErrorPhase::Commit,
        false,
    )
}

#[tokio::test]
async fn rt010_019_020_raw_rejections_are_durable_and_replay_after_deadline() -> TestResult {
    let store = Arc::new(Double::default());
    let clock = Arc::new(ManualClock::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(Producer::default());
    let h = durable(
        store.clone(),
        clock.clone(),
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    let mut bad = message()?;
    bad.headers
        .insert_text(CAPABILITY_VERSION_METADATA_KEY, "01")?;
    let rejection = h
        .process(&bad, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert_eq!(rejection.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
    assert_eq!(
        serde_json::from_slice::<Value>(&rejection.bytes)?["category"],
        "protocol"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(h.relay(&h.key(&bad)).await?);
    assert_eq!(
        h.process(&bad, DeliveryRoute::Ingress, worker_context())
            .await?,
        rejection
    );
    let mut m = message()?;
    m.headers
        .insert_text(EXECUTION_DEADLINE_METADATA_KEY, "1970-01-01T00:00:01Z")?;
    let terminal = h
        .process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    clock.advance(std::time::Duration::from_secs(2))?;
    let replay = h
        .process(&m, DeliveryRoute::Forward, worker_context())
        .await?;
    assert_eq!(terminal, replay);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(h.relay(&h.key(&m)).await?);
    assert_eq!(sink.0.lock().unwrap().len(), 2);
    // Changed input/negotiation creates a durable conflict refusal, never another invoke.
    let mut changed = m.clone();
    changed.bytes = b"changed".to_vec().into();
    let rejected = h
        .process(&changed, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&rejected.bytes)?["category"],
        "conflict"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // Unreadable/duplicate metadata has no fabricated operation/correlation/causation.
    let malformed = SerializedMessage::new("application/json", "{}").with_unreadable_metadata();
    let rejected = h
        .process(&malformed, DeliveryRoute::Ingress, worker_context())
        .await?;
    for key in [
        CORRELATION_ID_METADATA_KEY,
        CAUSATION_ID_METADATA_KEY,
        CAPABILITY_OPERATION_METADATA_KEY,
    ] {
        assert!(!rejected.headers.contains_key(key));
    }
    Ok(())
}

#[tokio::test]
async fn rt010_020_crash_after_evidence_recovers_only_finalization_and_fences_races() -> TestResult
{
    let store = Arc::new(Double::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(Producer::default());
    let clock = Arc::new(ManualClock::default());
    let h = durable(
        store.clone(),
        clock.clone(),
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    let m = message()?;
    store.fail_finish.store(true, Ordering::SeqCst);
    store.fail_seal.store(true, Ordering::SeqCst);
    assert!(
        h.process(&m, DeliveryRoute::Ingress, worker_context())
            .await
            .is_err()
    );
    let before = store.get(&h.key(&m));
    assert_eq!(before.stage(), DeliveryStage::Evidence);
    store.fail_finish.store(false, Ordering::SeqCst);
    store.fail_seal.store(false, Ordering::SeqCst);
    let restarted = durable(
        store.clone(),
        clock,
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    assert_eq!(restarted.relay_pending(8).await?, 1);
    let result = restarted
        .process(&m, DeliveryRoute::Forward, worker_context())
        .await?;
    assert_eq!(
        result.headers.get_text(MESSAGE_ID_METADATA_KEY)?,
        Some(before.result_id())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        store
            .change(&h.key(&m), before.revision(), DeliveryChange::Start)
            .await
            .is_err()
    );
    let input = message()?;
    let (a, b) = tokio::join!(
        h.process(&input, DeliveryRoute::Ingress, worker_context()),
        h.process(&input, DeliveryRoute::Ingress, worker_context())
    );
    assert!(a.is_ok() || b.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}
struct Transient {
    calls: Arc<AtomicUsize>,
    delay: u64,
    effect: PlenoraErrorRemoteEffect,
}
#[async_trait]
impl EvidenceComponent for Transient {
    async fn invoke(
        &self,
        _: WorkerContext,
        _: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> ComponentOutcome {
        self.calls.fetch_add(1, Ordering::SeqCst);
        budget.failure(
            PlenoraError::new(
                PlenoraErrorCategory::Internal,
                PlenoraErrorPhase::Read,
                self.effect,
                if self.effect == PlenoraErrorRemoteEffect::Unknown {
                    PlenoraErrorRetry::RequiresRecovery
                } else if self.delay == 0 {
                    PlenoraErrorRetry::Safe
                } else {
                    PlenoraErrorRetry::After {
                        delay_ms: self.delay,
                    }
                },
                "Transient fixture.",
            )
            .unwrap(),
        )
    }
}
#[tokio::test]
async fn rt010_safe_retry_attempt_time_and_delay_budgets_survive_restart() -> TestResult {
    for (effect, expected) in [
        (PlenoraErrorRemoteEffect::None, 3),
        (PlenoraErrorRemoteEffect::Unknown, 1),
    ] {
        let store = Arc::new(Double::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let h = durable(
            store.clone(),
            Arc::new(ManualClock::default()),
            "tenant",
            Transient {
                calls: calls.clone(),
                delay: 0,
                effect,
            },
            standard_plan(),
            1024,
            Arc::new(Producer::default()),
        )?;
        let m = message()?;
        let result = h
            .process(&m, DeliveryRoute::Ingress, worker_context())
            .await?;
        assert_eq!(calls.load(Ordering::SeqCst), expected);
        assert_eq!(
            h.process(&m, DeliveryRoute::Forward, worker_context())
                .await?,
            result
        );
    }
    let store = Arc::new(Double::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let clock = Arc::new(ManualClock::default());
    let h = durable(
        store.clone(),
        clock.clone(),
        "tenant",
        Transient {
            calls: calls.clone(),
            delay: 100,
            effect: PlenoraErrorRemoteEffect::None,
        },
        DeliveryPlan::new("results", 3, 250, 1024, 1024)?,
        1024,
        Arc::new(Producer::default()),
    )?;
    let m = message()?;
    assert!(
        h.process(&m, DeliveryRoute::Ingress, worker_context())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        h.process(&m, DeliveryRoute::Forward, worker_context())
            .await
            .is_err()
    );
    store.clock.advance(std::time::Duration::from_millis(300))?;
    let terminal = h
        .process(&m, DeliveryRoute::Forward, worker_context())
        .await?;
    assert_eq!(terminal.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
async fn rt024_component_dispatcher_sink_commit_cleanup_and_exact_journal_bound() -> TestResult {
    assert!(DeliveryPlan::new("results", 16, 1000, 8 * 1024 * 1024, 1024).is_err());
    for phase in [
        PlenoraErrorPhase::Read,
        PlenoraErrorPhase::Commit,
        PlenoraErrorPhase::Cleanup,
    ] {
        for effect in [
            PlenoraErrorRemoteEffect::None,
            PlenoraErrorRemoteEffect::Committed,
            PlenoraErrorRemoteEffect::Partial,
            PlenoraErrorRemoteEffect::Unknown,
        ] {
            for (output, dispatcher, sink, fail) in [
                (8, 1024, 1024, true),
                (2048, 1024, 4096, false),
                (256, 1024, 128, false),
                (8192, 1024, 1024, false),
            ] {
                let store = Arc::new(Double::default());
                let calls = Arc::new(AtomicUsize::new(0));
                let h = durable(
                    store.clone(),
                    Arc::new(ManualClock::default()),
                    "tenant",
                    component(calls.clone(), output, effect, phase, fail),
                    DeliveryPlan::new("results", 1, 0, 4096, sink)?,
                    dispatcher,
                    Arc::new(Producer::default()),
                )?;
                let m = message()?;
                let terminal = h
                    .process(&m, DeliveryRoute::Ingress, worker_context())
                    .await?;
                assert_eq!(terminal.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
                let error: Value = serde_json::from_slice(&terminal.bytes)?;
                assert_eq!(error["phase"], serde_json::to_value(phase)?);
                assert_eq!(error["remote_effect"], serde_json::to_value(effect)?);
                let j = store.get(&h.key(&m));
                assert_eq!(j.encoded_size()?, j.to_json()?.len());
                assert!(j.encoded_size()? <= JOURNAL_LIMIT);
                assert_eq!(calls.load(Ordering::SeqCst), 1);
            }
        }
    }
    Ok(())
}
#[derive(Default)]
struct Uncertain {
    messages: Mutex<Vec<SerializedMessage>>,
    unknown: AtomicBool,
}
#[async_trait]
impl MessageProducer for Uncertain {
    type Error = SettlementError;
    async fn publish(&self, m: SerializedMessage) -> Result<PublishOutcome, SettlementError> {
        self.messages.lock().unwrap().push(m);
        Ok(if self.unknown.load(Ordering::SeqCst) {
            PublishOutcome::OutcomeUnknown
        } else {
            PublishOutcome::Confirmed
        })
    }
}
#[tokio::test]
async fn rt026_027_immutable_result_route_uncertain_publish_and_tombstone() -> TestResult {
    let store = Arc::new(Double::default());
    let sink = Arc::new(Uncertain::default());
    sink.unknown.store(true, Ordering::SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    let h = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    let m = message()?;
    h.process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert!(h.relay(&h.key(&m)).await.is_err());
    assert!(!store.get(&h.key(&m)).delivered());
    sink.unknown.store(false, Ordering::SeqCst);
    assert!(h.relay(&h.key(&m)).await?);
    {
        let messages = sink.messages.lock().unwrap();
        assert_eq!(messages[0], messages[1]);
    }
    let other = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "tenant:sub",
        good(calls.clone()),
        standard_plan(),
        1024,
        Arc::new(Producer::default()),
    )?;
    assert!(h.relay(&other.key(&m)).await.is_err());
    let rejected = other
        .process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&rejected.bytes)?["category"],
        "conflict"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    store.clock.advance(std::time::Duration::from_hours(168))?;
    assert!(
        h.process(&m, DeliveryRoute::Forward, worker_context())
            .await
            .is_err()
    );
    assert_eq!(h.relay_pending(8).await?, 0);
    Ok(())
}
struct AckProbe {
    acks: Arc<AtomicUsize>,
    nacks: Arc<AtomicUsize>,
    lose: bool,
}
#[async_trait]
impl DeliveryAcknowledger for AckProbe {
    async fn ack(self: Box<Self>) -> Result<(), AckError> {
        self.acks.fetch_add(1, Ordering::SeqCst);
        if self.lose {
            Err(AckError::new(
                AckOperation::Ack,
                "fixture acknowledgement lost",
            ))
        } else {
            Ok(())
        }
    }
    async fn nack(self: Box<Self>, reason: NackReason) -> Result<(), AckError> {
        assert!(matches!(reason, NackReason::RetryAfter(_)));
        self.nacks.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn apalis_config() -> plenora_runtime_apalis::ApalisAdapterConfig {
    plenora_runtime_apalis::ApalisAdapterConfig::new(
        "durable",
        plenora_runtime_worker::WorkerConfig::new(
            plenora_runtime_worker::WorkerConcurrency::new(2).unwrap(),
            std::time::Duration::from_secs(2),
        ),
    )
    .unwrap()
}
#[tokio::test]
async fn rt026_027_apalis_ack_loss_dlq_redrive_and_no_store_confirmation() -> TestResult {
    use plenora_runtime_apalis::BrokerDeliveryService;
    let store = Arc::new(Double::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(Producer::default());
    let h = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    let worker = DurableWorker::new(h.clone(), DeliveryRoute::Ingress);
    let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "1", "test"));
    let service = BrokerDeliveryService::new(
        worker.clone(),
        worker,
        SettlementRetry,
        apalis_config(),
        runtime.shutdown_signal(),
    )?;
    let acks = Arc::new(AtomicUsize::new(0));
    let nacks = Arc::new(AtomicUsize::new(0));
    let m = message()?;
    let delivery = |message, lose| {
        Delivery::new(
            message,
            1,
            MessageMetadata::new(),
            AckProbe {
                acks: acks.clone(),
                nacks: nacks.clone(),
                lose,
            },
        )
    };
    assert!(service.execute(delivery(m.clone(), true)).await.is_err());
    service.execute(delivery(m.clone(), false)).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let forwarder = NegotiatedForwarder::new(h.clone(), standard_plan().route())?;
    let letter = DeadLetter {
        message: m.clone(),
        reason: "fixture".into(),
        attempts: 1,
        failed_at: std::time::UNIX_EPOCH.into(),
    };
    assert_eq!(
        DeadLetterSink::publish_dead_letter(&forwarder, letter).await?,
        PublishOutcome::Confirmed
    );
    let redrive = sink.0.lock().unwrap().last().cloned().ok_or("DLQ")?;
    assert_eq!(
        redrive.headers.get_text(REQUIRES)?,
        m.headers.get_text(REQUIRES)?
    );
    let worker = DurableWorker::new(h.clone(), DeliveryRoute::Forward);
    let redrive_service = BrokerDeliveryService::new(
        worker.clone(),
        worker,
        SettlementRetry,
        apalis_config(),
        runtime.shutdown_signal(),
    )?;
    redrive_service.execute(delivery(redrive, false)).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    store.unavailable.store(true, Ordering::SeqCst);
    service.execute(delivery(message()?, false)).await?;
    assert_eq!(nacks.load(Ordering::SeqCst), 1);
    assert_eq!(acks.load(Ordering::SeqCst), 3);
    assert!(MessageProducer::publish(&forwarder, m).await.is_err());
    Ok(())
}

#[tokio::test]
#[ignore = "requires the disposable PostgreSQL CI service"]
async fn postgres_real_product_settlement() -> TestResult {
    use plenora_runtime_chain_store::{PostgresStore, Retention};
    let store = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
    store.migrate(Retention::reduced(60, 120)?).await?;
    let scope = MessageId::random().to_string();
    let calls = Arc::new(AtomicUsize::new(0));
    let sink = Arc::new(Uncertain::default());
    let h = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        &scope,
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    let m = message()?;
    let first = h
        .process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    let restarted = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
    let h = durable(
        restarted.clone(),
        Arc::new(ManualClock::default()),
        &scope,
        good(calls.clone()),
        standard_plan(),
        1024,
        sink.clone(),
    )?;
    assert_eq!(
        h.process(&m, DeliveryRoute::Forward, worker_context())
            .await?,
        first
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    sink.unknown.store(true, Ordering::SeqCst);
    assert!(h.relay(&h.key(&m)).await.is_err());
    sink.unknown.store(false, Ordering::SeqCst);
    assert_eq!(h.relay_pending(8).await?, 1);
    assert!(restarted.load(&h.key(&m)).await?.0.delivered());
    let raw = SerializedMessage::new("application/json", "bad").with_unreadable_metadata();
    let rejection = h
        .process(&raw, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert_eq!(rejection.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
    assert!(h.relay(&h.key(&raw)).await?);
    // Independent hosts race for the same invocation authority, not just acceptance.
    let raced = message()?;
    let (a, b) = tokio::join!(
        h.process(&raced, DeliveryRoute::Ingress, worker_context()),
        h.process(&raced, DeliveryRoute::Ingress, worker_context())
    );
    assert!(a.is_ok() || b.is_ok());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    Ok(())
}

struct ObservedWorker<S> {
    worker: DurableWorker<S>,
    completed: Arc<AtomicUsize>,
}
#[async_trait]
impl<S: DurableExecutionStore> plenora_runtime_worker::WorkerHandler<RawDelivery>
    for ObservedWorker<S>
{
    type Error = SettlementError;
    async fn handle(
        &self,
        context: WorkerContext,
        message: RawDelivery,
    ) -> Result<(), SettlementError> {
        assert!(
            context.attempt >= 2,
            "broker must redeliver the unacknowledged request"
        );
        self.worker.handle(context, message).await?;
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[allow(
    clippy::too_many_lines,
    reason = "One real broker lifecycle keeps setup, ACK loss, Apalis, relay and DLQ evidence together"
)]
async fn real_broker<S: DurableExecutionStore>(store: Arc<S>) -> TestResult {
    use plenora_runtime_apalis::BrokerWorkerRunner;
    use plenora_runtime_nats::{
        InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig, NatsConfig,
        NatsConnection, NatsTlsConfig, TlsMode,
    };
    use std::time::Duration;
    let url = std::env::var("PLENORA_NATS_URL")?;
    let scope = MessageId::random().to_string();
    let root: Arc<str> = format!("plenora.product.{scope}").into();
    let work: Arc<str> = format!("{root}.work").into();
    let results: Arc<str> = format!("{root}.results").into();
    let dlq: Arc<str> = format!("{root}.dlq").into();
    let connection = NatsConnection::connect(
        NatsConfig {
            servers: vec![url.into()],
            tls: NatsTlsConfig {
                mode: TlsMode::AllowPlaintext,
                ..NatsTlsConfig::default()
            },
            ..NatsConfig::default()
        },
        plenora_runtime_core::HealthRegistry::new(),
    )
    .await?;
    let config = |subject: Arc<str>, name: &str| JetStreamConsumerConfig {
        stream: format!("PRODUCT_{}", scope.replace('-', "_")).into(),
        durable_name: format!("{name}_{}", scope.replace('-', "_")).into(),
        filter_subject: subject,
        ack_wait: Duration::from_secs(1),
        heartbeat: None,
        max_deliver: Some(10),
        max_ack_pending: Some(8),
        max_payload_bytes: 64 * 1024,
        shutdown_nak_delay: Duration::from_millis(25),
        infrastructure: InfrastructureMode::CreateIfMissing {
            stream_subjects: vec![work.clone(), results.clone(), dlq.clone()],
        },
    };
    let mut consumer = connection.consumer(config(work.clone(), "work")).await?;
    let mut output = connection
        .consumer(config(results.clone(), "results"))
        .await?;
    let mut dead = connection.consumer(config(dlq.clone(), "dlq")).await?;
    let producer = |subject: Arc<str>| {
        connection.producer(JetStreamProducerConfig {
            subject,
            max_payload_bytes: 64 * 1024,
            message_id_metadata_key: None,
        })
    };
    let input = producer(work)?;
    let result_producer = Arc::new(producer(results)?);
    let dlq_producer = Arc::new(producer(dlq)?);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = CapabilityRegistryBuilder::default();
    registry.register_evidenced(discovery(2)?, good(calls.clone()))?;
    let plan = DeliveryPlan::new(&format!("{root}.results"), 3, 60_000, 4096, 1024)?;
    let routes = RouteTable::new(vec![
        (plan.route().into(), discovery(2)?, result_producer.clone()),
        (format!("{root}.dlq"), discovery(2)?, dlq_producer),
    ])?;
    let boundary = RuntimeBoundary::new(
        CapabilityDispatcher::new(registry.build(), CapabilityDispatcherConfig::new(1024)?)?,
        Arc::new(ManualClock::default()),
        Arc::new(Policy),
        result_producer,
    );
    let h = Arc::new(DurableRuntime::new(boundary, store, &scope, plan, routes)?);
    let m = message()?;
    MessageProducer::publish(&input, m.clone()).await?;
    let received = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await??
        .ok_or("delivery")?;
    assert_eq!(received.message, m);
    let terminal = h
        .process(&received.message, DeliveryRoute::Ingress, worker_context())
        .await?;
    drop(received); // actual broker redelivery after losing the inbound ACK
    let runtime = RuntimeHandle::new(ServiceMetadata::new("product", "1", scope.clone()));
    let worker = DurableWorker::new(h.clone(), DeliveryRoute::Forward);
    let completed = Arc::new(AtomicUsize::new(0));
    let observed = ObservedWorker {
        worker: worker.clone(),
        completed: completed.clone(),
    };
    let runner = BrokerWorkerRunner::new(
        consumer,
        worker,
        observed,
        SettlementRetry,
        apalis_config(),
        runtime.shutdown_signal(),
    )?;
    let task = tokio::spawn(runner.run());
    assert_eq!(h.relay_pending(8).await?, 1);
    let delivered = tokio::time::timeout(Duration::from_secs(10), output.receive())
        .await??
        .ok_or("result")?;
    assert_eq!(delivered.message, terminal);
    delivered.ack().await?;
    tokio::time::timeout(Duration::from_secs(10), async {
        while completed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let _ = runtime.request_shutdown();
    tokio::time::timeout(Duration::from_secs(10), task).await???;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let forwarder = NegotiatedForwarder::new(h.clone(), &format!("{root}.dlq"))?;
    DeadLetterSink::publish_dead_letter(
        &forwarder,
        DeadLetter {
            message: m.clone(),
            reason: "fixture".into(),
            attempts: 1,
            failed_at: std::time::UNIX_EPOCH.into(),
        },
    )
    .await?;
    let redrive = tokio::time::timeout(Duration::from_secs(10), dead.receive())
        .await??
        .ok_or("DLQ")?;
    assert_eq!(
        redrive.message.headers.get_text(REQUIRES)?,
        Some("plenora-runtime-binding-v2")
    );
    assert_eq!(
        h.process(&redrive.message, DeliveryRoute::Forward, worker_context())
            .await?,
        terminal
    );
    redrive.ack().await?;
    let mut lost = m;
    let _ = lost.headers.remove(REQUIRES);
    assert!(MessageProducer::publish(&forwarder, lost).await.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    Ok(())
}
#[tokio::test]
#[ignore = "requires disposable NATS JetStream; PostgreSQL is independently exercised in the combined job"]
async fn nats_real_product_headers_redelivery_and_resend() -> TestResult {
    real_broker(Arc::new(Double::default())).await
}
#[tokio::test]
#[ignore = "requires BOTH disposable PostgreSQL and NATS JetStream from postgres-integration"]
async fn nats_postgres_real_product_end_to_end() -> TestResult {
    let store = Arc::new(plenora_runtime_chain_store::PostgresStore::new(
        &std::env::var("PLENORA_POSTGRES_URL")?,
    )?);
    store
        .migrate(plenora_runtime_chain_store::Retention::reduced(60, 120)?)
        .await?;
    real_broker(store).await
}

#[tokio::test]
async fn running_without_confirmed_evidence_never_repeats_and_forged_retry_is_rejected()
-> TestResult {
    let store = Arc::new(Double::default());
    store.before_seal.store(true, Ordering::SeqCst);
    let calls = Arc::new(AtomicUsize::new(0));
    let h = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        Arc::new(Producer::default()),
    )?;
    let m = message()?;
    assert!(
        h.process(&m, DeliveryRoute::Ingress, worker_context())
            .await
            .is_err()
    );
    store.before_seal.store(false, Ordering::SeqCst);
    assert!(
        h.process(&m, DeliveryRoute::Forward, worker_context())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(store.get(&h.key(&m)).stage(), DeliveryStage::Running);
    let mut corrupted = serde_json::to_value(store.get(&h.key(&m)))?;
    corrupted["stage"] = serde_json::to_value(DeliveryStage::Ready)?;
    assert!(DeliveryJournal::from_json(&serde_json::to_vec(&corrupted)?).is_err());
    let mut policy = store.get(&h.key(&m));
    assert!(policy.maintain(0, 604_800_001, 7_776_000_000).is_err());
    assert!(policy.maintain(0, 604_800_000, 7_776_000_001).is_err());
    let other = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "other",
        Transient {
            calls: Arc::new(AtomicUsize::new(0)),
            delay: 0,
            effect: PlenoraErrorRemoteEffect::Unknown,
        },
        standard_plan(),
        1024,
        Arc::new(Producer::default()),
    )?;
    let m = message()?;
    other
        .process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    let mut forged = serde_json::to_value(store.get(&other.key(&m)))?;
    forged["history"][0]["retry_ms"] = json!(0);
    assert!(DeliveryJournal::from_json(&serde_json::to_vec(&forged)?).is_err());
    // Report pruning is independent of delivery expiry, while identity tombstones remain.
    store
        .clock
        .advance(std::time::Duration::from_hours(24 * 91))?;
    let (j, _) = store.load(&other.key(&m)).await?;
    assert!(j.evidence().is_none());
    assert!(j.expired(store.now()));
    assert!(
        other
            .process(&m, DeliveryRoute::Ingress, worker_context())
            .await
            .is_err()
    );
    Ok(())
}
struct FullHeaders;
#[async_trait]
impl EvidenceComponent for FullHeaders {
    async fn invoke(
        &self,
        _: WorkerContext,
        _: CapabilityRequest,
        budget: OutcomeBudget,
    ) -> ComponentOutcome {
        let mut output = SerializedMessage::new("application/json", vec![255; budget.bytes()]);
        // 59 application headers + result routing and identity exhaust the portable map.
        for i in 0..59 {
            output
                .headers
                .insert_text(format!("fixture.header{i}"), "value")
                .unwrap();
        }
        budget
            .success(
                CapabilityResponse::new(ContractId::new("plenora-test-output-v1").unwrap(), output),
                PlenoraErrorPhase::Cleanup,
                PlenoraErrorRemoteEffect::Committed,
            )
            .unwrap()
    }
}
#[tokio::test]
async fn header_quota_uses_reserved_terminal_lane_and_json_oracle_counts_expansion() -> TestResult {
    let store = Arc::new(Double::default());
    let h = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "tenant",
        FullHeaders,
        DeliveryPlan::new("results", 1, 0, 4096, 8192)?,
        8192,
        Arc::new(Producer::default()),
    )?;
    let m = message()?;
    let result = h
        .process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert_eq!(result.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
    let error: Value = serde_json::from_slice(&result.bytes)?;
    assert_eq!(error["remote_effect"], "committed");
    let j = store.get(&h.key(&m));
    let report = j.evidence().ok_or("report")?.report();
    assert_eq!(
        report["output"]["bytes"].as_array().ok_or("output")?.len(),
        4096
    );
    assert_eq!(j.encoded_size()?, serde_json::to_vec(&j)?.len());
    assert!(j.encoded_size()? > 4096 * 4);
    Ok(())
}

#[tokio::test]
async fn host_idempotency_key_is_explicitly_refused_and_replayed_without_domain() -> TestResult {
    let store = Arc::new(Double::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let h = durable(
        store.clone(),
        Arc::new(ManualClock::default()),
        "tenant",
        good(calls.clone()),
        standard_plan(),
        1024,
        Arc::new(Producer::default()),
    )?;
    let mut m = message()?;
    m.headers
        .insert_text(IDEMPOTENCY_KEY_METADATA_KEY, "host-scope-not-yet-bound")?;
    let terminal = h
        .process(&m, DeliveryRoute::Ingress, worker_context())
        .await?;
    assert_eq!(
        serde_json::from_slice::<Value>(&terminal.bytes)?["category"],
        "unsupported"
    );
    assert_eq!(
        h.process(&m, DeliveryRoute::Forward, worker_context())
            .await?,
        terminal
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(store.get(&h.key(&m)).stage(), DeliveryStage::Finished);
    Ok(())
}
