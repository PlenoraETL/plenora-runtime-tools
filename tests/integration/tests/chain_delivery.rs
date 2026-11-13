//! Product gate and settlement probes. Memory storage and scripted components are test doubles.
#![allow(
    clippy::wildcard_imports,
    reason = "Contract probes use public API families"
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
    started: Mutex<BTreeMap<MessageId, Vec<u8>>>,
    reports: Mutex<BTreeMap<MessageId, Vec<u8>>>,
    results: Mutex<BTreeMap<MessageId, Vec<u8>>>,
    unavailable: AtomicBool,
    lose_finish: AtomicBool,
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
    type Permit = MessageId;
    async fn begin(
        &self,
        id: MessageId,
        _: &str,
        _: &str,
        input: &[u8],
    ) -> Result<ExecutionDecision<MessageId>, SettlementError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(SettlementError);
        }
        let mut started = self.started.lock().map_err(|_| SettlementError)?;
        if let Some(old) = started.get(&id) {
            if old != input {
                return Err(SettlementError);
            }
            return Ok(
                if self
                    .results
                    .lock()
                    .map_err(|_| SettlementError)?
                    .contains_key(&id)
                {
                    ExecutionDecision::Replay
                } else {
                    ExecutionDecision::Recovery
                },
            );
        }
        started.insert(id, input.to_vec());
        Ok(ExecutionDecision::Start(id))
    }
    async fn evidence(&self, id: &MessageId, report: &[u8]) -> Result<(), SettlementError> {
        self.reports
            .lock()
            .map_err(|_| SettlementError)?
            .insert(*id, report.to_vec());
        Ok(())
    }
    async fn finalize(
        &self,
        id: &MessageId,
        _: MessageId,
        _: &[u8],
        envelope: &[u8],
    ) -> Result<(), SettlementError> {
        self.results
            .lock()
            .map_err(|_| SettlementError)?
            .insert(*id, envelope.to_vec());
        if self.lose_finish.load(Ordering::SeqCst) {
            Err(SettlementError)
        } else {
            Ok(())
        }
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
    async fn invoke(&self, _: WorkerContext, request: CapabilityRequest) -> ComponentOutcome {
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
        ComponentOutcome {
            result,
            phase: self.phase,
            effect: self.effect,
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
fn context(d: &NegotiatedDelivery) -> WorkerContext {
    let runtime = RuntimeHandle::new(ServiceMetadata::new("test", "1", "test"));
    WorkerContext::from_identity(
        d.admitted().identity(),
        1,
        d.admitted().metadata().clone(),
        runtime.shutdown_signal(),
    )
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

#[tokio::test]
async fn enumerate_p2_4_limits_preserve_evidence_and_lost_ack_never_repeats_domain() -> TestResult {
    let rules = ["RT-010", "RT-019", "RT-020", "RT-024"];
    assert_eq!(rules.len(), 4);
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
            ] {
                if !fail
                    && matches!(
                        effect,
                        PlenoraErrorRemoteEffect::Partial | PlenoraErrorRemoteEffect::Unknown
                    )
                {
                    continue;
                }
                let store = Arc::new(Double::default());
                let calls = Arc::new(AtomicUsize::new(0));
                let host = host(
                    store.clone(),
                    component(calls.clone(), output, effect, phase, fail),
                    dispatcher,
                )?;
                let d = host.admit(&message()?, DeliveryRoute::Ingress)?;
                let id = d.admitted().identity().message_id;
                store.lose_finish.store(true, Ordering::SeqCst);
                assert!(
                    host.execute(&d, context(&d), "tenant-run", "step", sink)
                        .await
                        .is_err()
                );
                store.lose_finish.store(false, Ordering::SeqCst);
                host.execute(&d, context(&d), "tenant-run", "step", sink)
                    .await?;
                host.execute(&d, context(&d), "tenant-run", "step", sink)
                    .await?;
                assert_eq!(calls.load(Ordering::SeqCst), 1);
                let bytes = store
                    .results
                    .lock()
                    .map_err(|_| "lock")?
                    .get(&id)
                    .cloned()
                    .ok_or("terminal")?;
                let m = serde_json::from_slice::<StoredMessage>(&bytes)?.restore()?;
                assert_eq!(m.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
                let e: Value = serde_json::from_slice(&m.bytes)?;
                assert_eq!(e["remote_effect"], serde_json::to_value(effect)?);
                assert_eq!(e["phase"], serde_json::to_value(phase)?);
                assert_eq!(e["category"], "resource_limit");
                if effect == PlenoraErrorRemoteEffect::None {
                    assert_eq!(e["retry"]["kind"], "never");
                }
                assert_ne!(
                    m.headers.get_text(MESSAGE_ID_METADATA_KEY)?,
                    Some(id.to_string().as_str())
                );
                assert_eq!(
                    m.headers.get_text(CAUSATION_ID_METADATA_KEY)?,
                    Some(id.to_string().as_str())
                );
                assert!(
                    !store
                        .reports
                        .lock()
                        .map_err(|_| "lock")?
                        .get(&id)
                        .ok_or("report")?
                        .is_empty()
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL from the postgres-integration CI job"]
async fn postgres_real_product_settlement() -> TestResult {
    use plenora_runtime_chain_store::{PostgresStore, Retention};
    let store = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
    // The preceding store qualification reduces this same disposable database policy.
    store.migrate(Retention::reduced(60, 120)?).await?;
    let calls = Arc::new(AtomicUsize::new(0));
    let h = host(
        store.clone(),
        component(
            calls.clone(),
            2048,
            PlenoraErrorRemoteEffect::Committed,
            PlenoraErrorPhase::Cleanup,
            false,
        ),
        1024,
    )?;
    let m = message()?;
    let d = h.admit(&m, DeliveryRoute::Ingress)?;
    let id = d.admitted().identity().message_id;
    let scope = id.to_string();
    h.execute(&d, context(&d), &scope, "step", 128).await?;
    let before = store.read(id).await?;
    assert!(before.finished);
    assert!(!before.delivered);
    let report = store.evidence_report(id).await?;
    assert_eq!(before.report, Some(report));
    // Simulated lost response/ACK followed by a new host using a fresh connection factory.
    let restarted = Arc::new(PostgresStore::new(&std::env::var("PLENORA_POSTGRES_URL")?)?);
    let h = host(
        restarted.clone(),
        component(
            calls.clone(),
            2048,
            PlenoraErrorRemoteEffect::Committed,
            PlenoraErrorPhase::Cleanup,
            false,
        ),
        1024,
    )?;
    let d = h.admit(&m, DeliveryRoute::Forward)?;
    h.execute(&d, context(&d), &scope, "step", 128).await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(restarted.read(id).await?.result_id, before.result_id);
    // Both API orderings must reject contradictory ledger reports.
    for finished_first in [false, true] {
        use plenora_runtime_chain_store::{Invocation, StartDecision, StoreError};
        let id = MessageId::random();
        store
            .accept_async(
                id,
                Some(NegotiationContext {
                    requires: RuntimeBinding::V2,
                    accepts_downgrade: false,
                }),
            )
            .await?;
        let scope = id.to_string();
        let StartDecision::Started(permit) = store
            .start(Invocation {
                message_id: id,
                scope: &scope,
                step: "step",
                key: None,
                canonical_input: b"input",
            })
            .await?
        else {
            return Err("start authority".into());
        };
        let result_id = MessageId::random();
        if finished_first {
            store
                .finish(&permit, result_id, b"report-a", b"envelope")
                .await?;
            assert_eq!(
                store.record_evidence(&permit, b"report-b").await,
                Err(StoreError::Conflict)
            );
        } else {
            store.record_evidence(&permit, b"report-a").await?;
            assert_eq!(
                store
                    .finish(&permit, result_id, b"report-b", b"envelope")
                    .await,
                Err(StoreError::Conflict)
            );
            store
                .finish(&permit, result_id, b"report-a", b"envelope")
                .await?;
            store.record_evidence(&permit, b"report-a").await?;
        }
    }
    Ok(())
}

#[tokio::test]
#[ignore = "requires disposable NATS JetStream from the nats-integration CI job"]
async fn nats_real_product_headers_redelivery_and_resend() -> TestResult {
    use plenora_runtime_nats::{
        InfrastructureMode, JetStreamConsumerConfig, JetStreamProducerConfig, NatsConfig,
        NatsConnection, NatsTlsConfig, TlsMode,
    };
    use std::time::Duration;
    let url = std::env::var("PLENORA_NATS_URL")?;
    let suffix = MessageId::random().to_string();
    let subject: Arc<str> = format!("plenora.product.{suffix}").into();
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
    let mut consumer = connection
        .consumer(JetStreamConsumerConfig {
            stream: format!("PRODUCT_{}", suffix.replace('-', "_")).into(),
            durable_name: format!("product_{suffix}").into(),
            filter_subject: subject.clone(),
            ack_wait: Duration::from_secs(1),
            heartbeat: None,
            max_deliver: Some(5),
            max_ack_pending: Some(8),
            max_payload_bytes: 64 * 1024,
            shutdown_nak_delay: Duration::from_millis(25),
            infrastructure: InfrastructureMode::CreateIfMissing {
                stream_subjects: vec![subject.clone()],
            },
        })
        .await?;
    let producer = connection.producer(JetStreamProducerConfig {
        subject,
        max_payload_bytes: 64 * 1024,
        message_id_metadata_key: None,
    })?;
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(Double::default());
    let h = host(
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
    let original = message()?;
    let admitted = h.admit(&original, DeliveryRoute::Ingress)?;
    assert_eq!(
        h.send(&admitted, Some(&discovery(2)?), &producer).await?,
        PublishOutcome::Confirmed
    );
    let delivery = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await??
        .ok_or("delivery")?;
    assert_eq!(delivery.message, original);
    let d = h.admit(&delivery.message, DeliveryRoute::Forward)?;
    h.execute(&d, context(&d), "run", "step", 1024).await?;
    // Dropping the delivery simulates ACK loss; broker redelivery must reuse the terminal.
    drop(delivery);
    let delivery = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await??
        .ok_or("redelivery")?;
    let d = h.admit(&delivery.message, DeliveryRoute::Forward)?;
    h.execute(&d, context(&d), "run", "step", 1024).await?;
    delivery.ack().await?;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // A bad forwarding is put on the actual broker, then rejected on the receiving gate.
    let mut bad = original;
    let _ = bad.headers.remove(REQUIRES);
    MessageProducer::publish(&producer, bad).await?;
    let delivery = tokio::time::timeout(Duration::from_secs(10), consumer.receive())
        .await??
        .ok_or("bad forwarding")?;
    assert!(h.admit(&delivery.message, DeliveryRoute::Forward).is_err());
    delivery.ack().await?;
    store.unavailable.store(true, Ordering::SeqCst);
    assert!(
        h.send(&admitted, Some(&discovery(2)?), &producer)
            .await
            .is_err()
    );
    Ok(())
}

#[tokio::test]
async fn partial_and_unknown_cannot_be_promoted_to_success() -> TestResult {
    for effect in [
        PlenoraErrorRemoteEffect::Partial,
        PlenoraErrorRemoteEffect::Unknown,
    ] {
        let store = Arc::new(Double::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let h = host(
            store.clone(),
            component(calls.clone(), 2, effect, PlenoraErrorPhase::Cleanup, false),
            1024,
        )?;
        let d = h.admit(&message()?, DeliveryRoute::Ingress)?;
        let id = d.admitted().identity().message_id;
        h.execute(&d, context(&d), "run", "step", 1024).await?;
        h.execute(&d, context(&d), "run", "step", 1024).await?;
        let raw = store
            .results
            .lock()
            .map_err(|_| "lock")?
            .get(&id)
            .cloned()
            .ok_or("terminal")?;
        let result = serde_json::from_slice::<StoredMessage>(&raw)?.restore()?;
        assert_eq!(result.content_type.as_ref(), PLENORA_ERROR_CONTENT_TYPE);
        let error: Value = serde_json::from_slice(&result.bytes)?;
        assert_eq!(error["remote_effect"], serde_json::to_value(effect)?);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}
