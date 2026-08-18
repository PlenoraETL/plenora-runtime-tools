# plenora-runtime-tools — Agent Implementation Specification

**Documento:** Specifica tecnica implementativa per agenti di sviluppo  
**Versione:** 0.1  
**Data:** 15 agosto 2026  
**Stato:** Draft implementativo — skeleton e prima foundation runtime  
**Repository target proposto:** `PlenoraETL/plenora-runtime-tools`  
**Linguaggio:** Rust  
**Consumer principale di validazione:** PFM — Plenora Facility Management  
**Classificazione:** Uso interno  

---

## 0. Istruzioni per gli agenti

Questo documento deve essere trattato come specifica esecutiva.

Gli agenti devono:

1. rispettare rigorosamente i confini architetturali;
2. non introdurre business model PFM nel core;
3. non accoppiare il core a NATS, Apalis, PostgreSQL o altri provider concreti;
4. mantenere ogni crate compilabile e testabile in isolamento;
5. privilegiare trait, adapter e dependency inversion;
6. non introdurre nuove dipendenze infrastrutturali senza motivazione documentata;
7. aggiungere test per ogni requisito implementato;
8. evitare `unwrap`, `expect`, `panic!` nei path runtime di produzione;
9. preservare error category, phase, remote effect e retry disposition quando disponibili;
10. produrre codice leggibile, documentato e compatibile con gli standard già usati nelle altre librerie Plenora.

Quando una scelta non è definita da questa specifica, l'agente deve:

```text
a. non inventare una decisione irreversibile;
b. creare un ADR/TODO esplicito;
c. implementare dietro un trait quando possibile;
d. preferire la soluzione meno accoppiante.
```

---

# 1. Obiettivo

Costruire `plenora-runtime-tools`, una foundation Rust riusabile per l'esecuzione di microservizi e worker asincroni.

La libreria deve fornire una superficie comune per:

- lifecycle del processo;
- graceful shutdown;
- task supervision;
- worker execution;
- message producer/consumer abstraction;
- broker integration;
- retry/backoff;
- ack/nack;
- dead-letter handling;
- replay hooks;
- health/readiness;
- runtime context;
- correlation e causation;
- trace propagation;
- outbox relay;
- inbox/dedup abstractions;
- idempotency abstractions;
- observability hooks;
- testkit;
- integrazione opzionale con le altre foundation Plenora.

La libreria non deve implementare business logic.

---

# 2. Posizionamento nell'ecosistema Plenora

Foundation già esistenti:

```text
plenora-IO-tools
    file/formati ↔ Arrow

plenora-data-tools
    Arrow ↔ Arrow

plenora-database-tools
    database ↔ Arrow
```

Nuova foundation:

```text
plenora-runtime-tools
    service/process ↔ HTTP / broker / worker lifecycle
```

Composizione tipica:

```text
                   microservizio
                        │
          ┌─────────────┼──────────────┐
          │             │              │
          ▼             ▼              ▼
 runtime-tools     database-tools   data-tools / IO-tools
          │             │              │
          ▼             ▼              ▼
      broker          database       Arrow / file
```

## 2.1 Regola fondamentale

`plenora-runtime-tools` **non è un orchestratore delle altre librerie**.

Il microservizio compone le foundation di cui ha bisogno.

Dipendenza diretta ammessa dal runtime:

```text
runtime adapter → database-tools
```

solo per moduli opzionali come outbox/inbox persistence adapter.

Dipendenze dirette da evitare:

```text
runtime-core → data-tools
runtime-core → IO-tools
runtime-core → database-tools
```

---

# 3. Decisioni architetturali iniziali

## 3.1 Runtime asincrono

Il runtime asincrono di riferimento è Tokio.

La libreria non deve reinventare scheduler, async executor, timer, socket I/O o task primitive.

## 3.2 Middleware

Tower è la base preferita per middleware e composizione dei servizi.

Utilizzare Tower per capability come timeout, retry, rate limit, tracing, load shedding e middleware custom.

## 3.3 Worker engine

Apalis è il **worker engine candidato iniziale**.

> Apalis deve restare un implementation detail dietro API Plenora.

Nessun microservizio consumer deve essere obbligato a importare tipi Apalis.

## 3.4 Broker iniziale

NATS + JetStream è il **broker adapter candidato iniziale**.

> NATS deve restare un implementation detail dietro trait Plenora.

Il core deve poter supportare in futuro Kafka, RabbitMQ o altri broker senza modifica delle API business dei consumer.

## 3.5 HTTP

Axum è il candidato iniziale per l'adapter HTTP.

`runtime-core` non dipende da Axum.

## 3.6 Observability

Usare `tracing` e integrazione OpenTelemetry.

Il runtime non deve imporre un backend specifico.

---

# 4. Non-obiettivi

Questa libreria non deve diventare:

- ORM;
- database abstraction;
- ETL engine;
- data transformation engine;
- file parser;
- workflow di dominio;
- BPM engine;
- authorization engine;
- API gateway;
- schema registry;
- event sourcing engine;
- distributed transaction coordinator;
- saga business engine;
- scheduler infrastrutturale Kubernetes;
- container orchestrator;
- replacement di NATS;
- replacement di Apalis;
- replacement di Tokio.

Non introdurre nel core concetti di dominio come `Building`, `Asset`, `WorkOrder`, `Tenant`, `Organization`, `CadastralUnit`.

---

# 5. Repository layout

Creare il workspace:

```text
plenora-runtime-tools/
│
├── Cargo.toml
├── Cargo.lock
├── rust-toolchain.toml
├── README.md
├── LICENSE
├── SECURITY.md
├── CONTRIBUTING.md
│
├── docs/
│   ├── architecture.md
│   ├── boundaries.md
│   ├── messaging.md
│   ├── worker.md
│   ├── outbox-inbox.md
│   ├── observability.md
│   ├── cancellation.md
│   ├── error-model.md
│   └── adr/
│
├── crates/
│   ├── plenora-runtime-core/
│   ├── plenora-runtime-messaging/
│   ├── plenora-runtime-worker/
│   ├── plenora-runtime-outbox/
│   ├── plenora-runtime-http/
│   ├── plenora-runtime-observability/
│   └── plenora-runtime-testkit/
│
├── adapters/
│   ├── plenora-runtime-apalis/
│   └── plenora-runtime-nats/
│
├── examples/
│   ├── worker-basic/
│   ├── worker-nats/
│   ├── http-service/
│   └── http-worker/
│
├── tests/
│   ├── architecture/
│   ├── integration/
│   └── fault/
│
└── .github/
    └── workflows/
```

---

# 6. Dependency direction

La dependency direction è normativa.

```text
                     runtime-core
                    ▲     ▲     ▲
                    │     │     │
           messaging     http   observability
               ▲
               │
              worker
               ▲
               │
              outbox
               ▲
               │
        adapters concreti
          /          \
      apalis         nats
```

Regole:

1. `runtime-core` non dipende da nessun adapter.
2. `runtime-core` non dipende da Apalis.
3. `runtime-core` non dipende da NATS.
4. `runtime-messaging` non dipende da NATS.
5. `runtime-worker` non espone tipi Apalis.
6. `runtime-outbox` non dipende da PostgreSQL.
7. l'eventuale adapter database dipenderà da `database-tools`, mai il contrario.
8. i microservizi dipendono dalle API Plenora, non dalle implementazioni sottostanti.

---

# 7. Crate `plenora-runtime-core`

## 7.1 Responsabilità

Fornire:

- runtime lifecycle;
- shutdown coordination;
- task supervision;
- clock abstraction minima;
- service identity metadata;
- generic runtime context;
- health/readiness model;
- error primitives comuni del runtime.

## 7.2 API candidate

```rust
pub struct RuntimeHandle {
    // private
}

#[derive(Clone)]
pub struct ShutdownSignal {
    // wrapper private
}

impl ShutdownSignal {
    pub fn is_cancelled(&self) -> bool;
    pub async fn cancelled(&self);
}

pub struct ServiceMetadata {
    pub service_name: Arc<str>,
    pub service_version: Arc<str>,
    pub instance_id: Arc<str>,
    pub environment: Option<Arc<str>>,
}

pub struct RuntimeContext {
    pub metadata: ServiceMetadata,
    pub shutdown: ShutdownSignal,
}
```

Non introdurre tenant o domini PFM.

---

# 8. Task supervision

Il runtime deve distinguere:

```rust
pub enum TaskCriticality {
    Critical,
    Required,
    Optional,
}

pub struct TaskSpec {
    pub name: Arc<str>,
    pub criticality: TaskCriticality,
}
```

Fallimento di task `Critical`:

```text
→ runtime unhealthy
→ shutdown coordinato
```

Fallimento di task `Optional`:

```text
→ log / metric
→ policy configurabile
```

Il runtime deve catturare panic dei task e convertirli in outcome governato.

---

# 9. Graceful shutdown

Sequenza target:

```text
shutdown signal
      ↓
stop accepting new HTTP requests
      ↓
stop polling new broker deliveries
      ↓
finish in-flight work entro grace period
      ↓
nack/requeue ciò che non può terminare
      ↓
flush telemetry
      ↓
terminate
```

Requisiti:

- shutdown idempotente;
- signal multipli equivalenti a uno;
- grace timeout configurabile;
- nessun nuovo lavoro dopo entering-drain;
- in-flight work osservabile;
- timeout di drain produce esito esplicito;
- niente process hang indefinito.

---

# 10. Health e readiness

```rust
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

pub enum ReadinessStatus {
    Ready,
    NotReady,
}

pub struct ComponentHealth {
    pub component: Arc<str>,
    pub status: HealthStatus,
    pub message: Option<Arc<str>>,
}
```

Il core deve poter aggregare broker, database, worker, outbox relay e external dependency.

Health e readiness non devono essere sinonimi.

---

# 11. Crate `plenora-runtime-messaging`

Responsabilità: definire il contratto broker-agnostic.

## 11.1 Identificatori

```rust
pub struct MessageId(Uuid);
pub struct CorrelationId(Uuid);
pub struct CausationId(Uuid);
```

## 11.2 MessageEnvelope

```rust
pub struct MessageEnvelope<T> {
    pub message_id: MessageId,
    pub message_type: Arc<str>,
    pub schema_version: Arc<str>,
    pub occurred_at: DateTime<Utc>,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<CausationId>,
    pub metadata: MessageMetadata,
    pub payload: T,
}
```

## 11.3 MessageMetadata

Deve essere genericamente namespaced.

```text
myapp.key
plenora.message.*
plenora.trace.*
```

Non hardcodare namespace consumer come `pfm.*`.

---

# 12. Serialization

Il core messaging non deve imporre il payload format al dominio.

```rust
pub struct SerializedMessage {
    pub content_type: Arc<str>,
    pub bytes: Bytes,
    pub headers: MessageMetadata,
}
```

Codec futuri possibili:

- JSON;
- MessagePack;
- Protobuf;
- CBOR.

```rust
pub trait MessageCodec<T> {
    type Error;

    fn encode(&self, value: &T) -> Result<SerializedMessage, Self::Error>;
    fn decode(&self, message: &SerializedMessage) -> Result<T, Self::Error>;
}
```

---

# 13. MessageProducer

```rust
#[async_trait]
pub trait MessageProducer: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn publish(
        &self,
        message: SerializedMessage,
    ) -> Result<PublishOutcome, Self::Error>;
}

pub enum PublishOutcome {
    Confirmed,
    OutcomeUnknown,
}
```

Non trasformare un outcome incerto in successo.

---

# 14. MessageConsumer e Delivery

```rust
#[async_trait]
pub trait MessageConsumer: Send {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn receive(
        &mut self,
    ) -> Result<Option<Delivery>, Self::Error>;
}
```

La delivery possiede envelope/message, delivery attempt, broker metadata e ack handle.

```rust
pub struct Delivery {
    pub message: SerializedMessage,
    pub attempt: u32,
    // private ack state
}
```

Ack API:

```rust
impl Delivery {
    pub async fn ack(self) -> Result<(), AckError>;
    pub async fn nack(self, reason: NackReason) -> Result<(), AckError>;
}
```

Dopo `ack` / `nack`, la delivery non deve essere riutilizzabile.

---

# 15. Ack/Nack semantics

```rust
pub enum NackReason {
    Retryable,
    Permanent,
    Shutdown,
    ConsumerRejected,
}
```

Il mapping verso NATS/Kafka/RabbitMQ appartiene agli adapter.

---

# 16. Retry model

```rust
pub enum RetryDecision {
    RetryAfter(Duration),
    DoNotRetry,
    DeadLetter,
}

pub trait RetryPolicy<E> {
    fn decide(
        &self,
        attempt: u32,
        error: &E,
    ) -> RetryDecision;
}
```

Requisiti:

- jitter opzionale;
- exponential backoff;
- max attempts;
- max elapsed time;
- no retry su outcome sconosciuto salvo policy esplicita;
- retry policy non hardcoded nel worker engine.

---

# 17. Dead-letter e replay

```rust
pub struct DeadLetter {
    pub message: SerializedMessage,
    pub reason: Arc<str>,
    pub attempts: u32,
    pub failed_at: DateTime<Utc>,
}

pub struct ReplayRequest {
    pub source: ReplaySource,
}

pub enum ReplaySource {
    FromSequence(u64),
    FromTimestamp(DateTime<Utc>),
    All,
}
```

Non tutti i broker supporteranno tutto.

Definire capability esplicite:

```rust
pub struct BrokerCapabilities {
    pub durable_consumers: bool,
    pub replay: bool,
    pub ordered_delivery: bool,
    pub dead_letter_native: bool,
    pub exactly_once_claimed: bool,
}
```

Mai assumere capability inesistenti.

---

# 18. Crate `plenora-runtime-worker`

```rust
#[async_trait]
pub trait WorkerHandler<T>: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn handle(
        &self,
        ctx: WorkerContext,
        message: T,
    ) -> Result<(), Self::Error>;
}

pub struct WorkerContext {
    pub message_id: MessageId,
    pub correlation_id: CorrelationId,
    pub causation_id: Option<CausationId>,
    pub attempt: u32,
    pub metadata: MessageMetadata,
    pub shutdown: ShutdownSignal,
}
```

Nessun business field PFM.

Worker concurrency:

```rust
pub struct WorkerConcurrency {
    pub max_in_flight: usize,
}
```

No unbounded task spawn e no unbounded channel.

---

# 19. Apalis adapter

Crate:

```text
adapters/plenora-runtime-apalis
```

Nessun tipo Apalis deve comparire nelle API pubbliche core.

Responsabilità:

```text
Plenora WorkerHandler
        ↕
Apalis Service / Worker
```

Apalis gestisce:

- worker concurrency;
- worker lifecycle;
- Tower pipeline;
- scheduling/execution mechanics;
- retry middleware dove appropriato.

Plenora mantiene:

- envelope;
- context;
- ack semantics;
- error classification;
- outbox/inbox semantics;
- health conventions.

---

# 20. NATS JetStream adapter

Crate:

```text
adapters/plenora-runtime-nats
```

Responsabilità:

- `MessageProducer`;
- `MessageConsumer`;
- Delivery ack/nack;
- durable consumer;
- pull consumer;
- explicit ack;
- redelivery;
- replay capability;
- health check;
- broker metadata;
- reconnect.

Config candidata:

```rust
pub struct NatsConfig {
    pub servers: Vec<String>,
    pub credentials: Option<SecretRef>,
    pub connect_timeout: Duration,
}

pub struct JetStreamConsumerConfig {
    pub stream: String,
    pub consumer: String,
    pub filter_subject: Option<String>,
    pub ack_wait: Duration,
    pub max_deliver: Option<u32>,
    pub max_ack_pending: Option<usize>,
}
```

Non hardcodare subject PFM.

Non creare automaticamente infrastruttura di produzione senza flag esplicito.

---

# 21. Responsabilità NATS / Apalis / runtime-tools

```text
NATS JetStream
    persistenza
    subject
    stream
    durable consumer
    fan-out
    ack/redelivery
    retention
    replay

Apalis
    worker execution
    concurrency
    Tower integration
    retry mechanics
    worker lifecycle

runtime-tools
    public abstraction
    context
    conventions
    outbox/inbox
    error mapping
    health
    telemetry
```

---

# 22. Crate `plenora-runtime-outbox`

La prima versione deve offrire astrazioni e fake store, non persistenza PostgreSQL reale.

```rust
#[async_trait]
pub trait OutboxStore: Send + Sync {
    type Error;

    async fn claim_pending(
        &self,
        limit: usize,
    ) -> Result<Vec<OutboxEntry>, Self::Error>;

    async fn mark_published(
        &self,
        id: OutboxId,
    ) -> Result<(), Self::Error>;

    async fn mark_failed(
        &self,
        id: OutboxId,
        failure: PublishFailure,
    ) -> Result<(), Self::Error>;
}
```

```rust
pub struct OutboxEntry {
    pub id: OutboxId,
    pub message: SerializedMessage,
    pub created_at: DateTime<Utc>,
    pub attempts: u32,
}
```

---

# 23. OutboxRelay

Flow:

```text
claim pending
     ↓
publish
     ↓
confirmed?
  /        \
yes        unknown/fail
 │             │
mark         policy
published      │
               ▼
          retain/retry
```

`OutcomeUnknown` non deve essere automaticamente convertito in retry immediato.

Serve una policy esplicita.

---

# 24. InboxStore e IdempotencyStore

```rust
#[async_trait]
pub trait InboxStore: Send + Sync {
    type Error;

    async fn contains(
        &self,
        message_id: MessageId,
    ) -> Result<bool, Self::Error>;

    async fn record_processed(
        &self,
        message_id: MessageId,
    ) -> Result<(), Self::Error>;
}
```

```rust
#[async_trait]
pub trait IdempotencyStore: Send + Sync {
    type Error;

    async fn begin(
        &self,
        key: IdempotencyKey,
        fingerprint: RequestFingerprint,
    ) -> Result<IdempotencyDecision, Self::Error>;
}

pub enum IdempotencyDecision {
    Execute,
    ReturnStoredResult,
    Conflict,
    InProgress,
}
```

Persistenza concreta fuori dal core.

---

# 25. Database adapter — rinviato

Non implementare nella prima milestone l'adapter reale verso `plenora-database-tools`.

Lo skeleton deve però permettere:

```text
runtime-outbox traits
         ▲
         │
runtime-database adapter
         │
         ▼
database-tools
```

La transazione business rimane posseduta dal consumer.

Pattern futuro obbligatorio:

```text
BEGIN
  business write
  outbox append
COMMIT
```

Non equivalente:

```text
business COMMIT
outbox INSERT in seconda transazione
```

Inbox futura:

```text
BEGIN
  verify inbox
  business effect
  record inbox processed
COMMIT
ACK
```

---

# 26. Crate `plenora-runtime-http`

Candidato: Axum + Tower.

Fornire:

- bootstrap;
- request ID;
- correlation extraction/generation;
- trace integration;
- health endpoints;
- readiness endpoints;
- graceful shutdown hook;
- common error-response hooks.

Non implementare autorizzazione business.

---

# 27. Observability crate

Fornire convenzioni/helper per:

- structured tracing;
- span naming;
- correlation propagation;
- metric naming;
- worker metrics;
- broker metrics;
- outbox metrics;
- health metrics;
- redaction hooks.

Metriche candidate:

```text
runtime_tasks_active
runtime_tasks_failed_total
runtime_shutdown_duration

messages_received_total
messages_processed_total
messages_failed_total
messages_retried_total
messages_dead_lettered_total
message_processing_duration

outbox_pending
outbox_oldest_age
outbox_publish_total
outbox_publish_failed_total

broker_connected
broker_reconnect_total
consumer_lag
```

Non imporre Grafana, Datadog, Tempo, Jaeger o backend specifico.

---

# 28. Error model

Il runtime deve seguire la stessa filosofia delle altre librerie Plenora.

Ogni errore deve poter esprimere concetti equivalenti a:

```text
category
phase
remote effect
retry disposition
source
```

Non creare un error model incompatibile.

Se i tipi condivisi verranno stabilizzati in un crate comune futuro, migrare senza breaking API dove possibile.

---

# 29. Cancellation bridging

Le altre foundation possiedono primitive proprie.

`runtime-tools` non deve imporre un'unificazione forzata nella prima release.

Definire adapter futuri:

```text
Runtime ShutdownSignal
     │
     ├──> database cancellation
     ├──> data-tools cancellation
     └──> IO-tools cancellation
```

La cancellazione deve essere:

- cooperativa;
- bounded;
- osservabile;
- idempotente.

---

# 30. Crate `plenora-runtime-testkit`

First-class, non posticipato.

Fornire:

- FakeProducer;
- FakeConsumer;
- FakeDelivery;
- FakeBroker;
- FakeOutboxStore;
- FakeInboxStore;
- FakeIdempotencyStore;
- ManualClock / TestClock;
- ShutdownHarness;
- deterministic retry utilities.

FakeBroker deve supportare:

- enqueue;
- dequeue;
- duplicate injection;
- delayed delivery;
- nack;
- redelivery;
- disconnect simulation;
- ack failure simulation;
- publish outcome unknown simulation.

---

# 31. Test scenarios obbligatori

## Worker success

```text
enqueue
→ worker receives
→ handler succeeds
→ ack
```

## Worker failure retryable

```text
handler error retryable
→ retry policy
→ redelivery
→ success
→ ack
```

## Worker permanent failure

```text
handler permanent error
→ dead-letter
```

## Duplicate delivery

```text
same message_id delivered twice
→ dedup layer prevents duplicate effect
```

## Graceful shutdown

```text
shutdown
→ no new jobs
→ in-flight completes
→ process exits
```

## Forced shutdown

```text
shutdown
→ handler hangs
→ grace timeout
→ delivery nack/requeue where supported
```

## Broker disconnect

```text
broker unavailable
→ readiness false
→ reconnect
→ readiness true
```

## Publish OutcomeUnknown

```text
publish remote effect unknown
→ no blind retry
→ outcome propagated
```

---

# 32. Architecture tests

Impedire almeno:

```text
runtime-core imports async_nats
runtime-core imports apalis
runtime-messaging imports async_nats
runtime-worker exposes apalis types
runtime-outbox imports PostgreSQL driver
```

---

# 33. CI gates

Pipeline minima:

```text
cargo fmt --check
cargo clippy --all-targets --all-features
cargo test --workspace
cargo doc --workspace --no-deps
cargo audit
cargo deny check
```

Aggiungere, coerentemente con le altre librerie:

- no-panic/anti-unwrap checks;
- unsafe code policy;
- dependency pinning;
- MSRV check;
- Windows/Linux/macOS;
- same-SHA release qualification.

---

# 34. Security requirements

- no secret nei log;
- credential type redacted;
- NATS credentials non Debug-visible;
- metadata sensitive redacted;
- TLS richiesto dagli adapter production-oriented;
- health endpoint non espone secret/config interna;
- replay non abilitato automaticamente;
- DLQ payload non loggato per default;
- no broker credential in error messages;
- bounded payload configuration.

---

# 35. Milestone 1 — Skeleton + fakes

Implementare:

```text
runtime-core
runtime-messaging
runtime-worker
runtime-outbox
runtime-testkit
```

senza infrastruttura esterna.

Test E2E:

```text
OutboxEntry
   ↓
OutboxRelay
   ↓
FakeProducer
   ↓
FakeBroker
   ↓
Worker
   ↓
Handler
   ↓
FakeInbox
   ↓
Ack
```

---

# 36. Milestone 2 — Apalis

Deliverable:

```text
plenora-runtime-apalis
```

Dimostrare:

- worker startup;
- concurrency;
- handler dispatch;
- Tower middleware;
- retry;
- graceful shutdown;
- error propagation;
- zero leakage di tipi Apalis nelle API core.

---

# 37. Milestone 3 — NATS JetStream

Deliverable:

```text
plenora-runtime-nats
```

Dimostrare:

- connection;
- durable pull consumer;
- explicit ack;
- redelivery;
- reconnect;
- resume;
- replay;
- duplicate delivery;
- graceful shutdown.

Test con NATS ephemeral/containerizzato.

---

# 38. Milestone 4 — HTTP + observability

Deliverable:

```text
plenora-runtime-http
plenora-runtime-observability
```

Dimostrare:

- Axum bootstrap;
- correlation;
- tracing;
- health/readiness;
- shutdown;
- metric hooks.

---

# 39. Milestone 5 — Database adapter

Solo dopo stabilizzazione `plenora-database-tools`.

Deliverable futuro:

```text
plenora-runtime-database
```

Implementare:

- outbox store adapter;
- inbox store adapter;
- idempotency store adapter;
- health adapter.

---

# 40. Agent Work Packages

## AGENT-00 — Workspace bootstrap

Deliverable:

- root workspace;
- crate vuoti;
- README;
- docs skeleton;
- fmt/clippy/test workflow;
- dependency policy.

Acceptance:

```text
cargo check --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
```

passano.

## AGENT-01 — runtime-core

Implementare:

- ServiceMetadata;
- ShutdownSignal;
- RuntimeContext;
- RuntimeHandle;
- task supervision;
- health/readiness;
- graceful shutdown.

Test:

- shutdown idempotente;
- critical task failure;
- optional task failure;
- drain timeout;
- task panic captured.

## AGENT-02 — runtime-messaging

Implementare:

- MessageId;
- CorrelationId;
- CausationId;
- MessageMetadata;
- SerializedMessage;
- MessageEnvelope;
- Producer/Consumer traits;
- Delivery;
- Ack/Nack;
- RetryPolicy;
- BrokerCapabilities.

Test:

- metadata namespacing;
- ownership ack;
- retry decisions;
- serialization-neutral core.

## AGENT-03 — runtime-testkit

Implementare:

- FakeProducer;
- FakeConsumer;
- FakeDelivery;
- FakeBroker;
- fault injection;
- deterministic clock;
- shutdown harness.

## AGENT-04 — runtime-worker + Apalis adapter

Implementare core:

- WorkerHandler;
- WorkerContext;
- WorkerConfig.

Adapter:

- mapping verso Apalis;
- Tower layers;
- concurrency;
- graceful shutdown.

Vincolo: nessun tipo Apalis nelle API core.

## AGENT-05 — runtime-outbox

Implementare:

- OutboxStore trait;
- InboxStore trait;
- IdempotencyStore trait;
- OutboxEntry;
- OutboxRelay;
- dedup helper.

Solo fake store nella prima fase.

## AGENT-06 — NATS adapter

Implementare:

- async NATS client adapter;
- JetStream producer;
- durable pull consumer;
- explicit ack;
- reconnect;
- replay;
- capability report;
- health.

Test con NATS ephemeral.

## AGENT-07 — HTTP adapter

Implementare:

- Axum bootstrap;
- Tower middleware;
- request/correlation ID;
- health;
- readiness;
- graceful shutdown.

Non implementare authorization PFM.

## AGENT-08 — Observability

Implementare:

- tracing spans;
- propagation;
- worker metrics;
- broker metrics;
- outbox metrics;
- redaction interfaces.

## AGENT-09 — Cross-cutting QA

Verificare:

- dependency direction;
- implementation leakage;
- secret leakage;
- bounded queues;
- panic paths;
- cancellation;
- fault injection;
- docs;
- examples.

---

# 41. Merge order raccomandato

```text
AGENT-00
   ↓
AGENT-01 + AGENT-02 + AGENT-03
   ↓
AGENT-05
   ↓
AGENT-04
   ↓
AGENT-06
   ↓
AGENT-07 + AGENT-08
   ↓
AGENT-09
```

Alcuni stream possono procedere in parallelo dopo la stabilizzazione dei trait.

---

# 42. Scope creep guard

Non implementare nella v0.1:

- scheduler cron distribuito;
- workflow DAG;
- saga orchestration engine;
- distributed locks;
- distributed cache;
- leader election;
- schema registry;
- Kafka adapter;
- RabbitMQ adapter;
- Kubernetes operator;
- UI dashboard;
- admin console;
- PFM-specific middleware;
- database adapter reale prima della stabilizzazione richiesta.

Ogni elemento richiede issue/ADR separato.

---

# 43. API freeze gates

Non congelare API 1.0 prima di avere:

1. fake E2E;
2. Apalis integration;
3. NATS integration;
4. almeno un consumer reale di prova;
5. un microservizio PFM PoC;
6. outbox adapter su database-tools;
7. fault injection.

---

# 44. Definition of Done generale

`plenora-runtime-tools` skeleton è considerato completato quando:

- [ ] repository workspace creato;
- [ ] dependency direction verificata;
- [ ] runtime-core implementato;
- [ ] runtime-messaging implementato;
- [ ] runtime-worker implementato;
- [ ] runtime-outbox traits implementati;
- [ ] runtime-testkit implementato;
- [ ] fake E2E completo;
- [ ] Apalis adapter funzionante;
- [ ] NATS JetStream adapter funzionante;
- [ ] graceful shutdown verificato;
- [ ] retry verificato;
- [ ] duplicate delivery verificata;
- [ ] DLQ semantics verificata;
- [ ] OutcomeUnknown propagato;
- [ ] health/readiness disponibili;
- [ ] observability hooks disponibili;
- [ ] HTTP adapter disponibile o esplicitamente rinviato;
- [ ] nessun PFM business type nel core;
- [ ] nessun Apalis type leakage;
- [ ] nessun NATS type leakage;
- [ ] docs e examples completi;
- [ ] CI verde;
- [ ] security review completata.

---

# 45. PFM consumer readiness

Questa sezione è consumer-specifica ma non modifica il core.

PFM deve poter costruire sopra runtime-tools:

- metadata `pfm.*`;
- tenant/actor context;
- correlation/causation;
- transactional outbox;
- transactional inbox;
- replay governato;
- durable consumers;
- audit consumer;
- graph projection consumer;
- KPI consumer;
- notification consumer.

Nessuno di questi concetti deve essere hardcoded nel runtime generico.

---

# 46. Open decisions / ADR

Creare ADR per:

1. versione Apalis da fissare;
2. versione NATS client da fissare;
3. JSON vs altro codec per esempi/default;
4. modello error condiviso definitivo;
5. bridge cancellation con le altre foundation;
6. schema outbox fisico;
7. schema inbox fisico;
8. database adapter API;
9. HTTP adapter public surface;
10. OpenTelemetry propagation format;
11. release/MSRV policy;
12. eventuale supporto Kafka;
13. eventuale supporto RabbitMQ.

---

# 47. Esito architetturale atteso

Al termine, un consumer dovrà vedere:

```text
plenora-runtime-tools
    ↓
public Rust API
```

senza conoscere:

```text
Apalis
NATS
Tower internals
broker implementation details
```

L'implementazione iniziale sarà:

```text
Tokio
  ↓
Tower
  ↓
Apalis
  ↓
plenora-runtime-tools
  ↓
NATS JetStream
```

ma la dipendenza concettuale del consumer resterà:

```text
consumer
   ↓
plenora-runtime-tools
```

---

# 48. Cronologia

| Versione | Data | Sintesi |
|---|---|---|
| 0.1 | 15 agosto 2026 | Prima specifica implementativa agent-oriented dello skeleton `plenora-runtime-tools`, con core broker-agnostic, Apalis worker adapter, NATS JetStream adapter, outbox/inbox abstractions, lifecycle, health, observability e testkit. |
