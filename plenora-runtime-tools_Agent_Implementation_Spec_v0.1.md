# plenora-runtime-tools — Specifica implementativa per agenti

**Documento:** Specifica tecnica implementativa per agenti di sviluppo  
**Versione:** 0.1  
**Data:** 15 agosto 2026  
**Stato:** Bozza implementativa — scheletro e prima foundation runtime  
**Repository di destinazione proposto:** `PlenoraETL/plenora-runtime-tools`  
**Linguaggio:** Rust  
**Consumer principale di validazione:** PFM — Plenora Facility Management  
**Classificazione:** Uso interno  

---

## 0. Istruzioni per gli agenti

Questo documento deve essere trattato come specifica esecutiva.

Gli agenti devono:

1. rispettare rigorosamente i confini architetturali;
2. non introdurre modelli di business PFM nel core;
3. non accoppiare il core a NATS, Apalis, PostgreSQL o altri provider concreti;
4. mantenere ogni crate compilabile e testabile in isolamento;
5. privilegiare trait, adapter e inversione delle dipendenze;
6. non introdurre nuove dipendenze infrastrutturali senza motivazione documentata;
7. aggiungere test per ogni requisito implementato;
8. evitare `unwrap`, `expect`, `panic!` nei percorsi runtime di produzione;
9. preservare error category, phase, remote effect e retry disposition quando disponibili;
10. produrre codice leggibile, documentato e compatibile con gli standard già usati nelle altre
    librerie Plenora.

Quando una scelta non è definita da questa specifica, l'agente deve:

```text
a. non inventare una decisione irreversibile;
b. creare un ADR/TODO esplicito;
c. implementare dietro un trait quando possibile;
d. preferire la soluzione meno accoppiante.
```

---

# 1. Obiettivo

Costruire `plenora-runtime-tools`, una foundation Rust riusabile per l'esecuzione di microservizi e
worker asincroni.

La libreria deve fornire una superficie comune per:

- ciclo di vita del processo;
- graceful shutdown;
- supervisione dei task;
- esecuzione dei worker;
- astrazione producer/consumer dei messaggi;
- integrazione con il broker;
- retry/backoff;
- ack/nack;
- gestione dei dead-letter;
- hook di replay;
- health/readiness;
- contesto di runtime;
- correlation e causation;
- propagazione delle trace;
- outbox relay;
- astrazioni inbox/dedup;
- astrazioni di idempotenza;
- hook di osservabilità;
- testkit;
- integrazione opzionale con le altre foundation Plenora.

La libreria non deve implementare logica di business.

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

solo per moduli opzionali come gli adapter di persistenza per outbox/inbox.

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

La libreria non deve reinventare scheduler, executor asincroni, timer, I/O su socket o primitive dei
task.

## 3.2 Middleware

Tower è la base preferita per i middleware e la composizione dei servizi.

Utilizzare Tower per funzionalità come timeout, retry, rate limit, tracing, load shedding e
middleware personalizzati.

## 3.3 Motore dei worker

Apalis è il **motore dei worker candidato iniziale**.

> Apalis deve restare un dettaglio implementativo dietro API Plenora.

Nessun microservizio consumer deve essere obbligato a importare tipi Apalis.

## 3.4 Broker iniziale

NATS + JetStream è l'**adapter di broker candidato iniziale**.

> NATS deve restare un dettaglio implementativo dietro trait Plenora.

Il core deve poter supportare in futuro Kafka, RabbitMQ o altri broker senza modificare le API di
business dei consumer.

## 3.5 HTTP

Axum è il candidato iniziale per l'adapter HTTP.

`runtime-core` non dipende da Axum.

## 3.6 Osservabilità

Usare `tracing` e l'integrazione con OpenTelemetry.

Il runtime non deve imporre un backend specifico.

---

# 4. Non-obiettivi

Questa libreria non deve diventare:

- un ORM;
- un'astrazione di database;
- un motore ETL;
- un motore di trasformazione dei dati;
- un parser di file;
- un workflow di dominio;
- un motore BPM;
- un motore di autorizzazione;
- un API gateway;
- uno schema registry;
- un motore di event sourcing;
- un coordinatore di transazioni distribuite;
- un motore di saga di business;
- uno scheduler infrastrutturale Kubernetes;
- un orchestratore di container;
- un sostituto di NATS;
- un sostituto di Apalis;
- un sostituto di Tokio.

Non introdurre nel core concetti di dominio come `Building`, `Asset`, `WorkOrder`, `Tenant`,
`Organization`, `CadastralUnit`.

---

# 5. Struttura del repository

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

# 6. Direzione delle dipendenze

La direzione delle dipendenze è normativa.

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
7. l'eventuale adapter di database dipenderà da `database-tools`, mai il contrario.
8. i microservizi dipendono dalle API Plenora, non dalle implementazioni sottostanti.

---

# 7. Crate `plenora-runtime-core`

## 7.1 Responsabilità

Fornire:

- ciclo di vita del runtime;
- coordinamento dello shutdown;
- supervisione dei task;
- astrazione minima dell'orologio;
- metadati d'identità del servizio;
- contesto di runtime generico;
- modello di health/readiness;
- primitive d'errore comuni del runtime.

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

# 8. Supervisione dei task

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

Fallimento di un task `Critical`:

```text
→ runtime unhealthy
→ shutdown coordinato
```

Fallimento di un task `Optional`:

```text
→ log / metric
→ policy configurabile
```

Il runtime deve catturare i panic dei task e convertirli in un esito governato.

---

# 9. Graceful shutdown

Sequenza obiettivo:

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
- più segnali equivalgono a uno solo;
- timeout di grazia configurabile;
- nessun nuovo lavoro dopo l'ingresso in drain;
- lavoro in corso osservabile;
- il timeout del drain produce un esito esplicito;
- nessun blocco indefinito del processo.

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

Il core deve poter aggregare broker, database, worker, outbox relay e dipendenze esterne.

Health e readiness non devono essere sinonimi.

---

# 11. Crate `plenora-runtime-messaging`

Responsabilità: definire il contratto indipendente dal broker.

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

Deve essere organizzato per namespace in modo generico.

```text
myapp.key
plenora.message.*
plenora.trace.*
```

Non cablare nel codice namespace di consumer come `pfm.*`.

---

# 12. Serializzazione

Il core di messaging non deve imporre il formato del payload al dominio.

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

Non trasformare un esito incerto in un successo.

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

La delivery possiede envelope/messaggio, tentativo di consegna, metadati del broker e handle di ack.

```rust
pub struct Delivery {
    pub message: SerializedMessage,
    pub attempt: u32,
    // private ack state
}
```

API di ack:

```rust
impl Delivery {
    pub async fn ack(self) -> Result<(), AckError>;
    pub async fn nack(self, reason: NackReason) -> Result<(), AckError>;
}
```

Dopo `ack` / `nack`, la delivery non deve essere riutilizzabile.

---

# 15. Semantica di ack/nack

```rust
pub enum NackReason {
    Retryable,
    Permanent,
    Shutdown,
    ConsumerRejected,
}
```

La mappatura verso NATS/Kafka/RabbitMQ spetta agli adapter.

---

# 16. Modello di retry

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
- backoff esponenziale;
- numero massimo di tentativi;
- tempo massimo trascorso;
- nessun retry su esito sconosciuto, salvo policy esplicita;
- policy di retry non cablata nel motore dei worker.

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

Mai presumere capability inesistenti.

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

Nessun campo di business PFM.

Concorrenza dei worker:

```rust
pub struct WorkerConcurrency {
    pub max_in_flight: usize,
}
```

Nessuno spawn illimitato di task e nessun canale illimitato.

---

# 19. Adapter Apalis

Crate:

```text
adapters/plenora-runtime-apalis
```

Nessun tipo Apalis deve comparire nelle API pubbliche del core.

Responsabilità:

```text
Plenora WorkerHandler
        ↕
Apalis Service / Worker
```

Apalis gestisce:

- concorrenza dei worker;
- ciclo di vita dei worker;
- pipeline Tower;
- meccanica di scheduling/esecuzione;
- middleware di retry dove appropriato.

Plenora mantiene:

- envelope;
- contesto;
- semantica di ack;
- classificazione degli errori;
- semantica di outbox/inbox;
- convenzioni di health.

---

# 20. Adapter NATS JetStream

Crate:

```text
adapters/plenora-runtime-nats
```

Responsabilità:

- `MessageProducer`;
- `MessageConsumer`;
- ack/nack della Delivery;
- consumer durevole;
- consumer pull;
- ack esplicito;
- riconsegna;
- capability di replay;
- health check;
- metadati del broker;
- riconnessione.

Configurazione candidata:

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

Non cablare nel codice subject PFM.

Non creare automaticamente infrastruttura di produzione senza un flag esplicito.

---

# 21. Responsabilità di NATS / Apalis / runtime-tools

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

La prima versione deve offrire astrazioni e fake store, non una persistenza PostgreSQL reale.

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

Flusso:

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

`OutcomeUnknown` non deve essere convertito automaticamente in un retry immediato.

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

# 25. Adapter di database — rinviato

Non implementare nella prima milestone l'adapter reale verso `plenora-database-tools`.

Lo scheletro deve però permettere:

```text
runtime-outbox traits
         ▲
         │
runtime-database adapter
         │
         ▼
database-tools
```

La transazione di business resta di proprietà del consumer.

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
- estrazione/generazione della correlation;
- integrazione con il tracing;
- endpoint di health;
- endpoint di readiness;
- hook di graceful shutdown;
- hook comuni per le risposte d'errore.

Non implementare autorizzazione di business.

---

# 27. Crate di osservabilità

Fornire convenzioni/helper per:

- tracing strutturato;
- nomenclatura degli span;
- propagazione della correlation;
- nomenclatura delle metriche;
- metriche dei worker;
- metriche del broker;
- metriche dell'outbox;
- metriche di health;
- hook di mascheramento.

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

Non imporre Grafana, Datadog, Tempo, Jaeger o un backend specifico.

---

# 28. Modello degli errori

Il runtime deve seguire la stessa filosofia delle altre librerie Plenora.

Ogni errore deve poter esprimere concetti equivalenti a:

```text
category
phase
remote effect
retry disposition
source
```

Non creare un modello degli errori incompatibile.

Se i tipi condivisi verranno stabilizzati in un futuro crate comune, migrare senza rompere le API
dove possibile.

---

# 29. Ponte per la cancellazione

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
- limitata;
- osservabile;
- idempotente.

---

# 30. Crate `plenora-runtime-testkit`

Di prima classe, non rinviato.

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
- utility deterministiche per il retry.

FakeBroker deve supportare:

- enqueue;
- dequeue;
- iniezione di duplicati;
- consegna ritardata;
- nack;
- riconsegna;
- simulazione di disconnessione;
- simulazione di un fallimento dell'ack;
- simulazione di un esito di publish sconosciuto.

---

# 31. Scenari di test obbligatori

## Successo del worker

```text
enqueue
→ worker receives
→ handler succeeds
→ ack
```

## Fallimento del worker ripetibile

```text
handler error retryable
→ retry policy
→ redelivery
→ success
→ ack
```

## Fallimento permanente del worker

```text
handler permanent error
→ dead-letter
```

## Consegna duplicata

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

## Shutdown forzato

```text
shutdown
→ handler hangs
→ grace timeout
→ delivery nack/requeue where supported
```

## Disconnessione del broker

```text
broker unavailable
→ readiness false
→ reconnect
→ readiness true
```

## Publish con OutcomeUnknown

```text
publish remote effect unknown
→ no blind retry
→ outcome propagated
```

---

# 32. Test architetturali

Impedire almeno:

```text
runtime-core imports async_nats
runtime-core imports apalis
runtime-messaging imports async_nats
runtime-worker exposes apalis types
runtime-outbox imports PostgreSQL driver
```

---

# 33. Gate della CI

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

- controlli no-panic/anti-unwrap;
- policy sul codice unsafe;
- pinning delle dipendenze;
- verifica della MSRV;
- Windows/Linux/macOS;
- qualificazione del rilascio sullo stesso SHA.

---

# 34. Requisiti di sicurezza

- nessun segreto nei log;
- tipo delle credenziali mascherato;
- credenziali NATS non visibili tramite Debug;
- metadati sensibili mascherati;
- TLS richiesto dagli adapter orientati alla produzione;
- l'endpoint di health non espone segreti né configurazione interna;
- replay non abilitato automaticamente;
- payload della DLQ non registrato nei log per impostazione predefinita;
- nessuna credenziale del broker nei messaggi d'errore;
- configurazione con payload limitato.

---

# 35. Milestone 1 — Scheletro + fake

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

- avvio dei worker;
- concorrenza;
- dispatch verso gli handler;
- middleware Tower;
- retry;
- graceful shutdown;
- propagazione degli errori;
- nessuna fuoriuscita di tipi Apalis nelle API del core.

---

# 37. Milestone 3 — NATS JetStream

Deliverable:

```text
plenora-runtime-nats
```

Dimostrare:

- connessione;
- consumer pull durevole;
- ack esplicito;
- riconsegna;
- riconnessione;
- ripresa;
- replay;
- consegna duplicata;
- graceful shutdown.

Test con NATS effimero/in container.

---

# 38. Milestone 4 — HTTP + osservabilità

Deliverable:

```text
plenora-runtime-http
plenora-runtime-observability
```

Dimostrare:

- bootstrap Axum;
- correlation;
- tracing;
- health/readiness;
- shutdown;
- hook per le metriche.

---

# 39. Milestone 5 — Adapter di database

Solo dopo la stabilizzazione di `plenora-database-tools`.

Deliverable futuro:

```text
plenora-runtime-database
```

Implementare:

- adapter per lo store di outbox;
- adapter per lo store di inbox;
- adapter per lo store di idempotenza;
- adapter di health.

---

# 40. Pacchetti di lavoro per gli agenti

## AGENT-00 — Avvio del workspace

Deliverable:

- workspace radice;
- crate vuoti;
- README;
- scheletro della documentazione;
- workflow fmt/clippy/test;
- policy delle dipendenze.

Accettazione:

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
- supervisione dei task;
- health/readiness;
- graceful shutdown.

Test:

- shutdown idempotente;
- fallimento di un task critical;
- fallimento di un task optional;
- timeout del drain;
- panic di un task catturato.

## AGENT-02 — runtime-messaging

Implementare:

- MessageId;
- CorrelationId;
- CausationId;
- MessageMetadata;
- SerializedMessage;
- MessageEnvelope;
- trait Producer/Consumer;
- Delivery;
- Ack/Nack;
- RetryPolicy;
- BrokerCapabilities.

Test:

- namespace dei metadati;
- ownership dell'ack;
- decisioni di retry;
- core neutrale rispetto alla serializzazione.

## AGENT-03 — runtime-testkit

Implementare:

- FakeProducer;
- FakeConsumer;
- FakeDelivery;
- FakeBroker;
- iniezione di guasti;
- orologio deterministico;
- harness di shutdown.

## AGENT-04 — runtime-worker + adapter Apalis

Implementare nel core:

- WorkerHandler;
- WorkerContext;
- WorkerConfig.

Adapter:

- mappatura verso Apalis;
- layer Tower;
- concorrenza;
- graceful shutdown.

Vincolo: nessun tipo Apalis nelle API del core.

## AGENT-05 — runtime-outbox

Implementare:

- trait OutboxStore;
- trait InboxStore;
- trait IdempotencyStore;
- OutboxEntry;
- OutboxRelay;
- helper di dedup.

Solo fake store nella prima fase.

## AGENT-06 — Adapter NATS

Implementare:

- adapter per il client NATS asincrono;
- producer JetStream;
- consumer pull durevole;
- ack esplicito;
- riconnessione;
- replay;
- report delle capability;
- health.

Test con NATS effimero.

## AGENT-07 — Adapter HTTP

Implementare:

- bootstrap Axum;
- middleware Tower;
- ID di request/correlation;
- health;
- readiness;
- graceful shutdown.

Non implementare l'autorizzazione PFM.

## AGENT-08 — Osservabilità

Implementare:

- span di tracing;
- propagazione;
- metriche dei worker;
- metriche del broker;
- metriche dell'outbox;
- interfacce di mascheramento.

## AGENT-09 — QA trasversale

Verificare:

- direzione delle dipendenze;
- fuoriuscita di dettagli implementativi;
- fuoriuscita di segreti;
- code limitate;
- percorsi di panic;
- cancellazione;
- iniezione di guasti;
- documentazione;
- esempi.

---

# 41. Ordine di merge raccomandato

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

Alcuni filoni possono procedere in parallelo dopo la stabilizzazione dei trait.

---

# 42. Argine contro lo scope creep

Non implementare nella v0.1:

- scheduler cron distribuito;
- workflow DAG;
- motore di orchestrazione di saga;
- lock distribuiti;
- cache distribuita;
- elezione del leader;
- schema registry;
- adapter Kafka;
- adapter RabbitMQ;
- operator Kubernetes;
- dashboard UI;
- console di amministrazione;
- middleware specifici per PFM;
- adapter di database reale prima della stabilizzazione richiesta.

Ogni elemento richiede una issue/ADR separata.

---

# 43. Gate per il congelamento delle API

Non congelare le API 1.0 prima di avere:

1. E2E con i fake;
2. integrazione con Apalis;
3. integrazione con NATS;
4. almeno un consumer reale di prova;
5. un PoC di microservizio PFM;
6. adapter di outbox su database-tools;
7. iniezione di guasti.

---

# 44. Definition of Done generale

Lo scheletro di `plenora-runtime-tools` è considerato completato quando:

- [ ] workspace del repository creato;
- [ ] direzione delle dipendenze verificata;
- [ ] runtime-core implementato;
- [ ] runtime-messaging implementato;
- [ ] runtime-worker implementato;
- [ ] trait di runtime-outbox implementati;
- [ ] runtime-testkit implementato;
- [ ] E2E con i fake completo;
- [ ] adapter Apalis funzionante;
- [ ] adapter NATS JetStream funzionante;
- [ ] graceful shutdown verificato;
- [ ] retry verificato;
- [ ] consegna duplicata verificata;
- [ ] semantica della DLQ verificata;
- [ ] OutcomeUnknown propagato;
- [ ] health/readiness disponibili;
- [ ] hook di osservabilità disponibili;
- [ ] adapter HTTP disponibile o esplicitamente rinviato;
- [ ] nessun tipo di business PFM nel core;
- [ ] nessuna fuoriuscita di tipi Apalis;
- [ ] nessuna fuoriuscita di tipi NATS;
- [ ] documentazione ed esempi completi;
- [ ] CI verde;
- [ ] revisione di sicurezza completata.

---

# 45. Prontezza per il consumer PFM

Questa sezione è specifica del consumer ma non modifica il core.

PFM deve poter costruire sopra runtime-tools:

- metadati `pfm.*`;
- contesto tenant/actor;
- correlation/causation;
- outbox transazionale;
- inbox transazionale;
- replay governato;
- consumer durevoli;
- consumer di audit;
- consumer di proiezione sul grafo;
- consumer dei KPI;
- consumer delle notifiche.

Nessuno di questi concetti deve essere cablato nel runtime generico.

---

# 46. Decisioni aperte / ADR

Creare ADR per:

1. versione di Apalis da fissare;
2. versione del client NATS da fissare;
3. JSON o altro codec per esempi/default;
4. modello d'errore condiviso definitivo;
5. ponte di cancellazione con le altre foundation;
6. schema fisico dell'outbox;
7. schema fisico dell'inbox;
8. API dell'adapter di database;
9. superficie pubblica dell'adapter HTTP;
10. formato di propagazione OpenTelemetry;
11. policy di rilascio/MSRV;
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
| 0.1 | 15 agosto 2026 | Prima specifica implementativa orientata agli agenti per lo scheletro di `plenora-runtime-tools`, con core indipendente dal broker, adapter worker Apalis, adapter NATS JetStream, astrazioni outbox/inbox, ciclo di vita, health, osservabilità e testkit. |
