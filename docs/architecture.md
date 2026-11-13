# Architettura

`plenora-runtime-tools` fornisce fondamenta riutilizzabili per processi e servizi e,
come nuovo strato prodotto progettato, l’orchestrazione delle catene Plenora.
[ADR 0008](adr/0008-chain-product-scope.md) registra il cambio di perimetro.
Le fondamenta generiche non acquisiscono logica ETL; i crate catene non sono ancora creati.

La stratificazione implementata è:

```text
runtime-worker  ----> runtime-core
       +-----------> runtime-messaging

runtime-capabilities ----> runtime-core
             +----------> runtime-worker
             +----------> runtime-messaging

runtime-outbox  ----> runtime-messaging

runtime-testkit ----> runtime-core
       +-----------> runtime-messaging
       +-----------> runtime-outbox
       +-----------> runtime-worker
       +-----------> runtime-capabilities

runtime-apalis  ----> runtime-core
       +-----------> runtime-worker
       +-----------> runtime-messaging

runtime-nats    ----> runtime-core
       +-----------> runtime-messaging

runtime-http    ----> runtime-core
       +-----------> runtime-messaging

runtime-observability ----> runtime-messaging

runtime-resources ----> runtime-core
       +-------------> runtime-worker

runtime-scheduler ----> runtime-core

runtime-subprocess ----> Tokio process primitives only

runtime-control ----> runtime-messaging
       +-----------> runtime-worker
       +-----------> runtime-resources
       +-----------> runtime-scheduler
       +-----------> runtime-subprocess

runtime-control-http ----> runtime-control + Axum
       +-----------> runtime-worker
       +-----------> runtime-resources
       +-----------> runtime-scheduler
```

Il relay dell'outbox pubblica attraverso il contratto di producer di messaging e non dipende dal
worker. Il testkit dipende da core, messaging e outbox per poter fornire tutti i fake di prima
classe. Dipende anche da capabilities e worker per fornire il fake generico di adapter privo di
payload.

Il crate capabilities è un livello di integrazione applicativa sopra worker e messaging. Possiede un
registro di avvio limitato, un instradamento portabile e versionato e un dispatcher neutrale
rispetto al motore. Le librerie Rust concrete restano dipendenze del consumer Plenora che le
incorpora e sono avvolte da piccoli adapter posseduti dall'applicazione; non diventano mai
dipendenze dei crate generici di runtime. Il consumer catene è posseduto dal
repository nei nuovi crate prodotto descritti sotto.

Il crate resources osserva la pressione sul processo e controlla solo il cancello di ammissione
reversibile del worker. Lo scheduler dipende solo dai contratti di tempo e di arresto, più il
calcolo cron/fuso orario neutrale rispetto al backend; effetti collaterali del dispatch e
persistenza restano confini posseduti dall'applicazione. Il crate subprocess possiede il
contenimento ma nessun protocollo applicativo. Il crate control è un livello di aggregazione solo
verso l'interno; il suo adapter HTTP separato possiede il trasporto e richiede un'autorizzazione
dell'applicazione.

Il package HTTP è un adapter in ingresso pur vivendo insieme ai crate riutilizzabili: i tipi Axum e
Tower si fermano al confine di quel package. Il package observability definisce convenzioni stabili
per span, propagazione, metriche e oscuramento. Le applicazioni installano subscriber ed exporter
concreti.

Tokio, Tower, Apalis, NATS JetStream, Axum e i driver di database sono scelte implementative, non
concetti dell'API del consumer. Le decisioni rilevanti non stabilite dalla specifica richiedono un
ADR.

L'adapter Apalis fornisce anche un `BrokerWorkerRunner` neutrale rispetto al broker. Le
applicazioni compongono all'avvio un `MessageConsumer` come `JetStreamConsumer` con quel runner.
Questo mantiene l'adapter NATS indipendente dalla meccanica del worker, permettendo al tempo stesso
alla readiness di Apalis di applicare backpressure prima che venga prelevata la consegna successiva
dal broker.

## Strato catene proposto (P1.1)

La tabella descrive il grafo obiettivo, non attesta tutti i pacchetti.
Il prefisso è `plenora-runtime-`. Sono presenti chain-contracts, boundary e
le primitive PostgreSQL di [chain-store](../crates/plenora-runtime-chain-store/README.md).
Il resto del prodotto e il cablaggio dello store nell'host restano da realizzare.
Il prodotto usa direttamente le librerie Rust; `plenora-cli` DATA è una libreria,
non un subprocess. Pin e feature ETL appartengono al prodotto.

| Crate nuovo | Responsabilità esclusiva | Dipendenze ammesse |
|---|---|---|
| `plenora-runtime-chain-contracts` | Tipi pubblici piano/esito/eventi, parser chiuso, validazione strutturale, digest canonico, riferimenti a contratti versionati | Serializzazione e tipi pubblici generici; nessun engine, broker, Python o database |
| `plenora-runtime-boundary` | Ammissione canonica, negoziazione, costruzione risultati, settlement affidabile, interfacce di ledger/acceptance/outbox e wrapper del dispatcher | Crate generici capabilities/core/messaging/worker/outbox; nessuna libreria ETL |
| `plenora-runtime-chains` | Validazione semantica del grafo, scheduler topologico, budget, stati, cancellazione e recovery; facciata Rust | chain-contracts, boundary, trait di adapter/artefatti/stato; nessun engine concreto |
| `plenora-runtime-chain-resources` | Resolver autorizzati di artefatti, connessioni e segreti, spool, digest, lease, pubblicazione e cleanup | chain-contracts, interfacce di autorizzazione e storage dell'host; nessuna credenziale nel piano |
| `plenora-runtime-adapter-data` | Discovery del wrapper e traduzione verso API DATA, input/output multipli, errori e cancellazione | `plenora-cli` come **libreria Rust** (`plenora_cli::artefatti`), eventuali tipi core/pipeline necessari; non il binario CLI |
| `plenora-runtime-adapter-database` | Binding DB, provider, write@2, LossConsent e WriteOutcome v3 | `plenora-database-engine`, core e feature/provider qualificati |
| `plenora-runtime-adapter-io` | Binding IO, fidelity, Arrow IPC, formati/feature dichiarati | `plenora-io-tools` e tipi IO necessari |
| `plenora-runtime-adapter-storage` | Binding STORAGE, sette operazioni e resolver; nessuna interpretazione implicita del contenuto | `plenora-storage-core` e feature provider selezionate |
| `plenora-runtime-adapter-rest` | Binding REST, envelope di business, job residui, metadata e cancellazione | package `plenora-rest-core` nel percorso `crates/rest-engine-core`; feature effettive del prodotto |
| `plenora-runtime-chain-conversions` | Conversioni esplicite REST→Arrow e Arrow→REST, con contratti distinti; mapping di porte verso richieste di libreria | Tipi Arrow e contratti; adapters nominati/versionati, nessuna inferenza da nomi simili |
| `plenora-runtime-chain-store` | Backend durevole comune per acceptance, inbox, step, outbox, fence, eventi e manifest | Driver del database operativo scelto; implementazione dei trait boundary/chains. Non usa database.write ETL per registrare se stesso |
| `plenora-runtime-chain-host` | Composizione del prodotto: registry congelato, cinque adapter, store, resolver, NATS/Apalis, controllo, policy e discovery effettiva | Tutti i crate precedenti e adapter generici di trasporto |
| `plenora-runtime-chain-cli` | Binario pubblico `plenora-runtime`, parsing e presentazione; modalità locale e remota | Facciata chains/host/client; nessun algoritmo ETL duplicato |
| `plenora-runtime-chain-python` | Wheel `plenora_runtime`, binding nativo sync/async, client remoto e conversione errori | Stessa facciata; ponte PyO3/async da qualificare, nessun subprocess obbligatorio |

### Grafo e ownership dei trait

```text
chain-cli / chain-python -> chain-host / chains / chain-contracts
chain-host -> chains, boundary, chain-resources, chain-conversions, chain-store,
              adapter-data/database/io/storage/rest, adapter generici NATS/Apalis
adapter-* -> chains (trait adapter), boundary, chain-contracts -> generici
chain-store -> chains (trait stato), boundary (ledger/acceptance/outbox), chain-contracts
chain-resources -> chains (trait risorse e autorizzazione), chain-contracts
chain-conversions -> chains (trait conversione), chain-contracts
chains -> boundary, chain-contracts
boundary -> capabilities/core/messaging/worker/outbox generici
chain-contracts -> tipi pubblici generici e serializzazione
```

I trait di adapter, risorse, autorizzazione e stato del motore appartengono a
`chains`; l’host inietta implementazioni e policy. Il client remoto usa gli stessi
tipi/facciata: non introduce un altro motore. Il compilatore canvas, identità e
policy tenant restano nell’APP; RT esegue il piano risolto autorizzato.
`chain-resources` risolve e pubblica; `chain-store` registra manifest ed esiti;
`chain-conversions` possiede mapping di porte e trasformazioni esplicite;
i cinque adapter possiedono le chiamate e gli errori delle rispettive librerie.
Il driver PostgreSQL operativo sta solo nello store: non usa database.write ETL.

Sono vietati archi dai generici ai crate prodotto o a ETL/Arrow, anche transitivi;
engine concreti dentro contracts/boundary/chains/resources/store; cicli; dipendenze
fra adapter concreti; dipendenze dal motore verso host/CLI/Python/store concreto.
Contracts, boundary e chains non scelgono broker, Python o database concreti.
Risorse e conversioni usano interfacce host iniettate, senza dipendere da host.
Le facciate non duplicano algoritmi ETL. Le versioni Arrow incompatibili richiedono
un ponte IPC esplicito, limitato e provato contro lo stesso oracolo.

La regola preparata in [chain-crate-graph.json](chain-crate-graph.json) è applicata
al grafo Cargo risolto (tutte le feature, inclusi archi transitivi) da
`scripts/check_chain_graph.py`, in aggiunta ai test Rust di architettura esistenti.
Gli archi prodotto non elencati sono vietati. I crate proposti possono essere
assenti in P1.1; quando compaiono la regola si applica senza abilitarla a mano.
Il controllo automatico dei pacchetti di dominio nominati non sostituisce la
revisione di nuove dipendenze, feature/provider o fughe di tipi nelle API.

### Un solo ingresso del prodotto

Rust, CLI, SDK sync/async e trasporto passano per boundary e il vero
CapabilityDispatcher. Wrapper degli handler registrano intenzione ed esito;
il sink conferma l’outbox PostgreSQL, il relay conferma separatamente la consegna.
Il coordinatore di settlement usa le evidenze senza degradare commit noti in
unknown. Un timeout non libera lavoro sincrono ancora attivo. Se un’API generica
impedisce questa composizione, il gate resta aperto e il vincolo va riaperto col
titolare: non si aggira il dispatcher né si modifica il generico implicitamente.

Le prove del nuovo host non qualificano consumer legacy o esempi. La matrice
[funzionalità × superfici](chain-features.md) esplicita il lavoro ancora da fare.

## Migrazione P2.1b

CapabilityRegistry, CapabilityRegistryBuilder e CapabilityDispatcher sono ora
esportati da plenora-runtime-boundary. CapabilityRequest resta un candidato
outbound e CapabilityMessageCodec espone solo encode. Per ricevere usare
RuntimeBoundary come decoder e handler, con discovery, AdmissionPolicy, clock e
sink espliciti. Il dispatch grezzo è privato. WorkerContext.metadata contiene
solo metadati del messaggio; le osservazioni del broker sono in broker_metadata.
Vedere [allineamento del profilo](contract-alignment.md#p21b-confine-delle-facciate-esistenti).
