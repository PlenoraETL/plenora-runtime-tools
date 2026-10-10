# Stato della qualifica

Questo documento tiene distinti lo scheletro runtime v0.1, la qualifica di rilascio e il congelamento
dell'API per i consumer. Test locali verdi non qualificano da soli un commit e non provano che un
consumer Plenora sia pronto.

## Definition of Done dello scheletro

| Requisito | Stato | Prova o condizione mancante |
|---|---|---|
| Workspace e direzione delle dipendenze | Completo in locale | I test di architettura validano ogni arco interno e ogni confine di implementazione. |
| Core, messaging, worker, capability, trait dell'outbox e testkit | Completo in locale | I test del workspace coprono ciclo di vita, routing generico delle estensioni, retry, deduplicazione, guasti e memoria limitata. |
| Adapter Apalis e NATS JetStream | Completo in locale | I test con broker finto e quelli opzionali su JetStream reale coprono ACK/NAK, DLQ, replay, health e drain. |
| Scenari end-to-end finti obbligatori | Completo in locale | Sono coperti successo, retry/riconsegna, fallimento permanente, soppressione dei duplicati, shutdown ordinato e forzato, e `OutcomeUnknown`. |
| Suite di stabilità limitata | Completo in locale | Sono automatizzati backpressure sostenuta su 2.048 messaggi, cancellazione di 64 task, 100 cicli di vita del worker e un soak di riconnessione su JetStream reale con 256 messaggi; il soak finale sul consumer resta un gate del congelamento dell'API. |
| Qualifica di coverage e fuzz | Completo in locale | `cargo-llvm-cov 0.8.7` impone almeno il 90% di coverage di riga sul workspace runtime più i percorsi JetStream reali; sei target `cargo-fuzz 0.13.2` limitati coprono parsing portabile, retry, configurazione NATS, propagazione, ID di cron/controllo e specifiche dei sottoprocessi. |
| Qualifica sintetica della memoria | Completo in locale | Contratti deterministici provano il rilascio delle allocazioni possedute dai task; un soak Docker da 1 GiB valida plateau dell'allocatore, frammentazione, concorrenza, pulizia su errore e cancellazione, e un controllo con memoria in crescita voluta. Il comportamento di memoria delle librerie reali resta un gate del congelamento dell'API. |
| Transizione di readiness alla disconnessione | Completo in locale | Il test su NATS reale osserva `NotReady` durante la riconnessione forzata e `Ready` dopo il recupero. |
| Hook HTTP e di osservabilità | Completo in locale | Sono testati i limiti di ciclo di vita e sicurezza HTTP e span, propagazione, metriche e redazione indipendenti dal backend. |
| Heartbeat del worker e controllo dei task attivi | Completo in locale | Sono testati ciclo di vita dell'istanza, capacità limitata, cancellazione per ID di task o di messaggio e settlement sul broker. |
| Controllo di ammissione sotto pressione di memoria | Completo in locale | Campionamento dell'RSS su Linux, stati soft/hard, readiness fail-closed, isteresi e 1.000 cicli di recupero sono deterministici. |
| Pianificazione programmata generica | Completo in locale | Sono testati piani one-shot e a intervallo, trait di piano estensibile, misfire/recupero limitati, registrazione a runtime, riconciliazione, cursore al riavvio e shutdown. |
| Cron e controllo dello scheduler | Completo in locale | Sono testati comportamento esplicito su fuso orario IANA e ora legale, jitter deterministico senza deriva, pausa/ripresa durevoli e trigger manuale limitato. |
| Contenimento generico dei sottoprocessi | Completo in locale | Sono testati avvio diretto senza shell, ammissione e output limitati, timeout/cancellazione/terminazione dell'albero, reap, tetto RSS su Linux e riuso della capacità; non si dichiara alcun protocollo applicativo. |
| Controllo operativo generico | Completo in locale | Sono testati il registro congelato e limitato, gli snapshot e le mutazioni di worker/task/scheduler/risorse; l'HTTP è sempre autorizzato dall'applicazione e le route di mutazione sono opzionali. |
| Passaggio della persistenza del ciclo di vita | Completo come confine | Il dispatcher limitato espone scarti, proiezione sulla health e primitive di metrica; non si dichiara alcuna persistenza su database. |
| Documentazione ed esempi | Completo per lo scheletro generico | Il consumer NATS in forma di processo compone routing versionato delle capability, worker, health/readiness HTTP, heartbeat e drain coordinato; un template separato, compilato nei test, definisce il contratto del futuro adapter verso una libreria concreta. |
| Common Runtime Binding 1.0 | Percorso di richiesta/discovery/successo implementato | Vettori pinnati verificano routing e discovery REST. I risultati confermati conservano versione dell'operazione, contratto di output, content type e correlazione. La pubblicazione dell'errore terminale tipizzato dopo il settlement dei retry resta un gate di rilascio aperto. |
| Audit e policy delle dipendenze | Completo in locale | `cargo-audit 0.22.2` e `cargo-deny 0.20.2` pinnati passano; gli avvisi ammessi sulle versioni duplicate restano da rivedere. |
| Revisione di sicurezza | Tecnicamente completa, approvazione di rilascio in attesa | Vedi [`security-review.md`](security-review.md). L'approvazione organizzativa deve riferirsi al commit esatto. |
| CI su Windows, Linux e macOS | Gira a ogni push e pull request; Linux e Windows saranno i check obbligatori di `main`, macOS informativo, ma la protezione di `main` non è ancora attiva | Il dispatch di `ci.yml` sullo stesso SHA su `main` a `9972954` (2026-10-10) ha passato ogni job tranne la policy delle dipendenze: `cargo audit` ha segnalato RUSTSEC-2026-0285 in `rustls 0.23.43` e il `chacha20 0.10.1` ritirato (yanked), entrambi risolti da un aggiornamento del lockfile. Nessun candidato al rilascio è stato ancora qualificato; resta il gate 6 del congelamento dell'API. |

## Matrice degli scenari obbligatori

| Scenario | Finto/in-process | Infrastruttura reale |
|---|---:|---:|
| Successo del worker e ACK | Sì | Sì, attraverso NATS, routing tipizzato delle capability, worker e ACK |
| Retry, riconsegna, successo, ACK | Sì | Sì, compresa la gestione di una capability che fallisce una volta |
| Fallimento permanente e DLQ confermata | Sì | Sì |
| Consegna duplicata ed effetto deduplicato | Sì | La deduplicazione persistente attende l'adapter del database |
| Shutdown ordinato e drain | Sì | Sì |
| Shutdown forzato e rimessa in coda | Sì | Sì, con un worker sostitutivo sullo stesso durable |
| Disconnessione, `NotReady`, riconnessione, `Ready` | N/A | Sì |
| Pubblicazione `OutcomeUnknown` senza retry alla cieca | Sì | Iniezione di guasti; una rete reale non può provare in modo deterministico l'effetto remoto |

## Gate del congelamento dell'API

L'API pubblica resta pre-1.0. Questi gate della specifica di implementazione sono ancora aperti:

1. un vero consumer Plenora che usi l'API Rust (l'esempio generico in forma di processo registra
   ancora un adapter di capability segnaposto);
2. un proof of concept di microservizio PFM;
3. la persistenza transazionale di outbox/inbox attraverso `database-tools`;
4. i ponti di cancellazione verso `data-tools`, `database-tools` e `IO-tools`;
5. definizioni e cursori durevoli delle pianificazioni e osservazioni del ciclo di vita attraverso
   `database-tools`;
6. CI sullo stesso SHA e approvazione di sicurezza per il candidato al rilascio.
7. la pubblicazione dell'errore terminale di Runtime Binding 1.0, coordinata con l'esaurimento dei
   retry e il settlement.

Nessuno di questi gate aperti va nascosto segnando come completo lo scheletro generico.

## Comandi di qualifica locale

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --all-features --no-deps --locked
cargo audit
cargo deny check
```

La qualifica su NATS reale esegue in più entrambe le suite JetStream ignorate contro il container
effimero e pinnato di `scripts/nats-docker.ps1`.

Il job di coverage della CI unisce i profili del workspace normale, di NATS reale e di NATS verso
Apalis prima di applicare `cargo llvm-cov report --fail-under-lines 90`. Il job di fuzz smoke esegue
ogni corpus con la toolchain pinnata `nightly-2026-08-01`. Vedi [`testing.md`](testing.md) per
ambito e comandi locali.
