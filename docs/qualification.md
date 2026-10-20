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
| Hook HTTP e di osservabilità | Completo in locale | Sono testati i limiti di ciclo di vita e sicurezza HTTP e span, propagazione, metriche e oscuramento dei dati sensibili indipendenti dal backend. |
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

## Adozione 3aec9f7: gate aperti

- Regola RT-007; ambito runtime; hazard: Retry di mutazioni senza prova di deduplicazione. Rientro: Qualificare adapter e almeno-un-solo-avvio per identita senza chiave.
- Regola RT-010; ambito runtime; hazard: Caller senza risultato terminale. Rientro: Collegare error sink dopo esaurimento retry con pubblicazione confermata.
- Regola RT-012; ambito runtime; hazard: Identita non canoniche possono attraversare un adapter. Rientro: Eseguire tutti i probe sulle vere frontiere codec/trasporto.
- Regola RT-013; ambito runtime; hazard: Percorsi o segreti presenti in input di adapter non qualificati. Rientro: Qualificare adapter con schemi di proprieta del componente.
- Regola RT-016; ambito runtime; hazard: Semantica pubblica del rifiuto diversa da RT-016. Rientro: Uniformare codec, dispatcher e settlement e provare tutti i probe.
- Regola RT-017; ambito runtime; hazard: Normalizzazione di valori come 01. Rientro: Validazione lessicale esatta prima della conversione in tutto il workspace.
- Regola RT-018; ambito runtime; hazard: Categoria dipende dal percorso adapter. Rientro: Applicare un unico ordine e provare difetti multipli.
- Regola RT-019; ambito runtime; hazard: Metadati mancanti o impropri nel risultato. Rientro: Costruire rifiuti al confine del trasporto con probe neri.
- Regola RT-020; ambito runtime; hazard: Caller non puo correlare ogni rifiuto al messaggio. Rientro: Verificare successi e rifiuti sui trasporti reali.
- Regola RT-021; ambito runtime; hazard: Deadline non UTC accettata. Rientro: Parser esatto UTC e prova ManualClock su <= now.
- Regola RT-022; ambito runtime; hazard: Deduplicazione non garantita. Rientro: Qualificare grammatica, limiti e store deduplicazione del componente.
- Regola RT-023; ambito runtime; hazard: Doppio canale deadline. Rientro: Adapter rifiuta campo payload e metadata insieme.
- Regola RT-024; ambito runtime; hazard: Garanzia indebolita: perduta fase/certezza dopo pubblicazione; sink puo avere un limite inferiore. Rientro: Trasportare fase/effetto provati e tradurre tutti i limiti del sink; qualificare trasporto reale.
- Regola RT-026; ambito runtime; hazard: Downgrade implicito possibile nel routing legacy. Rientro: Collegare policy a ogni destinazione scelta prima dell invio.
- Regola RT-027; ambito runtime; hazard: Contesto perso su retry/redrive/resend; nessuna finestra durevole garantita. Rientro: Store atomico autorevole con tombstone e finestra dichiarata; cablare ogni hop e testare riavvio/race.
- Regola ADOPTION-v4; ambito release; hazard: Nessuna evidenza immutabile di release o endpoint conforme. Rientro: Produrre e verificare artefatti con digest reali e manifesto v4, senza claim per contratti con deviazioni.

Stato dettagliato e comportamento osservato: [registro contratti](contract-alignment.md). Nessuna conformita runtime end-to-end o release v4 attestata da queste prove locali.

Regola CAP-004; ambito discovery con versioni simultanee. Rientrata il 2026-10-20: operazioni indicizzate per coppia nome/versione, lookup pubblici e dispatch esplicitamente versionati; duplicati della stessa coppia rifiutati. Rischio precedente: documento multi-versione non registrabile. Condizione di chiusura: lookup e dispatch esatti e prove multi-versione, ora presenti; dettagli nel [registro contratti](contract-alignment.md#cap-004).

## Esiti locali dell'adozione (2026-10-18)

Esecuzione offline su Windows, Rust/Cargo 1.98.1, Python 3.11.15.

| Gate | Esito |
|---|---|
| `cargo test --workspace --all-targets --all-features --offline --locked` | 357 passati, 8 ignorati; nessun fallimento |
| `cargo test -p plenora-runtime-capabilities --offline --locked` | 43 passati, incluso controllo pin finale |
| `cargo fmt --all --check` | Passato |
| Clippy workspace/all-targets/all-features, `-D warnings`, offline/locked | Passato |
| Clippy workspace/lib/all-features, `-D warnings` e unwrap/expect/panic/unreachable, offline/locked | Passato |
| `python scripts/check-contract-adoption.py --upstream C:/pg/contracts-main` | Passato: 128 copie, hash e registri; confrontate anche con i blob Git del pin |
| `python -m unittest discover -s scripts -p 'test_contract_adoption.py'` | 5 passati, inclusi diagnostica senza dati e controlli di fallimento |
| `python C:/pg/contracts-main/tools/validate_specs.py` | Passato: 26 schemi, 10 cataloghi, 3 binding, 18 archi, 236 vettori |
| Regressioni upstream mirate | 136 passate; moduli elencati sotto |
| Regressioni upstream complete `test_*.py` | Interrotte dopo circa 7 minuti durante prove estese: nessun esito complessivo positivo attribuito |
| Budget dimensione | Nessun budget aggregato locale trovato; nessun tetto alzato. Il limite Clippy di 100 righe/funzione resta attivo |
| Seconda lettura Codex | Completata su codice critico e test, nessun rilievo bloccante |

Moduli upstream eseguiti: `test_conformance_checks`, `test_runtime_vectors`,
`test_runtime_negotiation`, `test_runtime_probes`, `test_surface_side_effects`,
`test_unknown_commit`, `test_intentional_absences`, `test_interop_vectors`,
`test_messages`, `test_gate_fuzz`. Con `PYTHONDONTWRITEBYTECODE=1` e il percorso
`C:/pg/contracts-main/tools` in `sys.path`, si caricano con
`unittest.defaultTestLoader.loadTestsFromNames` e si eseguono con
`unittest.TextTestRunner`; l'uscita e non zero se `wasSuccessful()` e falso.
Il validatore normative e questi test non attestano il comportamento di un
endpoint runtime-tools: i relativi limiti restano registrati sopra.

Non eseguiti: CI Linux/macOS e remota Windows, NATS reale (test ignorati),
coverage, campagne libFuzzer, cargo audit/deny, build rustdoc e guardia upstream
di immutabilita con verifica di origin/main remoto (vietata la rete).
Il manifesto v4 di artefatti rilasciati non e stato prodotto: e una deviazione,
non un gate passato. Nessun commit o push.
