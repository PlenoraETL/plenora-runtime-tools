# Stato della qualifica

## Gate del prodotto catene

[ADR 0008](adr/0008-chain-product-scope.md) estende il perimetro del repository.
Le attestazioni storiche sotto riguardano lo scheletro generico, non il nuovo host.
Il prodotto catene è progettato: tutte le celle dell’[inventario](chain-features.md)
sono da fare. La completezza delle celle non prova l’implementazione.

| Gate / regola | Ambito e rischio | Condizione di chiusura |
|---|---|---|
| P1.1 perimetro | Ownership esclusiva e cinque superfici; rischio di contaminare i generici o omettere funzionalità. | ADR e grafo registrati, inventario e test negativi verdi, nessun file Rust generico modificato. |
| P1.2/P1.3 contratti | Piano/esito/eventi, versioni, archi e validazione; rischio di annunciare bozze come contratti ratificati. | Ratifica distinta, fixture e validatore concordi, rifiuti prima degli effetti. |
| P2.1–P2.5 boundary | Nuovo host, cinque ingressi e trasporti; rischio di perdita esiti, replay e downgrade. Nessuna deviazione RT chiusa in P1.1. | PostgreSQL/NATS reali, corpus RT e conferma integrata stesso SHA; evidenze separate dai consumer legacy. |
| Policy operativa prima di P2.2 | Finestre delivery/recovery e retention report; rischio di riaccettare identità scadute. | Durate fissate dal deployment e verificate contro broker/client; tombstone conservati fino a rifiuto equivalente via epoche. |
| P1.4/P1.5 engine e facciate | Adapter scriptati non provano engine reali; rischio di dichiarare parità dal solo codec. | Stati/crash/budget/cancel/resume e tutte le funzioni da API pubblica, binario e wheel installati, SDK sync/async distinti. |
| S3/S4/S5 qualifica prodotto | Librerie/provider/OS, APP, memoria e release; rischio di estendere claim a feature assenti o legacy. | Discovery effettive, pin/digest pubblicati DB10/IO8, oracoli per hop, inventario residuale APP, CI Linux e Windows stesso SHA e gate di rilascio. |

Il grafo proposto si controlla con `python scripts/check_chain_graph.py` (Cargo
metadata risolto, offline e locked); la matrice con
`python scripts/check_chain_inventory.py`. Entrambi sono gate CI bloccanti.
Il primo non compila e non qualifica le feature/provider del futuro prodotto.


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

## Allineamento dipendenze 0.1.1: qualifica aperta

Regola AGENTS.md 7 e policy delle dipendenze; ambito: candidato locale con i
lock aggiornati descritti nel [rapporto 0.1.1](dependency-alignment-0.1.1.md).
Rischio: i gate offline Windows e le sonde differenziali non qualificano
Linux, NATS reale, coverage, fuzz o il consumatore aggregato; il database
advisory usato è fermo al commit RustSec del 2 novembre 2026.
Condizione di chiusura: eseguire i gate CI sul medesimo SHA candidato, con
Linux e Windows verdi, controlli supply-chain aggiornati, NATS reale,
coverage e fuzz smoke; verificare il consumatore con il vettore coerente
delle versioni Plenora. Nessuno dei gate omessi è dichiarato passato.

## Gate P1.2: bozze dei contratti catene

Regola: schemi chiusi e validazione di riferimento concordi sulle fixture;
v1 invariato, ratifica e implementazione separate.
Ambito: [CHAIN-PLAN-2.0](specs/CHAIN-PLAN-2.0.md), schema piano/esito/eventi/errori,
archi e inventario. Il test `scripts/test_chain_contract.py` verifica parser,
schema Draft 2020-12 e verdetti semantici distinti, incluse regressioni su
duplicati/cicli/versioni/porte/consenso/budget/campi ignoti/limiti.
Rischio residuo: la referenza non prova discovery, payload, autorizzazioni,
conservazione geometrica, pubblicazione o ripresa reali. Condizione di chiusura
successiva: P1.3 per registri/barriere effettivi, P2.x per boundary/store e S3
per adapter e ratifica centrale. CI Linux e Windows sullo SHA resta obbligatoria
prima del merge; un test locale non chiude quel gate.

## Gate P1.3: tipi e validazione preventiva Rust

Regola: rifiuto statico prima di invoke, parità delle 88 fixture e nessuna
dipendenza vietata. Ambito: nuovo `plenora-runtime-chain-contracts`, parser,
registro fixture, DAG/confini/claim/consenso/budget, esiti/eventi/errori e digest.
Prove versionate: [parità](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
e [limiti e regressioni](../crates/plenora-runtime-chain-contracts/tests/limits.rs).
Rischio: estendere l'evidenza fixture a provider, autorizzazioni, fidelity reale
o runtime. Condizione di chiusura dei gate successivi: host/adapter S3 con
barriere effettive, P2.x per boundary/store, ratifica centrale e CI sul medesimo
SHA Linux e Windows. Schema dinamico e fidelity hanno verdetti rinviati espliciti;
non sono compatibilità provate. Le sole celle Rust statiche dell'inventario sono
marcate `provata su fixture`; le altre superfici restano da qualificare.

## Gate P2.1: ammissione raw e limite della consegna

Regola: tutti i probe RT-012/016…023 all'ingresso grezzo, nessun invoke/resolver
nei rifiuti e stessa semantica nelle facciate. Ambito implementato: nuovo
[boundary](../crates/plenora-runtime-boundary/README.md), dispatcher generico
con rifiuto della deadline uguale a now e decoder NATS senza perdita di duplicati.
Le prove sono elencate in [boundary-probes.json](boundary-probes.json).

**Gate complessivo aperto.** Rischio: scambiare prove di serializzazione e
ammissione locale per pubblicazione terminale, dedup durevole o migrazione di
tutti i consumer legacy. Condizione di chiusura: cablaggio di tutte le facciate
esistenti al boundary con gestione esplicita del rifiuto, nessuna terminazione
silenziosa del runner, CAS/digest/restart per RT-022 e persistenza/replay/settlement
per RT-019/020. Il progetto assegna store a P2.2 e finalizzatore a P2.4;
queste parti non sono attestate da P2.1. CI Linux e Windows sul medesimo SHA,
NATS reale e prove del profilo di deployment restano gate separati non eseguiti
dalla suite locale del boundary.

## P2.1b: ammissione e limiti della prova

Regola RT-012/016..023: il dispatch registrato richiede il tipo ammesso dal
boundary. Ambito: API Rust, wire NATS e delivery Apalis locali. La raccolta
completa precede la migrazione del registro/dispatcher; i metadati operativi del
broker sono separati da quelli ammessi. Rischio residuo: attribuire a questa
prova la durabilità o la pubblicazione terminale, oppure estenderla ai futuri
adapter di dominio. Condizione di chiusura: P2.2 (CAS/digest, race e restart),
P2.4 (settlement terminale confermato), S3 (schemi reali), CI Linux/Windows e
profilo del broker effettivo. Questi gate rimangono aperti; nessuna garanzia
end-to-end è dedotta dalla sola parità locale delle facciate.


## CI PR 21: rifiuto del consumer NATS

Regola: nessuna Delivery esposta prima dell'ammissione di trasporto (RT-012/016..023).
Ambito: consumer JetStream operativo e replay; la parità locale del carrier wire
non copre la precedenza dei rifiuti di trasporto, descritta in
[contract-alignment](contract-alignment.md#p21b-confine-delle-facciate-esistenti).
Rischio residuo: TERM inviato tramite `ack_with` non equivale a conferma durevole
del server; i fake verificano richiesta e propagazione errori, non durabilità.
Gate aperto: real_nats e coverage su Linux con Docker, CI Linux/Windows sulla
correzione e profilo di deployment. Condizione di chiusura: esecuzione di questi
gate e qualificazione del settlement confermato in P2.4. Nessun gate reale è
attestato dalla sola prova offline della sequenza rifiuto/valido.


## Gate P2.2 PostgreSQL

Regola: race/crash/restart su backend reale, lettura autorevole e rifiuto senza
conferma. Ambito: primitive di [chain-store](../crates/plenora-runtime-chain-store/README.md),
RT-022 e RT-027. Implementazione e prove ignorate presenti, con job CI dedicato;
PostgreSQL reale non disponibile sull'host locale. Rischio: confondere test
scritti o doppio memoria con qualifica del database. Condizione di chiusura:
job PostgreSQL verde e gate Linux/Windows sul medesimo candidato; P2.3/P2.4 e
P2.5 restano necessari per claim end-to-end. Nessun gate non eseguito è passato.


## Qualifica P2.3/P2.4

Ambito: facciata prodotto, wrapper, journal PostgreSQL e relay instradato.
Le lacune locali del candidato sono chiuse nella logica e nelle prove enumerate
RT-026/027 e RT-010/019/020/024, con rilettura indipendente. Gli esiti dei gate
locali del blocco sono registrati in `target/P2-3-4.md` (artefatto locale).
Restano aperti i gate CI Linux/Windows e servizi reali: tre test integrazione
ignorati e i test PostgreSQL del crate store sono cablati in `.github/workflows/ci.yml`.
Il job PostgreSQL esegue anche il percorso combinato con NATS, con entrambi i
container effimeri e cleanup always. Non sono stati eseguiti sull'host.
Rischio: confondere prove locali con qualifica infrastrutturale o engine reali.
Chiusura: job verdi sul medesimo candidato; nessuna superficie CLI/SDK promossa.

## Gate P1.4/P1.5 sperimentale

Regola: P1.4/P1.5 richiedono macchina a stati/crash matrix completa e parita su
cinque superfici. Ambito: nuovi crate chains, chain-host, chain-cli e chain-python.
Il coordinatore P1.4b applica retry bounded e worker fenced; le facciate sono un
profilo scriptato che attraversa il boundary/dispatcher durevole in P1.4a.
Restano aperti: prenotazione completa delle risorse,
manifest/lease degli artefatti e autorizzazione reali, enumerazione RT/catalogo su ogni superficie,
opzioni finali CLI, errori di dominio finali e qualifica dei test di controllo da
wheel/binario su PostgreSQL/NATS. Iterator sync/async e paginazione CLI condividono
EventCursor Rust; le prove installate PostgreSQL sono cablate in un database CI
isolato, senza attestazione locale del backend. Il trasporto NATS del solo harness non e P2.5.

Rischio: confondere prove incrementali con chiusura dei blocchi. Condizione di
chiusura: integrazione e prove mancanti, rilettura senza rilievi bloccanti, CI
Linux/Windows sul candidato. Il resoconto locale e target/P1-4-5.md. Nessun gate
reale o non eseguito e attestato dai doppi scriptati.

Il sotto-passo P1.4c-1 implementa e prova localmente prenotazione e misura
del journal serializzato; rapporti oltre il canale intero sono leggibili a chunk.
La prova reale e installata e cablata in CI, non attestata come eseguita localmente.
Il lookup idempotente precede ora il controllo della nuova deadline; il loop del
server drena i task accettati dopo errore di pubblicazione, con regressioni locali
scriptate. Restano non qualificati abort/drop esterni del server e settlement
end-to-end del nuovo host. I gate aperti impediscono la chiusura del blocco anche
in presenza di compilazione verde.

P1.4c remains OPEN. Complete-plan confirmation is explicit and default-deny in
the scripted profile. P1.4c-1 separately covers the serialized journal only;
the resource resolver/lease/cleanup gates above remain open. Tests of immutable plan confirmation
and pre-invocation denial are incremental evidence only. Real PostgreSQL/NATS
coverage remains ignored locally and wired to the existing CI job.

## Evidenza incrementale P1.4c-2

Regola: resolver, digest, schema e fedelta si verificano prima dell'effetto.
Ambito: provider immutabile di risorse e oracoli JSON dichiaratamente fake,
comune a Rust, CLI, SDK sync/async e client remoto. I test host esercitano
riferimenti mancanti/alterati, documenti chiusi, schema statico/dinamico,
metadata/primaria/counts, consenso DB e recovery dopo cambio delle risorse.
Il corpus differenziale controlla la barriera dynamic-static in Rust/Python.
I test remote/worker PostgreSQL/NATS e quelli wheel/binario installati usano
il nuovo corpus e sono eseguiti dai job CI esistenti, ignorati localmente.
Rischio: scambiare l'oracolo fake per un decoder Arrow o una libreria reale.
Condizione di chiusura dell'adozione: prove equivalenti sui provider reali e
CI Linux/Windows verde. Lease/fan-out/cleanup restano aperti. Esiti locali
puntuali e gate non eseguiti sono registrati in target/P1-4-5.md.


## P1.4c-3: ownership, lease e cleanup del provider dichiarato fake

Regola: ownership prima dell'effetto, scadenza distinta dalla prova di stop,
nessuna cancellazione di evidenze o tombstone. Ambito: il provider di byte
scriptato registra sullo store P2.2 i buffer realmente restituiti tramite
riferimenti opachi per tenant/run. I template immutabili della configurazione
sono risorse prese in prestito; non sono output posseduti e non vengono eliminati.
Gli input tra step leggono gli stessi byte pubblicati, con controllo SHA-256.
La lettura applica clock e retention del primario anche senza sweep.

Il ledger riserva l'encoding JSON peggiore, copie di staging e handle di fan-out
contro memory/spool globali; le copie fisiche consumano anche artifact_bytes.
Admission atomica, lease per identita immutabile di tentativo, rinnovo prima
della scadenza e riconciliazione senza nuova autorita di invoke. Il worker
rinnova il fence di lavoro mentre continua a far avanzare lo step. Il provider
fake termina immediatamente dopo registrazione/rinnovo: non si attesta un
provider reale di lunga durata o un heartbeat di libreria.

Stop noto e cleanup sono parte della CAS del journal: errore prima del commit
conserva la ricevuta durevole; risposta persa dopo commit non duplica l'effetto.
I temporanei si liberano dopo stop; output/evidenze restano fino alla retention
rapporti (90 giorni massimi). A scadenza i consumer mai avviati sono marcati
abbandonati. Unknown conserva i buffer in quarantena, anche con lease scaduto:
solo riconciliazione autenticata autorizza la pulizia. Lo sweep riavviabile del
worker e maintain lavora per chiavi in batch limitati e conserva tombstone,
manifest e ricevute dopo la liberazione dei byte. Delivery resta 7 giorni massimi;
le riduzioni della policy primaria valgono anche per risorse e letture.

Prove: resources.rs, retry_worker.rs, host/tests/support/ownership.rs;
resources_tests.rs nel filtro postgres_real e remote.rs nel job PostgreSQL/NATS
gia esistente. Le prove reali restano ignorate localmente. Rischio residuo:
scambiare i buffer del doppio per ownership di file, processi o librerie reali.
Condizione di chiusura dell'adozione: adapter reali con registrazione prima
dell'effetto, rinnovo durante il lavoro, prova di stop e cleanup idempotente
qualificati su tutte le superfici. Nessuna promozione dei criteri 5-6 o della
conformita complessiva; esiti dei gate locali in target/P1-4-5.md.

Esito P1.4c-3: verde locale nel profilo fake; con c-1 e c-2 chiude localmente
il criterio 3. Suite workspace 496 passati/20 ignorati, suite installata
7 passati/5 saltati. Infrastruttura reale e CI multipiattaforma non eseguite
localmente; i limiti di adozione sopra restano aperti.


## P1.5 criterio 5: protocollo e matrice delle facciate

Regola: una sola semantica su Rust pubblico, CLI, SDK sync/async e remoto.
Ambito: sintassi chiusa con recovery observe/continue/reconcile; Response.failure
calcolato da Rust preserva categoria/fase/effetto/retry del dominio e del boundary,
oltre a contesto e identita della run nota. Gli errori di coordinazione non
attestano assenza di effetti per una run attiva. SDK costruisce fuori dal loop,
rilascia il GIL e traduce uniformemente gli errori; cancellare l'attesa non cancella
il lavoro. Interfaccia e compatibilita in [chain-interface](chain-interface.md).

La matrice eseguibile enumera 37 operazioni (anche condizionali/legacy), 18 righe
RT e 11 controlli su cinque superfici; la sottrazione di ogni cella e testata.
Per le operazioni si usano piani a uno step e provider dichiarati fake, senza
porte di dominio; i contratti solo inventariati restano fixture esplicite.
Corretto il vincolo locale che rifiutava nomi catalogati database.transaction.*:
segmenti canonici multipli, mai vuoti, con vettori Rust/Python condivisi.
Non e un cambiamento ai contratti upstream fissati ne un'attestazione librerie.

Rischio: confondere enumerazione/selezione delle facciate con qualifica della
semantica ETL o con copertura raw-ingress/crash completa. Condizione di chiusura:
adozione provider reali separata e criterio 6 con intera matrice C e controlli
remoti end-to-end. Le nuove prove PostgreSQL/NATS della wheel e del binario sono
cablati nei job esistenti e restano ignorati localmente quando manca infrastruttura.
Esiti locali e stato corrente dei criteri in target/P1-4-5.md.


## P1.5 criterio 6: controlli installati e crash matrix C

Regola: ogni riga C.4 deve avere prove eseguibili, senza confondere assenza di
risultato con assenza di effetto. Ambito: esecutore/host con adapter dichiarati
fake, piu prove PostgreSQL/NATS dei sottosistemi durevoli e del trasporto.
La fonte enumerata e [CHAIN-CRASH-MATRIX-C](specs/CHAIN-CRASH-MATRIX-C.md);
[chain-crash-evidence.json](chain-crash-evidence.json) associa tutte le sette
righe a test locali e reali ignorati. Il gate verifica fonte, righe, test,
classificazione ignored/local e cablaggio CI; omissioni e prove mancanti sono
regressioni negative. Non si attesta un process-kill end-to-end per ogni riga.

I controlli positivi installati usano CLI nominata, Client e AsyncClient sia
sul primario reale sia tramite NATS. La prova remota mantiene lease primarie
rinnovate sui run accettati finche cancel e persistito; quindi le rilascia al
worker. Verifica observe inerte, revisioni obsolete rifiutate, resume fino a
cancelled, nessun tentativo/effetto, pagine eventi complete e duplicate control
senza nuovi eventi. La barriera di scheduling e dichiarata: non si prova stop
di un provider reale. Subprocess e costruttori sync sono fuori dai loop async.

Rischio residuo: attribuire questi esiti a librerie ETL/provider reali o alla
CI non eseguita. Condizione di chiusura infrastrutturale: job esistenti verdi
sullo stesso SHA con wheel/binario installati e container effimeri; adozione
librerie reali resta successiva. Il cablaggio e compilato localmente, i test
infrastrutturali restano ignorati dove mancano i servizi. I criteri 1-6 sono
riportati separatamente in target/P1-4-5.md, con gli esiti realmente osservati.


## PR23: raccolta completa delle campagne reali

Regola: un test reale fallito non deve impedire di osservare gli altri casi.
I job NATS, PostgreSQL e coverage tentano ogni passo; ogni comando test usa
no-fail-fast, i comandi indipendenti hanno passi distinti. Il riepilogo finale
legge outcome originale (non conclusion ammorbidita), include setup/install e
cleanup, e rifiuta anche skipped/cancelled/missing. Test negativi verificano
inventario e fallimento in ciascun passo. La migrazione prova ogni prefisso del
catalogo senza arrestare la raccolta al primo database fallito.

La CI di e4979ef si e fermata prima delle altre prove reali: queste non sono
qualificate da quella esecuzione. La chiusura richiede la nuova campagna intera
sullo SHA corretto, inclusi i job reali e la soglia coverage. I gate locali non
sostituiscono quella evidenza. Dettagli correnti in target/P1-4-5.md.


## PR23 ci23c: gate infrastrutturali aperti

Sul commit 674943d le migrazioni passano; cinque prove host PostgreSQL/NATS
falliscono e la coverage e separatamente al 89.69%, sotto il 90% richiesto.
La selezione retention V3 e la diagnostica del delta richiedono una nuova campagna
reale aggregata. I test ignorati verificano anche backfill popolato, rifiuto di
metadati incoerenti senza perdita e mancata decodifica di payload non scaduti.
Rischio: attribuire ai gate locali una qualifica reale non eseguita. Chiusura:
job Linux/Windows e campagne infrastrutturali verdi sul medesimo SHA; soglia 90%
invariata. Backfill sotto timeout da qualificare anche alla dimensione operativa.


## PR23 ci23d: osservazione e coverage host

Due prove run via NATS scadono a 10 secondi senza errori SQL associati. La fixture
consente 100 secondi piu cleanup: le prove dichiarano 131000 ms di osservazione
(100000 esecuzione, 1000 cleanup, due statement SQL da 15000). Il default pubblico
rimane 10000. Una nuova prova NATS reale distingue timeout, reply tardiva, assenza
di servizio e risposta oltre il timer implicito del driver, senza replay.
Il job coverage raccoglie anche host/worker PostgreSQL-NATS e queste prove di
trasporto, senza esclusioni e senza ridurre la soglia. Database host separato
per preservare i 7/90 giorni dopo i test store che riducono la policy.
Resta aperta la conferma CI sul medesimo SHA; nessuna attestazione locale di
PostgreSQL/NATS. I log delle chiamate indicano operazione, durata e categoria,
senza valori del piano. Non si attribuisce a uno specifico costo SQL una causa
non dimostrata dai log disponibili.
