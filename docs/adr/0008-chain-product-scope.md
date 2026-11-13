# ADR 0008: prodotto catene dentro runtime-tools e parità delle superfici

- Stato: accettato dal titolare; implementazione del prodotto da fare
- Data: 2026-11-12
- Ambito: P1.1 del progetto catene, base runtime-tools `07c29a7`

## Contesto

Le fondamenta esistenti instradano singole capability, ma non possiedono un
esecutore di piani ETL. Il progetto approvato `PROGETTO-CATENE.md`, letto nella
raccolta locale `C:/pg/inventario/runtime-tools/`, richiede una sola composizione
affidabile di DATA, DATABASE, IO, STORAGE e REST. La raccolta `RACCOLTA.md` nella
stessa directory distingue codice, candidati e release: non prova disponibilità
o conformità degli artefatti. Le sezioni A–H del progetto sono la base di questo
passo; il digest della fonte è registrato nell’inventario versionato.

Questo ADR sostituisce il precedente divieto documentale di orchestrazione
riferito all’intero repository. Conserva i confini generici degli ADR precedenti,
in particolare [ADR 0004](0004-milestone-one-boundaries.md).

## Decisione

Il repository possiede sia le fondamenta generiche sia un prodotto catene in
nuovi crate sopra di esse. Nessun generico acquisisce engine ETL, interpretazione
Arrow o resolver di dominio. Ownership e dipendenze dei quattordici crate
proposti sono definite in [architecture](../architecture.md). P1.1 non crea crate.

Ogni funzionalità, operazione, opzione, rapporto, controllo e conversione del
prodotto ha Rust puro, CLI, SDK Python sync, SDK Python async e trasporto runtime.
La [matrice versionata](../chain-features.md) richiede per ciascuna cella prove
positive, rifiuti, limiti ed effetti. Una superficie upstream mancante è lavoro
di wrapper o contratto, mai una cella omessa. Una sola facciata condivisa evita
divergenze di validazione, retry e aggregazione; i binding Rust diretti preservano
errori strutturati e cancellazione senza ricostruire protocolli di cinque CLI.

### Decisioni G adottate nel riesame unico del 12 novembre

| Decisione | Scelta adottata e motivazione | Condizione operativa successiva |
|---|---|---|
| Backend durevole | PostgreSQL condiviso, profilo distribuito iniziale: transazioni locali, unicità, CAS e letture autorevoli per acceptance/inbox/journal/outbox/fence/eventi/manifest. Non è il database ETL. | Driver esatto motivato e ADR di integrazione prima di introdurlo; backend embedded solo con profilo e qualifica distinti. |
| Delivery/recovery e retention | Unica policy di deployment verificata contro broker e client, retention report separata; tombstone conservati fino a un rifiuto equivalente tramite epoche. Evita riaccettazione di identità vecchie. | Durate numeriche da fissare con requisiti operativi prima di P2.2; nessun TTL arbitrario introdotto da questo ADR. |
| Isolamento e memoria | Crate diretti, Rust puro sempre disponibile e host isolabile per deployment ETL. Budget governato non equivale a hard cap RSS. | Qualificare profilo OS, parte non governata e stop dei discendenti prima del claim di memoria. |
| DAG | Sequenziale iniziale, `max_parallel_steps=1`, ordine topologico deterministico con spareggio ID lessicografico. DATA è già parallelo dentro le singole operazioni; la sua concorrenza consuma la prenotazione dello step. | Parallelismo fra rami solo come estensione esplicita con nuove prove; modalità non supportate rifiutate. |
| Errore | Stop al primo errore terminale, nessun nuovo ramo; discendenti blocked, altri non avviati skipped, successi precedenti conservati. Riduce effetti aggiuntivi dopo un fallimento. | Nessun successo aggregato parziale, compensazione automatica o retry cieco di committed/partial/unknown. |
| Prima release e APP | Prodotto catene per capacità qualificate; legacy esplicito nell’APP con inventario residuale visibile. Non si promette migrazione completa delle quattro funzioni. | Equivalenza canvas e provider da provare; nessuna riduzione della parità del prodotto catene. |
| Superfici upstream mancanti | Wrapper Rust condiviso per DATA run@3; vere lacune contrattuali come query Arrow, execute e transazioni richiedono lavoro upstream e ratifica. Nessun alias privato. | Discovery del wrapper separata da quella upstream; ratifica prima dell’annuncio. Query summary non è Arrow. |
| Perdite | Default deny; consenso circoscritto per step, destinazione, operazione/versione e input/digest, con una sola conferma informata del piano completo. | Cambio destinazione/ingresso/policy richiede nuovo consenso. Il consenso DB non autorizza perdite IO/REST/conversioni. Conservare rapporti completi anche negli errori. |
| Contratti e nomi | Adottare come proposte CHAIN-PLAN-2.0, plenora-chain-plan/result/event-v2, capability plenora.runtime-tools e chain.*@2; ratificare nuovi archi senza reinterpretare v1. | Ratifica centrale distinta da P1.1/P1.2; nomi crate modificabili prima del freeze, responsabilità no. Wire resta 1, indipendente da piano/binding/pacchetto. |
| UTC e limiti piano | Ricevere Z/z, T/t e +00:00 realmente UTC; rifiutare -00:00 e offset non zero; emettere Z. Massimi iniziali: 1024 step, 64 porte/step, profondità JSON 64, 1 MiB, restringibili dal deployment. | Parser esatto e prove di confine in P2.1; limiti strutturali da ratificare in P1.2. Nessuna normalizzazione permissiva. |
| Scheduler/Dramatiq | Prima collegare submit/status/cancel al task runtime esistente. Il compilatore canvas e le policy tenant restano APP, piano risolto ed esecuzione sono RT. | Migrazione code/scheduler in progetto separato dopo equivalenza; nessun fallback implicito dopo effetti. |

Le scelte di G sono risolte; i valori di deployment e le ratifiche elencate sono
condizioni di implementazione esplicite, non approvazioni generiche mancanti.

## Conseguenze

Il primo profilo usa handoff durevoli immutabili e spool limitato. Streaming
producer/consumer e checkpoint a metà operazione non si deducono da Arrow IPC.
Il nuovo host impone boundary e dispatcher reale; lo store registra intenzioni
ed esiti, l’outbox separa persistenza da consegna esterna. Le prove del solo host
non chiudono deviazioni dei consumer legacy. Le deviazioni RT esistenti restano
invariate fino ai passi P2.x e alla conferma integrata sul medesimo SHA.

Non si promettono atomicità distribuita, rollback complessivo, exactly-once degli
effetti remoti, snapshot comune delle fonti, hard cap RSS universale o equivalenza
automatica con canvas legacy. Le garanzie riguardano soltanto profili provati.
Se i generici non consentono la composizione prevista, il gate resta aperto:
occorre riaprire esplicitamente il vincolo col titolare, non aggirare il dispatcher.

P1.1 registra progettazione e controlli di completezza. Schema/tipi, engine,
store, adapter, CLI e wheel rimangono da realizzare e qualificare secondo i
[gate catene](../qualification.md#gate-del-prodotto-catene).
