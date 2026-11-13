# Confini architetturali

I vincoli seguenti sono normativi:

- `plenora-runtime-core` non deve dipendere da adapter concreti, Apalis, NATS, driver di database o
  crate del dominio di business.
- `plenora-runtime-messaging` deve restare agnostico rispetto al broker.
- `plenora-runtime-worker` non deve esporre tipi Apalis.
- `plenora-runtime-capabilities` può dipendere da core, worker e messaging ma non deve dipendere da
  una libreria Rust di fondazione concreta, broker, motore di worker, framework HTTP, driver di
  database o backend di telemetria.
- `plenora-runtime-outbox` non deve dipendere da PostgreSQL o da un altro store concreto.
- `plenora-runtime-resources` può dipendere dai contratti di core e worker ma non deve scegliere un
  broker, un motore di worker, un database, un framework HTTP o un backend di telemetria.
- `plenora-runtime-scheduler` può dipendere dai contratti di runtime di core e da calcolatori
  cron/fuso orario pinnati e neutrali rispetto al backend. Dispatch verso il broker, persistenza e
  comandi applicativi restano dietro trait pubblici.
- `plenora-runtime-subprocess` possiede solo il contenimento limitato dei processi del sistema
  operativo; non deve conoscere worker, broker, database, librerie applicative, codec di comando o
  payload di business.
- `plenora-runtime-control` può aggregare i contratti di worker, resource, scheduler, subprocess e
  messaging ma non deve scegliere HTTP, un motore broker/worker, un database, un backend di
  telemetria o una libreria applicativa.
- `plenora-runtime-control-http` è un adapter facoltativo verso l'esterno. Ogni route richiede un
  authorizer fornito dall'applicazione e le route di modifica richiedono un'abilitazione esplicita.
- `plenora-runtime-http` possiede l'integrazione HTTP di Axum e Tower; i tipi del framework web non
  devono trapelare nei contratti di core, messaging, worker o outbox.
- `plenora-runtime-observability` deve restare neutrale rispetto al backend e non deve installare
  exporter, collector, SDK di fornitori o subscriber globali di processo.
- Il bootstrap HTTP possiede solo le questioni di trasporto. Autenticazione e autorizzazione di
  business restano questioni di composizione dell'applicazione.
- gli adapter possono dipendere verso l'interno dai contratti pubblici Plenora; i crate core non
  dipendono mai verso l'esterno dagli adapter.
- tutte le code, i canali, i payload e la concorrenza dei task devono avere limiti espliciti.
- registri delle pianificazioni, dispatch per tick, lavoro di recupero e durata del dispatch devono
  avere limiti espliciti; nessun arretrato di occorrenze può essere materializzato come coda
  illimitata.
- gli handler di capability sono registrati all'avvio e congelati prima dell'esecuzione del worker;
  il dispatch a runtime non modifica né scopre codice dinamicamente.

Il crate `plenora-runtime-architecture-tests` impone queste direzioni di dipendenza e scandisce i
sorgenti di produzione alla ricerca di scorciatoie che abortiscono.


Lo strato prodotto catene è un consumer interno al repository: ownership e archi
ammessi sono in [architecture](architecture.md#strato-catene-proposto-p11).
I divieti sopra continuano ad applicarsi alle fondamenta generiche.
`scripts/check_chain_graph.py` aggiunge i confini del prodotto e la ricerca
transitiva di dipendenze ETL/Arrow nei generici, senza alterare i test Rust.

## Migrazione P2.1b

CapabilityRegistry, CapabilityRegistryBuilder e CapabilityDispatcher sono ora
esportati da plenora-runtime-boundary. CapabilityRequest resta un candidato
outbound e CapabilityMessageCodec espone solo encode. Per ricevere usare
RuntimeBoundary come decoder e handler, con discovery, AdmissionPolicy, clock e
sink espliciti. Il dispatch grezzo è privato. WorkerContext.metadata contiene
solo metadati del messaggio; le osservazioni del broker sono in broker_metadata.
Vedere [allineamento del profilo](contract-alignment.md#p21b-confine-delle-facciate-esistenti).


## API negoziata e durevole P2.3/P2.4

Classificazione delle superfici inventariate in `boundary-api.txt`:

- NegotiatedRuntime e NegotiatedDelivery: ammissione e invio canonici con
  acceptance autorevole; carrier ammesso senza accesso mutabile.
- DurableRuntime, DurableWorker, RawDelivery, SettlementRetry: coordinamento
  del journal e integrazione runner; RawDelivery non ha costruttore pubblico,
  il decoder conserva il carrier per finalizzare anche rifiuti anteriori all'ammissione.
- DeliveryPlan e RouteTable: configurazione host validata e congelata prima del
  dominio. NegotiatedForwarder usa soltanto una route di questa tabella.
- EvidenceComponent, OutcomeBudget e ComponentOutcome: adapter con evidenze;
  esiti costruiti entro il budget, autorita d'invocazione soltanto dal wrapper.
- DurableExecutionStore, DeliveryOpen, DeliveryChange, DeliveryStage e
  DeliveryJournal: SPI PostgreSQL e riduttore con CAS; nessuna autorita di
  dominio nasce da una lettura. PreparedResult e StoredMessage sono codec
  persistiti, validati dal riduttore, non ingressi alternativi al dispatcher.
- PostgresStore record_evidence/evidence_report: primitive legacy fenced;
  schema v2 impedisce doppia autorita tra legacy e nuova facciata.

I probe locali e reali ignorati sono in `tests/integration/tests/chain_delivery.rs`.
L'inventario classifica API, non attesta qualifica remota. Scope e condizioni
sono in [allineamento](contract-alignment.md).

La facciata negoziata distingue ora Storage da Negotiation: un guasto dello
store non costituisce un rifiuto di protocollo. admit resta sincrono per host
sincroni; admit_async e tutti i percorsi prodotto process/send/forward/relay
eseguono la SPI sincrona su blocking worker. L'acceptance confermata viene
validata dal contratto comune nella stessa chiamata, senza cache fra consegne.
Il nuovo ingresso admit_async non bypassa policy, deadline o discovery; il
boundary viene ricontrollato dopo l'I/O. Le nuove superfici e la categoria
Storage sono classificate nell'inventario API.
