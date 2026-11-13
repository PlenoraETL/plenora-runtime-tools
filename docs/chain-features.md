# Inventario delle funzionalità catene

Vista generata da [chain-features.json](chain-features.json), versione 1. Le celle **specificata** rimandano a [CHAIN-PLAN-2.0](specs/CHAIN-PLAN-2.0.md), bozza locale; **provata su fixture** attesta solo l'ambito statico Rust descritto nella cella JSON, senza host o adapter reali. Le altre restano **da fare**. P/R/L/E restano requisiti delle superfici complete.

Fonte: PROGETTO-CATENE del 12 novembre 2026, sezioni indicate per riga. Perimetro e decisioni: [ADR 0008](adr/0008-chain-product-scope.md). Le operazioni sono proposte da ratificare e provare sulle discovery effettive.

Owner indica la responsabilità della funzionalità; CLI e wheel possiedono i rispettivi binding secondo il grafo architetturale. Le estensioni database condizionate restano lavoro esplicito di contratto se necessarie al catalogo prodotto: non sono promesse della prima release. Parallelismo DAG, continue-on-error, streaming fra step e backend embedded sono profili successivi distinti, non opzioni abilitate inizialmente. L’APP mantiene compilatore canvas e inventario legacy; le righe APP provano il supporto RT ai piani, non una migrazione automatica.

Ogni cella richiede quattro prove: positiva (P), rifiuto (R), limite (L), effetto (E). Il percorso di ogni superficie si applica separatamente a tutte e quattro le prove della riga; non basta un test del codec. I dettagli sono conservati anche in ogni cella JSON.

| Superficie | Percorso di prova obbligatorio |
|---|---|
| rust | API pubblica Rust in-process, async e facciata blocking; stesso dispatcher reale |
| cli | binario plenora-runtime installato, modalità locale e remota; JSON/exit code |
| python_sync | wheel plenora_runtime installata, Client sync nativo e remoto; GIL e interruzione |
| python_async | wheel plenora_runtime installata, AsyncClient nativo e remoto; await/async iterator e heartbeat event loop |
| runtime | raw ingress boundary/codec/dispatcher, adapter e sink; PostgreSQL e NATS/Apalis reali, header e settlement |

| ID / funzione / owner / fonte | Rust | CLI | SDK sync | SDK async | Runtime |
|---|---|---|---|---|---|
| discovery — Discovery effettiva / `plenora-runtime-chain-host` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| validate — Validazione preventiva / `plenora-runtime-chains` / E.1 | provata su fixture (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| submit — Avvio durevole / `plenora-runtime-chains` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| run — Esecuzione con attesa / `plenora-runtime-chains` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| status — Stato / `plenora-runtime-chains` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| result — Risultato / `plenora-runtime-chains` / E.1 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| events — Eventi con cursore durevole / `plenora-runtime-chains` / E.1 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| cancel — Cancellazione esplicita / `plenora-runtime-chains` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| resume — Ripresa selettiva / `plenora-runtime-chains` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| artifact — Recupero artefatti / `plenora-runtime-chain-resources` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| report — Recupero rapporti / `plenora-runtime-chain-resources` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| data.catalog@2 — data.catalog@2 / `plenora-runtime-adapter-data` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| data.describe@1 — data.describe@1 / `plenora-runtime-adapter-data` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| data.validate@2 — data.validate@2 / `plenora-runtime-adapter-data` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| data.run@2 — data.run@2 / `plenora-runtime-adapter-data` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| data.run@3 — data.run@3 / `plenora-runtime-adapter-data` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.test_connection@1 — database.test_connection@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.list_catalogs@1 — database.list_catalogs@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.list_schemas@1 — database.list_schemas@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.list_objects@1 — database.list_objects@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.describe_object@1 — database.describe_object@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.read@1 — database.read@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.query@1 — database.query@1 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database.write@2 — database.write@2 / `plenora-runtime-adapter-database` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io.catalog@2 — io.catalog@2 / `plenora-runtime-adapter-io` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io.inspect@1 — io.inspect@1 / `plenora-runtime-adapter-io` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io.layers@1 — io.layers@1 / `plenora-runtime-adapter-io` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io.read@2 — io.read@2 / `plenora-runtime-adapter-io` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io.write@2 — io.write@2 / `plenora-runtime-adapter-io` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io.convert@1 — io.convert@1 / `plenora-runtime-adapter-io` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.test@1 — storage.test@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.list@1 — storage.list@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.stat@1 — storage.stat@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.get@1 — storage.get@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.put@1 — storage.put@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.copy@1 — storage.copy@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| storage.delete@1 — storage.delete@1 / `plenora-runtime-adapter-storage` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest.test@1 — rest.test@1 / `plenora-runtime-adapter-rest` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest.generate@1 — rest.generate@1 / `plenora-runtime-adapter-rest` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest.enrich@1 — rest.enrich@1 / `plenora-runtime-adapter-rest` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest.download@1 — rest.download@1 / `plenora-runtime-adapter-rest` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest.upload@1 — rest.upload@1 / `plenora-runtime-adapter-rest` / B.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| plan-canonical — Piano chiuso, digest e assi di versione / `plenora-runtime-chain-contracts` / B.1 | provata su fixture (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| dag-ports — DAG, porte e dipendenze di controllo / `plenora-runtime-chains` / B.1/B.7/C.1 | provata su fixture (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| admission — Ammissione e barriere schema / `plenora-runtime-chains` / B.7 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| budgets — Budget cumulativi e prenotazioni / `plenora-runtime-chains` / B.4 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| deadlines — Deadline e timeout di attesa distinti / `plenora-runtime-boundary` / B.4/D.2/E.1 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| loss-consent — Consenso circoscritto e conferma unica / `plenora-runtime-chains` / B.5 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| loss-reports — Rapporti perdite, fidelity e parzialità / `plenora-runtime-chains` / B.5/B.6 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| result-aggregation — Esiti completi e assi indipendenti / `plenora-runtime-chains` / B.6 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| arrow-handoff — Handoff Arrow e identità dei campi / `plenora-runtime-chain-conversions` / B.3 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| primary-geometry — Primary Geometry per provenienza / `plenora-runtime-chain-conversions` / B.3/E.2 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| storage-io — File pipeline STORAGE verso IO / `plenora-runtime-chain-resources` / B.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest-arrow — Conversione esplicita REST verso Arrow / `plenora-runtime-chain-conversions` / B.3/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| arrow-rest — Conversione esplicita Arrow verso REST / `plenora-runtime-chain-conversions` / B.3/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| resolvers — Resolver autorizzati e segreti / `plenora-runtime-chain-resources` / A.1/B.7/D.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| artifacts-lifecycle — Manifest, spool, lease e cleanup / `plenora-runtime-chain-resources` / B.3/C.2 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| durable-state — Journal PostgreSQL, CAS e fence / `plenora-runtime-chain-store` / C.3/D.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| idempotency — Inbox e idempotenza scoped / `plenora-runtime-chain-store` / C.4/D.2 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| retention — Retention e tombstone / `plenora-runtime-chain-store` / C.4/G | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| cancellation-bridge — Stop dei binding e lavoro sincrono / `plenora-runtime-chains` / C.5 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| negotiation — Negoziazione end-to-end / `plenora-runtime-boundary` / D.1/D.2/D.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| terminal-settlement — Errori terminali, ledger e outbox / `plenora-runtime-boundary` / D.1/D.2/D.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| surface-behavior — Comportamento CLI e SDK / `plenora-runtime-chain-host` / E.1 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| isolated-host — Profilo host isolabile / `plenora-runtime-chain-host` / A.1/G | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io-data-io — IO → DATA → IO / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io-data-database-io — IO → DATA → database → IO / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database-data-io — database → DATA → IO / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| io-database — IO → database / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database-io — database → IO / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database-roundtrip — database andata/ritorno / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| rest-data — REST generate → Arrow → DATA / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| enrich-roundtrip — Arrow → REST enrich → Arrow → join DATA / `plenora-runtime-chain-host` / E.2/E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| app-estrai — Supporto al piano APP estrai / `plenora-runtime-chain-host` / E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| app-manipola — Supporto al piano APP manipola / `plenora-runtime-chain-host` / E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| app-carica — Supporto al piano APP carica / `plenora-runtime-chain-host` / E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| app-arricchisci — Supporto al piano APP arricchisci / `plenora-runtime-chain-host` / E.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| runtime-identities — Identità, codec e riflessione runtime / `plenora-runtime-boundary` / D.2 RT-012/016/017/019/020/025 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| retry-policy — Retry limitati e recovery / `plenora-runtime-chains` / B.1/B.6/C.4 | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) | specificata (P/R/L/E) |
| host-controls — Host, readiness e drain / `plenora-runtime-chain-host` / A.1/D.1/D.3 | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database-query-arrow-extension — Estensione condizionata: query Arrow pubblica / `plenora-runtime-adapter-database` / B.2/E.3/G | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database-execute-extension — Estensione condizionata: database.execute / `plenora-runtime-adapter-database` / B.2/G | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |
| database-transactions-extension — Estensione condizionata: transazioni database / `plenora-runtime-adapter-database` / B.2/G | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) | da fare (P/R/L/E) |

## Prove richieste per riga

Ogni requisito seguente va provato su ciascuna delle cinque superfici.

### discovery

Discovery effettiva

- **positive**: capability immutabile, versioni e provider reali
- **rejection**: catalogo target senza disponibilità
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### validate

Validazione preventiva

- **positive**: report con controlli statici e rinviati
- **rejection**: ciclo, porta assente, versione non esatta
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

Ambito Rust provato: Validazione preventiva offline contro registro/discovery fixture: versioni, confini, claim Primary Geometry, consenso scoped, budget e barriere schema; fidelity esplicitamente da provare. Nessun invoke.

Resta da qualificare: Host e facciata chains, discovery reali, payload e autorizzazione autenticata, prenotazioni e fidelity Arrow effettiva; CLI/Python/runtime e qualifica Linux/Windows sullo SHA non attestati.

- Evidenza positive: [crates/plenora-runtime-chain-contracts/tests/parity.rs](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
- Evidenza rejection: [crates/plenora-runtime-chain-contracts/tests/parity.rs](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
- Evidenza limit: [crates/plenora-runtime-chain-contracts/tests/limits.rs](../crates/plenora-runtime-chain-contracts/tests/limits.rs)
- Evidenza effect: [crates/plenora-runtime-chain-contracts/tests/limits.rs](../crates/plenora-runtime-chain-contracts/tests/limits.rs)

### submit

Avvio durevole

- **positive**: RunHandle dopo conferma PostgreSQL
- **rejection**: store indisponibile
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### run

Esecuzione con attesa

- **positive**: ChainResult integrale e stessi tipi via submit più attesa
- **rejection**: errore tipizzato con risultato allegato
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### status

Stato

- **positive**: versione corrente del run autorizzato
- **rejection**: run di altro tenant
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### result

Risultato

- **positive**: lettura anche di esito fallito e rapporti completi
- **rejection**: run sconosciuto senza risultato inventato
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### events

Eventi con cursore durevole

- **positive**: ordine, paginazione e ripresa del cursore dopo restart
- **rejection**: cursore alterato o di altro run
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### cancel

Cancellazione esplicita

- **positive**: ricevuta durevole idempotente e stop confermato
- **rejection**: expected_revision stale
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### resume

Ripresa selettiva

- **positive**: riuso di step riusciti con manifest integri
- **rejection**: unknown senza evidenza autenticata
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### artifact

Recupero artefatti

- **positive**: contenuto integro autorizzato e limitato
- **rejection**: digest errato o riferimento revocato
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### report

Recupero rapporti

- **positive**: report completo anche dopo fallimento
- **rejection**: report indisponibile distinto da zero perdite
- **limit**: quota richiesta/risposta e timeout attesa finiti; nessun troncamento
- **effect**: nessun nuovo effetto durante controllo/lettura; per submit/run/cancel/resume verificare ledger, replay e assi none/committed/partial/unknown

### data.catalog@2

data.catalog@2

- **positive**: piano a uno step data.catalog@2 su libreria reale: tutti gli output e manifest nominati; run@2 distinto da run@3
- **rejection**: data.catalog@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: data.catalog@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: data.catalog@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### data.describe@1

data.describe@1

- **positive**: piano a uno step data.describe@1 su libreria reale: tutti gli output e manifest nominati; run@2 distinto da run@3
- **rejection**: data.describe@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: data.describe@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: data.describe@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### data.validate@2

data.validate@2

- **positive**: piano a uno step data.validate@2 su libreria reale: tutti gli output e manifest nominati; run@2 distinto da run@3
- **rejection**: data.validate@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: data.validate@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: data.validate@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### data.run@2

data.run@2

- **positive**: piano a uno step data.run@2 su libreria reale: tutti gli output e manifest nominati; run@2 distinto da run@3
- **rejection**: data.run@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: data.run@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: data.run@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### data.run@3

data.run@3

- **positive**: piano a uno step data.run@3 su libreria reale: tutti gli output e manifest nominati; run@2 distinto da run@3
- **rejection**: data.run@3: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: data.run@3: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: data.run@3: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.test_connection@1

database.test_connection@1

- **positive**: piano a uno step database.test_connection@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.test_connection@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.test_connection@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.test_connection@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.list_catalogs@1

database.list_catalogs@1

- **positive**: piano a uno step database.list_catalogs@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.list_catalogs@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.list_catalogs@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.list_catalogs@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.list_schemas@1

database.list_schemas@1

- **positive**: piano a uno step database.list_schemas@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.list_schemas@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.list_schemas@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.list_schemas@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.list_objects@1

database.list_objects@1

- **positive**: piano a uno step database.list_objects@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.list_objects@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.list_objects@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.list_objects@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.describe_object@1

database.describe_object@1

- **positive**: piano a uno step database.describe_object@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.describe_object@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.describe_object@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.describe_object@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.read@1

database.read@1

- **positive**: piano a uno step database.read@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.read@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.read@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.read@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.query@1

database.query@1

- **positive**: piano a uno step database.query@1 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.query@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.query@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.query@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### database.write@2

database.write@2

- **positive**: piano a uno step database.write@2 su libreria reale: query restituisce summary JSON, read Arrow; write@2 conserva WriteOutcome v3 e consenso
- **rejection**: database.write@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: database.write@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: database.write@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### io.catalog@2

io.catalog@2

- **positive**: piano a uno step io.catalog@2 su libreria reale: dataset, fidelity, primaria e pubblicazione
- **rejection**: io.catalog@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: io.catalog@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: io.catalog@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### io.inspect@1

io.inspect@1

- **positive**: piano a uno step io.inspect@1 su libreria reale: dataset, fidelity, primaria e pubblicazione
- **rejection**: io.inspect@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: io.inspect@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: io.inspect@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### io.layers@1

io.layers@1

- **positive**: piano a uno step io.layers@1 su libreria reale: dataset, fidelity, primaria e pubblicazione
- **rejection**: io.layers@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: io.layers@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: io.layers@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### io.read@2

io.read@2

- **positive**: piano a uno step io.read@2 su libreria reale: dataset, fidelity, primaria e pubblicazione
- **rejection**: io.read@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: io.read@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: io.read@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### io.write@2

io.write@2

- **positive**: piano a uno step io.write@2 su libreria reale: dataset, fidelity, primaria e pubblicazione
- **rejection**: io.write@2: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: io.write@2: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: io.write@2: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### io.convert@1

io.convert@1

- **positive**: piano a uno step io.convert@1 su libreria reale: dataset, fidelity, primaria e pubblicazione
- **rejection**: io.convert@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: io.convert@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: io.convert@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.test@1

storage.test@1

- **positive**: piano a uno step storage.test@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.test@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.test@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.test@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.list@1

storage.list@1

- **positive**: piano a uno step storage.list@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.list@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.list@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.list@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.stat@1

storage.stat@1

- **positive**: piano a uno step storage.stat@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.stat@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.stat@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.stat@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.get@1

storage.get@1

- **positive**: piano a uno step storage.get@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.get@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.get@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.get@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.put@1

storage.put@1

- **positive**: piano a uno step storage.put@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.put@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.put@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.put@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.copy@1

storage.copy@1

- **positive**: piano a uno step storage.copy@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.copy@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.copy@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.copy@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### storage.delete@1

storage.delete@1

- **positive**: piano a uno step storage.delete@1 su libreria reale: contenuto opaco, pagine/cursor e integrità
- **rejection**: storage.delete@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: storage.delete@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: storage.delete@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### rest.test@1

rest.test@1

- **positive**: piano a uno step rest.test@1 su libreria reale: envelope business completo, errori per record e job residui
- **rejection**: rest.test@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: rest.test@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: rest.test@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### rest.generate@1

rest.generate@1

- **positive**: piano a uno step rest.generate@1 su libreria reale: envelope business completo, errori per record e job residui
- **rejection**: rest.generate@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: rest.generate@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: rest.generate@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### rest.enrich@1

rest.enrich@1

- **positive**: piano a uno step rest.enrich@1 su libreria reale: envelope business completo, errori per record e job residui
- **rejection**: rest.enrich@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: rest.enrich@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: rest.enrich@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### rest.download@1

rest.download@1

- **positive**: piano a uno step rest.download@1 su libreria reale: envelope business completo, errori per record e job residui
- **rejection**: rest.download@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: rest.download@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: rest.download@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### rest.upload@1

rest.upload@1

- **positive**: piano a uno step rest.upload@1 su libreria reale: envelope business completo, errori per record e job residui
- **rejection**: rest.upload@1: selettore, contratto, parametri o provider incompatibili prima di invoke
- **limit**: rest.upload@1: limiti input/output e risorse della discovery effettiva, nessun troncamento
- **effect**: rest.upload@1: errore nativo, cancellazione e risposta persa; conservare assi provati e report senza replay cieco

### plan-canonical

Piano chiuso, digest e assi di versione

- **positive**: digest identico per forma canonica; versioni pacchetto/catalogo/operazione/payload/wire distinte
- **rejection**: duplicati, ignoti, UUID/versioni non canonici, latest e overflow
- **limit**: 1024 step, 64 porte/step, profondità 64, 1 MiB e restrizioni host
- **effect**: rifiuto prima di resolver/invoke

Ambito Rust provato: Tipi pubblici e parser chiuso Rust, duplicati e numeri canonici, limiti, CHAIN-C14N-1/SHA-256; 88 verdetti condivisi con la referenza Python.

Resta da qualificare: Host e facciata chains, discovery reali, payload e autorizzazione autenticata, prenotazioni e fidelity Arrow effettiva; CLI/Python/runtime e qualifica Linux/Windows sullo SHA non attestati.

- Evidenza positive: [crates/plenora-runtime-chain-contracts/tests/parity.rs](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
- Evidenza rejection: [crates/plenora-runtime-chain-contracts/tests/parity.rs](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
- Evidenza limit: [crates/plenora-runtime-chain-contracts/tests/limits.rs](../crates/plenora-runtime-chain-contracts/tests/limits.rs)
- Evidenza effect: [crates/plenora-runtime-chain-contracts/tests/limits.rs](../crates/plenora-runtime-chain-contracts/tests/limits.rs)

### dag-ports

DAG, porte e dipendenze di controllo

- **positive**: fan-in/out e sink senza output, ordine topologico con spareggio lessicografico
- **rejection**: cicli, sorgenti multiple per porta e output obbligatorio mancante
- **limit**: step e porte al limite e oltre
- **effect**: stop al primo errore: blocked discendenti, skipped altri; successi precedenti conservati

Ambito Rust provato: Unicita, porte nominate, riferimenti e cicli dati/controllo; ordine topologico lessicografico deterministico fino a 1024 step.

Resta da qualificare: Host e facciata chains, discovery reali, payload e autorizzazione autenticata, prenotazioni e fidelity Arrow effettiva; CLI/Python/runtime e qualifica Linux/Windows sullo SHA non attestati.

- Evidenza positive: [crates/plenora-runtime-chain-contracts/tests/parity.rs](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
- Evidenza rejection: [crates/plenora-runtime-chain-contracts/tests/parity.rs](../crates/plenora-runtime-chain-contracts/tests/parity.rs)
- Evidenza limit: [crates/plenora-runtime-chain-contracts/tests/limits.rs](../crates/plenora-runtime-chain-contracts/tests/limits.rs)
- Evidenza effect: [crates/plenora-runtime-chain-contracts/tests/limits.rs](../crates/plenora-runtime-chain-contracts/tests/limits.rs)

### admission

Ammissione e barriere schema

- **positive**: controlli rinviati espliciti, barriera sullo schema effettivo prima dei sink
- **rejection**: provisional, summary JSON usato come Arrow, discovery mutata
- **limit**: limiti documento e disponibilità sink/spool
- **effect**: zero mutazioni prima della barriera

### budgets

Budget cumulativi e prenotazioni

- **positive**: minimo piano/step/host/componente/trasporto; copie, buffer nativi, spool e righe per tentativo conteggiati
- **rejection**: budget mancanti, zero usato come infinito, step che amplia quota
- **limit**: fan-out, join espansivo, retry e fallimenti consumano quote globali
- **effect**: resource_limit con fase/effetto provati, nessun reset al retry

### deadlines

Deadline e timeout di attesa distinti

- **positive**: ManualClock; UTC Z/z T/t +00:00 accettati, emissione Z; deadline persistita
- **rejection**: -00:00, offset non zero, deadline doppia e deadline <= now
- **limit**: attesa/backoff/pubblicazione inclusi; cleanup finito separato
- **effect**: restart non rinnova deadline; timeout attesa non cancella run

### loss-consent

Consenso circoscritto e conferma unica

- **positive**: default deny; consenso step/destinazione/operazione/input/digest fino a LossConsent
- **rejection**: nuovo input/destinazione/policy senza nuova autorizzazione
- **limit**: NULL, input vuoto, valori esatti e perdita effettiva; strict/range/provider non aggirati
- **effect**: rollback confermato, committed e uncertain; rischio non equivale a perdita

### loss-reports

Rapporti perdite, fidelity e parzialità

- **positive**: risk/evaluation/effective completi in successo ed errore; scarti deliberati distinti dai filtri
- **rejection**: rapporto mancante non vale zero; consenso DB non autorizza IO/REST
- **limit**: report grande come artefatto solo se previsto dal contratto
- **effect**: elenco per step/output/regola senza sommare celle non uniche; partial mai successo

### result-aggregation

Esiti completi e assi indipendenti

- **positive**: tutti gli step anche non avviati, output, errori ordinati, budget e recovery
- **rejection**: file presente non basta per succeeded
- **limit**: quote risultati e canale minimo errore separate
- **effect**: effetto dominio, pubblicazione e consegna separati; unknown conserva commit noti

### arrow-handoff

Handoff Arrow e identità dei campi

- **positive**: oracolo schema/valori/null/ordine/field metadata/field_id/CRS; IPC esplicito fra versioni incompatibili
- **rejection**: cast, stringificazione o perdita implicita di colonne/metadata
- **limit**: IPC stream/file limitato, non caricare tutto per deduzione dal content type
- **effect**: digest byte distinto da uguaglianza semantica; pubblicazione confermata

### primary-geometry

Primary Geometry per provenienza

- **positive**: 51 vettori 0023 e vettori 0022: rename/drop/join/append/union/remap, mixed
- **rejection**: primaria invalida o richiesta assente: schema; GeoParquet senza primaria: unsupported
- **limit**: più geometrie/CRS, primaria non prima; sink mono-geometria rifiuta perdita
- **effect**: nessuna elezione implicita; none conservato e claim per ogni hop

### storage-io

File pipeline STORAGE verso IO

- **positive**: download pubblicato, digest e identificazione formato prima di IO
- **rejection**: content type dichiarato falso o file incompleto
- **limit**: spool e lease limitati
- **effect**: nessun output da temporaneo non confermato

### rest-arrow

Conversione esplicita REST verso Arrow

- **positive**: intero risultato: ordine/cardinalità/null/tipi/errori record/parzialità
- **rejection**: estrazione dei soli records senza errori
- **limit**: batch e risultati multipli entro quota
- **effect**: error envelope non diventa successo; job residui conservati

### arrow-rest

Conversione esplicita Arrow verso REST

- **positive**: contratto distinto: row identity, tipi/null, batch e riassociazione risposte
- **rejection**: mapping ambiguo o contratto non ratificato
- **limit**: batch e risposta entro quota
- **effect**: nessuna perdita di righe o errori nella riassociazione

### resolvers

Resolver autorizzati e segreti

- **positive**: scope tenant/progetto/sorgente/sink, connessioni e segreti opachi
- **rejection**: path/traversal/file:/credenziali inline, cross-tenant e revoca
- **limit**: riferimenti e accessi limitati
- **effect**: zero aperture prima di ammissione; sentinelle assenti da log/Debug/Display/source

### artifacts-lifecycle

Manifest, spool, lease e cleanup

- **positive**: artefatti immutabili con SHA-256, schema, producer/attempt e report; checkpoint durevole
- **rejection**: manifest corrotto o pubblicazione non confermata
- **limit**: fan-out fino ultimo consumer, retention e cleanup idempotente
- **effect**: recovery non perde artefatti per liberare quota; errore cleanup conserva effetto

### durable-state

Journal PostgreSQL, CAS e fence

- **positive**: intenzione prima invoke; esito/manifest/outbox in transazione locale
- **rejection**: store non autorevole o worker stale
- **limit**: race multi-host e lease scaduto
- **effect**: crash in ogni finestra C.4; commit remoto non atomico col journal

### idempotency

Inbox e idempotenza scoped

- **positive**: stessa chiave/input riusa run e risultato; identità messaggio/tentativo distinte
- **rejection**: stessa chiave/input diverso: conflict; controllo non supportato: unsupported
- **limit**: chiave vuota/oltre bound; race e restart
- **effect**: assenza esito non prova none; niente dedup remota inventata

### retention

Retention e tombstone

- **positive**: policy unica deployment coerente con broker/client; report separati
- **rejection**: identità scaduta rifiutata anche dopo resend
- **limit**: confini finestra delivery/recovery con ManualClock
- **effect**: tombstone conservati fino a epoche con rifiuto equivalente

### cancellation-bridge

Stop dei binding e lavoro sincrono

- **positive**: token a query/job/resolver; handle e prenotazione fino a fine lavoro
- **rejection**: drop future non vale stop thread
- **limit**: finestra stop finita e job residui REST
- **effect**: cancelled solo dopo arresto confermato; unknown se non provabile

### negotiation

Negoziazione end-to-end

- **positive**: 21 sequenze RT-027 e header esterni su send/retry/redrive/resend
- **rejection**: v1/downgrade implicito, header perso/alterato, record stale
- **limit**: race/restart/store indisponibile
- **effect**: zero send non ammessi; nuova destinazione rivalutata

### terminal-settlement

Errori terminali, ledger e outbox

- **positive**: dispatcher reale e wrapper: terminale unico persistito, causation/correlation valide
- **rejection**: protocol poi unsupported poi timeout; riflessione solo campi ben formati
- **limit**: limiti componente/dispatcher/sink prima e dopo commit; riserva errore
- **effect**: risposta/ACK persi ripubblicano stesso result_message_id senza nuovo dominio

### surface-behavior

Comportamento CLI e SDK

- **positive**: stessi tipi/errori, stdout JSON/JSON-lines e exit 0/2/3/4/5/6; GIL rilasciato e heartbeat async
- **rejection**: API/opzione mancante su una superficie
- **limit**: IO/DATA sincroni non bloccano event loop; client locale/remoto
- **effect**: Ctrl-C/interruzione sync/coroutine interrompono attesa; cancel remoto esplicito

### isolated-host

Profilo host isolabile

- **positive**: stessi crate e protocollo in-process e host separato
- **rejection**: catena di CLI upstream come motore implicito
- **limit**: qualifica OS/RSS e discendenti; nessun hard cap universale
- **effect**: terminazione non confermata resta tale

### io-data-io

IO → DATA → IO

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### io-data-database-io

IO → DATA → database → IO

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### database-data-io

database → DATA → IO

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### io-database

IO → database

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### database-io

database → IO

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### database-roundtrip

database andata/ritorno

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### rest-data

REST generate → Arrow → DATA

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### enrich-roundtrip

Arrow → REST enrich → Arrow → join DATA

- **positive**: stesso piano e oracolo interop per hop: schema/valori/metadata/rapporti
- **rejection**: incompatibilità di arco, provider o consenso prima del sink
- **limit**: budget globale, output multipli e soak con engine reali
- **effect**: errori, sentinelle, limite dopo effetto e commit incerto: stop senza replay cieco

### app-estrai

Supporto al piano APP estrai

- **positive**: piano risolto equivalente al canvas rappresentativo; mapping node/step/porta; identità autorizzata
- **rejection**: lacuna legacy o provider non qualificato non annunciato equivalente
- **limit**: limiti e cancel/status via task runtime esistente
- **effect**: errori globali senza nodo inventato; nessun fallback dopo possibile effetto

### app-manipola

Supporto al piano APP manipola

- **positive**: piano risolto equivalente al canvas rappresentativo; mapping node/step/porta; identità autorizzata
- **rejection**: lacuna legacy o provider non qualificato non annunciato equivalente
- **limit**: limiti e cancel/status via task runtime esistente
- **effect**: errori globali senza nodo inventato; nessun fallback dopo possibile effetto

### app-carica

Supporto al piano APP carica

- **positive**: piano risolto equivalente al canvas rappresentativo; mapping node/step/porta; identità autorizzata
- **rejection**: lacuna legacy o provider non qualificato non annunciato equivalente
- **limit**: limiti e cancel/status via task runtime esistente
- **effect**: errori globali senza nodo inventato; nessun fallback dopo possibile effetto

### app-arricchisci

Supporto al piano APP arricchisci

- **positive**: piano risolto equivalente al canvas rappresentativo; mapping node/step/porta; identità autorizzata
- **rejection**: lacuna legacy o provider non qualificato non annunciato equivalente
- **limit**: limiti e cancel/status via task runtime esistente
- **effect**: errori globali senza nodo inventato; nessun fallback dopo possibile effetto

### runtime-identities

Identità, codec e riflessione runtime

- **positive**: UUID canonici message/correlation/causation e nuovo result_message_id persistito; wire 1 e coppie binding v1/1 v2/2 distinte
- **rejection**: alias metadata, duplicati, numeri JSON, null, 01/+1/spazi e UUID non canonici; versione canonica non supportata: unsupported
- **limit**: versione numerica lunga entro bound documento; rifiuto prima di invoke/resolver
- **effect**: errore plenora-error-v1 con validate/none/never, riflette solo campi validi; replay conserva identità risultato

### retry-policy

Retry limitati e recovery

- **positive**: tentativi finiti, backoff limitato e selezione di step riprendibili secondo evidenza
- **rejection**: retry intera catena come OR degli step; chiave inoltrata a componente senza supporto
- **limit**: max_attempts, deadline e quote cumulative anche su fallimento
- **effect**: committed/partial/unknown non ritentati alla cieca; resource_limit none ha retry never

### host-controls

Host, readiness e drain

- **positive**: registry congelato, policy e discovery effettiva; profilo locale/remoto protetto identico
- **rejection**: host costruito senza store/sink/negoziazione o ingresso legacy alternativo
- **limit**: backpressure e capacità finita, canale minimo errore riservato
- **effect**: readiness degradata se terminale non consegnabile; terminale resta interrogabile e nessun ACK perde ultima copia

### database-query-arrow-extension

Estensione condizionata: query Arrow pubblica

- **positive**: solo dopo contratto upstream appropriato, dataset Arrow con filtri/bind/ordine/null provati
- **rejection**: summary JSON spacciato per dataset; annuncio senza ratifica
- **limit**: query/output limitati e streaming qualificato
- **effect**: fase/effetto preservati; nessun alias del query@1 esistente

### database-execute-extension

Estensione condizionata: database.execute

- **positive**: contratto pubblico e semantica qualificati prima di inserimento in catalogo release
- **rejection**: alias privato o setup SQL contato come prova runtime
- **limit**: limiti richiesta/risposta dichiarati
- **effect**: effetti mutanti e commit ignoto senza replay cieco

### database-transactions-extension

Estensione condizionata: transazioni database

- **positive**: begin/commit/rollback/savepoint soltanto dopo contratto upstream e cinque superfici
- **rejection**: promessa di transazione distribuita fra step o API non ratificata
- **limit**: durata/risorse/sessioni limitate
- **effect**: commit perso e rollback non confermato restano unknown
