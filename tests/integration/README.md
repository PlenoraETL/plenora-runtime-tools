# Test di integrazione

I test di accettazione fra crate stanno in `crates/plenora-runtime-testkit/tests/milestone_one.rs`.
I test su JetStream reale stanno in `adapters/plenora-runtime-nats/tests/real_nats.rs`,
`tests/nats_worker_dlq.rs` e `tests/nats_capability_e2e.rs`; la CI li esegue tutti tramite
l'harness Docker solo su loopback in `scripts/nats-docker.ps1`.

`consumer_process.rs` prova la forma generica di incorporamento senza infrastruttura esterna: un
solo runtime pilota un worker dinamico appoggiato al broker, decodifica il routing versionato delle
capability, esegue il dispatch tramite il registro congelato, serve health/readiness HTTP, conferma
il lavoro, poi ferma entrambi i servizi tramite il segnale di arresto condiviso. L'adapter
registrato è un fake limitato senza payload.

Tenere ogni suite accanto al confine pubblico che la possiede rende utili le esecuzioni dei test a
livello di pacchetto, mentre i gate del workspace e della CI same-SHA eseguono comunque la matrice
completa.
