# Test di iniezione dei guasti

Le suite deterministiche di guasti stanno accanto ai contratti che esercitano, così
`cargo test --workspace` le esegue senza un harness separato:

- `crates/plenora-runtime-core/tests/lifecycle.rs`: cattura dei panic, drain cooperativo, timeout
  limitato e cancellazione forzata dei task;
- `crates/plenora-runtime-testkit/tests/broker.rs`: disconnessione/riconnessione, consegna
  ritardata, guasti di acknowledgement, riconsegna, consegna terminale, iniezione di duplicati,
  script limitati ed esiti di pubblicazione sconosciuti;
- `crates/plenora-runtime-testkit/tests/milestone_one.rs`: flussi fra crate di retry, rimessa in
  coda all'arresto, disposizione terminale e de-duplicazione dei duplicati;
- `adapters/plenora-runtime-nats/tests/real_nats.rs`: copertura, ignorata per impostazione
  predefinita, di guasti, ciclo di vita e soak di riconnessione limitato su JetStream reale,
  pilotata da `scripts/nats-docker.ps1`;
- `tests/integration/tests/nats_worker_dlq.rs`: prova completa, ignorata per impostazione
  predefinita, da NATS all'handler Apalis di fallimento dell'handler, pubblicazione confermata
  nella DLQ, TERM dell'originale e assenza di riconsegna.
