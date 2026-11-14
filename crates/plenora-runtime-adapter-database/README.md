# Database adapter (S3)

Published database 11.0.0 at f4fdb9b5f09d8babe795f68e894949693aa9d0a1.
Real discovery: database.read@1 and database.write@2. read@2 is an explicit
upstream gap, not an alias. The native provider is PostgreSQL; other providers
require their own qualified host construction.

The host owns immutable, tenant-authorized configuration. `authorized_plans`
contains complete serialized plans. `artifacts` maps opaque references to byte
arrays. Upstream `operation_path` and `parameters_path` references must be
`artifact://sha256/<content digest>`; no filesystem or environment fallback.
`secret_environment` is a credential reference resolved by the host connection
map, never an environment lookup from the chain. Each write destination must
match the authorized connection destination and whole-plan consent.

Read/write ports carry exact schema digests or explicit dynamic-schema barriers.
IPC is materialized, uncompressed and capped at 1 MiB. The profile requires
32 MiB memory/spool reservations, up to 32 fields and 16 KiB schema metadata.
Conversions reject unsupported layouts and retain all accepted Arrow metadata;
there are no implicit casts. Read losses fail; write risks require both
reported fidelity and consent. Original upstream documents remain available
through the common artifact/report APIs, including uncertain writes.

Rust: `DatabaseHost::with_database` accepts the actual provider and primary.
Production config: `DatabaseHost::database(DatabaseConfig)`. CLI:
`plenora-runtime database-call CONFIG.json` reads the ordinary Request from stdin;
the named facade also accepts `--database-config CONFIG.json`. Wheel:
`Client(config, profile="database")` or `await AsyncClient.create(config,
profile="database")`. All use the same Rust dispatcher. TLS defaults on;
`connections.<credential>.allow_plaintext=true` is an explicit trusted-deployment
opt-in, used for ephemeral loopback tests only.

`cargo run -p plenora-runtime-adapter-database --example database_fixture`
emits a credential-free qualification adapter configuration and plan. It is
not a deployment authorization or proof of provider qualification.

Tests distinguish real Rust APIs with a Provider double from real PostgreSQL:
`tests/database.rs` versus `../plenora-runtime-chain-host/tests/database.rs`
(ignored locally, selected in the existing CI PostgreSQL job). Installed
CLI/wheel tests are `../plenora-runtime-chain-python/tests/test_database_real.py`.
Remaining gates and upstream gaps: docs/qualification.md and target/S3.md.

Il profilo database iniziale e tabellare: campi geometry/geography, metadata
plenora.primary_geometry (anche none) e porte primary_geometry=true sono
rifiutati esplicitamente. Nessun claim Primary Geometry pubblicizzato:
la conservazione semantica spaziale resta da qualificare, non si afferma che
il database perda questi metadati. IO resta il passo successivo previsto.
