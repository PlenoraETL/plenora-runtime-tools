# Integration tests

Cross-crate acceptance tests live in `crates/plenora-runtime-testkit/tests/milestone_one.rs`. Real
JetStream tests live in `adapters/plenora-runtime-nats/tests/real_nats.rs`,
`tests/nats_worker_dlq.rs`, and `tests/nats_capability_e2e.rs`; CI runs all of them through the
loopback-only Docker harness in `scripts/nats-docker.ps1`.

`consumer_process.rs` proves the generic embedding shape without external infrastructure: one
runtime drives a broker-backed dynamic worker, decodes versioned capability routing, dispatches
through the frozen registry, serves HTTP health/readiness, acknowledges work, then stops both
services through the shared shutdown signal. The registered adapter is a bounded payload-free fake.

Keeping each suite beside its owning public boundary makes package-level test runs useful while the
workspace and same-SHA CI gates still execute the complete matrix.
