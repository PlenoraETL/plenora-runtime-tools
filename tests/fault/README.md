# Fault-injection tests

The deterministic fault suites live with the contracts they exercise so `cargo test --workspace`
runs them without a separate harness:

- `crates/plenora-runtime-core/tests/lifecycle.rs`: panic capture, cooperative drain, bounded timeout
  and forced task cancellation;
- `crates/plenora-runtime-testkit/tests/broker.rs`: disconnect/reconnect, delayed delivery,
  acknowledgement faults, redelivery, terminal delivery, duplicate injection, bounded scripts and
  unknown publish outcomes;
- `crates/plenora-runtime-testkit/tests/milestone_one.rs`: cross-crate retry, shutdown requeue,
  terminal disposition and duplicate de-duplication flows;
- `adapters/plenora-runtime-nats/tests/real_nats.rs`: ignored-by-default real JetStream fault,
  lifecycle, and bounded reconnect-soak coverage driven by `scripts/nats-docker.ps1`;
- `tests/integration/tests/nats_worker_dlq.rs`: ignored-by-default full NATS to Apalis handler
  failure, confirmed DLQ publication, original TERM, and no-redelivery proof.
