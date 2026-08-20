# plenora-runtime-tools

Reusable Rust foundations for asynchronous services and workers in the Plenora ecosystem.
The project is infrastructure-agnostic at its public boundaries: concrete HTTP, worker, broker,
and persistence technologies live behind adapters.

## Status

The repository contains the version 0.1 core contracts, a bounded generic capability registry and
dispatcher, deterministic testkit, outbox fakes, Apalis and NATS JetStream adapters, and
backend-neutral HTTP, observability, scheduling, subprocess containment, and operational control
foundations. Public APIs are not stable yet.
The executable specification is
[`plenora-runtime-tools_Agent_Implementation_Spec_v0.1.md`](plenora-runtime-tools_Agent_Implementation_Spec_v0.1.md).

## Workspace

Core crates live under `crates/`, concrete integrations under `adapters/`, and runnable consumers
under `examples/`. See [`docs/architecture.md`](docs/architecture.md) and
[`docs/boundaries.md`](docs/boundaries.md) before adding dependencies. HTTP composition and telemetry
ownership are described in [`docs/http.md`](docs/http.md) and
[`docs/observability.md`](docs/observability.md). Per-task state, progress, deadlines, and
cancellation are specified in [`docs/task-lifecycle.md`](docs/task-lifecycle.md); worker capacity
and instance heartbeats are described in [`docs/worker.md`](docs/worker.md).
Generic process-memory pressure and reversible admission are described in
[`docs/resources.md`](docs/resources.md). Bounded one-shot, interval, timezone-aware cron,
pause/resume/manual scheduling and restart cursors are described in
[`docs/scheduler.md`](docs/scheduler.md).
Application-owned integration adapters and versioned generic routing are described in
[`docs/capabilities.md`](docs/capabilities.md); the compile-tested adapter template and its required
contract tests are in [`docs/integrating-a-library.md`](docs/integrating-a-library.md). Worker
capacity sizing, startup, runtime signals, and shutdown operations are covered by
[`docs/operations.md`](docs/operations.md).
Current qualification gates and residual security risks are tracked in
[`docs/qualification.md`](docs/qualification.md) and
[`docs/security-review.md`](docs/security-review.md).
The immutable common-contract pin, implemented request boundary, and remaining
Runtime Binding 1.0 gaps are tracked in
[`docs/contract-alignment.md`](docs/contract-alignment.md) and
[`contracts/source.json`](contracts/source.json).
Bounded load, cancellation, lifecycle, and real JetStream soak coverage is described in
[`docs/stability.md`](docs/stability.md). Coverage and fuzz qualification, including the enforced
90% line gate and six fuzz targets, is documented in
[`docs/testing.md`](docs/testing.md).
The same testing guide documents the Docker-bounded synthetic memory-retention probe and its
intentional leak control.
Generic child-process containment is documented in
[`docs/subprocess-execution.md`](docs/subprocess-execution.md); no concrete library is forced into
that mode before its real memory and cancellation behavior is measured. Payload-free operational
discovery and authorized HTTP control are described in
[`docs/control-plane.md`](docs/control-plane.md).

## Local validation

The pinned development toolchain and conservative v0.1 MSRV are Rust 1.97.1. When Rust is not
installed on the host, use the Docker helper from PowerShell:

```powershell
./scripts/cargo-docker.ps1 fmt --all --check
./scripts/cargo-docker.ps1 check --workspace --all-targets
./scripts/cargo-docker.ps1 clippy --workspace --all-targets --all-features
./scripts/cargo-docker.ps1 test --workspace --all-features
```

The NATS integration harness uses an ephemeral loopback-only container and emits connection data
as JSON. See [`tests/architecture/README.md`](tests/architecture/README.md).

Runnable compositions are under `examples/`: `worker-basic` demonstrates bounded engine-neutral
execution and retry classification; `worker-nats` binds NATS JetStream to a frozen generic
capability registry through Apalis, with dynamically admitted handler tasks, explicit maximum
concurrency, redaction-safe configuration, TLS, ACK/NAK, confirmed dead-letter routing, HTTP
health/readiness, and one coordinated drain path;
`capability-adapter` is a compile-tested template for connecting any application-owned Rust
library; `scheduler-basic` demonstrates bounded programmed dispatch and deterministic occurrence
identity; `runtime-benchmark` emits reproducible JSON for bounded worker/scheduler coordination;
`http-service` and `http-worker` demonstrate smaller HTTP-only compositions. A
deterministic cross-crate acceptance test exercises the same process shape without external
infrastructure.

## License

Proprietary and confidential. See [`LICENSE`](LICENSE).
