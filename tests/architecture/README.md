# Architecture tests

This test-only crate enforces dependency direction and production-source safety rules.

The root workspace must list tests/architecture as a member before running:

    cargo test -p plenora-runtime-architecture-tests

The tests use only the Rust standard library. They inspect manifests and production source beneath
crates and adapters. Test sources and examples are deliberately excluded from production scans.

The enforced boundaries cover broker and worker implementation leakage, database-driver leakage,
HTTP and telemetry dependencies in foundational crates, backend neutrality in observability, and
the exclusion of authentication, business, and PFM scope from the HTTP adapter. Panic-path and
secret-leakage scans mask Rust documentation, comments, and literals where those do not represent
executable code, while secret-bearing values inside formatting and logging macros remain checked.
Inline items guarded by `cfg(test)` are also excluded from production-only scans.

Workspace-wide gates additionally require exact or workspace-inherited dependency versions,
approved dependency direction, shared safety lints, `forbid(unsafe_code)` at production crate
roots, and no unsafe or abortive constructs in production source. Targeted static invariants cover
bounded queues, payload and concurrency validation, TLS-by-default NATS configuration, explicit
replay opt-in, redacted health and `Debug` views, cancellation hooks, and deterministic fault
injection hooks. Runtime task/report retention, fake-broker histories and fault scripts, and
message metadata entry/key/value/aggregate sizes must remain explicitly bounded. Every package
inherits the workspace MSRV, every Cargo-running CI job selects it, and every qualification job
depends on the workflow's explicit expected-SHA check.
