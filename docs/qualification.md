# Qualification status

This document separates the v0.1 runtime skeleton, release qualification, and consumer API freeze.
Passing local tests does not by itself qualify a commit or prove that a Plenora consumer is ready.

## Skeleton Definition of Done

| Requirement | Status | Evidence or remaining condition |
|---|---|---|
| Workspace and dependency direction | Complete locally | Architecture tests validate every internal edge and implementation boundary. |
| Core, messaging, worker, capabilities, outbox traits, and testkit | Complete locally | Workspace tests cover lifecycle, generic extension routing, retry, deduplication, faults, and bounded memory. |
| Apalis and NATS JetStream adapters | Complete locally | Fake broker tests and opt-in real JetStream tests cover ACK/NAK, DLQ, replay, health, and drain. |
| Mandatory fake end-to-end scenarios | Complete locally | Success, retry/redelivery, permanent failure, duplicate suppression, graceful and forced shutdown, and `OutcomeUnknown` are covered. |
| Bounded stability suite | Complete locally | Sustained 2,048-message backpressure, 64-task cancellation, 100 worker lifecycles, and a 256-message real JetStream reconnect soak are automated; final consumer soak remains an API-freeze gate. |
| Coverage and fuzz qualification | Complete locally | `cargo-llvm-cov 0.8.7` enforces at least 90% line coverage over the runtime workspace plus real JetStream paths; six bounded `cargo-fuzz 0.13.2` targets cover portable parsing, retry, NATS configuration, propagation, cron/control IDs, and subprocess specifications. |
| Synthetic memory qualification | Complete locally | Deterministic contracts prove task-owned allocation release; a 1 GiB Docker soak validates allocator plateau, fragmentation, concurrency, error/cancellation cleanup, and an intentional growing-memory control. Real library memory behavior remains an API-freeze gate. |
| Disconnect readiness transition | Complete locally | The real-NATS test observes `NotReady` during forced reconnect and `Ready` after recovery. |
| HTTP and observability hooks | Complete locally | HTTP lifecycle/security bounds and backend-neutral spans, propagation, metrics, and redaction are tested. |
| Worker heartbeat and active-task control | Complete locally | Instance lifecycle, bounded capacity, cancellation by task or message ID, and broker settlement are tested. |
| Memory pressure admission control | Complete locally | Linux RSS sampling, soft/hard states, fail-closed readiness, hysteresis, and 1,000 recovery cycles are deterministic. |
| Generic programmed scheduling | Complete locally | One-shot/interval plans, extensible plan trait, bounded misfires/catch-up, runtime registration, reconciliation, restart cursor, and shutdown are tested. |
| Cron and scheduler control | Complete locally | Explicit IANA timezone/DST behavior, deterministic no-drift jitter, durable pause/resume, and bounded manual trigger are tested. |
| Generic subprocess containment | Complete locally | Direct spawn without a shell, bounded admission/output, timeout/cancellation/tree termination, reap, Linux RSS ceiling, and capacity reuse are tested; no application protocol is claimed. |
| Generic operational control | Complete locally | Frozen bounded registry, worker/task/scheduler/resource snapshots and mutations are tested; HTTP is always application-authorized and mutation routes are opt-in. |
| Lifecycle persistence handoff | Complete as a boundary | The bounded dispatcher exposes drops, health projection, and metric primitives; no database persistence is claimed. |
| Documentation and examples | Complete for the generic skeleton | The process-shaped NATS consumer composes versioned capability routing, worker, HTTP health/readiness, heartbeats, and coordinated drain; a separate compile-tested template defines the future concrete-library adapter contract. |
| Dependency audit and policy | Complete locally | Pinned `cargo-audit 0.22.2` and `cargo-deny 0.20.2` pass; permitted duplicate-version warnings remain reviewable. |
| Security review | Technically complete, release approval pending | See [`security-review.md`](security-review.md). Organizational approval must reference the exact commit. |
| CI on Windows, Linux, and macOS | Pending commit | The workflow exists, but no local working tree can claim CI status before a commit is pushed and qualified. |

## Mandatory scenario matrix

| Scenario | Fake/in-process | Real infrastructure |
|---|---:|---:|
| Worker success and ACK | Yes | Yes, through NATS, typed capability routing, worker and ACK |
| Retry, redelivery, success, ACK | Yes | Yes, including fail-once capability handling |
| Permanent failure and confirmed DLQ | Yes | Yes |
| Duplicate delivery and deduplicated effect | Yes | Persistence-backed deduplication waits for the database adapter |
| Graceful shutdown and drain | Yes | Yes |
| Forced shutdown and requeue | Yes | Yes, with a replacement worker on the same durable |
| Disconnect, `NotReady`, reconnect, `Ready` | N/A | Yes |
| Publish `OutcomeUnknown` without blind retry | Yes | Fault injection; a real network cannot deterministically prove the remote effect |

## API freeze gates

The public API remains pre-1.0. These gates from the implementation specification are still open:

1. a real Plenora consumer using the Rust API (the generic process-shaped example still registers a placeholder capability adapter);
2. a PFM microservice proof of concept;
3. transactional outbox/inbox persistence through `database-tools`;
4. cancellation bridges into `data-tools`, `database-tools`, and `IO-tools`;
5. durable schedule definitions/cursors and lifecycle observations through `database-tools`;
6. same-SHA CI and security approval for the release candidate.

None of these open gates should be hidden by marking the generic skeleton complete.

## Local qualification commands

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
RUSTDOCFLAGS=-Dwarnings cargo doc --workspace --all-features --no-deps --locked
cargo audit
cargo deny check
```

Real NATS qualification additionally runs both ignored JetStream suites against the pinned,
ephemeral container from `scripts/nats-docker.ps1`.

The CI coverage job merges normal workspace, real NATS, and NATS-to-Apalis profiles before applying
`cargo llvm-cov report --fail-under-lines 90`. The fuzz smoke job runs every corpus with the pinned
`nightly-2026-08-01` toolchain. See [`testing.md`](testing.md) for scope and local commands.
