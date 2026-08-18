# Security review: v0.1 generic runtime skeleton

- Review date: 2026-08-16
- Scope: the complete `runtime-tools` workspace, examples, scripts, and CI configuration
- Status: technical review complete locally; organizational release approval pending
- Excluded: unfinished Plenora foundation adapters, consumer business authorization, and production
  deployment configuration

## Reviewed trust boundaries

| Boundary | Primary risks | Implemented controls |
|---|---|---|
| HTTP ingress | oversized bodies, identifier injection, secret headers, unlimited concurrency | bounded body and concurrency, validated IDs, sensitive headers, redacted common errors, graceful shutdown |
| NATS configuration and transport | credential disclosure, plaintext downgrade, unbounded reconnect or delivery state | redacted credential types, TLS required by default, explicit plaintext opt-in, finite reconnect and consumer bounds |
| Message metadata and payload | allocation abuse, secret logging, malformed identities | entry/key/value/total bounds, pre-decode encoded bounds, redacted `Debug`, canonical UUID validation |
| Worker execution | unbounded task growth, non-cooperative cancellation, stale control handles | semaphore bound, no executor-owned queue, bounded cleanup grace, RAII active-task removal, executor-local task IDs |
| Operational control | unauthenticated pause/drain/cancel/trigger, payload disclosure | application authorizer required for every HTTP route, mutation routes opt-in, validated bounded IDs, payload-free DTOs |
| Subprocess containment | shell injection, inherited credentials, runaway process/output/RSS | direct spawn without shell, cleared environment by default, bounded inputs/concurrency/output/time, tree termination and reap, optional Linux RSS ceiling |
| Lifecycle observation | handler blocking, memory growth, silent observation loss | non-blocking `try_send`, hard queue maximum, explicit full/closed counters, health projection and metric hooks |
| Broker settlement and DLQ | message loss, blind retry after unknown effect, duplicate DLQ publication | confirmed publication before TERM, deterministic DLQ message ID, fail-closed `OutcomeUnknown`, explicit ACK ownership |
| Outbox/inbox | split transactions, claim loss, duplicate business effects | persistence-neutral contracts and fakes document atomic transaction requirements; no production database claim is made |
| Observability | high-cardinality labels, PII/secret propagation | allowlisted bounded labels, payload-free lifecycle events, W3C-only transactional propagation, redaction policy |
| Supply chain | vulnerable, unlicensed, or unpinned dependencies | exact direct pins, lockfile, source policy, pinned audit/deny tools, architecture enforcement |

## Verification performed

- Workspace formatting, Clippy with warnings denied, all tests, and rustdoc warnings denied.
- Architecture scans reject unsafe blocks, panic shortcuts, secret-bearing debug output, known
  unbounded channels, reverse dependencies, git dependencies, and non-exact direct versions.
- Real ephemeral JetStream tests cover TLS/plaintext policy, metadata bounds, heartbeat renewal,
  retry/redelivery, reconnect readiness, replay isolation, confirmed DLQ, and drain.
- `cargo audit` found no advisory affecting the current lockfile.
- `cargo deny check` passed advisories, bans, licenses, and sources. Duplicate dependency versions
  remain warnings and must continue to be reviewed during upgrades.

## Residual risks and required owner actions

1. Runtime-tools deliberately implements no identity or role policy. The control HTTP adapter calls
   an application authorizer for every request and hides mutation routes by default, but the
   consumer must authenticate callers, authorize exact actions, rate-limit the route, and audit
   accepted mutations.
2. `WorkerTaskId` is executor-local. Remote APIs must pair it with the worker `instance_id` and use
   canonical `MessageId` when cross-restart identity is required.
3. Rust async cancellation cannot stop detached tasks or blocking native calls. Foundation adapters
   must bridge the supplied token and enforce their own bounded operation deadlines.
4. Lifecycle saturation drops the newest observation. A required deployment must select
   `WorkerLifecycleHealthCriticality::Required` and supervise the receiver; an optional deployment
   remains ready but degraded.
5. The in-memory outbox/inbox stores are test doubles. Production business effects are unsafe until
   the database adapter provides atomic business/outbox and business/inbox transactions, claim
   leases, crash recovery, and stored idempotency results.
6. Plaintext NATS is test/local-only and requires explicit configuration. Production deployment
   review must validate certificates, credentials, subject permissions, and infrastructure mode.
7. This review does not qualify an uncommitted working tree. Final approval must reference the
   exact commit that passed all GitHub Actions jobs.

## Release approval checklist

- [ ] Exact release commit created and pushed.
- [ ] Same-SHA Windows, Linux, macOS, dependency-policy, documentation, and NATS jobs passed.
- [ ] Consumer-specific authentication and authorization reviewed.
- [ ] Production NATS TLS and permissions reviewed.
- [ ] Any new foundation adapter included in the review scope.
- [ ] Security owner approval recorded outside the repository through the approved internal channel.
