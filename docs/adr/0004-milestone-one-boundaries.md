# ADR 0004: Milestone 1 dependency and execution boundaries

- Status: accepted
- Date: 2026-08-15

## Context

The specification's illustrative dependency diagram can be read as placing outbox below worker,
while its outbox API and recommended merge order describe publication through MessageProducer.
Worker execution also needs a bounded concurrency implementation without pre-committing the core
API to Apalis.

## Decision

- Runtime-outbox depends on runtime-messaging and publishes through MessageProducer. It does not
  depend on runtime-worker.
- Runtime-worker owns handler, context, configuration, error, retry-delegation, admission, and
  drain contracts. It owns no queue and spawns no task.
- Apalis will implement an adapter after the core worker contracts are frozen.
- Runtime-testkit depends on core, messaging, and outbox so it directly provides broker, outbox,
  inbox, and idempotency fakes. Its Milestone 1 acceptance test uses worker as a development
  dependency.
- The root workspace remains the sole dependency and lockfile integration point during parallel
  work.

## Consequences

Outbox relay can be reused with any producer and tested without a worker engine. Worker concurrency
is bounded without leaking adapter types. Parallel crate implementation remains safe as long as
shared manifests, lockfiles, and cross-cutting documentation are integrated serially.
