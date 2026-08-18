# ADR 0005: Apalis and NATS adapter baseline

- Status: accepted
- Date: 2026-08-15

## Context

Milestone 2 introduces Apalis as a worker engine adapter and NATS JetStream as a concrete broker
adapter. Both libraries evolve independently from the Plenora contracts and must remain behind
adapter boundaries. Publication and acknowledgement can have an unknown remote effect, while a
Plenora delivery owns its acknowledgement capability and cannot be cloned safely.

## Decision

- Pin stable Apalis 0.7.4 with default features disabled. Enable only concurrency limiting and
  panic capture. Do not adopt the Apalis 1.0 release candidate in the initial release.
- Keep retry classification in Plenora. Do not apply a Tower retry layer to an owned delivery.
- Pin async-nats 0.50.0 with default features disabled and enable JetStream, Ring TLS, Chrono time
  conversion, and NKey credentials.
- Require TLS by default for production connections. Local plaintext use requires explicit
  configuration.
- Bind producers to an explicit subject and consumers to an existing stream and durable consumer
  by default. Infrastructure creation requires an explicit provisioning flag.
- A JetStream publish is confirmed only after its publish acknowledgement resolves. Ambiguous
  post-send failures remain OutcomeUnknown and are never retried implicitly.
- Use explicit pull-consumer acknowledgements. Positive acknowledgement waits for server
  confirmation. Retryable failures map to NAK; permanent and consumer-rejected failures map to
  TERM. Shutdown maps to a delayed NAK configured by the adapter.
- Surface controlled reconnect for credential rotation and deterministic verification. Graceful
  drain is asynchronous: after `begin_drain`, callers poll consumer streams until they end before
  completing process shutdown.
- Report capabilities conservatively: durable consumers and replay are true; ordered delivery,
  native dead-letter, and exactly-once claims remain false.
- Preserve binary Plenora metadata through a reversible textual encoding in reserved NATS
  headers. Never place raw secrets or payloads in Debug or error output.
- Compose NATS and Apalis at the application boundary through the broker-neutral
  `BrokerWorkerRunner`; neither concrete adapter depends on the other.
- Poll the broker only after Apalis concurrency readiness grants capacity. Preserve retry delay in
  `NackReason::RetryAfter` and map it to JetStream delayed NAK.
- Pin the ephemeral integration server to the official multi-architecture image
  nats:2.14.5-alpine3.22 with OCI digest
  sha256:d4ac35882ac65aff236cd65b9d3fa4d24332c681e1a85f94eedccd3cdd65b1da.

## Consequences

The worker core remains independent of Apalis and messaging remains independent of NATS.
Applications must own idempotency for at-least-once delivery and uncertain publication. Adapter
tests can exercise real JetStream behavior without making production infrastructure implicitly.
Future adoption of Apalis 1.0 or a different TLS provider requires a new compatibility review.
