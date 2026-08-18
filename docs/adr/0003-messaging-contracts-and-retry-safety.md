# ADR 0003: Broker-neutral messaging contracts and retry safety

- Status: accepted
- Date: 2026-08-15

## Context

The runtime needs portable message identity, propagation metadata, delivery acknowledgement,
retry, dead-letter, and replay contracts. Concrete brokers differ in all of these areas. Payload
serialization is also an application choice and must not become an accidental framework mandate.

Publication can fail with an unknown remote outcome. Automatically retrying that state can
duplicate an effect, while treating it as success can lose a message.

## Decision

The messaging crate:

- represents encoded payloads as media type plus opaque bytes and requires codecs at the boundary;
- stores binary-safe metadata under validated portable namespaced keys;
- owns the acknowledgement capability inside each delivery, so ACK or NACK consumes the delivery;
- reports broker features through explicit capabilities rather than inferred guarantees;
- classifies retryable, permanent, dead-letter, and unknown-outcome errors separately;
- uses validated capped exponential backoff with optional seeded deterministic jitter;
- refuses to retry unknown outcomes unless configuration explicitly opts in.

Debug output exposes identities, sizes, and keys, but redacts payload and metadata values.

## Consequences

Adapters translate their native semantics without leaking broker types into application code.
Applications choose codecs and metadata namespaces. Deterministic jitter makes policy tests
repeatable. A caller that enables retry for unknown outcomes owns the corresponding idempotency
and duplication risk.
