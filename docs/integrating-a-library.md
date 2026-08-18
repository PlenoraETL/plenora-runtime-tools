# Integrating a Rust library

This is the implementation checklist for connecting `data-tools`, `database-tools`, `IO-tools`, or
a future Rust library to a Plenora consumer. The libraries may evolve independently: no concrete
library becomes a dependency of `runtime-tools`.

## Ownership and shape

The final consumer owns a small adapter implementing `CapabilityHandler`. Register it once during
startup under a versioned identity such as `plenora.data-tools@v1`. The registry is then frozen and
shared by every dynamically admitted worker task.

```text
JetStream -> broker adapter -> bounded worker -> capability dispatcher -> app adapter -> library
                                      |                    |
                                      |                    +-- operation/input/error translation
                                      +-- timeout/cancellation/progress/heartbeat
```

The complete, compiling template is
[`examples/capability-adapter`](../examples/capability-adapter/README.md). Do not copy its simulated
library types into production; replace them with the real public API when that API is stable.

## Decisions required for each adapter

Before implementation, freeze these items for every operation:

| Contract | Required decision |
|---|---|
| Identity | Namespaced capability name and positive wire version. |
| Operation | Stable lowercase operation name; never infer routing from payload contents. |
| Input | Content type, schema/version, byte limit, and validation performed before invocation. |
| Output | Repository, result publisher, object store, or explicit no-result semantics. |
| Cancellation | Which concrete calls are cooperative and what happens when a future is dropped. |
| Progress | Numeric units and optional total; no paths, rows, credentials, or arbitrary text. |
| Retry | Concrete error variants mapped to `Retryable`, `Permanent`, `DeadLetter`, or `OutcomeUnknown`. |
| Effect | Proof that an error happened before any effect, or `Unknown` when proof is impossible. |
| Idempotency | Stable key, conflict behavior, transaction boundary, and stored-result behavior. |
| Secrets | Which fields must never enter `Debug`, errors, lifecycle events, labels, or DLQ reasons. |

If any result side effect is required, the adapter owns it explicitly. A successful library return
must not be silently discarded. If a database write and message publication must be atomic, use a
database-backed outbox rather than publishing from the handler transaction.

## Cancellation bridge

Check `context.shutdown` or `context.cancellation` before invoking the library. During long calls,
forward a clone of the task token when the library supports it and also race the call against
`context.cancelled()`. Dropping a Rust future is not proof that a thread, subprocess, database
statement, or remote request stopped. If cancellation can race an external effect, classify the
result as `OutcomeUnknown`; do not claim `NotStarted`.

## Minimum adapter contract tests

Each real adapter must add deterministic tests for:

1. every supported operation and content type;
2. unknown operation, malformed input, and oversize rejection before library invocation;
3. every concrete error variant and its retry/effect mapping;
4. cancellation before start and during a long-running call;
5. progress monotonicity and absence of payloads or secrets in observations;
6. result publication or persistence, including ambiguous outcomes;
7. duplicate delivery and idempotency behavior where the operation has side effects;
8. `Debug`, `Display`, and source chains without credential or payload leakage;
9. graceful drain with an active call and forced termination after the configured bound;
10. one consumer-level acceptance test through the real library adapter, not only direct calls.

Use `FakeBroker`, `ManualClock`, and the bounded lifecycle dispatcher for deterministic cases. Keep
real service tests opt-in and bounded by wall-clock time. The final consumer soak must exercise all
three real adapters together before the public API is frozen.

## Adding a fourth library

Add a new application-owned adapter and registration. Do not modify the messaging format beyond a
new versioned capability identity, and do not add the concrete dependency to core, messaging,
worker, NATS, Apalis, HTTP, observability, or outbox crates. Existing worker capacity, heartbeats,
retry, dead-letter, health, and shutdown behavior remains shared.
