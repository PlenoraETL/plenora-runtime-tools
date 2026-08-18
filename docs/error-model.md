# Error model

Runtime errors must preserve, when known:

- category;
- execution phase;
- remote-effect certainty;
- retry disposition;
- source error.

The concrete shared representation remains an open ADR. Implementations should avoid irreversible
public choices until the wider Plenora error vocabulary is stabilized.

The worker core currently provides these concepts through WorkerExecutionError without exposing
an engine type. The outbox relay uses a generic RelayError that preserves store and producer
sources, a partial batch report, and the failed operation. Debug output redacts concrete sources.

Capability adapters return `CapabilityFailure` with an explicit `RetryErrorClass` and
`CapabilityRemoteEffect`. `CapabilityDispatchError` distinguishes unknown routing, pre-invocation
payload rejection, and an invoked adapter failure. Concrete sources are preserved for error chains
but excluded from `Display` and `Debug`.
