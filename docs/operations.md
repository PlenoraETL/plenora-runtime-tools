# Worker operations and capacity

Plenora workers are asynchronous handler futures created on demand. They are not a fixed pool of
operating-system processes. With `max_in_flight = 5`, zero handlers exist while idle, at most five
messages execute concurrently, and additional work remains in JetStream until capacity is
available. Tokio runtime threads execute those futures; blocking or CPU-heavy library calls must
use a separately bounded blocking/compute facility.

## Choosing `max_in_flight`

RAM alone cannot determine a safe worker count. Start from the tighter of these limits:

```text
memory_limit = floor((memory_budget - process_baseline) / peak_memory_per_task)
resource_limit = min(database_pool, remote_api_limit, file_handle_limit, subprocess_limit, ...)
safe_max_in_flight = min(memory_limit, resource_limit, measured_stable_concurrency)
```

Measure peak resident memory with representative payloads; do not use the encoded message size as
a proxy for decoded dataframes or buffers. On a 32 GiB host, reserve memory for the OS, NATS,
databases, caches, and allocator spikes. A task peaking at 500 MiB makes `5` materially different
from a task peaking at 10 MiB. Begin conservatively, run the real consumer soak, then increase one
step at a time while observing latency, memory, CPU, database pool wait, redelivery, and failures.

Keep these bounds aligned:

- worker `max_in_flight` is the application execution ceiling;
- JetStream `max_ack_pending` must be finite and normally equal to that ceiling per consumer;
- database and HTTP client pools must not allow the worker to create a larger hidden queue;
- request and message payload limits must reflect worst-case decoded memory, not available RAM;
- shutdown grace must exceed normal cooperative cancellation time but remain operationally finite;
- delivery heartbeat failure window must remain shorter than JetStream `ack_wait`.

## Startup readiness

Provision streams and durable consumers outside the worker. At startup:

1. validate every local bound and secret source;
2. connect and probe NATS;
3. bind the existing durable and verify its ACK, delivery, pending, and filter configuration;
4. construct and freeze the capability registry;
5. start the fail-closed resource monitor and obtain its first successful sample;
6. restore scheduler definitions and cursors from the application persistence adapter;
7. freeze the bounded runtime control registry and mount its router behind application
   authorization, enabling mutation routes only when operationally required;
8. start the worker runner, scheduler, and instance heartbeat as supervised tasks;
9. report ready only after all required components are ready.

A configuration mismatch is a startup failure, not a reason to mutate production infrastructure.
Resource creation and plaintext transport remain explicit local-development opt-ins.

## Runtime signals

Monitor at least:

| Signal | Operational meaning |
|---|---|
| readiness | Whether the instance may accept traffic and continue consuming. |
| worker heartbeat age | Whether the worker process and periodic monitor are alive. |
| `in_flight / max_in_flight` | Current execution saturation. |
| lifecycle queue depth/drops | Whether observational persistence or telemetry is falling behind. |
| NATS reconnects and redelivery attempts | Broker instability or handlers exceeding ownership bounds. |
| retry and DLQ dispositions | Application failure pressure; alert on changes in rate, not payload text. |
| `OutcomeUnknown` count | Effects requiring reconciliation rather than blind retry. |
| drain duration and forced stops | Whether shutdown bounds match real task behavior. |
| process RSS and pressure state | Whether admission pauses before the container limit is reached. |
| scheduler saturation/misfires/reconciliation | Whether programmed dispatch is keeping pace safely. |
| subprocess in-flight/timeouts/RSS terminations | Whether isolated operations fit their explicit bounds. |

Heartbeats and lifecycle events are observational. Their bounded handoff may drop observations under
pressure, but it must never block handler execution or determine ACK/NAK behavior.

## Shutdown runbook

One coordinated shutdown path must stop HTTP and worker admission first, then allow active handlers
to observe cancellation and drain within the configured grace period. Broker deliveries are ACKed
only after success; unfinished work is NAKed or left eligible for server redelivery according to the
adapter outcome. Finally drain the NATS connection and runtime supervisor.

If an instance repeatedly exceeds its drain bound, do not simply increase the timeout. Identify the
specific library call that ignores cooperative cancellation, add a bounded bridge for it, and test
the forced-stop/requeue path.

## Qualification cadence

Run the normal workspace suite on every change. Run real JetStream tests and the 2,048-message
bounded stability suite before merging worker, broker, retry, or shutdown changes. Run longer fuzz
and production-like soak campaigns before a release candidate and after changing any real library
adapter. See [`testing.md`](testing.md), [`stability.md`](stability.md), and
[`qualification.md`](qualification.md) for exact commands and release gates.
