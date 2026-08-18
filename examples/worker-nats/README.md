# NATS worker lifecycle example

This example is the generic process-shaped consumer proof of concept. It connects to an existing
NATS JetStream deployment, binds a durable pull consumer, runs an Apalis worker, and serves HTTP
health and readiness from the same runtime lifecycle. Handler futures are created on demand up to
`PLENORA_MAX_WORKERS`. Apalis waits for capacity before polling NATS, so excess messages remain in
JetStream until a slot is free. It does not create streams, consumers, or any other broker
infrastructure.

Environment:

| Variable | Required | Meaning |
|---|---:|---|
| `PLENORA_NATS_SERVERS` | No | Comma-separated URLs; defaults to `tls://127.0.0.1:4222`. |
| `PLENORA_NATS_TOKEN` | No | Bearer token. The value is wrapped in `SecretString` and never printed. |
| `PLENORA_NATS_ALLOW_PLAINTEXT` | No | Must be `true` (case-insensitive) to permit a `nats://` URL. |
| `PLENORA_MAX_WORKERS` | No | Maximum concurrent handlers; defaults to `32`, hard maximum `4096`. |
| `PLENORA_NATS_STREAM` | No | Existing stream name; defaults to `PLENORA_WORK`. |
| `PLENORA_NATS_DURABLE` | No | Existing durable consumer; defaults to `plenora-worker-nats`. |
| `PLENORA_NATS_SUBJECT` | No | Consumer filter subject; defaults to `plenora.work`. |
| `PLENORA_NATS_DLQ_SUBJECT` | No | Dedicated dead-letter subject; defaults to `plenora.work.dlq`. |
| `PLENORA_HTTP_BIND` | No | HTTP bind address; defaults to `127.0.0.1:3001`. |

The example freezes one placeholder capability at startup: `plenora.example-tools@v1`, operation
`execute`. In addition to the canonical message and correlation UUIDs, every delivery therefore
contains these portable routing keys:

| Metadata key | Example value |
|---|---|
| `plenora.capability.name` | `plenora.example-tools` |
| `plenora.capability.version` | `1` |
| `plenora.capability.operation` | `execute` |

The existing stream must retain both the operational subject and the configured dead-letter
subject. The existing durable must use explicit ACK, `max_deliver=5`, `max_ack_pending` equal to
`PLENORA_MAX_WORKERS`, and a 30-second ACK wait. Every message must contain canonical UUID metadata
keys `plenora.message.id` and `plenora.trace.correlation_id`; malformed deliveries are terminated
without invoking the handler.

Long-running handlers renew their JetStream ownership every five seconds. Three consecutive
heartbeat failures cancel the handler future and trigger a retryable NAK. The resulting 15-second
failure window is validated to remain shorter than the configured 30-second ACK wait.

Handler failures use the dedicated JetStream producer as a `DeadLetterSink`. The original delivery
is terminated only after JetStream confirms the DLQ publication. A missing sink, publication error,
or `OutcomeUnknown` leaves the original eligible for redelivery instead of silently losing it.

The example emits a payload-free worker-instance heartbeat every ten seconds and on lifecycle
transitions. It reports `instance_id`, worker name, `Starting/Ready/Draining/Stopped`, maximum
capacity, active handlers, and available slots. A production observer must hand these snapshots to
persistence or telemetry through an explicitly bounded queue.

Before moving the runner into `run()`, an embedding service can retain `runner.task_control()`.
That engine-neutral handle lists at most `PLENORA_MAX_WORKERS` payload-free active task snapshots
and requests cooperative cancellation by executor-local task ID or canonical message ID. Task IDs
must be paired with the heartbeat `instance_id` outside the process. Requested cancellation is
terminal for the current broker message; handlers must bridge the supplied cancellation token into
long-running data, database, or I/O operations when those libraries become available.

Production-style TLS:

```text
PLENORA_NATS_SERVERS=tls://nats.example.internal:4222
```

Explicit local plaintext:

```text
PLENORA_NATS_SERVERS=nats://127.0.0.1:4222
PLENORA_NATS_ALLOW_PLAINTEXT=true
```

Run from the workspace root:

```text
cargo run -p plenora-example-worker-nats
```

While the process is running, `GET /health` reports aggregate component health and `GET /ready`
reports whether this instance may keep receiving traffic. A terminal worker or HTTP failure and
Ctrl-C all enter the same bounded shutdown path: stop worker admission, drain HTTP, drain NATS,
then finish the runtime.

The registered handler is intentionally a placeholder. The future Plenora consumer will register
application-owned adapters for `data-tools`, `database-tools`, and `IO-tools` in the same bounded
registry; those libraries do not become dependencies of the generic runtime crates. Adding another
library is one additional `CapabilityHandler` registration and does not change NATS, Apalis, HTTP,
or worker code.

Server lists, URL sizes, token size, worker concurrency, payload size, pending ACKs, reconnect
attempts, operation timeouts, and shutdown drain are bounded. Console output contains identities
and lifecycle state, never message bodies or credentials.
