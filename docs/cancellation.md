# Cancellation

Runtime cancellation is cooperative, bounded, observable, and idempotent. The initial release does
not force cancellation primitives from other Plenora foundations into one shared type.

Worker drain closes admission first. Work waiting for capacity is rejected, while already admitted
handlers receive the shared shutdown signal and may finish during the bounded grace period. A drain
timeout is observable. `RuntimeHandle` then aborts any still-supervised Tokio tasks and reports the
number that exceeded the grace period; their completion handles resolve with a redaction-safe
cancelled report. The worker executor itself owns no tasks, so an engine adapter still owns forced
handler termination and delivery requeue policy.

The broker-backed Apalis runner stops polling before its terminator starts. Deliveries that observe
shutdown through normal service admission receive a shutdown NAK. If a non-cooperative handler is
dropped after the grace deadline, its unacknowledged broker delivery remains eligible for server
redelivery after the configured acknowledgement wait.

Bridges to database, data, and I/O cancellation will be implemented behind adapters when their
contracts are stable.

Every admitted worker task now also carries a first-writer-wins `TaskCancellationToken`. A handler
can await `WorkerContext::cancelled` to distinguish runtime shutdown from task-local timeout,
lease-loss, explicit request, or execution drop. An execution deadline signals the task token,
allows the configured bounded cleanup grace, then returns a typed timeout with an explicit retry
disposition. Dropping an execution future signals `ExecutionDropped` synchronously, allowing
cooperative child operations that retained the token to stop even though their parent future no
longer exists. JetStream heartbeat exhaustion sets `LeaseLost` before dropping the handler future.

HTTP shutdown follows the same ordering: the shared runtime signal stops new admission, Axum is
asked to drain active connections, and the adapter returns a structured timeout when its configured
grace period expires. Liveness and readiness remain distinct during the transition.
