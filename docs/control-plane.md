# Runtime control plane

`plenora-runtime-control` aggregates bounded, payload-free handles already owned by an application.
It does not discover processes or libraries dynamically. A startup builder registers a finite
number of named workers, schedulers, memory monitors and subprocess supervisors, then freezes the
registry.

The generic API supports:

- component discovery;
- worker admission/capacity and bounded active-task snapshots;
- reversible worker pause/resume, terminal drain, and cooperative cancellation by local task ID or
  canonical message ID;
- scheduler cursor snapshots, pause/resume, and bounded manual trigger;
- process-memory pressure state;
- subprocess capacity and lifecycle counters.

No task payload, subprocess argument/environment value, handler error text, credential or command
result is exposed. `WorkerTaskId` is local to one executor lifetime, so an external system must pair
it with the registered worker/instance identity. Cancellation remains cooperative; the endpoint is
not a thread-kill primitive.

`plenora-runtime-control-http` converts those contracts into composable Axum routes below
`/runtime/control`. Construction always requires an application-owned `ControlRequestAuthorizer`.
It receives request headers plus a validated action/component/target and must fail closed without
logging raw headers. The adapter starts read-only; pause, resume, drain, cancel and manual-trigger
routes return 404 until `enable_mutations()` is called explicitly.

Manual trigger accepts a bounded JSON object containing `triggered_at_unix_ms`. That timestamp is
the deterministic occurrence identity: a caller retrying after an uncertain HTTP result must reuse
the same value, and the downstream dispatcher must use the occurrence identity as its idempotency
key. The adapter never silently replaces it with a new server timestamp.

Plenora should mount the router only on an internal listener or protected administrative route,
apply its real identity/role policy in the authorizer, retain HTTP request-rate/concurrency limits,
and audit accepted mutations outside the synchronous authorization callback. Runtime-tools does
not implement users, roles, tokens, persistence, or business authorization.
