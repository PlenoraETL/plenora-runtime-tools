# Architectural boundaries

The following constraints are normative:

- `plenora-runtime-core` must not depend on concrete adapters, Apalis, NATS, database drivers, or
  business-domain crates.
- `plenora-runtime-messaging` must remain broker-agnostic.
- `plenora-runtime-worker` must not expose Apalis types.
- `plenora-runtime-capabilities` may depend on worker and messaging but must not depend on a
  concrete Rust foundation library, broker, worker engine, HTTP framework, database driver, or
  telemetry backend.
- `plenora-runtime-outbox` must not depend on PostgreSQL or another concrete store.
- `plenora-runtime-resources` may depend on core and worker contracts but must not choose a broker,
  worker engine, database, HTTP framework, or telemetry backend.
- `plenora-runtime-scheduler` may depend on core runtime contracts and pinned backend-neutral
  cron/timezone calculators. Broker dispatch, persistence, and application commands remain behind
  public traits.
- `plenora-runtime-subprocess` owns bounded OS-process containment only; it must not know workers,
  brokers, databases, application libraries, command codecs, or business payloads.
- `plenora-runtime-control` may aggregate worker, resource, scheduler, subprocess and messaging
  contracts but must not select HTTP, a broker/worker engine, database, telemetry backend, or
  application library.
- `plenora-runtime-control-http` is an optional outward adapter. Every route requires an
  application-supplied authorizer and mutation routes require explicit enablement.
- `plenora-runtime-http` owns the Axum and Tower HTTP integration; web-framework types must not
  leak into core, messaging, worker, or outbox contracts.
- `plenora-runtime-observability` must remain backend-neutral and must not install an exporter,
  collector, vendor SDK, or process-global subscriber.
- HTTP bootstrap owns transport concerns only. Authentication and business authorization remain
  application composition concerns.
- adapters may depend inward on public Plenora contracts; core crates never depend outward on
  adapters.
- all queues, channels, payloads, and task concurrency must have explicit bounds.
- schedule registries, dispatches per tick, catch-up work, and dispatch duration must have explicit
  bounds; no occurrence backlog may be materialized as an unbounded queue.
- capability handlers are registered during startup and frozen before worker execution; runtime
  dispatch does not mutate or discover code dynamically.

The `plenora-runtime-architecture-tests` crate enforces these dependency directions and scans
production sources for abortive shortcuts.
