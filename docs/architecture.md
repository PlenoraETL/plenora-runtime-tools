# Architecture

`plenora-runtime-tools` provides reusable process and service foundations. It does not orchestrate
Plenora data, database, or I/O libraries and contains no business logic.

The implemented layering is:

```text
runtime-worker  ----> runtime-core
       +-----------> runtime-messaging

runtime-capabilities ----> runtime-worker
             +----------> runtime-messaging

runtime-outbox  ----> runtime-messaging

runtime-testkit ----> runtime-core
       +-----------> runtime-messaging
       +-----------> runtime-outbox
       +-----------> runtime-capabilities

runtime-apalis  ----> runtime-worker
       +-----------> runtime-messaging

runtime-nats    ----> runtime-core
       +-----------> runtime-messaging

runtime-http    ----> runtime-core
       +-----------> runtime-messaging

runtime-observability ----> runtime-messaging

runtime-resources ----> runtime-core
       +-------------> runtime-worker

runtime-scheduler ----> runtime-core

runtime-subprocess ----> Tokio process primitives only

runtime-control ----> runtime-messaging
       +-----------> runtime-worker
       +-----------> runtime-resources
       +-----------> runtime-scheduler
       +-----------> runtime-subprocess

runtime-control-http ----> runtime-control + Axum
```

The outbox relay publishes through the messaging producer contract and does not depend on the
worker. The testkit depends on core, messaging, and outbox so it can provide all first-class fakes.
It also depends on capabilities and worker to provide the payload-free generic adapter fake.

The capabilities crate is an application integration layer above worker and messaging. It owns a
bounded startup registry, portable versioned routing, and an engine-neutral dispatcher. Concrete
Rust libraries remain dependencies of the embedding Plenora consumer and are wrapped by small
application-owned adapters; they never become dependencies of runtime crates.

The resources crate observes process pressure and controls only the worker's reversible admission
gate. The scheduler depends only on time and shutdown contracts plus backend-neutral cron/timezone
calculation; dispatch side effects and persistence remain application-owned boundaries. The
subprocess crate owns containment but no application protocol. The control crate is an inward-only
aggregation layer; its separate HTTP adapter owns transport and requires application authorization.

The HTTP package is an inbound adapter despite living with the reusable crates: Axum and Tower
types stop at that package boundary. The observability package defines stable span, propagation,
metric, and redaction conventions. Applications install concrete subscribers and exporters.

Tokio, Tower, Apalis, NATS JetStream, Axum, and database drivers are implementation choices rather
than consumer API concepts. Material decisions that are not settled by the specification require
an ADR.

The Apalis adapter also provides a broker-neutral `BrokerWorkerRunner`. Applications compose a
`MessageConsumer` such as `JetStreamConsumer` with that runner at startup. This keeps the NATS
adapter independent of worker mechanics while allowing Apalis readiness to apply backpressure
before the next broker delivery is pulled.
