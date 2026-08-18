# Scheduler basic

This example builds a bounded scheduler, registers one small command, and dispatches its one-shot
occurrence. A Plenora consumer replaces `ExampleDispatcher` with an application-owned dispatcher
that publishes to NATS or calls another bounded runtime boundary. The deterministic occurrence id
must become the downstream idempotency key.

Run it with:

```text
cargo run -p plenora-runtime-example-scheduler-basic
```
