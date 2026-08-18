# Basic worker example

This example uses only Plenora-owned worker contracts. It demonstrates:

- a typed `WorkerHandler`;
- a bounded concurrency configuration;
- an injected exponential retry policy;
- non-blocking lifecycle handoff through a bounded in-memory dispatcher;
- redaction-safe inspection of a failed attempt;
- coordinated worker and runtime drain.

Run it from the workspace root:

```text
cargo run -p plenora-example-worker-basic
```

The example intentionally triggers one retryable failure, but prints only its stable category and
retry decision. It never prints the message payload or handler source.
The lifecycle receiver is drained explicitly in the same async flow: the dispatcher owns no
background task and reports accepted, delivered, and saturation-drop counters. The example also
projects the final dispatcher state into runtime health using the optional criticality policy.
