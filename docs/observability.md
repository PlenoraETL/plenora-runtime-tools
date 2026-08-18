# Observability

`plenora-runtime-observability` defines conventions for structured tracing, W3C-compatible context
propagation, low-cardinality metrics, and redaction without choosing a telemetry backend.

Applications own subscriber installation, sampling, exporters, collectors, storage, and dashboards.
Library code emits through a backend-neutral sink and remains valid when the no-op sink is used.

The stable metric vocabulary covers runtime tasks and shutdown, message receipt and processing,
worker lifecycle queue depth and drops, outbox relay state, broker connectivity, and consumer lag.
Lifecycle drop counters take deltas from the worker dispatcher snapshot and use only the bounded
`full` or `closed` reason vocabulary. Attribute keys and values are validated and bounded. Request,
message, and correlation identifiers belong in spans or logs, not in metric labels.

Secrets, authorization and cookie headers, broker credentials, arbitrary metadata, payloads,
dead-letter bodies, internal configuration, and concrete source errors must not be emitted by
default. Diagnostic wrappers display a fixed redaction marker unless an application makes a
separate, explicit disclosure decision.
