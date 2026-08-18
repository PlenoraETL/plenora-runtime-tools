# ADR 0006: HTTP and observability baseline

- Status: accepted
- Date: 2026-08-16

## Context

Milestone 4 adds a reusable HTTP service boundary and common telemetry conventions. The HTTP
surface must compose with application routes without absorbing business authorization. Telemetry
must propagate across HTTP and messaging boundaries without choosing an exporter, collector, or
vendor backend. Health endpoints and diagnostics must not disclose component details, credentials,
payloads, or internal configuration by default.

## Decision

- Pin Axum 0.8.9 and tower-http 0.7.0 with default features disabled and only the HTTP server,
  JSON, Tokio, tracing, request-id, sensitive-header, and trace features required by the adapter.
- Keep Axum and Tower HTTP types inside `plenora-runtime-http`; core, messaging, worker, and outbox
  contracts remain independent of the web stack.
- Treat `x-request-id` as a transport-local diagnostic identifier and `x-correlation-id` as the
  cross-boundary operation identifier. Validate bounded incoming values, generate missing values,
  attach both to request context, and return them in response headers.
- Expose liveness and readiness as separate endpoints. Return only aggregate, redaction-safe state
  by default; detailed component messages remain internal.
- Connect HTTP admission and graceful serving to the runtime shutdown signal with a bounded grace
  period.
- Pin tracing 0.1.44, OpenTelemetry API 0.32.0, and tracing-opentelemetry 0.33.0. The observability
  crate supplies conventions and an interoperability bridge, but does not install a global
  subscriber, SDK, exporter, collector, or vendor backend.
- Keep metrics behind a small sink contract with stable instrument names and validated,
  low-cardinality attributes. Provide a no-op default so libraries never require a telemetry
  runtime.
- Redact secrets, credentials, payloads, authorization headers, cookies, and arbitrary internal
  error sources from default diagnostic output.

## Consequences

Applications retain ownership of route composition, authentication, authorization, subscriber
installation, exporter selection, and collector configuration. HTTP and messaging can share
correlation and trace context without making either core contract depend on a telemetry backend.
Backend-specific metrics and tracing adapters can be introduced later without changing the
runtime lifecycle or messaging contracts.
