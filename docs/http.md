# HTTP runtime

`plenora-runtime-http` is a transport adapter around the shared runtime contracts. Applications
compose their own routes with the runtime router; authentication, authorization, rate policy, and
business error mapping remain application responsibilities.

Admission is bounded before application handlers run. The default global in-flight limit is 256
requests and the default body limit is 1 MiB; both are validated, configurable nonzero bounds.
Oversized bodies are normalized through the common redaction-safe `payload_too_large` error hook.

Every admitted request has two identifiers:

- `x-request-id` identifies one HTTP exchange. A missing value is generated; a malformed supplied
  value is rejected and never reflected.
- `x-correlation-id` identifies a logical operation across HTTP and messaging. It uses the shared
  messaging correlation identifier, is generated when absent, and is rejected when malformed.

Both values are placed in request extensions, recorded on the HTTP span, and copied to the response.
They are deliberately excluded from metric attributes because they have unbounded cardinality.

`/health` reports aggregate liveness and `/ready` reports admission readiness. Successful aggregate
states return `200`; unhealthy or not-ready states return `503`. Responses expose stable status
words only. Component names, operator messages, error sources, credentials, and configuration stay
inside the process.

Shutdown is cooperative and bounded. The runtime signal begins graceful connection draining; if the
configured grace period elapses, the adapter returns a structured timeout to its owner. The process
owner decides whether to log, supervise, or terminate after that result.
