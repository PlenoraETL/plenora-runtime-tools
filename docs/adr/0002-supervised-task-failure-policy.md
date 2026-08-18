# ADR 0002: Supervised task failure policy

- Status: Accepted for runtime-core 0.1
- Date: 2026-08-15

## Context

The specification requires a critical task failure to make the runtime unhealthy and initiate
shutdown. It requires optional-task behavior to be configurable, but does not define the default
reaction to required-task failure.

## Decision

- Critical failure marks health unhealthy, marks readiness not ready, and starts shutdown.
- Required failure marks health degraded and readiness not ready, but keeps the process alive for
  diagnostics and external recovery.
- Optional failure records a report and, by default, degrades health without changing readiness.
- Optional failure can instead be ignored for health aggregation or configured to start shutdown.

Every failure remains available in task reports regardless of health policy. Panics and runtime
cancellation are converted into governed task failures.

## Consequences

Health and readiness remain distinct. A process can stay alive while refusing new work after a
required component fails. Optional policy can change without exposing an executor-specific type.
