# Task lifecycle

`plenora-runtime-worker` emits payload-free, ordered events for each message delivery attempt:

```text
Queued -> Running -> Succeeded
                  -> Failed
                  -> TimedOut
                  -> Cancelled(reason)
```

Each event carries only message ID, correlation ID, attempt, monotonic per-attempt sequence,
observer time, and a typed event. Progress consists of `completed_units` plus an optional non-zero
`total_units`; arbitrary messages, row data, paths, connection strings, and labels are excluded.
Automatic lifecycle heartbeats include the latest numeric progress snapshot.

`TaskLifecycleObserver` is synchronous and must be non-blocking. It is an observational boundary:
observer availability never changes ACK, NAK, retry, or handler success.
`WorkerLifecycleDispatcher` now supplies the explicitly bounded queue used by a future persistence
adapter. It drops the newest observation on saturation, exposes full/closed counters, projects
optional or required status into runtime health, and has backend-neutral metric hooks. Database or
network I/O remains forbidden inside `record`; the application owns and supervises the receiver.

`WorkerConfig` independently bounds maximum concurrency, shutdown drain, optional execution
timeout, task cancellation cleanup grace, and optional lifecycle heartbeat cadence. A timeout has
an explicit `RetryDecision`. Zero durations are rejected.

`TaskCancellationToken` preserves the first cancellation reason. Handlers should pass a clone into
cooperative database, data, or I/O adapter calls once those integrations exist. Runtime shutdown
remains a separate process-level signal so a handler can distinguish graceful drain from a task
whose lease or deadline was lost.
