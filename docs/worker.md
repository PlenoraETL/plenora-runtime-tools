# Worker execution

Worker APIs expose Plenora-owned handler and context types. Concurrency and channels are bounded,
retry policy is injected, and shutdown stops intake before draining in-flight work.

WorkerExecutor owns no queue and starts no tasks. A semaphore bounds caller-driven handler futures.
Jobs waiting for capacity observe both runtime shutdown and drain. Admitted work completes
cooperatively until the configured grace period. A worker drain timeout reports the remaining count;
because the engine-neutral executor owns no task handles, forced termination and delivery requeue
remain the concrete engine adapter's responsibility. Tasks spawned through `RuntimeHandle` are
aborted by the core supervisor after its own grace period expires.

Admission has three states: `Accepting`, reversible `Paused`, and terminal `Draining`. Resource
pressure uses `Paused`: new jobs are rejected before handler invocation with a retryable adapter
disposition, while active handlers continue. Recovery may reopen only a paused gate; drain can never
be reversed.

Handler failures preserve their source while exposing stable category, phase, remote-effect
certainty, and retry decision. The engine adapter remains responsible for mapping those decisions
to ACK, NACK, retry, or dead-letter behavior.

`plenora-runtime-apalis` is the concrete engine bridge. It wraps a `WorkerExecutor` in a cloneable
Tower service, configures Apalis concurrency and panic capture, and connects the Apalis monitor
signal to Plenora's admission stop and bounded drain. Retry decisions remain explicit adapter
outcomes: the bridge never clones an owned delivery.

`BrokerWorkerRunner` composes that service with any broker-neutral `MessageConsumer`. The Apalis
concurrency layer becomes ready before the consumer stream is polled. With `max_in_flight = 5`, no
handler task exists while the queue is idle, at most five delivery futures run concurrently, and
additional messages remain in the broker until a slot is released. A successful handler maps to
ACK, `RetryAfter` to a broker-native delayed NAK, `DoNotRetry` to permanent NAK/TERM, and shutdown
admission to shutdown NAK. `DeadLetter` requires an explicitly attached `DeadLetterSink`: the
record is published first and the original receives TERM only after broker confirmation. Canonical
metadata decoding requires stable message and
correlation UUIDs before a handler can run. A terminal consumer polling failure stops the monitor
and is returned with its concrete source rather than being treated as a clean queue shutdown.

Deliveries may carry a validated heartbeat policy. While the handler future is active, the broker
runner renews ownership without consuming the delivery. NATS maps renewal to JetStream `+WPI`.
The heartbeat failure window must be shorter than the durable consumer's `ack_wait`. Successful
renewals reset the consecutive-failure counter. Exhausting the failure budget drops the handler
future, attempts a retryable NAK, and reports heartbeat and settlement failures separately. Dropping
an async handler future does not stop detached work or an already-running blocking operation; those
operations must use their own cooperative cancellation bridge.

Worker-instance heartbeat is a third, independent signal. `WorkerInstanceHeartbeatReporter`
samples the executor's live bounded capacity and emits stable process/worker identity, monotonic
sequence, `Starting/Ready/Draining/Stopped`, `max_in_flight`, `in_flight`, available slots, and
observer time. The Apalis runner owns the validated periodic timer, emits transition snapshots, and
drops the timer with the monitor; no detached heartbeat task remains after shutdown. The observer
is synchronous and must be non-blocking. Future persistence adapters must use an explicitly bounded
handoff rather than performing database or network I/O inside `record`.

## Active task control

Every `WorkerExecutor`, `ApalisWorkerService`, `BrokerDeliveryService`, and `BrokerWorkerRunner`
exposes a cloneable `WorkerTaskControl`. The handle owns no handler, engine, or broker type and can
therefore be retained by `plenora-runtime-control` or a Plenora CLI before the runner is moved into
its async execution future. `WorkerAdmissionHandle` independently exposes payload-free capacity and
pause/resume/drain operations.

```rust,ignore
let task_control = runner.task_control();
let active = task_control.active_tasks();

if let Some(task) = active.first() {
    let outcome = task_control.request_cancellation(task.task_id);
    println!("cancellation={outcome:?}");
}

let report = task_control.request_message_cancellation(message_id);
println!("matched={} requested={}", report.matched, report.requested);
```

The registry contains admitted handlers only; messages still waiting in JetStream remain broker
state and consume no registry entry. Its capacity is exactly `max_in_flight`, snapshots are
payload-free and bounded, and entries are removed by an execution guard on success, failure,
cancellation, timeout, or dropped futures. `WorkerTaskId` is local to one executor lifetime, so a
remote control API must pair it with the worker `instance_id`; cancelling by canonical `MessageId`
is also available and reaches every simultaneously active delivery attempt for that message.

Cancellation is cooperative and preserves the first reason. Long-running handlers must await
`WorkerContext::cancelled()` or bridge `context.cancellation` into the operation they invoke. The
executor polls the handler for the configured bounded cleanup grace and then returns a typed
cancelled outcome. In the current Apalis broker runner, an explicit `Requested` cancellation maps
to `DoNotRetry`, then to permanent NAK/TERM. Process shutdown and lease loss retain their separate
retryable settlement paths.

## Bounded lifecycle handoff

`WorkerLifecycleDispatcher::channel` connects both `TaskLifecycleObserver` and
`WorkerInstanceHeartbeatObserver` to one explicitly bounded async receiver. Observer calls use
`try_send` and never wait: when full, the newest observation is dropped and `dropped_full`
advances; after receiver closure, `dropped_closed` advances. The snapshot also exposes current
occupancy, accepted and delivered totals, and `Open/Saturated/Closed` state. Capacity defaults to
1,024 and has a hard maximum of 65,536 observations.

The dispatcher starts no task and performs no persistence. An embedding application owns the
single receiver and may drive it as a supervised runtime task. This is the future connection point
for a database or telemetry adapter; neither the worker crate nor its broker adapter imports those
unfinished libraries. Saturation affects observation freshness only and never changes handler
execution or broker settlement.

`WorkerLifecycleHealthReporter` maps snapshots into the shared `HealthRegistry`. Optional handoff
stays ready while degraded; required handoff becomes `NotReady` when saturated and unhealthy when
the receiver closes. Applications call `refresh` from their supervised monitor, keeping registry
locking out of observer callbacks. Observability exposes `worker_lifecycle_queue_depth` and
`worker_lifecycle_dropped_total`; callers record queue depth and counter deltas from the same
snapshot without introducing a worker dependency into the observability crate.

Apalis types remain confined to the adapter crate; `runtime-worker` exposes only Plenora-owned
configuration, context, handler, execution, and drain types.
