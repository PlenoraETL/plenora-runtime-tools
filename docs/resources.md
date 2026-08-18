# Resource pressure

`plenora-runtime-resources` monitors process resident memory without depending on a metrics vendor,
worker engine, broker, or database. It classifies the process as `Initializing`, `Normal`,
`Pressured`, `Critical`, or `Unavailable` and projects that state into the shared health registry.

The monitor is fail-closed. Before its first successful sample, after a sampling failure, and while
either limit is active, the attached `WorkerAdmissionControl` rejects new work. Active handlers are
not cancelled. Admission opens after the configured number of consecutive samples below the resume
threshold. The separate resume and soft thresholds provide hysteresis and avoid rapid state
flapping.

`ProcessMemorySampler` reads Linux `/proc/self/status` without unsafe code. Other platforms must
inject an application-owned sampler or omit the monitor; unsupported sampling never silently
reports zero usage.

Choose thresholds from the container limit rather than host RAM:

```text
resume threshold < soft admission threshold < hard unhealthy threshold < container limit
```

The hard threshold does not kill or recycle the process. It marks health unhealthy and keeps
admission closed so an external supervisor can apply its deployment policy. Process recycling is a
separate isolation decision described in [`subprocess-execution.md`](subprocess-execution.md).

The observer receives only counters, state, and byte values. It must remain non-blocking and must
not perform database writes inline; persistence belongs behind the future `database-tools` adapter.
