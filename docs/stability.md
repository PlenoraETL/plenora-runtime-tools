# Stability qualification

Stability tests complement functional correctness with bounded sustained load and repeated
lifecycle transitions. Deterministic acceptance checks assert invariants rather than jobs per
second; a separate release-mode microbenchmark records coordination throughput without turning
hardware-specific numbers into correctness thresholds.

## Always-on scenarios

The normal workspace suite runs these scenarios on Windows, Linux, and macOS:

- 2,048 broker messages with `max_in_flight=32`; exactly 32 handlers become active, 2,016 remain
  queued, every message is acknowledged once, and no active task or delivery remains;
- 64 simultaneously active tasks receive concurrent cancellation requests and release every task
  registry entry and execution permit;
- 100 independently constructed worker executors each process and drain eight concurrent tasks
  without residual state;
- 1,000 memory pressure/recovery cycles leave reversible worker admission open and sampling failure
  remains closed until confirmed recovery;
- a 10,001-occurrence scheduler backlog advances through bounded ticks without materializing a
  queue, while crash-before-effect, unknown outcome, timeout, and cursor restart paths remain
  deterministic;
- the process-shaped capability consumer continues to cover generic dispatch, HTTP readiness, ACK,
  and coordinated shutdown.

Every scenario has a wall-clock timeout. Queue, history, registry, task, message, and payload sizes
remain explicitly bounded during the test.

## Real JetStream soak

The ignored-by-default NATS suite runs in the existing pinned ephemeral Docker container during CI.
It publishes and confirms 256 uniquely identified messages, forces a client reconnect at the
midpoint while observing `NotReady` then `Ready`, receives and server-confirms every ACK, and rejects
loss, duplicates, out-of-range sequences, or incomplete drain. The entire scenario has a 60-second
timeout.

Run the in-process stability tests directly:

```text
cargo test -p plenora-runtime-worker --test stability --locked -- --nocapture
cargo test -p plenora-runtime-apalis --test stability --locked -- --nocapture
cargo test -p plenora-runtime-resources --test pressure --locked -- --nocapture
cargo test -p plenora-runtime-scheduler --test scheduler --locked -- --nocapture
```

Run the real soak only against the loopback Docker harness:

```powershell
$status = ./scripts/nats-docker.ps1 Start | ConvertFrom-Json
try {
    $env:PLENORA_NATS_URL = $status.client_url
    cargo test -p plenora-runtime-nats --test real_nats --locked -- --ignored --nocapture
}
finally {
    ./scripts/nats-docker.ps1 Cleanup -RunId $status.run_id
}
```

These tests detect boundedness, lifecycle, duplicate/loss, and recovery regressions. They do not
replace a long-running production-like soak on the final Plenora consumer with its real foundation
adapters and persistence.

## Synthetic memory soak

`tests/memory` exercises a single long-lived Rust worker with committed synthetic allocations. It
covers allocator plateau, fragmentation, maximum configured concurrency, handler error, timeout
cancellation, and a deliberately retained control allocation. The Docker wrapper constrains the
entire process and records Linux RSS without exposing the host to an unbounded OOM experiment:

```powershell
./scripts/memory-soak.ps1 -AllocationMiB 64 -MaxInFlight 4 -ContainerMemory 1g
```

Passing proves that runtime-owned task futures and cancellation paths release their synthetic
allocations and that the growth detector recognizes a known leak. It does not prove that unfinished
native libraries return or reuse memory; their real adapters must reuse this harness before choosing
in-process or isolated execution.

## Reproducible coordination benchmark

Run one warm-up and at least five recorded samples on the same host and power profile:

```text
cargo run --release -p plenora-runtime-example-benchmark -- \
  --iterations 20000 --max-in-flight 32
```

The command prints one JSON object containing worker and scheduler elapsed time and rates. The
worker driver keeps at most `max_in_flight` Tokio tasks in its `JoinSet`; the scheduler retains one
cursor rather than creating an occurrence queue. Record commit SHA, CPU, OS, Rust version and the
median. This benchmark isolates runtime coordination and must not be presented as expected NATS,
database, Python or real-library throughput.
