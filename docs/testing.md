# Coverage and fuzz qualification

The repository treats coverage as a failing qualification gate, not an informational badge. CI
uses `cargo-llvm-cov 0.8.7` with Rust 1.97.1 and fails when merged line coverage is below 90%.

## Coverage scope

The measured profile includes all production crates and adapters, their normal test suites, the
ignored tests against the pinned ephemeral NATS JetStream server, the NATS-to-Apalis dead-letter
test, and the complete capability/crash-recovery worker test. Runnable examples and the
architecture/integration harness packages
are excluded from the normal workspace invocation; the relevant integration binary is then added
explicitly. `cargo-llvm-cov` excludes standalone test-source files from the report by default.

CI builds the merged profile in this order:

```text
cargo llvm-cov clean --workspace
cargo llvm-cov --workspace <documented package exclusions> --locked --no-report
cargo llvm-cov -p plenora-runtime-nats --test real_nats --locked --no-report -- --ignored
cargo llvm-cov -p plenora-runtime-integration-tests --test nats_worker_dlq --locked --no-report -- --ignored
cargo llvm-cov -p plenora-runtime-integration-tests --test nats_capability_e2e --locked --no-report -- --ignored
cargo llvm-cov report --summary-only --fail-under-lines 90
```

The threshold is intentionally line coverage. Branch coverage is not claimed because the stable
Rust instrumentation used by this workspace does not currently report it.

## Fuzz targets

The independent `fuzz/` workspace pins `cargo-fuzz 0.13.2`, `libfuzzer-sys 0.4.13`, and
`nightly-2026-08-01`. Its six targets exercise:

- message metadata bounds, validation, and atomic mutation;
- capability routing codec inputs;
- exponential retry calculations across extreme attempts and durations;
- NATS connection, producer, consumer, and replay configuration validation;
- W3C propagation carrier injection and extraction.
- control component/schedule identifiers, cron/timezone plans, deterministic jitter, and subprocess
  specification bounds.

Seed corpora are versioned under `fuzz/corpus/`. Crashes and local build output remain ignored under
`fuzz/artifacts/` and `fuzz/target/`.

On a Unix-like host with the pinned nightly and `cargo-fuzz` installed, run one target with:

```text
cargo +nightly-2026-08-01 fuzz run retry_policy fuzz/corpus/retry_policy -- -max_total_time=60 -timeout=5 -max_len=32768
```

On Windows, run the same command in a Linux container or WSL because libFuzzer requires a
Unix-like environment. CI runs 2,000 deterministic smoke iterations for every target; longer local
or scheduled campaigns should increase `-max_total_time` and retain any minimized regression input.

## Memory-retention qualification

The normal workspace suite includes small deterministic contracts proving that success, handler
failure, timeout, and bounded concurrent execution release task-owned synthetic allocations. The
Linux RSS probe is intentionally opt-in because allocator behavior and process memory accounting are
environment-sensitive.

Run the complete probe from PowerShell:

```powershell
./scripts/memory-soak.ps1
```

It builds the long-lived probe once, then runs plateau, fragmentation, concurrency, error,
cancellation, and intentional-leak scenarios in network-disabled Docker containers with a hard
memory limit. CSV samples and one JSON run manifest are written below `target/memory-soak/`.

The classifier compares median RSS in the first and last quartiles after warm-up. A high but stable
allocator plateau passes; monotonic retained growth above the configured tolerance fails. The
intentional leak must fail with the dedicated growing exit code, otherwise the wrapper rejects the
run. This qualifies the runtime and detector only. Every concrete library adapter must later repeat
the same process-long workload with representative library operations.

## Resource and scheduler resilience

The always-on suite exercises fail-closed sampling, admission hysteresis, 1,000 pressure/recovery
cycles, bounded catch-up of a 10,001-occurrence backlog, dispatch timeout, safe retry after a
not-started crash, unknown-outcome reconciliation, cursor restoration after restart, and scheduler
shutdown:

```text
cargo test -p plenora-runtime-resources --all-targets --locked
cargo test -p plenora-runtime-scheduler --all-targets --locked
```

These are backend-neutral tests. Durable scheduler and lifecycle recovery must be repeated through
the future `database-tools` adapter before the consumer API is frozen.

Cron/timezone, pause/resume, manual-trigger saturation, subprocess lifecycle, and authorized
control-plane routing are covered by their normal crate suites:

```text
cargo test -p plenora-runtime-subprocess --all-targets --locked
cargo test -p plenora-runtime-control --all-targets --locked
cargo test -p plenora-runtime-control-http --all-targets --locked
```
