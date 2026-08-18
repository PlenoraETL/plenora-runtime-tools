# Runtime microbenchmark

This release-mode benchmark measures engine-neutral worker execution and scheduler dispatch. The
driver never has more Tokio tasks than `--max-in-flight`, accepts at most 1,000,000 iterations, and
prints one JSON object so runs can be archived and compared on the same host.

```text
cargo run --release -p plenora-runtime-example-benchmark -- \
  --iterations 20000 --max-in-flight 32
```

Run at least five samples after one warm-up, record CPU model, operating system, Rust version, power
profile, and commit SHA, then compare medians. These numbers measure runtime coordination overhead;
they do not predict NATS, database, Python, or real library throughput.
