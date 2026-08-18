#!/usr/bin/env bash
set -euo pipefail

cargo llvm-cov clean --workspace
cargo llvm-cov --workspace \
  --exclude plenora-example-worker-basic \
  --exclude plenora-example-worker-nats \
  --exclude plenora-example-http-worker \
  --exclude plenora-example-http-service \
  --exclude plenora-runtime-example-scheduler-basic \
  --exclude plenora-runtime-example-benchmark \
  --exclude plenora-runtime-architecture-tests \
  --exclude plenora-runtime-integration-tests \
  --exclude plenora-runtime-memory-tests \
  --locked --no-report

nats_binary="${PLENORA_NATS_BINARY:-/tmp/nats-server}"
nats_data="${PLENORA_NATS_DATA:-/tmp/plenora-coverage-nats-data}"
mkdir -p "$nats_data"
"$nats_binary" -js -sd "$nats_data" -a 127.0.0.1 -p 4222 -m 8222 \
  >/tmp/plenora-coverage-nats.log 2>&1 &
nats_pid=$!
cleanup() {
  kill "$nats_pid" 2>/dev/null || true
  wait "$nats_pid" 2>/dev/null || true
}
trap cleanup EXIT

ready=0
for _attempt in $(seq 1 100); do
  if (echo >/dev/tcp/127.0.0.1/4222) 2>/dev/null; then
    ready=1
    break
  fi
  sleep 0.05
done
if [[ "$ready" -ne 1 ]]; then
  cat /tmp/plenora-coverage-nats.log
  exit 1
fi

export PLENORA_NATS_URL="nats://127.0.0.1:4222"
cargo llvm-cov -p plenora-runtime-nats --test real_nats \
  --locked --no-report -- --ignored --nocapture
cargo llvm-cov -p plenora-runtime-integration-tests --test nats_worker_dlq \
  --locked --no-report -- --ignored --nocapture
cargo llvm-cov -p plenora-runtime-integration-tests --test nats_capability_e2e \
  --locked --no-report -- --ignored --nocapture
cargo llvm-cov report --summary-only --fail-under-lines 90
