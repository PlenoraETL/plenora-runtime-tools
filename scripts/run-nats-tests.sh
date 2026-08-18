#!/usr/bin/env bash
set -euo pipefail

nats_binary="${PLENORA_NATS_BINARY:-/tmp/nats-server}"
nats_data="${PLENORA_NATS_DATA:-/tmp/plenora-nats-data}"
nats_port="${PLENORA_NATS_PORT:-4222}"
nats_monitor_port="${PLENORA_NATS_MONITOR_PORT:-8222}"

mkdir -p "$nats_data"
"$nats_binary" \
  -js \
  -sd "$nats_data" \
  -a 127.0.0.1 \
  -p "$nats_port" \
  -m "$nats_monitor_port" \
  >/tmp/plenora-nats.log 2>&1 &
nats_pid=$!

cleanup() {
  kill "$nats_pid" 2>/dev/null || true
  wait "$nats_pid" 2>/dev/null || true
}
trap cleanup EXIT

ready=0
for _attempt in $(seq 1 100); do
  if (echo >/dev/tcp/127.0.0.1/"$nats_port") 2>/dev/null; then
    ready=1
    break
  fi
  sleep 0.05
done
if [[ "$ready" -ne 1 ]]; then
  cat /tmp/plenora-nats.log
  exit 1
fi

export PLENORA_NATS_URL="nats://127.0.0.1:$nats_port"
cargo test -p plenora-runtime-nats --test real_nats --locked -- --ignored --nocapture
cargo test -p plenora-runtime-integration-tests --test nats_worker_dlq --locked -- --ignored --nocapture
cargo test -p plenora-runtime-integration-tests --test nats_capability_e2e --locked -- --ignored --nocapture
