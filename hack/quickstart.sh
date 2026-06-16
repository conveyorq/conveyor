#!/usr/bin/env bash
#
# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

# quickstart.sh drives the README quickstart end to end against a fresh
# --dev server: build the binaries, boot the server, run the worker, and
# enqueue ten tasks. CI runs it under a 60-second budget (the DX gate), so
# every wait below is bounded.
set -euo pipefail

cd "$(dirname "$0")/.."

ADDR="http://localhost:8080"
LOG_DIR="$(mktemp -d)"
TASK_COUNT=10

cleanup() {
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

echo "quickstart: building"
go build -o bin/conveyord ./cmd/conveyord
go build -o bin/quickstart-worker ./examples/standalone/worker
go build -o bin/quickstart-client ./examples/standalone/client

echo "quickstart: starting conveyord --dev"
bin/conveyord --dev >"$LOG_DIR/conveyord.log" 2>&1 &

for _ in $(seq 1 100); do
  curl -fs "$ADDR/healthz" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -fs "$ADDR/healthz" >/dev/null || {
  echo "quickstart: server never became healthy" >&2
  cat "$LOG_DIR/conveyord.log" >&2
  exit 1
}

echo "quickstart: starting the worker"
bin/quickstart-worker >"$LOG_DIR/worker.log" 2>&1 &

echo "quickstart: enqueueing $TASK_COUNT tasks"
bin/quickstart-client

for _ in $(seq 1 100); do
  if [ "$(grep -c 'sent welcome email' "$LOG_DIR/worker.log" || true)" -ge "$TASK_COUNT" ]; then
    echo "quickstart: ok — $TASK_COUNT tasks processed end to end"
    exit 0
  fi
  sleep 0.2
done

echo "quickstart: the worker never processed all $TASK_COUNT tasks" >&2
cat "$LOG_DIR/worker.log" >&2
exit 1
