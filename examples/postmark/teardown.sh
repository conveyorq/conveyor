#!/usr/bin/env bash
#
# teardown.sh is the inverse of setup.sh: it removes the runtime configuration
# the Postmark platform added — the weekly-digest cron and the marketing queue's
# concurrency and rate-limit overrides — through the `conveyor` CLI. It does not
# delete the worker/producer Deployments; remove those with
#   kubectl -n <namespace> delete -f examples/postmark/deploy/postmark.yaml
# (or `make postmark-down` for the kind demo cluster).
#
# Point it at the running conveyord the same way as setup.sh:
#
#   CONVEYOR_ADDR=http://localhost:8080 CONVEYOR_TOKEN=<token> \
#     ./examples/postmark/teardown.sh
#
# Set CONVEYOR_CLI to an installed binary (CONVEYOR_CLI=conveyor) to skip the
# `go run` build.
set -euo pipefail

# The CLI invocation: the built binary, or `go run` from the repo root.
CONVEYOR_CLI=${CONVEYOR_CLI:-go run ./cmd/conveyor}

# The admin CLI exposes cron pause/resume, not delete, and the broker retains
# cron entries; pausing stops the digest from firing. Each removal tolerates an
# already-absent target so a partial setup still tears down cleanly.
echo "==> pausing the weekly digest cron"
${CONVEYOR_CLI} cron pause weekly-digest || true

echo "==> clearing the marketing concurrency limit"
${CONVEYOR_CLI} concurrency rm marketing || true

echo "==> clearing the marketing rate limit"
${CONVEYOR_CLI} ratelimit rm marketing || true

echo "==> remaining configuration"
${CONVEYOR_CLI} cron list
${CONVEYOR_CLI} concurrency ls
${CONVEYOR_CLI} ratelimit ls

echo "==> done. Remove the workload Deployments with kubectl delete -f deploy/postmark.yaml"
