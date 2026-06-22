#!/usr/bin/env bash
#
# setup.sh configures the runtime state the Postmark platform relies on but that
# is not declared by the worker or the producer: per-queue concurrency limits,
# a marketing rate limit, and the recurring weekly-digest cron. These are admin
# operations, so they go through the `conveyor` CLI rather than the SDK.
#
# Point it at a running conveyord (a `--dev` server, or the kind cluster's
# port-forwarded API) via CONVEYOR_ADDR / CONVEYOR_TOKEN, which the CLI reads:
#
#   CONVEYOR_ADDR=http://localhost:8080 CONVEYOR_TOKEN=e2e-token \
#     ./examples/postmark/setup.sh
#
# By default it runs the CLI from source with `go run ./cmd/conveyor`; set
# CONVEYOR_CLI to an installed binary (CONVEYOR_CLI=conveyor) to skip the build.
set -euo pipefail

# The CLI invocation: the built binary, or `go run` from the repo root.
CONVEYOR_CLI=${CONVEYOR_CLI:-go run ./cmd/conveyor}

# digestSpec fires the weekly digest every minute so it is watchable in a demo
# (and so you can kill the scheduler's node and watch it fire from another). A
# real platform would use a weekly spec, e.g. "0 0 9 * * 1" (Mondays at 09:00).
digestSpec="0 * * * * *"

echo "==> capping per-tenant campaign concurrency on the marketing queue"
# A campaign enqueues every recipient with ConcurrencyKey "tenant:<name>"; this
# limit makes that key bite, so one tenant runs at most 2 sends at a time.
${CONVEYOR_CLI} concurrency set marketing --max 2

echo "==> rate-limiting the marketing queue to protect the provider"
${CONVEYOR_CLI} ratelimit set marketing --rate 20 --burst 5

echo "==> registering the weekly digest cron (${digestSpec})"
${CONVEYOR_CLI} cron add weekly-digest "${digestSpec}" digest:weekly --queue default

echo "==> current configuration"
${CONVEYOR_CLI} cron list
${CONVEYOR_CLI} concurrency ls
${CONVEYOR_CLI} ratelimit ls
${CONVEYOR_CLI} stats

echo "==> done. Try: conveyor queues pause marketing   (then resume)"
