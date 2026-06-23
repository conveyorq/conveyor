#!/usr/bin/env bash
#
# Copyright 2026 ConveyorQ
#
# SPDX-License-Identifier: Apache-2.0

# cover.sh runs the Go test suite and reports the module's overall
# statement coverage over the library/core packages.
#
# Two package sets are derived from `go list`:
#
#   - the test set: every package whose tests should run. It drops only the
#     packages that have no tests of their own (the example programs, the
#     benchmarks, the generated protobuf code, and the broker test
#     scaffolding).
#
#   - the coverage denominator (-coverpkg): the curated core. On top of the
#     non-test packages above it also drops the command entrypoints (cmd),
#     the kind/load test helper (hack), and the dashboard embed wrapper
#     (web/dashboard). Those carry little or no testable logic and would only
#     deflate the reported number. Their tests still run; they are excluded
#     from the total, not skipped.
#
# Any extra arguments are forwarded to `go test` on top of the flags baked in
# below, so a caller can narrow the run (e.g. -run) without losing them.
set -euo pipefail

cd "$(dirname "$0")/.."

GO=${GO:-go}

# Packages with no tests of their own; excluded from both the test run and
# the coverage total.
NON_TEST='/examples/|/benchmark$|/internal/proto/|/internal/broker/brokertest$'

# Additionally excluded from the coverage total only: entrypoints, tooling,
# and the embed-only dashboard package.
NON_CORE="$NON_TEST|/cmd/|/hack/|/web/dashboard\$"

test_pkgs=$($GO list -mod=vendor ./... | grep -vE "$NON_TEST")
cover_pkgs=$($GO list -mod=vendor ./... | grep -vE "$NON_CORE" | paste -sd, -)

# -p 1: serialize packages so concurrent GoAkt clusters don't oversubscribe a
#   constrained runner (the 4-vCPU CI box) and starve under -race.
# -timeout 15m: per-package bound; packages run well under a minute, so this
#   only trips on a genuine hang.
# -race -covermode=atomic: the race detector requires atomic coverage counting.
$GO test -mod=vendor -p 1 -timeout 15m -race -v "$@" \
	-covermode=atomic -coverpkg="$cover_pkgs" -coverprofile=coverage.out $test_pkgs

echo
echo "Overall coverage (library/core packages):"
$GO tool cover -func=coverage.out | tail -1
