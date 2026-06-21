# The Go toolchain targets (proto, lint-go) run inside a Docker image built from
# Dockerfile.tools, so contributors only need Docker, Make, and Go for them.
#
# Tests and builds run on the host Go toolchain: the Postgres tests start
# their database through testcontainers-go, which needs the host Docker
# daemon — running them inside the tools image would mean docker-in-docker.
#
# The TypeScript and Python lint steps (lint-ts, lint-py) run on the host like
# the SDK and dashboard targets do: lint-ts needs Node + pnpm, lint-py needs
# Python + uv. `make lint` runs all three (Go, TypeScript, Python).

GO      ?= go
IMAGE   := conveyor-tools
WORKDIR := /src

UID := $(shell id -u)
GID := $(shell id -g)

# Cache dirs live under the repo (gitignored) so they are owned by the host
# user and backed by host disk.
_ := $(shell mkdir -p .cache/home .gocache .gomodcache)

DOCKER_RUN := docker run --rm \
	--user $(UID):$(GID) \
	-e HOME=$(WORKDIR)/.cache/home \
	-e GOCACHE=$(WORKDIR)/.gocache \
	-e GOMODCACHE=$(WORKDIR)/.gomodcache \
	-v "$(CURDIR)":$(WORKDIR) \
	-w $(WORKDIR) \
	$(IMAGE)

DASHBOARD_DIR   := web/dashboard
SDK_TS_DIR      := sdks/typescript
SDK_PY_DIR      := sdks/python
EXAMPLES_TS_DIR := examples/typescript

# License-header tooling. addlicense is fetched on demand with GOFLAGS cleared,
# so it resolves even when the build runs under -mod=vendor/-mod=readonly. The
# Go sources are listed explicitly because addlicense does not expand globs.
ADDLICENSE_VERSION := v1.2.0
COPYRIGHT_HOLDER   := ConveyorQ
GO_SOURCES         := $(shell find . -path ./vendor -prune -o -name '*.go' -print)
ADDLICENSE         := GOFLAGS= $(GO) run github.com/google/addlicense@$(ADDLICENSE_VERSION) -l apache -s -c "$(COPYRIGHT_HOLDER)"

.PHONY: help all image build test lint lint-go lint-ts lint-py license-check license-fix licenses proto proto-format proto-lint proto-breaking quickstart chaos e2e e2e-clean e2e-dashboard e2e-demo benchmark helm-lint release clean dashboard dashboard-gen dashboard-test sdk-gen sdk-ts-gen sdk-ts-test sdk-py-gen sdk-py-test

help: ## Show available targets
	@awk 'BEGIN{FS=":.*?## "} /^[a-zA-Z0-9_-]+:.*?## / {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

all: proto lint test build ## Regenerate protos, lint, test, and build

image: ## Build the tools Docker image
	docker build -t $(IMAGE) -f Dockerfile.tools .

build: ## Build the conveyord and conveyor binaries
	$(GO) build -o bin/conveyord ./cmd/conveyord
	$(GO) build -o bin/conveyor ./cmd/conveyor

quickstart: ## Run the scripted README quickstart (CI enforces a 60s budget)
	./hack/quickstart.sh

test: ## Run all tests with the race detector (needs the host Docker daemon)
	$(GO) test -race ./...

lint: lint-go lint-ts lint-py ## Lint every language (Go, TypeScript, Python)

lint-go: image ## Run golangci-lint in the tools image
	$(DOCKER_RUN) golangci-lint run --timeout 10m

# lint-ts type-checks every TypeScript package with the strict compiler (the
# project's enforced standard). pnpm install is frozen so a stale lockfile is a
# lint failure, not a silent update.
lint-ts: ## Type-check the TypeScript packages (dashboard, SDK, examples)
	cd $(DASHBOARD_DIR) && pnpm install --frozen-lockfile && pnpm exec tsc --noEmit
	cd $(SDK_TS_DIR) && pnpm install --frozen-lockfile && pnpm run typecheck
	cd $(EXAMPLES_TS_DIR) && pnpm install --frozen-lockfile && pnpm run typecheck

# lint-py lints the Python SDK with ruff and type-checks it with mypy, both
# configured in sdks/python/pyproject.toml. The venv is created on demand, the
# same way the codegen target builds it.
lint-py: ## Lint the Python SDK with ruff + mypy
	cd $(SDK_PY_DIR) && { test -d .venv || uv venv .venv; } && uv pip install --python .venv -e ".[dev]"
	cd $(SDK_PY_DIR) && .venv/bin/ruff check .
	cd $(SDK_PY_DIR) && .venv/bin/mypy src

license-check: ## Verify the Apache-2.0 SPDX header on every Go source file
	$(ADDLICENSE) -check $(GO_SOURCES)

license-fix: ## Add the Apache-2.0 SPDX header to any Go source file missing it
	$(ADDLICENSE) $(GO_SOURCES)

licenses: ## Print the dependency license report backing docs/licenses.md
	$(GO) run github.com/google/go-licenses@latest report \
		./cmd/conveyord ./cmd/conveyor ./sdks/go/... ./embedded

# Generation goes through the gen/ staging directory: internal/proto is
# replaced only after buf generate has succeeded, and nothing else under
# internal/ is ever touched.
proto: image ## Format and lint protos, then regenerate Go code
	$(DOCKER_RUN) sh -c '\
		buf format -w && \
		buf lint && \
		rm -rf gen && \
		buf generate && \
		rm -rf internal/proto && \
		mkdir -p internal/proto && \
		mv gen/conveyor internal/proto/conveyor && \
		rm -rf gen'

proto-format: image ## Format proto files with buf
	$(DOCKER_RUN) buf format -w

proto-lint: image ## Lint proto files with buf
	$(DOCKER_RUN) buf lint

proto-breaking: image ## Check wire-contract compatibility against main
	$(DOCKER_RUN) buf breaking --against '.git#branch=main'

# 3-node kill chaos: pin the queue grain to one node and a worker gateway to
# another, kill both mid-load, and require zero task loss. CHAOS_COUNT sets the
# consecutive-green gate (DESIGN Phase 5 accepts at 20).
CHAOS_COUNT ?= 20
chaos: ## Run the 3-node chaos suite CHAOS_COUNT times (default 20) under -race
	$(GO) test -race -run TestThreeNodeChaosLosesNothing -count=$(CHAOS_COUNT) ./internal/actors

# kind-based end-to-end packaging test: build the image, install the chart on a
# throwaway kind cluster, and assert rollout + cluster formation + metrics +
# dashboard + rolling-restart. The cluster is torn down on exit unless KEEP=1.
e2e: ## Run the kind-based e2e test (needs docker, kind, kubectl, helm; KEEP=1 keeps the cluster)
	./hack/e2e-kind.sh

# These mirror the values in hack/e2e-kind.sh; keep them in sync.
E2E_CLUSTER   ?= conveyor-e2e
E2E_NAMESPACE ?= conveyor
E2E_RELEASE   ?= conveyor
E2E_TOKEN     ?= e2e-token
e2e-clean: ## Delete a leftover e2e kind cluster (e.g. after KEEP=1 or an interrupted run)
	kind delete cluster --name $(E2E_CLUSTER)

e2e-dashboard: ## Open the e2e dashboard in a browser (run after KEEP=1 make e2e)
	@echo "Dashboard: http://localhost:8080/   (API token: $(E2E_TOKEN))"; \
	( sleep 2; \
	  if command -v open >/dev/null 2>&1; then open http://localhost:8080/; \
	  elif command -v xdg-open >/dev/null 2>&1; then xdg-open http://localhost:8080/; fi ) & \
	kubectl -n $(E2E_NAMESPACE) port-forward svc/$(E2E_RELEASE) 8080:8080

e2e-demo: ## One command: stand up a cluster with continuous load and open the live dashboard
	PLAYGROUND=1 ./hack/e2e-kind.sh

# Throughput/latency harness on the in-memory broker (no infra). See
# benchmark/README.md for the Postgres invocation and the honesty notes.
BENCH_TASKS ?= 20000
benchmark: ## Run the throughput/latency benchmark (in-memory broker)
	$(GO) run ./benchmark --tasks=$(BENCH_TASKS)

# The dashboard SPA is built by Vite into a git-ignored dist/ (only dist/.gitkeep
# is committed, which keeps the go:embed compiling), so `go build` never needs
# Node. CI and the Docker build run this; run it locally to preview UI changes.
dashboard: ## Rebuild the embedded dashboard bundle (needs Node)
	cd $(DASHBOARD_DIR) && pnpm install --frozen-lockfile && pnpm run build

dashboard-gen: ## Regenerate the dashboard's TypeScript Connect client from the protos
	cd $(DASHBOARD_DIR) && pnpm install --frozen-lockfile
	buf generate --template buf.gen.web.yaml

dashboard-test: ## Run the dashboard frontend unit tests (needs Node)
	cd $(DASHBOARD_DIR) && pnpm install --frozen-lockfile && pnpm test

sdk-ts-gen: ## Regenerate the TypeScript SDK's protobuf from the protos
	cd $(SDK_TS_DIR) && pnpm install
	buf generate --template buf.gen.ts.yaml

sdk-ts-test: ## Run the TypeScript SDK unit tests (needs Node)
	cd $(SDK_TS_DIR) && pnpm install && pnpm test

sdk-gen: sdk-ts-gen sdk-py-gen ## Regenerate both SDKs' protobuf stubs from the protos

# Generation runs buf with the protobuf-29-pinned remote plugins, then rewrites
# the generated cross-module imports to package-relative form (protoletariat) and
# restores the gen tree's __init__.py package markers (buf's clean step drops
# them). Needs Python + uv.
sdk-py-gen: ## Regenerate the Python SDK's protobuf + gRPC stubs from the protos
	cd $(SDK_PY_DIR) && { test -d .venv || uv venv .venv; } && uv pip install --python .venv -e ".[dev]"
	buf generate --template buf.gen.python.yaml
	$(SDK_PY_DIR)/.venv/bin/python -m protoletariat --in-place \
		--python-out $(SDK_PY_DIR)/src/conveyorq/gen \
		protoc --proto-path=protos \
		conveyor/v1/task.proto conveyor/v1/service.proto conveyor/v1/messages.proto
	touch $(SDK_PY_DIR)/src/conveyorq/gen/__init__.py \
		$(SDK_PY_DIR)/src/conveyorq/gen/conveyor/__init__.py \
		$(SDK_PY_DIR)/src/conveyorq/gen/conveyor/v1/__init__.py

sdk-py-test: ## Run the Python SDK unit tests (needs Python + uv)
	cd $(SDK_PY_DIR) && { test -d .venv || uv venv .venv; } && uv pip install --python .venv -e ".[dev]" && \
		.venv/bin/python -m pytest tests -k "not integration"

# Lint the chart and prove it renders with both standalone and clustered
# value sets. Runs on the host helm (not the tools image).
helm-lint: ## Lint and template-render the Helm chart
	helm lint deploy/helm/conveyor
	helm template conveyor deploy/helm/conveyor >/dev/null
	helm template conveyor deploy/helm/conveyor \
		--set broker.dsn=postgres://u:p@db:5432/conveyor \
		--set serviceMonitor.enabled=true \
		--set networkPolicy.enabled=true >/dev/null

# Stub until launch: goreleaser packaging.
release:
	@echo "release: not implemented yet" && exit 1

clean: ## Remove build artifacts and the tools image
	rm -rf bin
	docker rmi -f $(IMAGE)
