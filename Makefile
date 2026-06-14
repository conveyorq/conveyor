# Toolchain targets (proto, lint) run inside a Docker image built from
# Dockerfile.tools, so contributors only need Docker, Make, and Go.
#
# Tests and builds run on the host Go toolchain: the Postgres tests start
# their database through testcontainers-go, which needs the host Docker
# daemon — running them inside the tools image would mean docker-in-docker.

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

.PHONY: help all image build test lint proto proto-format proto-lint proto-breaking quickstart chaos e2e helm-lint release clean

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

lint: image ## Run golangci-lint in the tools image
	$(DOCKER_RUN) golangci-lint run --timeout 10m

# Generation goes through the gen/ staging directory: internal/proto is
# replaced only after buf generate has succeeded, and nothing else under
# internal/ is ever touched.
proto: image ## Lint protos and regenerate Go code
	$(DOCKER_RUN) sh -c '\
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
# throwaway kind cluster, and assert rollout + cluster formation + metrics.
e2e: ## Run the kind-based end-to-end deployment test (needs docker, kind, kubectl, helm)
	./hack/e2e-kind.sh

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
