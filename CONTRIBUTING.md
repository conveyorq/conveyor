# Contributing to Conveyor

Thanks for your interest in contributing. This guide covers how to build, test,
and submit changes.

## Prerequisites

- **Go 1.26+** — the toolchain for the server, SDK, and CLI.
- **Docker** — required for several workflows: the lint/proto tools run in a
  pinned image, the Postgres tests start a database through testcontainers, and
  the end-to-end test builds the container image.
- **Node 20+** — only needed to rebuild the web dashboard (`web/dashboard/`).
  The built bundle is committed, so `go build` and the test suite do **not**
  need Node.
- **kind, kubectl, helm** — only needed for the Kubernetes end-to-end test.

`buf` is invoked inside the tools image, so you don't need it installed locally.

## Getting started

```sh
git clone https://github.com/conveyorq/conveyor
cd conveyor
make build          # build conveyord and the conveyor CLI into bin/
go run ./cmd/conveyord --dev   # a dev server (in-memory broker, auth off) on :8080
```

`make help` lists every target.

## Build, test, lint

```sh
make test    # all tests with the race detector (Postgres tests need Docker)
make lint    # golangci-lint, run inside the pinned tools image
```

Both must pass before a change is merged. CI runs them on every pull request,
along with `make quickstart` (the scripted README walkthrough, under a 60s
budget) and the kind end-to-end test.

Other useful targets:

```sh
make chaos       # 3-node kill test, repeated for the zero-loss gate
make benchmark   # throughput/latency harness on the in-memory broker
```

The kind-based `make e2e` deployment test and the live `make e2e-demo` playground
have their own section [below](#end-to-end-deployment-test).

## End-to-end deployment test

`make e2e` runs `hack/e2e-kind.sh`, which stands up a throwaway [kind](https://kind.sigs.k8s.io)
cluster close to a production setup: a Postgres broker, three server replicas in
kubernetes mode discovering each other through the Kubernetes API, the database
DSN and API tokens delivered as Secrets, and metrics on their own port. It
builds the image, loads it into kind, installs the Helm chart, and asserts the
rollout completes, the three nodes form one cluster, and the metrics endpoint
serves. It then drives load through a **rolling restart** — an in-cluster
producer/worker enqueues and processes tasks through the API Service while the
StatefulSet is rolled one pod at a time — and asserts the cluster reforms and
every task completes with zero loss. It needs `docker`, `kind`, `kubectl`, and
`helm`, and runs the same way locally and in CI.

The cluster is torn down automatically on exit. To watch it **live** instead —
stand up the cluster, run a continuous producer/worker, and open the dashboard
so you can see tasks flow — use the one-command playground:

```sh
make e2e-demo   # cluster + continuous load + live dashboard at http://localhost:8080/ (token: e2e-token)
make e2e-clean  # tear the cluster down when finished
```

It runs the same health checks first, then goes live and blocks until Ctrl-C;
turn on **Auto-refresh** in the UI to watch the queues, tasks, and workers
update. The pieces are also available on their own: `KEEP=1 make e2e` (the
assert-and-exit test, kept), `make e2e-dashboard` (port-forward + open an
existing cluster's dashboard), and `make e2e-clean` (remove a cluster, including
one left by an interrupted run).

## Protobuf

The wire protocol lives in `protos/`. Generated Go and TypeScript are committed;
**never hand-edit generated files.** After changing a `.proto`:

```sh
make proto          # lint protos and regenerate the Go code (via the tools image)
make dashboard-gen  # regenerate the dashboard's TypeScript client (needs Node)
```

Keep the wire contract backward compatible (`make proto-breaking` checks against
`main`).

## Dashboard

The dashboard is a React + TypeScript app (`web/dashboard/`) built with Vite.
Its built bundle (`dist/`) is **not committed** — it is built in CI and baked
into the Docker image (a Node stage). `go build`/`go test` work without Node;
the dashboard tests simply skip when the bundle is absent, and the binary serves
an empty dashboard until you build it. To build it locally (needs Node):

```sh
make dashboard       # build web/dashboard/dist (embedded by conveyord)
make dashboard-test  # frontend unit tests (Vitest)
```

For a fast edit loop, run a Vite dev server against a local `conveyord --dev`:

```sh
go run ./cmd/conveyord --dev                   # API + dashboard on :8080
cd web/dashboard && npm install && npm run dev # hot-reloading UI on :5173
```

Open the dev server with `?api=http://localhost:8080` so it targets the running
server. After changing the UI, run `make dashboard` to rebuild the `dist/` bundle
that ships in the binary.

## Dependencies and vendoring

`vendor/` is git-ignored; CI and the release build regenerate it. When you add or
update a dependency, run:

```sh
go mod tidy && go mod vendor
```

Do not commit `vendor/`.

## Code conventions

- **Comments are godoc.** Every exported function, type, and field has a clear,
  concise doc comment written for the reader and IDE tooltips. Proto messages,
  fields, and services are documented too, so the docs flow into generated code.
- **Every implementation file has a co-located test file** (`foo.go` →
  `foo_test.go`), and new code ships with tests. The dashboard follows the same
  rule (every module has a Vitest test).
- **No magic values.** Give meaning to literals with named constants; name them
  for what they mean, not their value.
- **Clear names.** Prefer full, idiomatic Go names (`config`, not `cfg`).
- **Readable control flow.** Prefer early returns and `switch` over nested
  `if/else`; put a blank line around multi-line blocks.
- **Surgical changes.** Touch only what your change requires; match the
  surrounding style; don't refactor unrelated code in the same PR.
- **Apache-2.0 license header.** Every `.go` file carries the
  `SPDX-License-Identifier: Apache-2.0` header; run `make license-fix` to add it
  to new files (CI runs `make license-check`).

## Commits and pull requests

- Use [Conventional Commits](https://www.conventionalcommits.org): `feat:`,
  `fix:`, `docs:`, `test:`, `chore:`, `refactor:`, etc.
- Keep pull requests focused and small; describe the change and how you tested
  it.
- Make sure `make test` and `make lint` pass, and that any protocol or
  deployment changes keep the relevant e2e green.
- **Sign off every commit** (`git commit -s`) to certify the
  [DCO](#developer-certificate-of-origin); CI enforces it.

## Developer Certificate of Origin

Conveyor is released under the [Apache License 2.0](LICENSE). Contributions are
accepted under the same license — there is no separate contributor agreement to
sign. Instead, we use the [Developer Certificate of Origin](https://developercertificate.org)
(DCO): a lightweight statement that you wrote the contribution, or otherwise have
the right to submit it under the project's license.

You certify the DCO by adding a `Signed-off-by` line to each commit, which `git`
adds for you with the `-s` flag:

    git commit -s -m "feat: add the thing"

This produces a trailer using the name and email from your `git` config:

    Signed-off-by: Jane Doe <jane@example.com>

Use a real name and a reachable email. CI checks that every commit in a pull
request is signed off; if you forget, amend with `git commit --amend -s` (or, for
several commits, `git rebase --signoff`) and push again.
