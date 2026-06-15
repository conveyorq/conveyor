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
make e2e         # kind-based end-to-end deployment test (KEEP=1 keeps the cluster)
make e2e-demo    # run the e2e and open the dashboard, in one command
make benchmark   # throughput/latency harness on the in-memory broker
```

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

For a fast edit loop, see the "Dashboard development" section of the
[README](README.md#dashboard-development).

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
- **MIT license header.** Every `.go` file carries the standard MIT header (copy
  it from any existing file).

## Commits and pull requests

- Use [Conventional Commits](https://www.conventionalcommits.org): `feat:`,
  `fix:`, `docs:`, `test:`, `chore:`, `refactor:`, etc.
- Keep pull requests focused and small; describe the change and how you tested
  it.
- Make sure `make test` and `make lint` pass, and that any protocol or
  deployment changes keep the relevant e2e green.

## License

Conveyor is released under the [MIT License](LICENSE). By contributing, you agree
that your contributions are licensed under it.
