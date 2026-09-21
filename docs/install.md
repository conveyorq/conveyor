# Installation

Conveyor is a server (`conveyord`), a command-line client (`conveyor`), and an SDK in your application. This page collects the install steps from the repository: start a server, build the CLI, and add the SDK for your language.

## Prerequisites

- **Go 1.27+**, the toolchain for the server, SDK, and CLI.
- **Postgres** for every deployment mode except `--dev`, which uses the in-memory broker. See [deployment modes](operations.md#deployment-modes).
- **Docker**, only for the Compose stack below.
- **Node 20+**, only needed to rebuild the web [dashboard](dashboard.md) that the server embeds.

## Server

### Install with `go install`

The server and the CLI are `main` packages in the repository's Go module, so Go builds and installs them straight from a release tag with no clone:

```sh
go install github.com/conveyorq/conveyor/cmd/conveyord@latest   # server
go install github.com/conveyorq/conveyor/cmd/conveyor@latest    # CLI
```

Both land in `$GOBIN`, by default `$HOME/go/bin`. Pin a release with `@vX.Y.Z` instead of `@latest`. A server installed this way serves an empty [dashboard](dashboard.md): the dashboard bundle is built into the binary from a checkout (`make dashboard`, then `make build`), and the published image ships with it built in.

### Run a dev server from a checkout

```sh
git clone https://github.com/conveyorq/conveyor
cd conveyor
make build                     # build conveyord and the conveyor CLI into bin/
go run ./cmd/conveyord --dev   # a dev server (in-memory broker, auth off) on :8080
```

`--dev` selects the development preset: standalone mode, in-memory broker, authentication disabled, debug logs. Every other mode is configured through a `conveyor.yaml` file, `CONVEYOR_*` environment variables, or flags; see [configuration](operations.md#configuration).

### Compose: the published image with Postgres

The repository ships a one-command stack for trying Conveyor out: the published server image wired to a Postgres broker.

```sh
docker compose -f deploy/compose/quickstart.yaml up
```

The API and health endpoints are then on `http://localhost:8080` and metrics on `http://localhost:9464/metrics`. Authentication is off in this stack, so it is for local evaluation only.

### Kubernetes with Helm

The chart deploys `conveyord` as a clustered workload. Pre-create the broker DSN and API-token Secrets, then install from the published OCI registry (or from `./deploy/helm/conveyor` in a local checkout):

```sh
kubectl create secret generic conveyor-broker \
  --from-literal=dsn='postgres://user:pass@host:5432/conveyor?sslmode=require'
kubectl create secret generic conveyor-auth \
  --from-literal=auth-tokens='token-a,token-b'

helm install conveyor oci://ghcr.io/conveyorq/charts/conveyor \
  --set broker.dsnSecret.name=conveyor-broker \
  --set auth.tokensSecret.name=conveyor-auth

kubectl rollout status statefulset/conveyor
kubectl port-forward svc/conveyor 8080:8080   # API + dashboard on http://localhost:8080
```

Pin a chart version with `--version X.Y.Z`. The [chart README](../deploy/helm/conveyor/README.md) lists the prerequisites and every parameter, and the [high-availability guide](high-availability.md) walks through a complete clustered deployment.

### Binary on a host (systemd)

`make build` produces `bin/conveyord`, or install it with `go install` as above. The [systemd unit](../deploy/systemd/conveyord.service) in the repository has the install steps in its header: copy the binary to `/usr/local/bin`, create a `conveyor` system user, put `conveyor.yaml` under `/etc/conveyor`, and keep the broker DSN and auth tokens in an environment file rather than in the unit.

The [dashboard](dashboard.md) is embedded into the binary at build time. `go build` does not need Node, but the dashboard stays empty until you run `make dashboard` (needs Node) and rebuild.

## CLI

Install it with Go:

```sh
go install github.com/conveyorq/conveyor/cmd/conveyor@latest
```

Or build it from a checkout:

```sh
go build -o conveyor ./cmd/conveyor
```

Or run it without installing:

```sh
go run ./cmd/conveyor <command> [flags]
```

Every command takes the server URL from `--addr` or `CONVEYOR_ADDR` (default `http://localhost:8080`) and, outside `--dev`, a bearer token from `--token` or `CONVEYOR_TOKEN`. See the [CLI reference](cli.md).

## SDKs

### Go

The SDK is distributed as a Go module, so no package registry is involved; `go get` resolves it straight from the repository:

```sh
go get github.com/conveyorq/conveyor/sdks/go
```

Import it under a short alias, since the package name is `conveyor`:

```go
import conveyor "github.com/conveyorq/conveyor/sdks/go"
```

### TypeScript

`@conveyorq/conveyor` is not yet on npm. Install it from a local checkout of the repository; building the SDK once produces the `dist/` that consumers import:

```sh
git clone https://github.com/conveyorq/conveyor.git
cd conveyor/sdks/typescript
pnpm install && pnpm run build
```

Then reference it from your project as a `file:` dependency, with `pnpm link`, or as a packed tarball. The [TypeScript SDK README](../sdks/typescript/README.md) has the exact commands. Node 20+.

### Python

Install straight from the repository, no clone needed:

```sh
uv add "conveyorq @ git+https://github.com/conveyorq/conveyor.git#subdirectory=sdks/python"
```

The same git URL works with pip:

```sh
pip install "git+https://github.com/conveyorq/conveyor.git#subdirectory=sdks/python"
```

The [Python SDK README](../sdks/python/README.md) covers editable installs from a local checkout. Python 3.9+.

## Check that it works

The server answers on its health endpoint:

```sh
curl -f http://localhost:8080/healthz
```

The [dashboard](dashboard.md) is served at the API root, so the same address in a browser, `http://localhost:8080/`, opens the operations console. With `--dev` there is no token to enter.

The smallest end-to-end loop is one `conveyord`, one worker, and one client, in three terminals:

```sh
go run ./cmd/conveyord --dev          # 1: server (in-memory broker, auth off)
go run ./examples/standalone/worker   # 2: worker
go run ./examples/standalone/client   # 3: enqueue ten welcome emails
```

Next: [concepts](concepts.md) for the vocabulary, and the [usage guide](usage.md) for the worker and enqueue code in each SDK.
