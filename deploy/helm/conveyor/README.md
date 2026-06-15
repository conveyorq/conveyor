# Conveyor

[Conveyor](https://github.com/conveyorq/conveyor) is a distributed task queue
for Go: a persistent, push-based queue with at-least-once execution, no Redis
and no polling. This chart deploys the server, `conveyord`, as a clustered
workload on Kubernetes.

## TL;DR

```sh
# Pre-create the broker DSN and API-token secrets (see Prerequisites), then
# install from the published OCI registry (or use ./deploy/helm/conveyor for a
# local checkout):
helm install conveyor oci://ghcr.io/conveyorq/charts/conveyor \
  --set broker.dsnSecret.name=conveyor-broker \
  --set auth.tokensSecret.name=conveyor-auth
```

Released chart versions are published as OCI artifacts at
`oci://ghcr.io/conveyorq/charts/conveyor` (signed with cosign). Pin a version
with `--version X.Y.Z`.

## Introduction

This chart bootstraps a `conveyord` cluster on a Kubernetes cluster using the
Helm package manager. Nodes discover one another through the Kubernetes API
(pod-label discovery), durable state lives in Postgres, and the API — together
with the embedded operations dashboard — is served behind a `Service`.

The workload is a **StatefulSet**. `conveyord` keeps no durable disk state (so
there are no `PersistentVolumeClaim`s); the StatefulSet is chosen for stable
pod identity and one-pod-at-a-time rollouts, because the cluster is
membership-sensitive — queue-grain placement, singletons, and task leases
rebalance on every membership change.

## Prerequisites

- Kubernetes 1.23+
- Helm 3.x
- A **Postgres** database reachable from the cluster. This is the default and
  only production-grade broker; the chart does **not** deploy Postgres for you.
- Permission to create RBAC objects. The chart grants its `ServiceAccount`
  permission to **list pods** in the release namespace — that is how peers find
  each other. To supply your own, set `rbac.create=false` and
  `serviceAccount.create=false`.

## Installing the chart

`conveyord` is **fail-closed**: with no broker DSN it has nowhere durable to
store tasks, and with no auth tokens it refuses to start unless you explicitly
allow unauthenticated access. A real install supplies both via Secrets.

1. Create the Secrets out-of-band (recommended — keeps credentials out of
   release manifests and Helm history):

   ```sh
   kubectl create secret generic conveyor-broker \
     --from-literal=dsn='postgres://user:pass@host:5432/conveyor?sslmode=require'

   kubectl create secret generic conveyor-auth \
     --from-literal=auth-tokens='token-a,token-b'
   ```

2. Install the chart, referencing those Secrets:

   ```sh
   helm install conveyor ./deploy/helm/conveyor \
     --set broker.dsnSecret.name=conveyor-broker \
     --set auth.tokensSecret.name=conveyor-auth
   ```

   The command deploys `conveyord` with its default configuration. The
   [Parameters](#parameters) section lists everything that can be configured.

3. Verify the rollout and reach the API:

   ```sh
   kubectl rollout status statefulset/conveyor
   kubectl port-forward svc/conveyor 8080:8080
   # API + dashboard now at http://localhost:8080
   ```

The post-install notes print the in-cluster API URL and warn if the broker is
`memory` or auth is unconfigured.

> **Evaluation without Postgres (non-durable):**
> `--set broker.driver=memory --set auth.allowUnauthenticated=true --set replicaCount=1`.
> Tasks are lost on restart — never use this beyond a demo.

## Uninstalling the chart

```sh
helm uninstall conveyor
```

This removes all Kubernetes objects the chart created. Secrets you created
out-of-band (broker DSN, auth tokens, TLS) are **not** removed — delete them
separately. Task data persists in Postgres.

## Parameters

> Every `conveyord` tunable is rendered into a ConfigMap from
> [`files/conveyor.yaml`](files/conveyor.yaml), which doubles as a fully
> annotated config reference. The fully commented value set lives in
> [`values.yaml`](values.yaml). Three values are injected at runtime rather
> than through the ConfigMap: the pod IP (`cluster.bind_addr`, via the downward
> API), the broker DSN, and the API auth tokens (both from Secrets).

### Common parameters

| Name | Description | Default |
|---|---|---|
| `replicaCount` | Number of `conveyord` cluster nodes | `3` |
| `image.repository` | Image repository | `ghcr.io/conveyorq/conveyor` |
| `image.tag` | Image tag (empty uses the chart `appVersion`) | `""` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `imagePullSecrets` | Registry credentials for a private image | `[]` |
| `nameOverride` / `fullnameOverride` | Adjust generated resource names | `""` |
| `mode` | Deployment mode; the chart is built for `kubernetes` | `kubernetes` |

### Broker parameters (required for production)

| Name | Description | Default |
|---|---|---|
| `broker.driver` | `postgres` (durable) or `memory` (single-node, non-durable) | `postgres` |
| `broker.dsnSecret.name` | Existing Secret holding the DSN (**preferred**) | `""` |
| `broker.dsnSecret.key` | Key within that Secret | `dsn` |
| `broker.dsn` | Inline DSN (rendered into a chart-managed Secret) | `""` |

`broker.dsnSecret` takes precedence over `broker.dsn` when set.

### Authentication parameters

| Name | Description | Default |
|---|---|---|
| `auth.tokensSecret.name` | Existing Secret with a comma-separated token list (**preferred**) | `""` |
| `auth.tokensSecret.key` | Key within that Secret | `auth-tokens` |
| `auth.tokens` | Inline token list (rendered into a chart-managed Secret) | `[]` |
| `auth.allowUnauthenticated` | Run the API with auth off; leave `false` for production | `false` |

With no tokens and `allowUnauthenticated=false`, `conveyord` refuses to start.

### API & dashboard parameters

| Name | Description | Default |
|---|---|---|
| `api.dashboard` | Serve the embedded operations console at the API root | `true` |
| `api.corsOrigins` | Browser origins allowed cross-origin (host the UI off-box); empty disables CORS | `[]` |
| `api.grafanaUrl` | Surfaces a "Metrics" link in the dashboard | `""` |
| `api.tls.enabled` | Serve the API over TLS | `false` |
| `api.tls.certSecret` | Secret holding `tls.crt` and `tls.key` | `""` |

### Cluster parameters

| Name | Description | Default |
|---|---|---|
| `cluster.discovery.podLabels` | Peer-selecting labels; defaults to the release's own selector labels | `{}` |
| `cluster.tls.enabled` | Mutual TLS between cluster peers | `false` |
| `cluster.tls.certSecret` | Secret holding `tls.crt`, `tls.key`, and `ca.crt` | `""` |

### Engine parameters

Mirror `conveyord`'s own defaults; the ConfigMap is the single source of truth.

| Name | Description | Default |
|---|---|---|
| `engine.leaseTTL` | Lease lifetime before the reaper may reclaim a task | `60s` |
| `engine.leaseBatchMax` | Max tasks claimed per lease cycle | `100` |
| `engine.reapInterval` | Reaper tick cadence (recovery time ≈ `2×`) | `15s` |
| `engine.promoteInterval` | Scheduled-task promotion cadence | `1s` |
| `engine.passivateAfter` | Idle time before a queue grain deactivates | `5m` |
| `engine.defaultMaxRetry` | Retry budget for tasks that don't set their own | `25` |
| `engine.shutdownTimeout` | Graceful node drain on SIGTERM (must be `<` `terminationGracePeriodSeconds`) | `30s` |

### Observability parameters

| Name | Description | Default |
|---|---|---|
| `metrics.scrapeAnnotations` | Stamp `prometheus.io/{scrape,port,path}` on pods | `true` |
| `metrics.dropGatewaySeries` | Drop per-worker-session actor series to bound cardinality | `true` |
| `serviceMonitor.enabled` | Create a Prometheus-Operator `ServiceMonitor` (needs the CRD) | `false` |
| `serviceMonitor.interval` | Scrape interval | `30s` |
| `serviceMonitor.path` | Scrape path | `/metrics` |
| `otel.endpoint` | OTLP collector endpoint for metrics + traces; empty disables export | `""` |
| `otel.serviceName` | OTLP resource service name | `conveyord` |
| `log.level` | `debug` \| `info` \| `warn` \| `error` | `info` |
| `log.format` | `json` \| `text` | `json` |

### Scheduling & resilience parameters

| Name | Description | Default |
|---|---|---|
| `podDisruptionBudget.enabled` | Keep a quorum available during voluntary disruptions | `true` |
| `podDisruptionBudget.minAvailable` | Minimum available pods | `2` |
| `podAntiAffinity` | Spread peers across nodes: `soft` \| `hard` (`affinity` overrides) | `soft` |
| `terminationGracePeriodSeconds` | Must exceed `engine.shutdownTimeout` so sessions drain before SIGKILL | `45` |
| `networkPolicy.enabled` | Opt-in example restricting cluster ports to peer pods | `false` |
| `resources` | Container resource requests/limits | `250m`/`128Mi` requests |
| `nodeSelector` / `tolerations` / `affinity` | Standard pod scheduling controls | `{}` / `[]` / `{}` |
| `serviceAccount.create` | Create a dedicated ServiceAccount | `true` |
| `rbac.create` | Grant the ServiceAccount pod-list permission for discovery | `true` |

### Networking parameters

| Name | Description | Default |
|---|---|---|
| `service.type` | API Service type | `ClusterIP` |
| `service.port` | API Service port | `8080` |
| `ports.api` | API container port | `8080` |
| `ports.metrics` | Prometheus `/metrics` port (off the API port) | `9464` |
| `ports.remoting` | Cluster remoting port (pod-to-pod) | `9000` |
| `ports.gossip` | Cluster gossip/discovery port (pod-to-pod) | `9001` |
| `ports.cluster` | Cluster peers port (pod-to-pod) | `9002` |

The API and metrics ports are exposed through the `Service`; the three cluster
ports stay pod-to-pod via the headless service and are discovered by pod IP.

> **Tip:** specify the parameters above with `--set key=value[,key=value]`, or
> provide a YAML file with `-f my-values.yaml`. A values file is recommended for
> anything non-trivial — it is reviewable and reusable across upgrades.

## Configuration and installation details

### Securing the cluster with mutual TLS

```sh
kubectl create secret generic conveyor-cluster-tls \
  --from-file=tls.crt --from-file=tls.key --from-file=ca.crt

helm upgrade conveyor ./deploy/helm/conveyor --reuse-values \
  --set cluster.tls.enabled=true \
  --set cluster.tls.certSecret=conveyor-cluster-tls
```

### Scraping metrics with the Prometheus Operator

The `/metrics` endpoint is always live; with the Operator's CRD installed,
enable the `ServiceMonitor`:

```sh
helm upgrade conveyor ./deploy/helm/conveyor --reuse-values \
  --set serviceMonitor.enabled=true
```

Without the Operator, the default pod scrape annotations
(`metrics.scrapeAnnotations=true`) work with any annotation-based Prometheus.

### Hosting the dashboard on a separate origin

```sh
helm upgrade conveyor ./deploy/helm/conveyor --reuse-values \
  --set 'api.corsOrigins={https://ops.example.com}'
```

### Connecting clients and workers

In-cluster, connect to:

```
http://<release>.<namespace>.svc:8080
```

passing a configured token as a bearer credential (`conveyor.WithToken(...)` in
the Go SDK, or `--token` on the CLI). Externally, front the `Service` with an
Ingress/Gateway, or `kubectl port-forward` for local access.

## Upgrading

```sh
helm upgrade conveyor ./deploy/helm/conveyor -f my-values.yaml
```

- A ConfigMap checksum annotation rolls the pods automatically when the
  rendered configuration changes.
- The StatefulSet rolls **one pod at a time** (`OrderedReady`), so grain
  ownership, singletons, and leases rebalance gradually rather than all at once.
- Postgres schema migrations run automatically on node start.

## Limitations

- The chart targets `mode: kubernetes`. For non-Kubernetes deployments use the
  `standalone` or `cluster` modes — see the
  [operations guide](../../../docs/operations.md) and other artifacts under
  [`deploy/`](../..).
- No Postgres subchart is bundled — bring your own database.
- The `memory` broker is single-node and non-durable; never use it with
  `replicaCount > 1` or for data you cannot afford to lose on restart.
