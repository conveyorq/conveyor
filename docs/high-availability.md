# High-availability deployment

How the three tiers fit together for a Kubernetes deployment with no single point
of failure. This page is the high-level view that ties everything together. It
links out to the authoritative references rather than repeating them: the
[Helm chart README](../deploy/helm/conveyor/README.md) is the full install and
parameter reference, and the [operations guide](operations.md) covers
configuration, security, observability, and upgrades.

"High availability" here means that any one machine can die and the system keeps
running with nothing lost. Conveyor has three tiers, and HA means running more than
one of each:

1. **Servers** (`conveyord`): three nodes forming one cluster.
2. **Broker** (Postgres): the only durable store, so it needs its own HA setup.
3. **Workers**: your task-running processes, two or more, autoscaled on backlog.

Lose a server node and its work relocates. Lose a worker and its in-flight tasks
redeliver. The broker is the one tier that has to survive on its own.

## Topology

```
   producers                                   workers  (Deployment, autoscaled)
       │                                            │
       │  enqueue: HTTP/2 + bearer token            │  session stream (push)
       └─────────────────────┬──────────────────────┘
                             ▼
              ┌───────────────────────────┐
              │ Service  (ClusterIP)      │
              │ api :8080 · metrics :9464 │
              └───────────────────────────┘
                             │
                             ▼
   conveyord StatefulSet  ·  replicaCount: 3  ·  mode: kubernetes
   ┌──────────┐     ┌──────────┐     ┌──────────┐
   │  node-0  │◀───▶│  node-1  │◀───▶│  node-2  │   peer mTLS over
   └──────────┘     └──────────┘     └──────────┘   the headless service
        remoting :9000 · gossip :9001 · cluster :9002
                             │
                             ▼
   ┌─────────────────────────────────────────────────────────┐
   │ Postgres - HA: managed · CloudNativePG · Patroni/Stolon │
   └─────────────────────────────────────────────────────────┘
```

## Server tier

Install the cluster with the [Helm chart](../deploy/helm/conveyor/README.md). Its
README has the secrets, the install command, and every parameter. Nodes are
stateless. Queue grains and the cron and reaper singletons relocate on node loss
with lease recovery, so the server tier survives losing a node with zero task loss.

These are the settings that make it highly available. The chart defaults them
sensibly; they are listed here so you know what is doing the work:

- `replicaCount: 3` sets the number of cluster members.
- `podDisruptionBudget.minAvailable: 2` keeps a quorum during voluntary
  disruptions.
- `podAntiAffinity: soft` (or `hard`) spreads nodes across hosts.
- `cluster.tls.enabled: true` with a `certSecret` turns on mutual TLS between
  peers.
- `terminationGracePeriodSeconds` must exceed `engine.shutdownTimeout` so a node
  finishes draining before SIGKILL.

## Broker tier

Postgres is the durability and availability ceiling, and the chart does **not**
deploy it for you. Run an HA Postgres so the one stateful tier has no single point
of failure. Two common options:

- A managed HA service such as RDS, Cloud SQL, or Azure Database in Multi-AZ.
- An in-cluster operator such as [CloudNativePG](https://cloudnative-pg.io),
  [Patroni](https://patroni.readthedocs.io), or Stolon.

Point `broker.dsnSecret` at the cluster's read-write endpoint. Schema migrations
run automatically on connect. See [broker sizing](operations.md#broker-sizing-postgres)
for connection-pool and retention guidance.

## Worker tier

Workers are **your** processes, built with any [SDK](../README.md#sdks), external
and stateless. Run them as their own Deployment with two or more replicas. The
chart README's [connecting workers](../deploy/helm/conveyor/README.md#connecting-clients-and-workers)
note covers the address and token. A complete worker Deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata: { name: conveyor-worker, namespace: conveyor }
spec:
  replicas: 2                         # floor; the autoscaler grows it from here
  selector: { matchLabels: { app: conveyor-worker } }
  template:
    metadata: { labels: { app: conveyor-worker } }
    spec:
      terminationGracePeriodSeconds: 60   # > longest task; lets graceful drain finish
      containers:
        - name: worker
          image: your-registry/your-worker:1.0.0
          env:
            - { name: CONVEYOR_ADDR, value: "http://conveyor.conveyor.svc:8080" }
            - name: CONVEYOR_TOKEN
              valueFrom: { secretKeyRef: { name: conveyor-auth, key: auth-tokens } }
          # SIGTERM starts a drain: in-flight tasks are reported RELEASED (no retry
          # penalty) and redelivered to another worker, so rolling deploys cost
          # no work.
      affinity:                        # spread workers across nodes
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                topologyKey: kubernetes.io/hostname
                labelSelector: { matchLabels: { app: conveyor-worker } }
```

Handlers **must be idempotent**. Delivery is at-least-once, so a redelivered task
may run more than once.

### Autoscale on backlog (optional)

Queue depth is exported as the `conveyor_pending` metric, which is the natural
signal for scaling workers. With [KEDA](https://keda.sh) and a Prometheus instance
scraping Conveyor's `:9464`, scale the worker Deployment on backlog:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: conveyor-worker, namespace: conveyor }
spec:
  scaleTargetRef: { name: conveyor-worker }
  minReplicaCount: 2
  maxReplicaCount: 20
  cooldownPeriod: 120
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: sum(conveyor_pending)         # total backlog; scope by queue label if needed
        threshold: "100"                      # ~tasks pending per worker replica
```

## Failure scenarios (all zero-loss)

- **A server node is lost.** Its queue grains and singletons relocate to surviving
  nodes and recover their leases.
- **A worker is lost.** Its in-flight leases expire after roughly
  `2 × engine.reapInterval`, and the tasks redeliver to another worker.
- **A worker shuts down cleanly** (a rolling deploy). In-flight tasks are RELEASED
  and redelivered immediately with no retry penalty, so deploys cost no work.

See [upgrades & restarts](operations.md#upgrades--restarts) for the rolling-upgrade
order (server tier first, then workers) and the version-skew policy.

## Try it locally first

`hack/e2e-kind.sh` (`make e2e`) stands up exactly this topology on a throwaway
[kind](https://kind.sigs.k8s.io) cluster: three nodes, Postgres, Kubernetes
discovery, and a secret-injected DSN and token. It drives load through a
one-pod-at-a-time rolling restart and asserts that the cluster reforms and loses
zero tasks. Use it as the runnable reference for everything above.

## Where to go next

- [Helm chart README](../deploy/helm/conveyor/README.md): full install, every
  parameter, mTLS, and the ServiceMonitor.
- [Operations guide](operations.md): configuration, scaling, security,
  observability, and upgrades.
- [Architecture](architecture.md): how the cluster recovers work internally.
