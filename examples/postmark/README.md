# Postmark: a transactional email & notification platform

A production-like example application built on Conveyor: a miniature
transactional email and notification service, in the spirit of Postmark,
Resend, or Courier. An API accepts requests to notify users, and **every piece
of downstream work is a Conveyor task**. It runs as a real deployment: a
Postgres-backed, three-node `conveyord` cluster on Kubernetes with worker and
producer pods, so you can watch it flow, break it, and recover it.

The point is that each Conveyor feature falls out of the product naturally
rather than being bolted on. A password reset can't wait behind a
million-recipient newsletter, so queues are weighted. A 2FA code must jump the
line, so it carries a high priority. A flaky email provider must be retried but
a hard bounce must not, so failures map to retry or dead-letter. None of this is
contrived: it's how such a platform actually behaves.

> The "email provider" is **simulated**: no real SMTP, no credentials, no
> network egress. That keeps the example self-contained and, more importantly,
> makes its failure modes (flakiness, a full outage, hard bounces) controllable,
> so the retry, circuit-breaker, and dead-letter behaviors are demonstrable on
> demand rather than left to chance.

## What it shows

| Conveyor feature | How Postmark uses it |
| --- | --- |
| **Weighted queues** | `transactional` (10) ≫ `default` (5) ≫ `marketing` (1): resets and codes never wait behind a campaign blast. |
| **Push dispatch + concurrency** | Workers run 20 slots against a provider that accepts only 8 connections, so back-pressure is real, not theoretical. |
| **Per-task priority** | A 2FA code (priority 9) jumps ahead of a welcome email in the same transactional queue. |
| **Delayed / scheduled tasks** | The welcome series and trial-ending reminder are enqueued with `ProcessIn`, sent later, not now. |
| **Cron** | A weekly digest is materialized by a cron entry; kill the scheduler's node and it still fires. |
| **Unique tasks** | Password resets are keyed `user:<id>:password-reset`, so a "resend" storm collapses to one mail. |
| **Retries with backoff** | The provider fails a fraction of sends transiently; tasks retry and succeed. |
| **Dead-letter / archive** | A hard-bounce address returns `SkipRetry`; the task lands in the archive. |
| **Circuit breaker** | When the provider goes fully down, each task type's breaker trips, then recovers. |
| **Pause / resume** | The "stop all marketing now" incident button, while transactional keeps flowing. |
| **Per-key concurrency** | A campaign tags every send with its tenant, capping each tenant's in-flight sends. |
| **Rate limiting** | The marketing queue is capped at 20 sends/second to protect the provider. |
| **Retention** | Delivered receipts stay visible for the audit view before they're purged. |
| **Timeouts** | A 2FA send is abandoned if it can't complete fast, because a late code is useless. |
| **Crash safety / clustering** | Three Postgres-backed nodes; delete a pod under load and nothing is lost. |

## Run it

One command stands up the whole thing: a Postgres broker, a three-node
`conveyord` cluster, two worker pods, a producer pod, the queue and cron
configuration, and the live dashboard:

```sh
make postmark-demo
```

It builds the images, loads them into a throwaway [kind](https://kind.sigs.k8s.io)
cluster, installs the Helm chart, deploys the Postmark workload, configures the
queues and the weekly-digest cron, opens the dashboard, and blocks until you
press Ctrl-C (which tears the cluster down). Requires `docker`, `kind`,
`kubectl`, and `helm`.

The dashboard opens at <http://localhost:8080/>, already authenticated: the demo
opens it with the API token in the URL, which the dashboard stores client-side
and strips from the address bar, so there is nothing to type. Turn on
**Auto-refresh** and watch the three queues fill and drain: a deep, fast
`transactional` queue, a steady `default` queue, and a slow-draining `marketing`
queue.

## Things to try

While the demo runs (in another terminal), the cluster is yours to poke. Every
operation is a make target, so there's nothing to copy-paste:

```sh
make postmark-stats        # per-queue depth and pause flags
make postmark-pause        # stop all marketing sends (incident button); transactional keeps flowing
make postmark-resume       # resume marketing
make postmark-archived     # the dead-letter queue: hard-bounce campaign mail piling up
make postmark-events       # stream lifecycle events (enqueued, leased, retried, archived...)
make postmark-kill-node    # delete a server pod and watch zero task loss across failover
```

`make postmark-kill-node` is the headline: under continuous load it deletes a
node, the queues rebalance, in-flight work redelivers elsewhere, and the digest
cron survives, and nothing is lost.

The workers cycle a **provider outage every 4 minutes for 40 seconds**. During
each outage every send fails, each task type's circuit breaker trips, and the
dashboard's breaker indicator lights up; when the provider recovers, the
breakers close and the backlog drains. No action needed; just watch.

## Tear it down

Pressing Ctrl-C on `make postmark-demo` already deletes the throwaway cluster.
Otherwise:

```sh
make postmark-down    # remove just the Postmark workload + its queue/cron config; leave the cluster running
make postmark-clean   # delete the whole throwaway demo cluster
```

(`make postmark-down` pauses the weekly-digest cron rather than deleting it: the
broker keeps cron entries, and the admin CLI exposes pause/resume, not delete.)

## How it maps to the code

The application is broker-agnostic Go; the deployment makes it durable and
clustered.

| File | Responsibility |
| --- | --- |
| [`postmark.go`](postmark.go) | The vocabulary: queues, task types, priorities, and the `Email` payload. |
| [`provider.go`](provider.go) | The simulated provider: a connection limit, transient flakiness, hard bounces, and an outage switch. |
| [`handlers.go`](handlers.go) | The `Mux`: every send type, the weekly digest, and a logging middleware that labels each outcome (delivered / archived / retry). |
| [`produce.go`](produce.go) | The producer: the realistic, transactional-heavy mix of product actions and the per-action Conveyor options that exercise each feature. |
| [`cmd/worker`](cmd/worker) | The worker process: serves the three queues and delivers through the provider. |
| [`cmd/producer`](cmd/producer) | The producer process: drives a continuous workload. |
| [`deploy/`](deploy) | The image and Kubernetes manifests for running it in-cluster. |
| [`setup.sh`](setup.sh) | Configures per-queue concurrency, the marketing rate limit, and the digest cron through the admin CLI. |

## Deploy it on your own cluster

The `make postmark-*` targets above drive the throwaway kind cluster and need no
hand-typed `kubectl`. To run the same workload on a cluster you operate, the
building blocks are ordinary; adapt these to your namespace, image registry,
and API Service:

```sh
# 1. Build the workload image (worker + producer) and push it where your cluster can pull it.
docker build -f examples/postmark/deploy/Dockerfile -t postmark:e2e .

# 2. Deploy the workers and producer (they read the API URL and token from the
#    same conveyor-auth Secret the cluster setup creates).
kubectl -n conveyor apply -f examples/postmark/deploy/postmark.yaml

# 3. Configure the queues and the weekly-digest cron. Point the CLI at the API,
#    e.g. through a port-forward:
kubectl -n conveyor port-forward svc/conveyor 8080:8080 &
CONVEYOR_ADDR=http://localhost:8080 CONVEYOR_TOKEN=<your-token> \
  ./examples/postmark/setup.sh
```

Adjust the image reference, `imagePullPolicy`, and the API Service name in
`deploy/postmark.yaml` for your cluster. To remove the workload, delete the
manifest and run [`teardown.sh`](teardown.sh) (the inverse of `setup.sh`).

## Honest caveats

- **The provider is simulated.** A real SMTP or API integration would only
  distract from the queue mechanics, and would make the flaky/outage/bounce
  demos depend on a third party. The send is local and controllable by design.
- **Times are compressed.** The welcome follow-up (15s), trial-ending reminder
  (30s), and weekly digest (every minute) fire fast enough to watch in a demo. A
  real platform would use days and a Monday-morning cron; the mechanism is
  identical.
