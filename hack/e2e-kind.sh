#!/usr/bin/env bash
#
# kind-based end-to-end test for the deployment packaging, exercising a setup
# close to production: a Postgres broker, three conveyord replicas in
# kubernetes mode, peer discovery through the Kubernetes API (RBAC), the DSN
# and API tokens delivered as Secrets, and metrics on the dedicated port.
#
# It builds the image, loads it into a throwaway kind cluster, installs the
# Helm chart, and asserts the StatefulSet rolls out, the three nodes form one
# cluster, and the metrics endpoint serves. The cluster is always deleted on
# exit, so developers can run it locally with `make e2e`.
#
# Requires: docker, kind, kubectl, helm.
set -euo pipefail

readonly CLUSTER="conveyor-e2e"
readonly RELEASE="conveyor"
readonly NAMESPACE="conveyor"
readonly IMAGE="conveyor:e2e"
readonly LOAD_IMAGE="conveyor-e2e-load:e2e"
readonly REPLICAS=3
readonly TOKEN="e2e-token"
readonly DSN="postgres://conveyor:conveyor@postgres.${NAMESPACE}.svc:5432/conveyor?sslmode=disable"
readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

forward_pid=""

log() { printf '\n=== %s ===\n' "$*"; }

cleanup() {
  [[ -n "${forward_pid}" ]] && kill "${forward_pid}" >/dev/null 2>&1 || true
  log "tearing down kind cluster ${CLUSTER}"
  kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

log "creating kind cluster ${CLUSTER}"
kind create cluster --name "${CLUSTER}" --wait 120s

log "building conveyor image ${IMAGE}"
docker build -f "${ROOT}/deploy/docker/Dockerfile" -t "${IMAGE}" "${ROOT}"

log "loading image into kind"
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

kubectl create namespace "${NAMESPACE}"

log "deploying Postgres broker"
kubectl -n "${NAMESPACE}" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
        - name: postgres
          image: postgres:16-alpine
          env:
            - { name: POSTGRES_USER, value: conveyor }
            - { name: POSTGRES_PASSWORD, value: conveyor }
            - { name: POSTGRES_DB, value: conveyor }
          ports:
            - containerPort: 5432
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "conveyor", "-d", "conveyor"]
            periodSeconds: 3
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector:
    app: postgres
  ports:
    - port: 5432
      targetPort: 5432
YAML

log "waiting for Postgres to become ready"
kubectl -n "${NAMESPACE}" rollout status deployment/postgres --timeout 120s

log "creating broker DSN and API token Secrets"
kubectl -n "${NAMESPACE}" create secret generic conveyor-broker --from-literal=dsn="${DSN}"
kubectl -n "${NAMESPACE}" create secret generic conveyor-auth --from-literal=auth-tokens="${TOKEN}"

log "installing the chart (Postgres, ${REPLICAS} replicas, auth on)"
helm install "${RELEASE}" "${ROOT}/deploy/helm/conveyor" \
  --namespace "${NAMESPACE}" \
  --set image.repository=conveyor \
  --set image.tag=e2e \
  --set image.pullPolicy=Never \
  --set replicaCount="${REPLICAS}" \
  --set broker.driver=postgres \
  --set broker.dsnSecret.name=conveyor-broker \
  --set auth.tokensSecret.name=conveyor-auth \
  --wait --timeout 300s

log "waiting for the StatefulSet to roll out"
kubectl -n "${NAMESPACE}" rollout status "statefulset/${RELEASE}" --timeout 300s

log "asserting all ${REPLICAS} nodes formed one cluster via Kubernetes discovery"
# The distroless image ships the conveyor CLI; exec it directly (no shell). The
# CLI prints an "ADDRESS  STARTED_AT" table; each node row carries a host:port,
# so counting ":<port>" lines counts the members.
info=$(kubectl -n "${NAMESPACE}" exec "${RELEASE}-0" -- \
  conveyor cluster info --addr "http://localhost:8080" --token "${TOKEN}")
echo "${info}"
nodes=$(echo "${info}" | grep -cE ':[0-9]+' || true)
echo "cluster info reports ${nodes} node(s)"
if [[ "${nodes}" -ne "${REPLICAS}" ]]; then
  echo "FAIL: expected ${REPLICAS} cluster members, got ${nodes}"
  exit 1
fi

log "asserting the metrics endpoint serves conveyor + actor series"
kubectl -n "${NAMESPACE}" port-forward "statefulset/${RELEASE}" 9464:9464 >/dev/null 2>&1 &
forward_pid=$!
sleep 3
metrics=$(curl -fsS "http://localhost:9464/metrics")
echo "${metrics}" | grep -q "^conveyor_enqueued_total" || { echo "FAIL: missing conveyor metrics"; exit 1; }
echo "${metrics}" | grep -q "^actor_" || { echo "FAIL: missing GoAkt actor metrics"; exit 1; }

# ---------------------------------------------------------------------------
# Rolling-restart under load: a workload driver runs in-cluster as producer and
# worker against the load-balanced API Service while the StatefulSet is rolled
# one pod at a time. The driver loses nothing and finishes every task, proving
# clients keep processing across a server rolling upgrade.
# ---------------------------------------------------------------------------
log "building workload driver image ${LOAD_IMAGE}"
docker build -f "${ROOT}/hack/e2e-load.Dockerfile" -t "${LOAD_IMAGE}" "${ROOT}"
kind load docker-image "${LOAD_IMAGE}" --name "${CLUSTER}"

log "starting the workload driver Job (producer + worker)"
kubectl -n "${NAMESPACE}" apply -f - <<YAML
apiVersion: batch/v1
kind: Job
metadata:
  name: conveyor-load
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: load
          image: ${LOAD_IMAGE}
          imagePullPolicy: Never
          args: ["--total=300", "--interval=400ms", "--drain-timeout=4m"]
          env:
            - { name: CONVEYOR_ADDR, value: "http://${RELEASE}.${NAMESPACE}.svc:8080" }
            - { name: CONVEYOR_TOKEN, value: "${TOKEN}" }
YAML

log "waiting for the driver to begin producing under load"
kubectl -n "${NAMESPACE}" wait --for=condition=ready pod -l job-name=conveyor-load --timeout 60s
sleep 15

log "rolling the StatefulSet while the driver runs"
kubectl -n "${NAMESPACE}" rollout restart "statefulset/${RELEASE}"
kubectl -n "${NAMESPACE}" rollout status "statefulset/${RELEASE}" --timeout 300s

log "asserting the cluster reformed to ${REPLICAS} nodes after the roll"
info=$(kubectl -n "${NAMESPACE}" exec "${RELEASE}-0" -- \
  conveyor cluster info --addr "http://localhost:8080" --token "${TOKEN}")
echo "${info}"
nodes=$(echo "${info}" | grep -cE ':[0-9]+' || true)
if [[ "${nodes}" -ne "${REPLICAS}" ]]; then
  echo "FAIL: expected ${REPLICAS} cluster members after the roll, got ${nodes}"
  exit 1
fi

log "waiting for the workload driver to finish (zero task loss)"
if ! kubectl -n "${NAMESPACE}" wait --for=condition=complete job/conveyor-load --timeout 300s; then
  echo "FAIL: workload driver did not complete every task across the rolling restart"
  kubectl -n "${NAMESPACE}" logs job/conveyor-load --tail 50 || true
  exit 1
fi
kubectl -n "${NAMESPACE}" logs job/conveyor-load --tail 5

log "e2e PASSED: Postgres-backed ${REPLICAS}-node cluster formed, metrics served, survived a rolling restart under load"
