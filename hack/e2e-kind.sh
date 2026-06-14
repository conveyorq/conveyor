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

log "e2e PASSED: Postgres-backed ${REPLICAS}-node cluster formed, metrics served"
