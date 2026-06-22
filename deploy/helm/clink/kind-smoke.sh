#!/usr/bin/env bash
# kind-smoke.sh - end-to-end smoke for the clink Helm chart on a local kind
# cluster. Brings up a real (single-node) Kubernetes cluster, deploys the chart,
# and asserts the cluster converges: JobManager + TaskManagers Ready, every TM
# registered with the JM, and a clean cold start (0 TM restarts, proving the
# wait-for-jobmanager initContainer gates the JM race).
#
# It validates the CHART (deployment surface), not the engine - a job-submission
# end-to-end on k8s (build a CLINK_REGISTER_JOB .so matching the image's
# clink_node commit, submit via the control port) is a separate follow-on.
#
# Prereqs: docker, kind, kubectl, helm. The clink-runtime image must exist
# locally (build once: docker build -t clink-runtime:latest -f
# docker/Dockerfile.runtime . - needs clink-build:latest first).
#
# Usage:
#   deploy/helm/clink/kind-smoke.sh                 # run the smoke
#   deploy/helm/clink/kind-smoke.sh --cleanup       # delete the kind cluster
#
# Env overrides: CLUSTER, NAMESPACE, RELEASE, IMAGE, TM_REPLICAS, TM_SLOTS.
set -euo pipefail

CLUSTER="${CLUSTER:-clink-smoke}"
NAMESPACE="${NAMESPACE:-clink-smoke}"
RELEASE="${RELEASE:-clink}"
IMAGE="${IMAGE:-clink-runtime:latest}"
TM_REPLICAS="${TM_REPLICAS:-2}"
TM_SLOTS="${TM_SLOTS:-2}"
CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="kind-${CLUSTER}"

if [[ "${1:-}" == "--cleanup" ]]; then
  kind delete cluster --name "${CLUSTER}"
  exit 0
fi

step() { printf '\n=== %s ===\n' "$*"; }
fail() { printf 'SMOKE FAILED: %s\n' "$*" >&2; exit 1; }

step "preflight"
for t in docker kind kubectl helm; do command -v "$t" >/dev/null || fail "$t not found"; done
docker image inspect "${IMAGE}" >/dev/null 2>&1 || \
  fail "${IMAGE} not built (docker build -t clink-runtime:latest -f docker/Dockerfile.runtime .)"

step "kind cluster ${CLUSTER}"
kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" || kind create cluster --name "${CLUSTER}"
kubectl --context "${CTX}" wait --for=condition=Ready node --all --timeout=120s

step "load ${IMAGE} into the node"
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

step "helm upgrade --install"
helm --kube-context "${CTX}" upgrade --install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" --create-namespace \
  --set image.repository="${IMAGE%%:*}" --set image.tag="${IMAGE##*:}" \
  --set taskmanager.replicas="${TM_REPLICAS}" --set taskmanager.slots="${TM_SLOTS}" \
  --wait --timeout 4m

step "verify: all pods Ready"
kubectl --context "${CTX}" -n "${NAMESPACE}" wait --for=condition=Ready \
  pod -l "app.kubernetes.io/instance=${RELEASE}" --timeout=180s
kubectl --context "${CTX}" -n "${NAMESPACE}" get pods

step "verify: every TaskManager registered with the JobManager"
registered=$(kubectl --context "${CTX}" -n "${NAMESPACE}" exec "deploy/${RELEASE}-jobmanager" -- \
  curl -fsS "http://127.0.0.1:8081/api/v1/tms" | grep -o '"tm_id"' | wc -l | tr -d ' ')
echo "registered TMs: ${registered} (expected ${TM_REPLICAS})"
[[ "${registered}" == "${TM_REPLICAS}" ]] || fail "expected ${TM_REPLICAS} registered TMs, got ${registered}"

step "verify: clean cold start (0 TM restarts -> initContainer gated the JM race)"
restarts=$(kubectl --context "${CTX}" -n "${NAMESPACE}" get pods \
  -l "app.kubernetes.io/component=taskmanager" \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | paste -sd+ - | bc)
echo "total TM restarts: ${restarts}"
[[ "${restarts}" == "0" ]] || fail "expected 0 TM restarts on a clean cold start, got ${restarts}"

printf '\nSMOKE PASSED: %s TMs Ready + registered, 0 restarts.\n' "${TM_REPLICAS}"
printf 'Tear down with: %s --cleanup\n' "${BASH_SOURCE[0]}"
