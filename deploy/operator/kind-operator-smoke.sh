#!/usr/bin/env bash
#
# kind-operator-smoke.sh - end-to-end smoke for the clink Kubernetes operator.
#
# Spins up (or reuses) a kind cluster, builds + loads the operator image, installs
# the CRD + RBAC + manager, applies a ClinkCluster custom resource, and asserts the
# operator reconciles it into a running cluster: Coordinator + Workers Ready,
# every worker registered with the coordinator, and the ClinkCluster status reports Running with
# the expected WorkersReady count.
#
# Prereqs: docker, kind, kubectl. The clink-runtime image must exist locally
# (build once: docker build -t clink-runtime:latest -f docker/Dockerfile.runtime .).
#
# Usage:
#   deploy/operator/kind-operator-smoke.sh            # run the smoke
#   deploy/operator/kind-operator-smoke.sh --cleanup  # delete the kind cluster
set -euo pipefail

CLUSTER="${CLUSTER:-clink-op}"
CTX="kind-${CLUSTER}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-clink-runtime:latest}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-clink-operator:latest}"
CR_NAME="${CR_NAME:-demo}"
TM_REPLICAS="${TM_REPLICAS:-2}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }

if [[ "${1:-}" == "--cleanup" ]]; then
  kind delete cluster --name "${CLUSTER}"
  exit 0
fi

for t in docker kind kubectl; do command -v "$t" >/dev/null || fail "$t not found"; done
docker image inspect "${RUNTIME_IMAGE}" >/dev/null 2>&1 || \
  fail "${RUNTIME_IMAGE} not built (docker build -t clink-runtime:latest -f docker/Dockerfile.runtime .)"

step "build operator image ${OPERATOR_IMAGE}"
docker build -t "${OPERATOR_IMAGE}" "${HERE}"

step "kind cluster ${CLUSTER}"
kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" || kind create cluster --name "${CLUSTER}"
kubectl --context "${CTX}" wait --for=condition=Ready node --all --timeout=120s

step "load images into kind"
kind load docker-image "${OPERATOR_IMAGE}" --name "${CLUSTER}"
kind load docker-image "${RUNTIME_IMAGE}" --name "${CLUSTER}"

step "install CRD + RBAC + operator"
kubectl --context "${CTX}" apply -f "${HERE}/config/crd/"
kubectl --context "${CTX}" apply -f "${HERE}/config/manager/manager.yaml"
kubectl --context "${CTX}" apply -f "${HERE}/config/rbac/"

step "wait: operator Available"
kubectl --context "${CTX}" -n clink-operator-system rollout status deploy/clink-operator-controller --timeout=120s

step "apply ClinkCluster/${CR_NAME}"
kubectl --context "${CTX}" apply -f - <<YAML
apiVersion: clink.dev/v1alpha1
kind: ClinkCluster
metadata:
  name: ${CR_NAME}
spec:
  image:
    repository: ${RUNTIME_IMAGE%:*}
    tag: ${RUNTIME_IMAGE##*:}
    pullPolicy: IfNotPresent
  worker:
    replicas: ${TM_REPLICAS}
    slots: 4
YAML

step "wait: operator created the coordinator Deployment + worker StatefulSet"
for i in $(seq 1 30); do
  if kubectl --context "${CTX}" get deploy "${CR_NAME}-coordinator" >/dev/null 2>&1 && \
     kubectl --context "${CTX}" get statefulset "${CR_NAME}-worker" >/dev/null 2>&1; then
    echo "  owned workloads created (after ${i} attempt(s))"; break
  fi
  sleep 2
done
kubectl --context "${CTX}" get deploy "${CR_NAME}-coordinator" >/dev/null 2>&1 || fail "coordinator Deployment not created"
kubectl --context "${CTX}" get statefulset "${CR_NAME}-worker" >/dev/null 2>&1 || fail "worker StatefulSet not created"

step "wait: all cluster pods Ready"
kubectl --context "${CTX}" rollout status deploy/"${CR_NAME}-coordinator" --timeout=150s
kubectl --context "${CTX}" rollout status statefulset/"${CR_NAME}-worker" --timeout=150s
kubectl --context "${CTX}" get pods -l app.kubernetes.io/instance="${CR_NAME}"

step "verify: ClinkCluster status converges to Running + ${TM_REPLICAS} workers"
ok=""
for i in $(seq 1 40); do
  phase=$(kubectl --context "${CTX}" get clinkcluster "${CR_NAME}" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  ready=$(kubectl --context "${CTX}" get clinkcluster "${CR_NAME}" -o jsonpath='{.status.workersReady}' 2>/dev/null || true)
  echo "  status: phase=${phase:-<none>} workersReady=${ready:-0} (attempt ${i})"
  if [[ "${phase}" == "Running" && "${ready}" == "${TM_REPLICAS}" ]]; then ok=1; break; fi
  sleep 3
done
[[ -n "${ok}" ]] || fail "ClinkCluster never reached Running with ${TM_REPLICAS} workers"

step "verify: owner refs (deleting the CR would GC the workloads)"
owner=$(kubectl --context "${CTX}" get deploy "${CR_NAME}-coordinator" -o jsonpath='{.metadata.ownerReferences[0].kind}' 2>/dev/null || true)
[[ "${owner}" == "ClinkCluster" ]] || fail "coordinator Deployment not owned by ClinkCluster (got '${owner}')"

kubectl --context "${CTX}" get clinkcluster "${CR_NAME}"
printf '\nOPERATOR SMOKE PASSED: CR reconciled to Running, %s workers registered, workloads owner-refed.\n' "${TM_REPLICAS}"
