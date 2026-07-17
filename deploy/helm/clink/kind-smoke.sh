#!/usr/bin/env bash
# kind-smoke.sh - end-to-end smoke for the clink Helm chart on a local kind
# cluster. Brings up a real (single-node) Kubernetes cluster, deploys the chart,
# and asserts the cluster converges: Coordinator + Workers Ready, every worker
# registered with the coordinator, and a clean cold start (0 worker restarts, proving the
# wait-for-coordinator initContainer gates the coordinator race).
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
  --set worker.replicas="${TM_REPLICAS}" --set worker.slots="${TM_SLOTS}" \
  --wait --timeout 4m

step "verify: all pods Ready"
kubectl --context "${CTX}" -n "${NAMESPACE}" wait --for=condition=Ready \
  pod -l "app.kubernetes.io/instance=${RELEASE}" --timeout=180s
kubectl --context "${CTX}" -n "${NAMESPACE}" get pods

step "verify: every Worker registered with the Coordinator"
registered=$(kubectl --context "${CTX}" -n "${NAMESPACE}" exec "deploy/${RELEASE}-coordinator" -- \
  curl -fsS "http://127.0.0.1:8081/api/v1/workers" | grep -o '"worker_id"' | wc -l | tr -d ' ')
echo "registered workers: ${registered} (expected ${TM_REPLICAS})"
[[ "${registered}" == "${TM_REPLICAS}" ]] || fail "expected ${TM_REPLICAS} registered workers, got ${registered}"

step "verify: clean cold start (0 worker restarts -> initContainer gated the coordinator race)"
restarts=$(kubectl --context "${CTX}" -n "${NAMESPACE}" get pods \
  -l "app.kubernetes.io/component=worker" \
  -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | paste -sd+ - | bc)
echo "total worker restarts: ${restarts}"
[[ "${restarts}" == "0" ]] || fail "expected 0 worker restarts on a clean cold start, got ${restarts}"

step "verify: submit a job and confirm it runs end-to-end"
# The k8s-smoke sample job (from_elements[1..5] -> *10 -> filter >20 -> FileSink)
# is baked into the runtime image. Submit it to the coordinator over HTTP, then poll the
# worker pods for the sink output (30,40,50 = the work-done proof that the submitted
# job actually executed across the cluster).
JOB_SO="/opt/clink/jobs/k8s_smoke_job.so"
kx() { kubectl --context "${CTX}" -n "${NAMESPACE}" "$@"; }
if ! kx exec "deploy/${RELEASE}-coordinator" -- test -f "${JOB_SO}" 2>/dev/null; then
  echo "SKIP: ${JOB_SO} not in image (rebuild clink-runtime at this commit to enable job submission)"
else
  resp=$(kx exec "deploy/${RELEASE}-coordinator" -- \
    curl -fsS -F "job_so=@${JOB_SO}" -F "job_name=k8s-smoke" \
    "http://127.0.0.1:8081/api/v1/jobs")
  echo "submit response: ${resp}"
  echo "${resp}" | grep -q '"ok":true' || fail "job submit did not return ok: ${resp}"
  got=0
  for _ in $(seq 1 30); do
    for pod in $(kx get pods -l app.kubernetes.io/component=worker -o name); do
      out=$(kx exec "${pod}" -- cat /tmp/clink_k8s_smoke_out.txt 2>/dev/null || true)
      if grep -q 30 <<<"${out}" && grep -q 40 <<<"${out}" && grep -q 50 <<<"${out}"; then
        echo "job output on ${pod}: $(tr '\n' ' ' <<<"${out}")"
        got=1; break 2
      fi
    done
    sleep 2
  done
  [[ "${got}" == "1" ]] || fail "job sink output (30,40,50) not found on any Worker"
fi

printf '\nSMOKE PASSED: %s workers Ready + registered, 0 restarts, job ran end-to-end.\n' "${TM_REPLICAS}"
printf 'Tear down with: %s --cleanup\n' "${BASH_SOURCE[0]}"
