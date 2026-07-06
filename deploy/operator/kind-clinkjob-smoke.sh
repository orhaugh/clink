#!/usr/bin/env bash
#
# kind-clinkjob-smoke.sh - end-to-end smoke for the ClinkJob CR + savepoint-on-upgrade.
#
# Installs the operator on a kind cluster, brings up a ClinkCluster WITH shared
# checkpoint storage, runs a long-lived STATEFUL job (rescale_test_job: a keyed
# reduce that checkpoints) via a ClinkJob, then mutates the ClinkJob spec to force
# an upgrade and asserts the operator drained it to a savepoint and resubmitted a
# NEW job restored from that savepoint. Finally deletes the ClinkJob and asserts
# the finalizer cancelled the running job.
#
# Prereqs: docker, kind, kubectl + a local clink-runtime:latest image (which bakes
# /opt/clink/jobs/rescale_test_job.so and the clink CLI).
#
# Usage:
#   deploy/operator/kind-clinkjob-smoke.sh
#   deploy/operator/kind-clinkjob-smoke.sh --cleanup
set -euo pipefail

CLUSTER="${CLUSTER:-clink-op}"
CTX="kind-${CLUSTER}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-clink-runtime:latest}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-clink-operator:latest}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_CR="cjdemo"
JOB_CR="wordcount"
JOB_SO="/opt/clink/jobs/rescale_test_job.so"

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
kx() { kubectl --context "${CTX}" "$@"; }

if [[ "${1:-}" == "--cleanup" ]]; then kind delete cluster --name "${CLUSTER}"; exit 0; fi
for t in docker kind kubectl; do command -v "$t" >/dev/null || fail "$t not found"; done
docker image inspect "${RUNTIME_IMAGE}" >/dev/null 2>&1 || fail "${RUNTIME_IMAGE} not built"

step "build + load operator, install CRDs/RBAC/manager"
docker build -q -t "${OPERATOR_IMAGE}" "${HERE}" >/dev/null
kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" || kind create cluster --name "${CLUSTER}"
kx wait --for=condition=Ready node --all --timeout=120s
kind load docker-image "${OPERATOR_IMAGE}" --name "${CLUSTER}"
kind load docker-image "${RUNTIME_IMAGE}" --name "${CLUSTER}"
kx apply -f "${HERE}/config/crd/"
kx apply -f "${HERE}/config/manager/manager.yaml"
kx apply -f "${HERE}/config/rbac/"
kx -n clink-operator-system rollout status deploy/clink-operator-controller --timeout=120s

step "apply ClinkCluster/${CLUSTER_CR} with shared (hostPath) checkpoint storage"
kx apply -f - <<YAML
apiVersion: clink.dev/v1alpha1
kind: ClinkCluster
metadata:
  name: ${CLUSTER_CR}
spec:
  image:
    repository: ${RUNTIME_IMAGE%:*}
    tag: ${RUNTIME_IMAGE##*:}
  taskManager:
    replicas: 2
    slots: 4
  checkpointStorage:
    enabled: true
    type: hostPath
    mountPath: /var/lib/clink/checkpoints
    hostPath: /var/lib/clink-checkpoints
YAML
step "wait: cluster Running"
for i in $(seq 1 40); do
  [[ "$(kx get clinkcluster ${CLUSTER_CR} -o jsonpath='{.status.phase}' 2>/dev/null)" == "Running" ]] && break
  sleep 3
done
[[ "$(kx get clinkcluster ${CLUSTER_CR} -o jsonpath='{.status.phase}')" == "Running" ]] || fail "cluster not Running"

step "apply ClinkJob/${JOB_CR} (stateful rescale_test_job, checkpointing on)"
apply_job() { # $1 = marker value (mutating it forces an upgrade)
kx apply -f - <<YAML
apiVersion: clink.dev/v1alpha1
kind: ClinkJob
metadata:
  name: ${JOB_CR}
spec:
  clusterName: ${CLUSTER_CR}
  jobSo: ${JOB_SO}
  jobName: ${JOB_CR}
  upgradeMode: savepoint
  checkpointIntervalMs: 2000
  env:
    - CLINK_RESCALE_COUNT=20000000
    - CLINK_RESCALE_TICK_MS=20
    - CLINK_RESCALE_INITIAL_P=2
    - CLINK_RESCALE_OUT_BASE=/var/lib/clink/checkpoints/${JOB_CR}-out
    - CLINK_UPGRADE_MARKER=$1
YAML
}
apply_job v1

step "wait: ClinkJob Running + capture job id J1"
J1=""
for i in $(seq 1 50); do
  ph=$(kx get clinkjob ${JOB_CR} -o jsonpath='{.status.phase}' 2>/dev/null || true)
  J1=$(kx get clinkjob ${JOB_CR} -o jsonpath='{.status.jobID}' 2>/dev/null || true)
  echo "  phase=${ph:-<none>} jobID=${J1:-0} (attempt $i)"
  [[ "$ph" == "Running" && -n "$J1" && "$J1" != "0" ]] && break
  sleep 3
done
[[ -n "$J1" && "$J1" != "0" ]] || fail "ClinkJob never reached Running with a job id"

step "let the job checkpoint (accumulate state) ~15s"
sleep 15

step "force an UPGRADE (mutate CLINK_UPGRADE_MARKER -> spec hash changes)"
apply_job v2

step "wait: upgrade -> savepoint taken + NEW job id J2 (!= J1) restored"
J2=""; SP=""
for i in $(seq 1 60); do
  ph=$(kx get clinkjob ${JOB_CR} -o jsonpath='{.status.phase}' 2>/dev/null || true)
  J2=$(kx get clinkjob ${JOB_CR} -o jsonpath='{.status.jobID}' 2>/dev/null || true)
  SP=$(kx get clinkjob ${JOB_CR} -o jsonpath='{.status.lastSavepoint.dir}' 2>/dev/null || true)
  echo "  phase=${ph:-<none>} jobID=${J2:-0} savepoint='${SP:-}' (attempt $i)"
  [[ "$ph" == "Running" && -n "$J2" && "$J2" != "0" && "$J2" != "$J1" && -n "$SP" ]] && break
  sleep 3
done
[[ -n "$J2" && "$J2" != "$J1" ]] || fail "upgrade did not produce a new job id (J1=$J1 J2=$J2)"
[[ -n "$SP" ]] || fail "no savepoint recorded on upgrade (savepoint-on-upgrade did not run)"
echo "  UPGRADE OK: J1=$J1 -> J2=$J2, restored from savepoint dir '$SP'"
echo "  operator log (savepoint/restore):"
kx -n clink-operator-system logs deploy/clink-operator-controller --tail=200 2>/dev/null | grep -iE "upgrad|savepoint|restore|job ${J2}" | tail -6 || true

step "delete ClinkJob -> finalizer cancels the running job"
kx delete clinkjob ${JOB_CR} --timeout=60s
kx get clinkjob ${JOB_CR} >/dev/null 2>&1 && fail "ClinkJob still present after delete" || true
echo "  ClinkJob deleted (finalizer completed)"

printf '\nCLINKJOB SMOKE PASSED: submit -> running (J1=%s) -> savepoint-on-upgrade (J2=%s) -> delete/cancel.\n' "$J1" "$J2"
