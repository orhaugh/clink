#!/usr/bin/env bash
# kind-ha-smoke.sh - multi-JobManager HA failover smoke on a kind cluster.
#
# Deploys the chart with ha.enabled (N JobManagers sharing an --ha-dir via a
# hostPath volume - correct on single-node kind), confirms a leader is elected
# and the TaskManagers register with it, then KILLS the leader and asserts a
# standby takes over (epoch bump + a different leader pod) and the TMs
# re-register with the new leader. This exercises clink's file HA coordinator
# (fcntl lock on the shared dir) across pods.
#
# Single-node kind makes the hostPath genuinely shared (fcntl works across
# co-located pods). A real multi-node cluster needs ha.storage.type=pvc with an
# RWX StorageClass; the etcd coordinator is the more robust election primitive
# but needs an etcd-enabled clink_node image (a documented follow-on).
#
# Usage: kind-ha-smoke.sh [--cleanup]   Env: CLUSTER NAMESPACE RELEASE IMAGE
#                                              JM_REPLICAS TM_REPLICAS
set -euo pipefail

CLUSTER="${CLUSTER:-clink-ha-smoke}"
NAMESPACE="${NAMESPACE:-clink-ha}"
RELEASE="${RELEASE:-clink}"
IMAGE="${IMAGE:-clink-runtime:latest}"
JM_REPLICAS="${JM_REPLICAS:-2}"
TM_REPLICAS="${TM_REPLICAS:-2}"
MOUNT="/var/lib/clink/ha"
CHART_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CTX="kind-${CLUSTER}"

if [[ "${1:-}" == "--cleanup" ]]; then kind delete cluster --name "${CLUSTER}"; exit 0; fi

step() { printf '\n=== %s ===\n' "$*"; }
fail() { printf 'HA SMOKE FAILED: %s\n' "$*" >&2; exit 1; }
kx() { kubectl --context "${CTX}" -n "${NAMESPACE}" "$@"; }
leader_json() { kx exec "deploy/${RELEASE}-jobmanager" -- cat "${MOUNT}/active-leader.json" 2>/dev/null || true; }
# Extract a top-level JSON scalar (string or number) by key; portable across
# BSD (macOS) and GNU sed - no \? extension.
jget() {
  local in; in="$(cat)"
  local v; v="$(grep -oE "\"$1\":\"[^\"]*\"" <<<"${in}" | head -1 | sed -E 's/^"[^"]*":"//; s/"$//')"
  [[ -n "${v}" ]] || v="$(grep -oE "\"$1\":[0-9]+" <<<"${in}" | head -1 | sed -E 's/^"[^"]*"://')"
  printf '%s' "${v}"
}
pod_for_ip() { kx get pods -l app.kubernetes.io/component=jobmanager \
  -o jsonpath='{range .items[*]}{.metadata.name} {.status.podIP}{"\n"}{end}' | awk -v ip="$1" '$2==ip{print $1}'; }
# registered TM count as seen by a specific JM pod (the leader)
registered_on() { kx exec "$1" -- curl -fsS "http://127.0.0.1:8081/api/v1/tms" 2>/dev/null | grep -o '"tm_id"' | wc -l | tr -d ' '; }

step "preflight"
for t in docker kind kubectl helm; do command -v "$t" >/dev/null || fail "$t not found"; done
docker image inspect "${IMAGE}" >/dev/null 2>&1 || fail "${IMAGE} not built"

step "kind cluster ${CLUSTER}"
kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" || kind create cluster --name "${CLUSTER}"
kubectl --context "${CTX}" wait --for=condition=Ready node --all --timeout=120s
kind load docker-image "${IMAGE}" --name "${CLUSTER}"

step "helm install (HA: ${JM_REPLICAS} JobManagers, shared hostPath --ha-dir)"
helm --kube-context "${CTX}" upgrade --install "${RELEASE}" "${CHART_DIR}" \
  --namespace "${NAMESPACE}" --create-namespace \
  --set image.repository="${IMAGE%%:*}" --set image.tag="${IMAGE##*:}" \
  --set ha.enabled=true --set ha.storage.type=hostPath --set ha.replicas="${JM_REPLICAS}" \
  --set taskmanager.replicas="${TM_REPLICAS}" --set taskmanager.slots=2 \
  --wait --timeout 4m
kx wait --for=condition=Ready pod -l "app.kubernetes.io/instance=${RELEASE}" --timeout=180s
kx get pods -o wide

step "verify: a leader is elected (${JM_REPLICAS} JM pods, one holds the lock)"
[[ "$(kx get pods -l app.kubernetes.io/component=jobmanager --no-headers | wc -l | tr -d ' ')" == "${JM_REPLICAS}" ]] \
  || fail "expected ${JM_REPLICAS} JobManager pods"
lj=""; for _ in $(seq 1 30); do lj="$(leader_json)"; grep -q '"host"' <<<"${lj}" && break; sleep 2; done
grep -q '"host"' <<<"${lj}" || fail "no leader elected (active-leader.json never appeared)"
host1="$(jget host <<<"${lj}")"; epoch1="$(jget epoch <<<"${lj}")"
leader1="$(pod_for_ip "${host1}")"
echo "leader: ${leader1} (host=${host1}, epoch=${epoch1})"
[[ -n "${leader1}" ]] || fail "could not map leader host ${host1} to a pod"

step "verify: TaskManagers registered with the leader"
got=0; for _ in $(seq 1 30); do [[ "$(registered_on "${leader1}")" == "${TM_REPLICAS}" ]] && { got=1; break; }; sleep 2; done
[[ "${got}" == "1" ]] || fail "TMs did not register with the leader (${leader1})"
echo "leader ${leader1} sees ${TM_REPLICAS} TMs"

step "FAILOVER: kill the leader; a standby must take over"
# The file coordinator's epoch is PER-PROCESS (each JM's first acquisition is
# epoch 1), so the unambiguous takeover signal is the leader HOST moving to a
# different pod, not a global epoch bump.
kx delete pod "${leader1}" --wait=false
new=0
for _ in $(seq 1 45); do
  lj="$(leader_json)"; h="$(jget host <<<"${lj}")"; e="$(jget epoch <<<"${lj}")"
  if [[ -n "${h}" && "${h}" != "${host1}" ]]; then new=1; break; fi
  sleep 2
done
[[ "${new}" == "1" ]] || fail "no standby took over (leader host stayed ${host1}): ${lj}"
leader2="$(pod_for_ip "${h}")"
echo "new leader: ${leader2:-<recreating>} (host=${h}, epoch=${e}) - took over from ${leader1} (${host1})"

step "verify: TaskManagers re-register with the new leader"
got=0; for _ in $(seq 1 45); do [[ "$(registered_on "${leader2}")" == "${TM_REPLICAS}" ]] && { got=1; break; }; sleep 2; done
[[ "${got}" == "1" ]] || fail "TMs did not re-register with the new leader (${leader2})"

printf '\nHA SMOKE PASSED: leader %s killed, standby %s took over (leader host %s -> %s), %s TMs re-registered.\n' \
  "${host1}" "${h}" "${host1}" "${h}" "${TM_REPLICAS}"
printf 'Tear down with: %s --cleanup\n' "${BASH_SOURCE[0]}"
