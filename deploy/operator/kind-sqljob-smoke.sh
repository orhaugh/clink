#!/usr/bin/env bash
#
# kind-sqljob-smoke.sh - a SQL ClinkJob on a secured HA ClinkCluster, end to end.
#
# On a kind cluster: the operator, a single-node Kafka, and a ClinkCluster with
# two Coordinators (HA over a hostPath directory), token auth on the HTTP API and
# mutual TLS on the control plane. A SQL ClinkJob counts events per key per
# minute into a transactional Kafka topic, reading from the latest offset, so a
# job that lost its restore point would miss every event before it. Then:
#
#   1. the API refuses a request without the token and answers one with it;
#   2. an upgrade (a new script): the operator takes a savepoint over the API,
#      cancels the job, and submits the new script restored from the savepoint;
#   3. a failover: the leading Coordinator's pod is deleted, the standby takes
#      over, and the job runs on under the same id;
#   4. the window closes and its count is read: every event counted once,
#      across the upgrade and the failover;
#   5. deleting the ClinkJob cancels the job.
#
# Prereqs: docker, kind, kubectl, openssl, python3, and a clink runtime image
# (RUNTIME_IMAGE) whose Coordinator takes catalog=own on POST /api/v1/jobs/sql.
#
# Usage:
#   RUNTIME_IMAGE=clink-runtime:dev-<sha> deploy/operator/kind-sqljob-smoke.sh
#   deploy/operator/kind-sqljob-smoke.sh --cleanup
set -euo pipefail

CLUSTER="${CLUSTER:-clink-sqlop}"
CTX="kind-${CLUSTER}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-clink-runtime:latest}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-clink-operator:latest}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.9.1}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NS=default
CC=sqldemo
JOB=counts
KAFKA="clink-kafka.${NS}.svc.cluster.local:9092"
API="http://${CC}-coordinator:8081"

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
kx() { kubectl --context "${CTX}" -n "${NS}" "$@"; }
kafka() { local tool=$1; shift; kx exec clink-kafka -- "/opt/kafka/bin/${tool}" "$@"; }
jsonpath() { kx get "$1" -o jsonpath="{$2}" 2>/dev/null || true; }
# api <pod> <path> [curl args...]: the Coordinator API from inside the cluster,
# with the token the pod holds.
api() {
  local pod=$1 path=$2; shift 2
  kx exec "$pod" -c coordinator -- sh -c \
    "curl -sS -H \"Authorization: Bearer \$CLINK_AUTH_TOKEN\" $* '${API}${path}'"
}
role_of() { kx exec "$1" -c coordinator -- curl -s "http://localhost:8081/readyz" 2>/dev/null | tr -d '[:space:]'; }
leader() {
  local p
  for p in "${CC}-coordinator-0" "${CC}-coordinator-1"; do
    [[ "$(role_of "$p")" == "leader" ]] && { echo "$p"; return; }
  done
}
produce() { # produce <key> <first-ts> <count>: one event per 100 ms of event time
  python3 -c "
import sys
k, t0, n = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
for i in range(n):
    print('{\"k\":\"%s\",\"ts\":%d}' % (k, t0 + 100 * i))" "$@" |
    kx exec -i clink-kafka -- /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 --topic smoke.events >/dev/null
}

if [[ "${1:-}" == "--cleanup" ]]; then kind delete cluster --name "${CLUSTER}"; exit 0; fi
for t in docker kind kubectl openssl python3; do command -v "$t" >/dev/null || fail "$t not found"; done
docker image inspect "${RUNTIME_IMAGE}" >/dev/null 2>&1 || fail "${RUNTIME_IMAGE} not built"
docker image inspect "${KAFKA_IMAGE}" >/dev/null 2>&1 || docker pull -q "${KAFKA_IMAGE}" >/dev/null

step "build + load the operator, install CRDs, RBAC and the manager"
docker build -q -t "${OPERATOR_IMAGE}" "${HERE}" >/dev/null
kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" || kind create cluster --name "${CLUSTER}"
kubectl --context "${CTX}" wait --for=condition=Ready node --all --timeout=120s
# The HA directory and checkpoints are hostPaths on the one node: start them empty.
docker exec "${CLUSTER}-control-plane" rm -rf /var/lib/clink-sqlop-ha /var/lib/clink-sqlop-checkpoints
for img in "${OPERATOR_IMAGE}" "${RUNTIME_IMAGE}" "${KAFKA_IMAGE}"; do
  kind load docker-image "${img}" --name "${CLUSTER}" >/dev/null
done
kubectl --context "${CTX}" apply --server-side --force-conflicts -f "${HERE}/config/crd/" >/dev/null
kubectl --context "${CTX}" apply -f "${HERE}/config/manager/manager.yaml" >/dev/null
kubectl --context "${CTX}" apply -f "${HERE}/config/rbac/" >/dev/null
kubectl --context "${CTX}" -n clink-operator-system rollout restart deploy/clink-operator-controller >/dev/null
kubectl --context "${CTX}" -n clink-operator-system rollout status deploy/clink-operator-controller --timeout=120s

step "single-node Kafka, with the events and counts topics"
kx apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: clink-kafka
  labels: { app: clink-kafka }
spec:
  containers:
    - name: kafka
      image: ${KAFKA_IMAGE}
      ports: [{ containerPort: 9092 }, { containerPort: 9093 }]
      env:
        - { name: KAFKA_NODE_ID, value: "1" }
        - { name: KAFKA_PROCESS_ROLES, value: "broker,controller" }
        - { name: KAFKA_LISTENERS, value: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093" }
        - { name: KAFKA_ADVERTISED_LISTENERS, value: "PLAINTEXT://${KAFKA}" }
        - { name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER" }
        - { name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" }
        - { name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093" }
        - { name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1" }
        - { name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1" }
        - { name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1" }
        - { name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS, value: "0" }
        - { name: CLUSTER_ID, value: "MkU3OEVBNTcwNTJENDM2Qg" }
---
apiVersion: v1
kind: Service
metadata:
  name: clink-kafka
spec:
  selector: { app: clink-kafka }
  ports: [{ port: 9092, targetPort: 9092 }]
YAML
kx wait --for=condition=Ready pod/clink-kafka --timeout=120s >/dev/null
for _ in $(seq 1 30); do
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && break
  sleep 2
done
for topic in smoke.events smoke.counts; do
  kafka kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "${topic}" --partitions 1 --replication-factor 1 >/dev/null
done

step "secrets: the API token, the Coordinators' certificate and the Workers' client certificate"
PKI="$(mktemp -d)"; trap 'rm -rf "${PKI}"' EXIT
(
  umask 077
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj "/CN=sqljob smoke CA" \
    -keyout "${PKI}/ca.key" -out "${PKI}/ca.crt" 2>/dev/null
  issue() { # issue <name> <subjectAltName> <extendedKeyUsage>
    openssl req -newkey rsa:2048 -nodes -subj "/CN=$1" -keyout "${PKI}/$1.key" -out "${PKI}/$1.csr" 2>/dev/null
    openssl x509 -req -in "${PKI}/$1.csr" -CA "${PKI}/ca.crt" -CAkey "${PKI}/ca.key" -CAcreateserial \
      -days 2 -out "${PKI}/$1.crt" \
      -extfile <(printf 'subjectAltName=%s\nextendedKeyUsage=%s\n' "$2" "$3") 2>/dev/null
  }
  svc="${CC}-coordinator"
  issue coordinator "DNS:${svc},DNS:${svc}.${NS},DNS:${svc}.${NS}.svc,DNS:${svc}.${NS}.svc.cluster.local,DNS:*.${svc}-headless.${NS}.svc.cluster.local" serverAuth
  issue worker "DNS:${CC}-worker" clientAuth
  printf '%s' "$(openssl rand -hex 32)" > "${PKI}/token"
)
secret() { kx create secret "$@" --dry-run=client -o yaml | kx apply -f - >/dev/null; }
secret generic clink-auth --from-file=token="${PKI}/token"
for who in coordinator worker; do
  secret generic "clink-tls-${who}" --type=kubernetes.io/tls --from-file=tls.crt="${PKI}/${who}.crt" \
    --from-file=tls.key="${PKI}/${who}.key" --from-file=ca.crt="${PKI}/ca.crt"
done

step "ClinkCluster/${CC}: two Coordinators, two Workers, token auth, mutual TLS"
kx apply -f - >/dev/null <<YAML
apiVersion: clink.dev/v1alpha1
kind: ClinkCluster
metadata:
  name: ${CC}
spec:
  image:
    repository: ${RUNTIME_IMAGE%:*}
    tag: ${RUNTIME_IMAGE##*:}
  worker:
    replicas: 2
    slots: 4
  ha:
    enabled: true
    replicas: 2
    type: hostPath
    hostPath: /var/lib/clink-sqlop-ha
  checkpointStorage:
    enabled: true
    type: hostPath
    mountPath: /var/lib/clink/checkpoints
    hostPath: /var/lib/clink-sqlop-checkpoints
  auth:
    existingSecret: clink-auth
  tls:
    enabled: true
    coordinatorSecret: clink-tls-coordinator
    workerSecret: clink-tls-worker
YAML
for _ in $(seq 1 60); do
  [[ "$(jsonpath "clinkcluster/${CC}" .status.phase)" == "Running" ]] && break
  sleep 5
done
[[ "$(jsonpath "clinkcluster/${CC}" .status.phase)" == "Running" ]] ||
  fail "cluster not Running (workersReady=$(jsonpath "clinkcluster/${CC}" .status.workersReady))"
LEADER="$(leader)"
[[ -n "${LEADER}" ]] || fail "no Coordinator says it leads"
echo "  Running: workersReady=$(jsonpath "clinkcluster/${CC}" .status.workersReady), leader ${LEADER}"

step "the API refuses a request without the token, and answers one with it"
# The pod the smoke asks the API from (through the Service, so any will do).
VIA="${CC}-coordinator-0"; [[ "${LEADER}" == "${VIA}" ]] && VIA="${CC}-coordinator-1"
code="$(kx exec "${VIA}" -c coordinator -- curl -s -o /dev/null -w '%{http_code}' "${API}/api/v1/jobs")"
[[ "${code}" == "401" ]] || fail "a request without the token got HTTP ${code}"
code="$(api "${VIA}" /api/v1/jobs "-o /dev/null -w '%{http_code}'")"
[[ "${code}" == "200" ]] || fail "a request with the token got HTTP ${code}"
echo "  without: 401, with: 200"

apply_job() { # apply_job <version>: the same job, a new script
kx apply -f - >/dev/null <<YAML
apiVersion: clink.dev/v1alpha1
kind: ClinkJob
metadata:
  name: ${JOB}
spec:
  clusterName: ${CC}
  checkpointIntervalMs: 2000
  sql: |
    -- Events per key per minute. Script version $1.
    CREATE TABLE events (k VARCHAR, ts BIGINT) WITH (
        connector         = 'kafka',
        brokers           = '${KAFKA}',
        topic             = 'smoke.events',
        format            = 'json',
        auto_offset_reset = 'latest',
        event_time_column = 'ts',
        watermark_lag_ms  = '1000'
    );
    CREATE TABLE counts (k VARCHAR, window_start BIGINT, window_end BIGINT, n BIGINT) WITH (
        connector          = 'kafka',
        brokers            = '${KAFKA}',
        topic              = 'smoke.counts',
        format             = 'json',
        delivery_guarantee = 'exactly_once',
        transactional_id   = 'smoke-counts'
    );
    INSERT INTO counts
    SELECT k, window_start, window_end, COUNT(*) AS n
    FROM events
    GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE), k;
YAML
}
checkpoint_of() { # checkpoint_of <job id>: its latest completed checkpoint, or nothing
  api "${VIA}" "/api/v1/jobs/$1" 2>/dev/null |
    python3 -c 'import json,sys; print(json.load(sys.stdin)["latest_completed_checkpoint_id"])' 2>/dev/null || true
}
await_checkpoint() { # await_checkpoint <job id>: a checkpoint completes after now
  local id=$1 before="" now
  for _ in $(seq 1 20); do
    before="$(checkpoint_of "${id}")"; [[ -n "${before}" ]] && break
    sleep 1
  done
  [[ -n "${before}" ]] || fail "cannot read job ${id}'s checkpoints"
  for _ in $(seq 1 30); do
    now="$(checkpoint_of "${id}")"
    [[ -n "${now}" ]] && (( now > before )) && return
    sleep 1
  done
  fail "job ${id} completed no checkpoint after ${before}"
}

step "ClinkJob/${JOB} v1: submitted over the API, reading from the latest offset"
apply_job 1
J1=""
for _ in $(seq 1 40); do
  [[ "$(jsonpath "clinkjob/${JOB}" .status.phase)" == "Running" ]] && J1="$(jsonpath "clinkjob/${JOB}" .status.jobID)" && break
  sleep 3
done
[[ -n "${J1}" && "${J1}" != "0" ]] || fail "ClinkJob never ran: $(jsonpath "clinkjob/${JOB}" .status.message)"
name="$(api "${VIA}" "/api/v1/jobs/${J1}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["name"])')"
[[ "${name}" == "${JOB}" ]] || fail "job ${J1} is named '${name}'"
# Its first checkpoint carries the offset it resolved, so what is produced
# from here on is read.
await_checkpoint "${J1}"
produce a 1000 50
await_checkpoint "${J1}"
echo "  job ${J1} running as '${name}'; 50 events produced"

step "upgrade to v2: savepoint, cancel, submit restored from the savepoint"
apply_job 2
J2=""; SP_DIR=""; SP_ID=""
for _ in $(seq 1 40); do
  J2="$(jsonpath "clinkjob/${JOB}" .status.jobID)"
  if [[ "$(jsonpath "clinkjob/${JOB}" .status.phase)" == "Running" && -n "${J2}" && "${J2}" != "${J1}" ]]; then
    SP_DIR="$(jsonpath "clinkjob/${JOB}" .status.lastSavepoint.dir)"
    SP_ID="$(jsonpath "clinkjob/${JOB}" .status.lastSavepoint.checkpointId)"
    break
  fi
  sleep 3
done
[[ -n "${J2}" && "${J2}" != "${J1}" ]] || fail "no new job after the upgrade: $(jsonpath "clinkjob/${JOB}" .status.message)"
[[ -n "${SP_DIR}" && -n "${SP_ID}" ]] || fail "the upgrade recorded no savepoint"
restored="$(api "${VIA}" "/api/v1/jobs/${J2}" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("%s@%s" % (d["restore_from_dir"], d["restore_from_checkpoint_id"]))')"
[[ "${restored}" == "${SP_DIR}@${SP_ID}" ]] || fail "job ${J2} restored from '${restored}', not the savepoint ${SP_DIR}@${SP_ID}"
old="$(api "${VIA}" "/api/v1/jobs/${J1}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
[[ "${old}" == "CANCELLED" ]] || fail "the old job ${J1} is ${old}"
await_checkpoint "${J2}"
produce a 6000 30
await_checkpoint "${J2}"
echo "  job ${J1} (CANCELLED) -> job ${J2}, restored from ${SP_DIR}@${SP_ID}; 30 more events"

step "failover: delete the leading Coordinator's pod"
OLD_LEADER="$(leader)"
kx delete pod "${OLD_LEADER}" --wait=false >/dev/null
NEW_LEADER=""
for _ in $(seq 1 60); do
  NEW_LEADER="$(leader || true)"
  [[ -n "${NEW_LEADER}" && "${NEW_LEADER}" != "${OLD_LEADER}" ]] && break
  NEW_LEADER=""
  sleep 2
done
[[ -n "${NEW_LEADER}" ]] || fail "no Coordinator took over from ${OLD_LEADER}"
VIA="${NEW_LEADER}"
for _ in $(seq 1 40); do
  st="$(api "${VIA}" "/api/v1/jobs/${J2}" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))' 2>/dev/null || true)"
  [[ "${st}" == "RUNNING" ]] && break
  sleep 2
done
[[ "${st:-}" == "RUNNING" ]] || fail "job ${J2} is '${st:-unknown}' under the new leader"
await_checkpoint "${J2}"
[[ "$(jsonpath "clinkjob/${JOB}" .status.jobID)" == "${J2}" ]] || fail "the ClinkJob moved off job ${J2}: $(jsonpath "clinkjob/${JOB}" .status.message)"
produce a 9000 20
echo "  ${OLD_LEADER} -> ${NEW_LEADER}; job ${J2} runs on; 20 more events"

step "close the window and read its count"
produce z 300000 1
count=""
for _ in $(seq 1 30); do
  count="$(kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic smoke.counts \
    --from-beginning --isolation-level read_committed --timeout-ms 5000 2>/dev/null |
    python3 -c '
import json, sys
rows = [json.loads(l) for l in sys.stdin if l.strip().startswith("{")]
a = [r for r in rows if r.get("k") == "a"]
print(";".join("%s:%s" % (r["window_start"], r["n"]) for r in a))' || true)"
  [[ -n "${count}" ]] && break
  sleep 2
done
[[ "${count}" == "0:100" ]] || fail "key a's window holds '${count:-nothing}', not one row of 100"
echo "  a [0, 60000): 100 events, each counted once"

step "delete the ClinkJob: the finalizer cancels its job"
kx delete clinkjob "${JOB}" --timeout=60s >/dev/null
final="$(api "${VIA}" "/api/v1/jobs/${J2}" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("cancelled" if d.get("cancel_requested") else d.get("status"))')"
[[ "${final}" == "cancelled" || "${final}" == "CANCELLED" ]] || fail "job ${J2} is ${final} after the delete"

printf '\nSQL CLINKJOB SMOKE PASSED: token 401/200; job %s -> %s restored from %s@%s; failover %s -> %s; 100 events counted once; cancelled on delete.\n' \
  "${J1}" "${J2}" "${SP_DIR}" "${SP_ID}" "${OLD_LEADER}" "${NEW_LEADER}"
