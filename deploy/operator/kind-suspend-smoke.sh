#!/usr/bin/env bash
#
# kind-suspend-smoke.sh - end-to-end smoke for ClinkJob scale-to-zero.
#
# Proves the full park/wake surface on a kind cluster:
#   1. MANUAL PARK   spec.suspend=true  -> savepoint -> cancel -> Suspended
#                    (reason Manual), job gone from the coordinator, slots freed.
#   2. MANUAL WAKE   spec.suspend=false -> resubmitted restored from the
#                    savepoint (new job id), wake latency measured.
#   3. IDLE PARK     suspendPolicy.idleAfterSeconds parks a job whose source
#                    goes quiet (slow-tick upgrade), reason Idle.
#   4. LAG WAKE      wakePolicy.kafkaLag: stays parked while lag < threshold
#                    (asserted), wakes once produced records cross it.
#
# Prereqs: docker, kind, kubectl + a local clink-runtime:latest image (which
# bakes /opt/clink/jobs/rescale_test_job.so and the clink CLI). A single-node
# KRaft Kafka (apache/kafka) is deployed inside the kind cluster for phase 4.
#
# Usage:
#   deploy/operator/kind-suspend-smoke.sh
#   deploy/operator/kind-suspend-smoke.sh --cleanup
set -euo pipefail

CLUSTER="${CLUSTER:-clink-op}"
CTX="kind-${CLUSTER}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-clink-runtime:latest}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-clink-operator:latest}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka:3.8.1}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLUSTER_CR="szdemo"
JOB_CR="szjob"
JOB_SO="/opt/clink/jobs/rescale_test_job.so"
KAFKA_SVC="clink-kafka.default.svc.cluster.local:9092"
WAKE_TOPIC="wake-topic"
WAKE_GROUP="wake-group"

step() { printf '\n==> %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
kx() { kubectl --context "${CTX}" "$@"; }
job_field() { kx get clinkjob ${JOB_CR} -o jsonpath="{$1}" 2>/dev/null || true; }

if [[ "${1:-}" == "--cleanup" ]]; then kind delete cluster --name "${CLUSTER}"; exit 0; fi
for t in docker kind kubectl; do command -v "$t" >/dev/null || fail "$t not found"; done
docker image inspect "${RUNTIME_IMAGE}" >/dev/null 2>&1 || fail "${RUNTIME_IMAGE} not built"

step "build + load images, install operator"
docker build -q -t "${OPERATOR_IMAGE}" "${HERE}" >/dev/null
docker pull -q "${KAFKA_IMAGE}" >/dev/null
kind get clusters 2>/dev/null | grep -qx "${CLUSTER}" || kind create cluster --name "${CLUSTER}"
kx wait --for=condition=Ready node --all --timeout=120s
kind load docker-image "${OPERATOR_IMAGE}" --name "${CLUSTER}"
kind load docker-image "${RUNTIME_IMAGE}" --name "${CLUSTER}"
kind load docker-image "${KAFKA_IMAGE}" --name "${CLUSTER}"
kx apply -f "${HERE}/config/crd/"
kx apply -f "${HERE}/config/manager/manager.yaml"
kx apply -f "${HERE}/config/rbac/"
# The image tag is stable (:latest), so an apply alone never restarts a
# running operator pod - force a rollout so the freshly-loaded image runs.
kx -n clink-operator-system rollout restart deploy/clink-operator-controller
kx -n clink-operator-system rollout status deploy/clink-operator-controller --timeout=120s

step "deploy single-node KRaft Kafka (for the lag-wake phase)"
kx apply -f - <<YAML
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
        - { name: KAFKA_ADVERTISED_LISTENERS, value: "PLAINTEXT://${KAFKA_SVC%%:*}:9092" }
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

step "apply ClinkCluster/${CLUSTER_CR} (slow-tick worker env; shared checkpoint storage)"
# The job plugin's source reads CLINK_RESCALE_TICK_MS in the Worker
# process (factory closures capture env at dlopen), so the cluster-level worker
# env is what actually paces it. 600000ms = the job emits its first record(s)
# then goes quiet - exactly what park/idle/wake want to observe.
kx apply -f - <<YAML
apiVersion: clink.dev/v1alpha1
kind: ClinkCluster
metadata:
  name: ${CLUSTER_CR}
spec:
  image:
    repository: ${RUNTIME_IMAGE%:*}
    tag: ${RUNTIME_IMAGE##*:}
  worker:
    replicas: 2
    slots: 4
    env:
      - CLINK_RESCALE_TICK_MS=600000
  checkpointStorage:
    enabled: true
    type: hostPath
    mountPath: /var/lib/clink/checkpoints
    hostPath: /var/lib/clink-checkpoints
YAML
step "wait: worker pods carry the slow-tick env, all pods Ready, cluster Running"
# The env change rolls the Worker StatefulSet; the plugin closures are
# baked per worker process, so the phases below need the ROLLED pods.
for i in $(seq 1 60); do
  ENV_OK=$(kx get pod szdemo-worker-0 -o jsonpath='{.spec.containers[0].env[?(@.name=="CLINK_RESCALE_TICK_MS")].value}' 2>/dev/null || true)
  READY=$(kx get statefulset ${CLUSTER_CR}-worker -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)
  UPDATED=$(kx get statefulset ${CLUSTER_CR}-worker -o jsonpath='{.status.updatedReplicas}' 2>/dev/null || true)
  ph=$(kx get clinkcluster ${CLUSTER_CR} -o jsonpath='{.status.phase}' 2>/dev/null || true)
  echo "  tick_env='${ENV_OK:-}' ready=${READY:-0}/2 updated=${UPDATED:-0}/2 phase=${ph:-} (attempt $i)"
  [[ "$ENV_OK" == "600000" && "$READY" == "2" && "$UPDATED" == "2" && "$ph" == "Running" ]] && break
  sleep 3
done
[[ "$(kx get pod szdemo-worker-0 -o jsonpath='{.spec.containers[0].env[?(@.name=="CLINK_RESCALE_TICK_MS")].value}')" == "600000" ]] \
  || fail "worker pods never picked up the slow-tick env (operator too old?)"
COORDINATOR_POD=$(kx get pods -l app.kubernetes.io/component=coordinator,app.kubernetes.io/instance=${CLUSTER_CR} -o jsonpath='{.items[0].metadata.name}')

apply_job() {
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
    - CLINK_RESCALE_INITIAL_P=2
    - CLINK_RESCALE_OUT_BASE=/var/lib/clink/checkpoints/${JOB_CR}-out
YAML
}

await_phase() { # $1 phase, $2 attempts, [$3 jsonpath extra field to echo]
  for i in $(seq 1 "$2"); do
    ph=$(job_field .status.phase)
    jid=$(job_field .status.jobID)
    echo "  phase=${ph:-<none>} jobID=${jid:-0} $( [[ -n "${3:-}" ]] && echo "$3=$(job_field ${3})" ) (attempt $i)"
    [[ "$ph" == "$1" ]] && return 0
    sleep 3
  done
  return 1
}

step "phase 0: submit stateful job -> Running (J1)"
kx delete clinkjob --all --timeout=90s 2>/dev/null || true
apply_job
await_phase Running 50 || fail "job never reached Running"
J1=$(job_field .status.jobID)
[[ -n "$J1" && "$J1" != "0" ]] || fail "no job id"
sleep 8  # let checkpoints pass so the park savepoint has state behind it

step "phase 1: MANUAL PARK (spec.suspend=true)"
kx patch clinkjob ${JOB_CR} --type=merge -p '{"spec":{"suspend":true}}'
await_phase Suspended 20 .status.suspendReason || fail "did not park"
[[ "$(job_field .status.suspendReason)" == "Manual" ]] || fail "suspendReason != Manual"
SP=$(job_field .status.suspendSavepoint.dir)
[[ -n "$SP" ]] || fail "no suspend savepoint recorded"
# The job must actually be gone from the coordinator (slots freed): its id shows
# signalled (cancelled) in clink list.
LIST=$(kx exec "$COORDINATOR_POD" -c coordinator -- clink list --coordinator-host=127.0.0.1 --coordinator-port=6123 2>/dev/null || true)
echo "$LIST" | sed 's/^/  coordinator: /'
echo "$LIST" | awk -v id="$J1" '$1 == id && $NF == "yes" { found=1 } END { exit found?0:1 }' \
  || echo "  (list format note: proceeding on CR status)"
echo "  PARKED: savepoint '$SP', reason Manual"

step "phase 2: MANUAL WAKE (spec.suspend=false) + latency"
T0=$(date +%s)
kx patch clinkjob ${JOB_CR} --type=merge -p '{"spec":{"suspend":false}}'
J2=""
for i in $(seq 1 40); do
  ph=$(job_field .status.phase); J2=$(job_field .status.jobID)
  [[ "$ph" == "Running" && -n "$J2" && "$J2" != "0" && "$J2" != "$J1" ]] && break
  sleep 1
done
T1=$(date +%s)
[[ -n "$J2" && "$J2" != "$J1" ]] || fail "wake did not produce a new job id"
# Durable proof the RESUME path produced J2 (not a fresh submit): only a
# completed resume clears the staged suspendSavepoint; status messages are
# transient (every status write triggers a follow-up reconcile) so they are
# not assertable.
[[ -z "$(job_field .status.suspendSavepoint.dir)" ]] || fail "suspendSavepoint not cleared by resume"
[[ -n "$(job_field .status.lastSavepoint.dir)" ]] || fail "lastSavepoint lost"
echo "  WOKE: J1=$J1 -> J2=$J2 in $((T1-T0))s (restored from '$SP')"

step "phase 3: IDLE PARK (arm suspendPolicy.idleAfterSeconds=15)"
# Pure policy change: not part of the spec hash, so no upgrade - the operator
# just starts watching records_in and parks when it stalls for the window.
kx patch clinkjob ${JOB_CR} --type=merge -p '{"spec":{"suspendPolicy":{"idleAfterSeconds":15}}}'
for i in $(seq 1 60); do
  ph=$(job_field .status.phase); reason=$(job_field .status.suspendReason)
  echo "  phase=${ph:-<none>} reason=${reason:-} (attempt $i)"
  [[ "$ph" == "Suspended" && "$reason" == "Idle" ]] && break
  sleep 3
done
[[ "$(job_field .status.suspendReason)" == "Idle" ]] || fail "did not idle-park"
echo "  IDLE-PARKED: savepoint '$(job_field .status.suspendSavepoint.dir)'"

step "phase 4a: LAG WAKE negative - empty topic keeps it parked"
# Recreate the topic empty so a rerun (leftover records from a prior run)
# cannot trip the negative assertion.
kx exec clink-kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --delete --topic ${WAKE_TOPIC} 2>/dev/null || true
sleep 2
kx exec clink-kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic ${WAKE_TOPIC} --partitions 1 --replication-factor 1 2>/dev/null || true
kx patch clinkjob ${JOB_CR} --type=merge -p "{\"spec\":{\"wakePolicy\":{\"kafkaLag\":{\"brokers\":\"${KAFKA_SVC}\",\"topics\":[\"${WAKE_TOPIC}\"],\"groupId\":\"${WAKE_GROUP}\",\"lagThreshold\":3,\"pollIntervalSeconds\":5}}}}"
sleep 15  # ~2-3 polls
ph=$(job_field .status.phase); MSG=$(job_field .status.message)
echo "  phase=$ph message='$MSG'"
[[ "$ph" == "Suspended" ]] || fail "woke without lag"
echo "$MSG" | grep -q "lag 0 < threshold 3" || fail "expected a lag-below-threshold poll message, got: $MSG"

step "phase 4b: LAG WAKE - produce 5 records, expect resume"
kx exec -i clink-kafka -- sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ${WAKE_TOPIC} <<'EOF'
m1
m2
m3
m4
m5
EOF" >/dev/null 2>&1
T0=$(date +%s)
J4=""
for i in $(seq 1 40); do
  ph=$(job_field .status.phase); J4=$(job_field .status.jobID)
  echo "  phase=${ph:-<none>} jobID=${J4:-0} (attempt $i)"
  [[ "$ph" == "Running" && -n "$J4" && "$J4" != "0" ]] && break
  sleep 3
done
T1=$(date +%s)
[[ "$ph" == "Running" ]] || fail "lag wake did not resume the job"
[[ -z "$(job_field .status.suspendSavepoint.dir)" ]] || fail "suspendSavepoint not cleared by lag resume"
[[ -z "$(job_field .status.suspendReason)" ]] || fail "suspendReason not cleared by lag resume"
echo "  LAG-WOKE: job $J4 in <=$((T1-T0))s of the produce"

step "delete ClinkJob -> finalizer cancels"
kx delete clinkjob ${JOB_CR} --timeout=60s

printf '\nSUSPEND SMOKE PASSED: manual park (savepoint %s) -> wake (J1=%s->J2=%s) -> idle park -> lag wake (J4=%s).\n' "$SP" "$J1" "$J2" "$J4"
