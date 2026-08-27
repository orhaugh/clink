#!/usr/bin/env bash
# Does the laptop tutorial's workflow hold on a real distributed cluster?
#
# NOT a qualification campaign - see README.md. One run, one fault, one
# question: the tutorial's own pipeline.sql, workload and verifier, on a
# real Coordinator/Worker deployment with a real broker, at parallelism 4,
# with a Worker SIGKILLed mid-stream.
#
#   RUN_ID=tutorial-dist-20260827 ./check.sh
#   RUN_ID=... SKIP_PROVISION=1 ./check.sh      # reuse a live rig
#
# Teardown is the caller's, so a FAILED check can be inspected before its
# rig disappears; the run id is printed on every exit path.
#   scripts/qualification/destroy.sh <run-id> --yes
#   qualification/infra/teardown.sh --check
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
EXAMPLE="$REPO_ROOT/examples/kafka-to-clickhouse"

RUN_ID="${RUN_ID:?set RUN_ID (e.g. tutorial-dist-20260827)}"
# The published image by default: the check asks whether what a reader
# pulls behaves the same way distributed, so it must not silently test a
# private build.
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:latest}"
PARALLELISM="${PARALLELISM:-4}"
PARTITIONS="${PARTITIONS:-4}"
RATE="${RATE:-120}"
# Kill a Worker once this many (sensor, window) rows have reached
# ClickHouse: enough that the job is demonstrably mid-flight with open
# windows in state, early enough that plenty of input is still to come.
KILL_AFTER_WINDOWS="${KILL_AFTER_WINDOWS:-40}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-2000}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"

# Host-side prerequisites BEFORE anything billable exists.
[ -x "$SUBMIT_BIN" ] || { echo "check: SUBMIT_BIN missing or not executable: $SUBMIT_BIN" >&2
                          echo "check: build clink_submit_sql in $REPO_ROOT/build or set SUBMIT_BIN" >&2
                          exit 78; }
for f in pipeline.sql clickhouse-init.sql scripts/workload.py scripts/produce_events.py scripts/verify.py; do
    [ -f "$EXAMPLE/$f" ] || { echo "check: the tutorial is missing $f" >&2; exit 78; }
done
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR/verify.txt" "$OUT_DIR/verdict.txt"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() {  # host, command - retried on TRANSPORT failure only (ssh's own 255)
    local host=$1 cmd=$2 attempt=1 rc=0
    while : ; do
        ssh -n "${SSH_OPTS[@]}" "root@$host" "$cmd" && rc=0 || rc=$?
        if [ "$rc" -ne 255 ] || [ "$attempt" -ge 4 ]; then return "$rc"; fi
        echo "check: ssh to $host failed at the transport (attempt $attempt); retrying" >&2
        sleep $(( attempt * 5 )); attempt=$(( attempt + 1 ))
    done
}
to_host() { scp "${SSH_OPTS[@]}" -q "$2" "root@$1:$3"; }

fail() {
    echo >&2
    echo "check: FAILED - $1" >&2
    collect_evidence || true
    echo "check: the rig is still up for inspection. Destroy it with:" >&2
    echo "  scripts/qualification/destroy.sh $RUN_ID --yes" >&2
    echo "  qualification/infra/teardown.sh --check" >&2
    exit 3
}

collect_evidence() {
    mkdir -p "$OUT_DIR/logs"
    on_host "$COORD_PUB" "docker logs --tail 100000 clink-coordinator 2>&1" \
        > "$OUT_DIR/logs/coordinator.log" 2>/dev/null \
        || echo "check: WARNING - no coordinator container log" >&2
    local i=0
    for wp in $WORKER_PUBS; do
        on_host "$wp" "docker logs --tail 100000 clink-worker 2>&1" \
            > "$OUT_DIR/logs/worker-$i.log" 2>/dev/null \
            || echo "check: WARNING - no worker $i container log" >&2
        i=$(( i + 1 ))
    done
    on_host "$OPS_PUB" "docker logs --tail 20000 tut-clickhouse 2>&1" \
        > "$OUT_DIR/logs/clickhouse.log" 2>/dev/null || true
    on_host "$OPS_PUB" "cat /qual/produce.log 2>/dev/null" > "$OUT_DIR/logs/produce.log" 2>/dev/null || true
    curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs" > "$OUT_DIR/jobs.json" 2>/dev/null || true
    curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID:-1}/operators" \
        > "$OUT_DIR/operators.json" 2>/dev/null || true
}

echo "check: tutorial portability, run $RUN_ID (NOT a qualification campaign)"
echo "check: image $CLINK_IMAGE, parallelism $PARALLELISM, $PARTITIONS partitions"

# --- rig ----------------------------------------------------------------
if [ "${SKIP_PROVISION:-0}" != "1" ]; then
    # Two workers, one broker: enough for a genuinely distributed job with
    # a survivable Worker loss, without paying for the campaign topology.
    RUN_ID="$RUN_ID" WORKERS=2 BROKERS=1 "$REPO_ROOT/qualification/infra/provision.sh"
fi
RUN_ID="$RUN_ID" "$REPO_ROOT/qualification/infra/inventory.sh" "$RUN_ID" "$OUT_DIR"

read_inv() { python3 -c "
import json
inv = json.load(open('$OUT_DIR/inventory.json'))
print(*[h['$2'] for h in inv['hosts'] if h['role'] == '$1'])
"; }
OPS_PUB=$(read_inv ops public_ip);          OPS_PRIV=$(read_inv ops private_ip)
COORD_PUB=$(read_inv coordinator public_ip); COORD_PRIV=$(read_inv coordinator private_ip)
WORKER_PUBS=$(read_inv worker public_ip);    WORKER_PRIVS=$(read_inv worker private_ip)
BROKER_PUB=$(read_inv broker public_ip);     BROKER_PRIV=$(read_inv broker private_ip)
echo "check: ops=$OPS_PUB coordinator=$COORD_PRIV workers=$WORKER_PRIVS broker=$BROKER_PRIV"

for h in $OPS_PUB $COORD_PUB $WORKER_PUBS $BROKER_PUB; do
    until on_host "$h" "docker info >/dev/null 2>&1"; do
        echo "check: waiting for docker on $h"; sleep 15
    done
done

RUN_ID="$RUN_ID" IMAGE="$CLINK_IMAGE" "$REPO_ROOT/qualification/infra/pull-image.sh"

# Shared checkpoint state over the ops host's NFS export. Checkpoint state
# is per-subtask under <dir>/v1/<subtask>/, resolved on each node's own
# filesystem, so a killed Worker's subtask redeployed elsewhere would find
# nothing and the restore would refuse. Without this the check would
# measure a failed restore rather than the tutorial's recovery.
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || fail "/qual/state is not a shared mount on $h; a Worker-kill check on per-host local
  state would fail by construction and prove nothing"
done
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID && echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || fail "/qual/state on $h does not see the ops host's writes"
done
echo "check: shared checkpoint state verified on the coordinator and every worker"

# --- broker -------------------------------------------------------------
on_host "$BROKER_PUB" "mkdir -p /qual"
to_host "$BROKER_PUB" "$REPO_ROOT/qualification/infra/broker.yml" /qual/broker.yml
on_host "$BROKER_PUB" "cd /qual && NODE_ID=1 PRIVATE_IP=$BROKER_PRIV SEEDS='${BROKER_PRIV}:33145' \
    docker compose -f broker.yml up -d"
until on_host "$BROKER_PUB" "docker exec redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'"; do
    echo "check: waiting for the broker"; sleep 5
done
# The topic is created explicitly and owned by this run: clink assigns
# partitions itself at start-up, and a topic carrying another run's records
# is a stream the verifier's expectation knows nothing about.
on_host "$BROKER_PUB" "docker exec redpanda rpk topic delete readings >/dev/null 2>&1 || true"
on_host "$BROKER_PUB" "docker exec redpanda rpk topic create readings -p $PARTITIONS -r 1" \
    || fail "could not create the readings topic"
echo "check: broker up, topic 'readings' created with $PARTITIONS partitions"

# --- sink ---------------------------------------------------------------
# The tutorial's own table definition, shipped verbatim: a rig-only variant
# would make the comparison meaningless.
on_host "$OPS_PUB" "mkdir -p /qual/tut"
to_host "$OPS_PUB" "$HERE/clickhouse.yml" /qual/tut/clickhouse.yml
to_host "$OPS_PUB" "$EXAMPLE/clickhouse-init.sql" /qual/tut/clickhouse-init.sql
on_host "$OPS_PUB" "docker rm -f tut-clickhouse >/dev/null 2>&1 || true"
on_host "$OPS_PUB" "cd /qual/tut && docker compose -f clickhouse.yml up -d"
until on_host "$OPS_PUB" "docker exec tut-clickhouse clickhouse-client --user clink --password clink \
        -q 'SELECT 1 FROM sensor_window_stats LIMIT 0' >/dev/null 2>&1"; do
    echo "check: waiting for ClickHouse"; sleep 5
done
echo "check: ClickHouse up on the ops host with the tutorial's table"

# --- cluster ------------------------------------------------------------
# The campaigns' own compose files, and their .env discipline: anything
# that restarts a container reproduces the deployment rather than guessing
# at it from compose defaults.
on_host "$COORD_PUB" "docker rm -f clink-coordinator >/dev/null 2>&1 || true; rm -rf /qual/ha/jobs /qual/ha/history"
on_host "$COORD_PUB" "mkdir -p /qual /qual/ha"
to_host "$COORD_PUB" "$REPO_ROOT/qualification/infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\n' '$CLINK_IMAGE' '$COORD_PRIV' > /qual/.env"
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml up -d"
until curl -fsS --max-time 10 "http://${COORD_PUB}:8095/api/v1/health" >/dev/null 2>&1; do
    echo "check: waiting for the coordinator"; sleep 5
done

wid=0
for wp in $WORKER_PUBS; do
    wid=$((wid+1))
    wpriv=$(echo "$WORKER_PRIVS" | cut -d' ' -f$wid)
    on_host "$wp" "docker rm -f clink-worker >/dev/null 2>&1 || true"
    on_host "$wp" "mkdir -p /qual /qual/state"
    to_host "$wp" "$REPO_ROOT/qualification/infra/worker.yml" /qual/worker.yml
    on_host "$wp" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=w%s\nWORKER_IP=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' '$wid' '$wpriv' > /qual/.env"
    on_host "$wp" "cd /qual && docker compose -f worker.yml up -d"
done
# Slots, not a sleep: the job needs somewhere to deploy before it is sent.
NEED_SLOTS=$(( PARALLELISM ))
until [ "$(curl -fsS --max-time 10 "http://${COORD_PUB}:8095/api/v1/cluster" 2>/dev/null \
          | sed -n 's/.*"total_slot_capacity":\([0-9]*\).*/\1/p')" -ge "$NEED_SLOTS" ] 2>/dev/null; do
    echo "check: waiting for worker slots"; sleep 5
done
echo "check: cluster up ($(curl -fsS "http://${COORD_PUB}:8095/api/v1/cluster" | sed -n 's/.*"total_slot_capacity":\([0-9]*\).*/\1/p') slots)"

# --- pipeline -----------------------------------------------------------
# The tutorial's SQL, adapted ONLY in its connection addresses. Anything
# else changed here would make the portability claim about a different
# pipeline. The substitutions are asserted, so a renamed host in the
# tutorial cannot silently leave this pointing at a compose service name.
python3 - "$EXAMPLE/pipeline.sql" "$OUT_DIR/pipeline.sql" \
         "${BROKER_PRIV}:9092" "$OPS_PRIV" <<'PY'
import sys
src, dst, brokers, ch_host = sys.argv[1:5]
sql = open(src).read()
subs = [("brokers           = 'kafka:9092'", f"brokers           = '{brokers}'"),
        ("host      = 'clickhouse'", f"host      = '{ch_host}'")]
for old, new in subs:
    if old not in sql:
        sys.exit(f"check: the tutorial's pipeline.sql no longer contains {old!r}; "
                 "the distributed check cannot adapt it blindly")
    sql = sql.replace(old, new)
open(dst, "w").write(sql)
print(f"check: pipeline adapted (brokers={brokers}, clickhouse={ch_host}), everything else verbatim")
PY

"$SUBMIT_BIN" \
    --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARALLELISM" \
    --checkpoint-dir "/qual/state/$RUN_ID" \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --max-restarts-on-worker-loss 100 \
    | tee "$OUT_DIR/submit.log"
JOB_ID=$(python3 - "$OUT_DIR/submit.log" <<'PY'
import json, sys
ids = []
for line in open(sys.argv[1]):
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        doc = json.loads(line)
    except ValueError:
        continue
    if doc.get("ok") and doc.get("job_id"):
        ids.append(int(doc["job_id"]))
print(ids[-1] if ids else "")
PY
)
[ -n "$JOB_ID" ] || fail "could not determine the job id - see $OUT_DIR/submit.log"
echo "check: job $JOB_ID submitted at parallelism $PARALLELISM"

# The subtask count is the portability claim's substance: a "distributed"
# run that planned to one subtask would pass every later gate while
# proving nothing.
SUBTASKS=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" \
           | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('tasks', [])))")
WORKERS_USED=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" \
           | python3 -c "import json,sys; print(len({t['worker_id'] for t in json.load(sys.stdin).get('tasks', [])}))")
echo "check: $SUBTASKS subtasks across $WORKERS_USED worker(s)"
[ "$SUBTASKS" -ge "$PARALLELISM" ] || fail "the job planned $SUBTASKS subtasks at parallelism $PARALLELISM"
[ "$WORKERS_USED" -ge 2 ] || fail "every subtask landed on one worker; this is not a distributed run"

# --- input --------------------------------------------------------------
# The tutorial's own producer and workload definition. Only the transport
# differs: --stdout emits `key<TAB>json` exactly as the console producer
# consumes it locally, and here those lines go to the rig's broker through
# a client on the ops host.
for f in workload.py produce_events.py verify.py; do
    to_host "$OPS_PUB" "$EXAMPLE/scripts/$f" "/qual/tut/$f"
done
to_host "$OPS_PUB" "$HERE/rig_produce.py" /qual/tut/rig_produce.py
on_host "$OPS_PUB" "pip3 install --break-system-packages -q confluent-kafka 2>/dev/null \
    || pip3 install -q confluent-kafka"
on_host "$OPS_PUB" "cd /qual/tut && (setsid nohup sh -c \
    'python3 produce_events.py --stdout --rate $RATE | python3 rig_produce.py --brokers ${BROKER_PRIV}:9092 --topic readings' \
    </dev/null >/qual/produce.log 2>&1 &) ; exit 0"
sleep 5
on_host "$OPS_PUB" "pgrep -f '[p]roduce_events.py' >/dev/null" \
    || { on_host "$OPS_PUB" "tail -20 /qual/produce.log" >&2 || true
         fail "the producer did not start - see /qual/produce.log on the ops host"; }
echo "check: producing at ~${RATE} readings/s"

ch_query() { on_host "$OPS_PUB" "docker exec tut-clickhouse clickhouse-client --user clink --password clink -q \"$1\"" | tr -d '\r'; }
windows_now() { ch_query "SELECT uniqExact((sensor_id, window_start)) FROM sensor_window_stats" 2>/dev/null || echo 0; }

# --- the fault ----------------------------------------------------------
DEADLINE=$(( $(date +%s) + 300 ))
until [ "$(windows_now)" -ge "$KILL_AFTER_WINDOWS" ] 2>/dev/null; do
    [ "$(date +%s)" -lt "$DEADLINE" ] || fail "only $(windows_now) windows reached ClickHouse in 5 minutes;
  the pipeline is not keeping up or not running (see $OUT_DIR/operators.json)"
    sleep 5
done
echo "check: $(windows_now) windows in ClickHouse; killing a worker"

VICTIM=$(echo "$WORKER_PUBS" | cut -d' ' -f1)
on_host "$VICTIM" "docker kill --signal=KILL clink-worker"
KILLED_AT=$(date -u +%H:%M:%S)
# The coordinator's own account of the loss, not the kill command's exit
# status: a fault that leaves no trace in the engine did not happen.
DEADLINE=$(( $(date +%s) + 120 ))
until curl -fsS --max-time 10 "http://${COORD_PUB}:8095/metrics" 2>/dev/null \
      | awk '/^clink_coordinator_workers_lost_total /{exit ($2 >= 1) ? 0 : 1}'; do
    [ "$(date +%s)" -lt "$DEADLINE" ] || fail "the coordinator never reported a lost worker after the kill"
    sleep 3
done
echo "check: the coordinator declared the worker lost (killed at ${KILLED_AT}Z); restarting it"
on_host "$VICTIM" "cd /qual && docker compose -f worker.yml up -d"

DEADLINE=$(( $(date +%s) + 300 ))
until curl -fsS --max-time 10 "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" 2>/dev/null \
      | grep -q '"status":"RUNNING"'; do
    [ "$(date +%s)" -lt "$DEADLINE" ] || fail "the job did not return to RUNNING within 5 minutes of the restart"
    sleep 5
done
RESTORE_LINE=$(on_host "$COORD_PUB" "docker logs clink-coordinator 2>&1 | grep 'restore point' | tail -1" || true)
echo "check: recovered - ${RESTORE_LINE:-<no restore line logged>}"

# --- verification -------------------------------------------------------
DEADLINE=$(( $(date +%s) + 600 ))
until ! on_host "$OPS_PUB" "pgrep -f '[p]roduce_events.py' >/dev/null"; do
    [ "$(date +%s)" -lt "$DEADLINE" ] || fail "the producer did not finish within 10 minutes"
    sleep 10
done
echo "check: input complete; verifying"

# The tutorial's verifier, unchanged, recomputing every window from the
# workload definition rather than from anything the engine reported.
set +e
on_host "$OPS_PUB" "cd /qual/tut && python3 verify.py --url http://127.0.0.1:8123 --timeout 180" \
    > "$OUT_DIR/verify.txt" 2>&1
VERIFY_RC=$?
set -e
cat "$OUT_DIR/verify.txt"
collect_evidence

{
  echo "run_id=$RUN_ID"
  echo "image=$CLINK_IMAGE"
  echo "parallelism=$PARALLELISM"
  echo "partitions=$PARTITIONS"
  echo "subtasks=$SUBTASKS"
  echo "workers_hosting_subtasks=$WORKERS_USED"
  echo "worker_killed_at_utc=$KILLED_AT"
  echo "restore_line=${RESTORE_LINE:-none}"
  echo "verify_exit=$VERIFY_RC"
  echo "result=$([ "$VERIFY_RC" -eq 0 ] && echo PASS || echo FAIL)"
} > "$OUT_DIR/verdict.txt"
cat "$OUT_DIR/verdict.txt"

echo
echo "check: evidence in $OUT_DIR"
echo "check: the rig BILLS UNTIL DESTROYED:"
echo "  scripts/qualification/destroy.sh $RUN_ID --yes"
echo "  qualification/infra/teardown.sh --check"
[ "$VERIFY_RC" -eq 0 ] || exit 1
echo "check: PASS - the tutorial's pipeline behaves the same distributed"
