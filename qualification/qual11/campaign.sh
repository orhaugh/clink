#!/usr/bin/env bash
# QUAL-11: state schema evolution across a live boundary.
#
# THE CLAIM. A running stateful job survives a change to its STATE TYPE:
# savepoint on v1, swap the operator code for a v2 whose keyed-state value
# gained fields, restore THROUGH a registered migration - and an oracle
# that spans the boundary (the generator never stops) accounts for every
# event exactly once, with every key's count carried rather than restarted.
#
# WHAT IS BEING EVOLVED. Not the engine (QUAL-08 covered an engine swap
# with an unchanged schema): one engine revision throughout, and the JOB
# changes. The workload is a compiled plugin - the typed surface where
# SchemaVersionTrait and StateMigrationRegistry actually live - built as
# three .so variants from one source at the image's own revision, because
# the plugin ABI fingerprint IS the engine's git SHA.
#
# FOUR GATES, each able to fail on its own (see summarise.py):
#   1. The pre-deploy check passes for the good v2 before it is deployed.
#      A refusal stops the campaign INCONCLUSIVE - the campaign never
#      deploys past a check that said no.
#   2. THE NEGATIVE CONTROL, in the same run: the identical v2 built
#      WITHOUT the migration must be REFUSED by that same check. If it is
#      accepted, the instrument is inert and gate 1 proved nothing - a
#      FAIL, and every prior green reading with it.
#   3. Continuity and exactness across the boundary: one logical job, and
#      every key's final count and sum equal to the deterministic spec.
#   4. The migration's effect, predicted: pre-existing keys carry their
#      counts, and their first post-boundary row shows the migrated range
#      fields collapsing onto that row's own amount - the signature the
#      registered migration's sentinel seeding produces.
#
# The fault battery runs entirely on the MIGRATED job, so the restored and
# migrated state is also the state that survives faults (QUAL-08's rule).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID, e.g. qual11-20260825a}"
CLINK_IMAGE="${CLINK_IMAGE:?set CLINK_IMAGE - the runtime image carrying the qual11 jobs}"
DURATION_S="${DURATION_S:-1800}"
PROFILE="${PROFILE:-aggressive}"
RATE="${RATE:-1000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-2000}"
# The key space TURNS OVER, deliberately. A fixed key space at any real
# rate touches every key within seconds of the restore, and gate 4's
# predicted-output half needs keys that are still UNTOUCHED when the
# second savepoint is taken - those are the ones still holding the
# sentinels the migration wrote. With epochs, keys from earlier epochs go
# dormant and provide exactly that population, while the current epoch's
# keys provide the carried-and-active one. (No TTL on this job, so
# dormant keys persist rather than expiring.)
KEY_EPOCH_MS="${KEY_EPOCH_MS:-30000}"
SEED="${SEED:-20260825}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-15000}"
FILL_S="${FILL_S:-240}"

SAVEPOINT_TIMEOUT_S="${SAVEPOINT_TIMEOUT_S:-240}"
DEPLOY_TIMEOUT_S="${DEPLOY_TIMEOUT_S:-600}"
MIN_GAP_S="${MIN_GAP_S:-120}"
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-300}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
RESTART_DRAIN_TIMEOUT_MS="${RESTART_DRAIN_TIMEOUT_MS:-300000}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-60}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-30}"
FINAL_WAIT_S="${FINAL_WAIT_S:-180}"
CATCHUP_TIMEOUT_S="${CATCHUP_TIMEOUT_S:-900}"
CATCHUP_STALL_S="${CATCHUP_STALL_S:-600}"
LOCAL_MODE="${LOCAL_MODE:-0}"

IN_TOPIC="qual11_in"
OUT_TOPIC="qual11_out"
JOBS_DIR=/opt/clink/jobs

OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" "$OUT_DIR/boundary.txt" \
      "$OUT_DIR/catchup.txt" "$OUT_DIR/final-quiesce.txt" "$OUT_DIR/q11-verify.json"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=4
          -i "$KEY_FILE")

on_host() {
    local host=$1 cmd=$2 attempt=1 rc=0
    while : ; do
        ssh -n "${SSH_OPTS[@]}" "root@$host" "$cmd" && rc=0 || rc=$?
        if [ "$rc" -ne 255 ] || [ "$attempt" -ge "${SSH_RETRIES:-4}" ]; then
            return "$rc"
        fi
        echo "campaign: ssh to $host failed at the transport (attempt $attempt); retrying" >&2
        sleep $(( attempt * 5 )); attempt=$(( attempt + 1 ))
    done
}
to_host() {
    local host=$1 src=$2 dst=$3 attempt=1 rc=0
    while : ; do
        scp "${SSH_OPTS[@]}" -q "$src" "root@$host:$dst" && rc=0 || rc=$?
        if [ "$rc" -eq 0 ] || [ "$attempt" -ge "${SSH_RETRIES:-4}" ]; then
            return "$rc"
        fi
        sleep $(( attempt * 5 )); attempt=$(( attempt + 1 ))
    done
}
collect_container_logs() {
    local sfx="${1:-}"
    mkdir -p "$OUT_DIR/logs"
    on_host "$COORD_PUB" "docker logs --tail 200000 clink-coordinator 2>&1" \
        > "$OUT_DIR/logs/coordinator$sfx.log" 2>/dev/null || true
    local wi=0
    for wp in $WORKER_PUBS; do
        on_host "$wp" "docker logs --tail 200000 clink-worker 2>&1" \
            > "$OUT_DIR/logs/worker-$wi$sfx.log" 2>/dev/null || true
        wi=$(( wi + 1 ))
    done
}
start_on_host() {
    local host=$1 log=$2 cmd=$3
    on_host "$host" "cd /qual && (setsid nohup $cmd </dev/null >/qual/$log 2>&1 &) ; exit 0"
    sleep 3
    local script; script=$(echo "$cmd" | awk '{print $2}')
    local pattern="[${script:0:1}]${script:1}"
    on_host "$host" "pgrep -f '$pattern' >/dev/null" \
        || { echo "campaign: $log did not start - see /qual/$log on $host" >&2
             on_host "$host" "tail -20 /qual/$log" >&2 || true; exit 3; }
}
verify_fail() {
    echo "campaign: FUNCTIONAL VERIFICATION FAILED - $1" >&2
    on_host "$OPS_PUB" "touch /qual/q11-chaos.jsonl.stop; pkill -INT -f '[c]haos.py'; true" || true
    on_host "$OPS_PUB" "touch /qual/progress.json.stop; pkill -INT -f '[g]enerator.py'; true" || true
    collect_container_logs "-at-failure" || true
    [ -n "${JOB_ID:-}" ] && curl -fsS -X POST \
        "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
    echo "campaign: stopping here. Evidence in $OUT_DIR" >&2
    exit 4
}

echo "campaign: QUAL-11 run $RUN_ID on $CLINK_IMAGE"
echo "campaign: fill ${FILL_S}s on v1, then the boundary, then ${DURATION_S}s of battery on v2"

# --- rig ---------------------------------------------------------------------
RIG_RUN_ID="${RIG_RUN_ID:-$RUN_ID}"
[ "$RIG_RUN_ID" != "$RUN_ID" ] && SKIP_PROVISION=1
if [ -n "${INVENTORY:-}" ]; then
    cp "$INVENTORY" "$OUT_DIR/inventory.json"
else
    if [ "${SKIP_PROVISION:-0}" != "1" ]; then
        RUN_ID="$RUN_ID" "$REPO_ROOT/qualification/infra/provision.sh"
    fi
    RUN_ID="$RUN_ID" "$HERE/../infra/inventory.sh" "$RIG_RUN_ID" "$OUT_DIR"
fi
read_inv() { python3 -c "
import json
inv = json.load(open('$OUT_DIR/inventory.json'))
print(*[h['$2'] for h in inv['hosts'] if h['role'] == '$1'])
"; }
OPS_PUB=$(read_inv ops public_ip)
OPS_PRIV=$(read_inv ops private_ip)
COORD_PUB=$(read_inv coordinator public_ip)
COORD_PRIV=$(read_inv coordinator private_ip)
WORKER_PUBS=$(read_inv worker public_ip)
BROKER_PRIVS=$(read_inv broker private_ip)
BROKER_LIST=$(for ip in $BROKER_PRIVS; do printf "%s:9092," "$ip"; done | sed 's/,$//')
SEED_LIST=$(for ip in $BROKER_PRIVS; do printf "%s:33145," "$ip"; done | sed 's/,$//')
BROKER_ONE=$(echo "$BROKER_PRIVS" | awk '{print $1}')
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV ops=$OPS_PUB"

for h in $OPS_PUB $COORD_PUB $WORKER_PUBS $(read_inv broker public_ip); do
    until on_host "$h" "docker info >/dev/null 2>&1"; do
        echo "campaign: waiting for docker on $h"; sleep 15
    done
done

# Shared checkpoint state: the savepoint written before the boundary must
# be readable by whichever worker the restored subtask lands on.
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h" >&2; exit 2; }
done
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID/v1 /qual/state/$RUN_ID/v2"
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done
CKPT_DIR_V1="/qual/state/$RUN_ID/v1"
CKPT_DIR_V2="/qual/state/$RUN_ID/v2"

if [ "${SKIP_IMAGE_PULL:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" IMAGE="$CLINK_IMAGE" "$HERE/../infra/pull-image.sh"
fi
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "docker image inspect '$CLINK_IMAGE' >/dev/null 2>&1" \
        || { echo "campaign: image $CLINK_IMAGE is not present on $h" >&2; exit 2; }
done
on_host "$COORD_PUB" "docker run --rm --entrypoint clink '$CLINK_IMAGE' --capabilities-json 2>/dev/null" \
    | python3 -c "
import json,sys
d=json.load(sys.stdin)
sys.exit(0 if d.get('build',{}).get('fault_injection') else 1)
" || { echo "campaign: $CLINK_IMAGE has NO fault injection compiled in" >&2; exit 2; }

# The three job variants must be IN the image (built at its revision - the
# ABI fingerprint is the git SHA). Their absence is a build-wiring error,
# not something to discover after the fill.
for so in qual11_job_v1.so qual11_job_v2.so qual11_job_v2_broken.so; do
    on_host "$COORD_PUB" "docker run --rm --entrypoint test '$CLINK_IMAGE' -f $JOBS_DIR/$so" \
        || { echo "campaign: $CLINK_IMAGE does not carry $JOBS_DIR/$so - rebuild it with" >&2
             echo "  --build-arg CLINK_BUILD_QUAL11_JOBS=ON" >&2; exit 2; }
done
echo "campaign: image carries all three job variants"

# --- brokers + topics ---------------------------------------------------------
bi=0
for bp in $(read_inv broker public_ip); do
    bpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$bp'][0])")
    to_host "$bp" "$HERE/../infra/broker.yml" /qual/broker.yml
    # broker.yml interpolates NODE_ID / PRIVATE_IP / SEEDS, passed INLINE
    # (the QUAL-09 form). Writing differently-named keys into .env left
    # --node-id empty and every broker crash-looped on an unparseable
    # flag, which the campaign only saw as "brokers never became
    # reachable" four minutes later.
    on_host "$bp" "cd /qual && NODE_ID=$bi PRIVATE_IP=$bpriv SEEDS='$SEED_LIST' \
        docker compose -f broker.yml up -d"
    bi=$(( bi + 1 ))
done

# WAIT for the brokers rather than guessing: a fixed sleep is a race the
# campaign loses on a cold rig (the first attempt got "connection refused"
# 25s in). Poll the cluster until it answers, then create topics.
BROKERS_UP=no
for _t in $(seq 1 40); do
    if on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        cluster info --brokers $BROKER_ONE:9092" >/dev/null 2>&1; then
        BROKERS_UP=yes; break
    fi
    sleep 6
done
[ "$BROKERS_UP" = "yes" ] \
    || { echo "campaign: the brokers never became reachable" >&2; exit 2; }
echo "campaign: brokers up"

# The proven helper shape (QUAL-08/09): --entrypoint rpk against ONE
# broker. A previous version used --network host with the full broker
# list, silenced its failure with `|| true`, and the campaign then spent
# the whole fill window watching a job that could not see its input
# topic. Topic creation is now LOUD: no topic, no run.
REPL=$(python3 -c "print(min(3, len('$BROKER_PRIVS'.split())))")
rpk_ops() {
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
for t in "$IN_TOPIC" "$OUT_TOPIC"; do
    rpk_ops "topic delete $t" >/dev/null 2>&1 || true
    rpk_ops "topic create $t -p $PARTITIONS -r $REPL" >/dev/null \
        || { echo "campaign: could not create topic $t" >&2; exit 2; }
done
for t in "$IN_TOPIC" "$OUT_TOPIC"; do
    rpk_ops "topic list" 2>/dev/null | grep -q "$t" \
        || { echo "campaign: topic $t is not visible after creation" >&2; exit 2; }
done
echo "campaign: topics $IN_TOPIC and $OUT_TOPIC created (p=$PARTITIONS r=$REPL)"

# --- engine -------------------------------------------------------------------
to_host "$COORD_PUB" "$HERE/../infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml down >/dev/null 2>&1; \
    rm -rf /qual/ha/jobs /qual/ha/history; mkdir -p /qual/ha; \
    printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nRESTART_DRAIN_TIMEOUT_MS=%s\n' \
    '$CLINK_IMAGE' '$COORD_PRIV' '$RESTART_DRAIN_TIMEOUT_MS' > /qual/.env && \
    docker compose -f coordinator.yml up -d"
wi=0
for wp in $WORKER_PUBS; do
    wpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$wp'][0])")
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=%s\nWORKER_IP=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' 'w$wi' '$wpriv' > /qual/.env && \
        docker compose -f worker.yml up -d"
    wi=$(( wi + 1 ))
done
sleep 25

job_status() { curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/$1" 2>/dev/null \
    | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("status",""))
except Exception: print("")'; }
ckpt_id() { curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/$1" 2>/dev/null \
    | python3 -c 'import json,sys
try: print(int(json.load(sys.stdin).get("latest_completed_checkpoint_id",0) or 0))
except Exception: print(0)'; }

submit_job() {  # so-name, ckpt-dir, [restore-dir restore-id] -> prints job id
    local so=$1 ckpt=$2 restore_dir="${3:-}" restore_id="${4:-0}"
    local extra=""
    if [ -n "$restore_dir" ]; then
        extra="--restore-from-dir=$restore_dir --restore-from-checkpoint-id=$restore_id"
    fi
    on_host "$COORD_PUB" "docker exec -e QUAL11_BROKERS='$BROKER_LIST' clink-coordinator \
        clink run --job=$JOBS_DIR/$so \
        --coordinator-host=127.0.0.1 --coordinator-port=6123 \
        --name=qual11 --parallelism=$PARTITIONS \
        --checkpoint-dir=$ckpt --checkpoint-interval-ms=$CHECKPOINT_INTERVAL_MS \
        --max-restarts-on-worker-loss=$MAX_RESTARTS \
        --wait-timeout-s=0 $extra" 2>&1
}
# --wait-timeout-s=0 above: the submitter's wait is for job COMPLETION, and
# a streaming job never completes - waiting turns every submit into a
# timeout that reads like a failure (it did, on the first local run). The
# campaign waits for RUNNING via the coordinator's own status instead.
#
# The id comes from the machine-readable line the submitter prints
# ({"ok":..,"job_id":N,..}), the same contract the SQL submit path has.
parse_job_id() {
    python3 -c "
import json, sys
jid = ''
for line in open(sys.argv[1]):
    line = line.strip()
    if line.startswith('{'):
        try:
            d = json.loads(line)
        except Exception:
            continue
        if d.get('job_id'):
            jid = str(d['job_id'])
print(jid)" "$1"
}

# --- generator ----------------------------------------------------------------
on_host "$OPS_PUB" "mkdir -p /qual && rm -f /qual/progress.json /qual/progress.json.spec \
    /qual/progress.json.stop /qual/q11-chaos.jsonl /qual/q11-chaos.jsonl.stop"
for f in "$HERE/../qual01/detspec.py" "$HERE/../qual01/generator.py" \
         "$HERE/verifier.py" "$HERE/../chaos/chaos.py"; do
    to_host "$OPS_PUB" "$f" "/qual/$(basename "$f")"
done
BASE_MS=$(python3 -c 'import time;print(int(time.time()*1000))')
echo "$BASE_MS" > "$OUT_DIR/base_ms"
start_on_host "$OPS_PUB" q11-generator.log \
    "python3 /qual/generator.py --brokers '$BROKER_LIST' --topic $IN_TOPIC \
     --rate $RATE --partitions $PARTITIONS --keys $KEYS --seed $SEED \
     --base-ms $BASE_MS --key-epoch-ms $KEY_EPOCH_MS --progress /qual/progress.json"
echo "campaign: generator started"

# --- v1: the pre-boundary job -------------------------------------------------
submit_job qual11_job_v1.so "$CKPT_DIR_V1" > "$OUT_DIR/submit-v1.log" 2>&1 || true
JOB_ID=$(parse_job_id "$OUT_DIR/submit-v1.log")
[ -n "$JOB_ID" ] || { tail -20 "$OUT_DIR/submit-v1.log" >&2; verify_fail "the v1 job did not submit"; }
JOB_ID_V1="$JOB_ID"
echo "campaign: v1 job $JOB_ID"
dw=0
while [ "$dw" -lt "$DEPLOY_TIMEOUT_S" ]; do
    [ "$(job_status "$JOB_ID")" = "RUNNING" ] && break
    sleep 5; dw=$(( dw + 5 ))
done
[ "$(job_status "$JOB_ID")" = "RUNNING" ] || verify_fail "the v1 job never reached RUNNING"

fstart=$(date +%s)
while [ $(( $(date +%s) - fstart )) -lt "$FILL_S" ]; do
    sleep "$SAMPLE_INTERVAL_S"
    echo "campaign: fill $(( ($(date +%s) - fstart) ))s, checkpoint $(ckpt_id "$JOB_ID")"
done
[ "$(ckpt_id "$JOB_ID")" -ge 1 ] || verify_fail "the v1 job never completed a checkpoint"

# =============================================================================
# THE EVOLUTION BOUNDARY
# =============================================================================
echo "campaign: === boundary: savepoint on v1 ==="
T_SP0=$(date +%s)
# Judged by the tool's printed ok= field, not by exit status (see the
# check gates below for why a piped status is not the command's).
SP_OUT=$(on_host "$COORD_PUB" "docker exec clink-coordinator clink savepoint \
    --job-id=$JOB_ID --timeout-s=$SAVEPOINT_TIMEOUT_S" 2>&1 | tr -d '\r') || true
echo "$SP_OUT" > "$OUT_DIR/savepoint.txt"
SP_OK=$(echo "$SP_OUT" | sed -n 's/.*ok=\([01]\).*/\1/p' | head -1)
SP_DIR=$(echo "$SP_OUT" | sed -n 's/.*dir=\([^ ]*\).*/\1/p' | head -1 | tr -d '"')
SP_ID=$(echo "$SP_OUT" | sed -n 's/.*id=\([0-9]*\).*/\1/p' | head -1)
SAVEPOINT_S=$(( $(date +%s) - T_SP0 ))
write_boundary() {  # check_v2 check_broken restore_ok same_job
    { echo "savepoint_ok=${1:-no}"; echo "savepoint_id=${SP_ID:-0}";
      echo "savepoint_s=$SAVEPOINT_S"; echo "check_v2=${2:-missing}";
      echo "check_v2_broken=${3:-missing}"; echo "restore_ok=${4:-no}";
      echo "restore_s=${RESTORE_S:--1}"; echo "same_job_id=${5:-no}";
      echo "job_id_v1=${JOB_ID_V1:-0}"; echo "job_id_v2=${JOB_ID_V2:-0}";
    } > "$OUT_DIR/boundary.txt"
}
if [ "${SP_OK:-0}" != "1" ] || [ -z "$SP_DIR" ] || [ -z "$SP_ID" ]; then
    write_boundary no missing missing no no
    verify_fail "the savepoint did not complete: $SP_OUT"
fi
echo "campaign: savepoint id=$SP_ID dir=$SP_DIR in ${SAVEPOINT_S}s"

# Relocate immediately (QUAL-08's lesson: retention garbage-collects the
# savepoint's files one checkpoint after the handle is printed). Hard
# links, layout preserved.
SP_PORTABLE="/qual/state/$RUN_ID/savepoint-$SP_ID"
on_host "$OPS_PUB" "rm -rf '$SP_PORTABLE' && mkdir -p '$SP_PORTABLE' && cd '$SP_DIR' && \
    find . -type f \\( -name 'checkpoint-${SP_ID}.snap' -o -name 'checkpoint-${SP_ID}.snap.meta' \\
    -o -path './_jobs/*' \\) -exec cp -l --parents {} '$SP_PORTABLE/' \\; " \
    || verify_fail "could not relocate the savepoint to a portable path"
SNAP_ONE=$(on_host "$OPS_PUB" "find '$SP_PORTABLE' -name 'checkpoint-${SP_ID}.snap' | head -1" | tr -d '\r')
[ -n "$SNAP_ONE" ] || verify_fail "the relocated savepoint holds no snapshot files"

# --- gate 1: the pre-deploy check for the job we intend to deploy -------------
echo "campaign: === gate 1: pre-deploy check of the v2 job ==="
# NOT `$(... | tr -d '\r')`: a pipeline's exit status is the LAST command's,
# so piping through tr would report tr's 0 for every check - the gate would
# read "pass" whatever the engine said, and the negative control below
# would read "accepted" and fail a healthy run. Capture the status first,
# strip afterwards.
CHECK_V2_OUT=$(on_host "$COORD_PUB" "docker exec clink-coordinator clink check-savepoint \
    --file='$SNAP_ONE' --expected=$JOBS_DIR/qual11_job_v2.so" 2>&1)
CHECK_V2_RC=$?
CHECK_V2_OUT=$(printf '%s' "$CHECK_V2_OUT" | tr -d '\r')
echo "$CHECK_V2_OUT" > "$OUT_DIR/check-v2.txt"
if [ "$CHECK_V2_RC" -eq 0 ]; then
    CHECK_V2=pass
else
    CHECK_V2=refused
fi
echo "campaign: v2 check rc=$CHECK_V2_RC ($CHECK_V2)"

# --- gate 2: the negative control ---------------------------------------------
# The SAME check against a v2 built without the migration must refuse (exit
# 3). If it passes, the check approves anything and gate 1 proved nothing.
echo "campaign: === gate 2: negative control (v2 WITHOUT the migration) ==="
CHECK_BROKEN_OUT=$(on_host "$COORD_PUB" "docker exec clink-coordinator clink check-savepoint \
    --file='$SNAP_ONE' --expected=$JOBS_DIR/qual11_job_v2_broken.so" 2>&1)
CHECK_BROKEN_RC=$?
CHECK_BROKEN_OUT=$(printf '%s' "$CHECK_BROKEN_OUT" | tr -d '\r')
echo "$CHECK_BROKEN_OUT" > "$OUT_DIR/check-v2-broken.txt"
if [ "$CHECK_BROKEN_RC" -eq 3 ]; then
    CHECK_BROKEN=refused
elif [ "$CHECK_BROKEN_RC" -eq 0 ]; then
    CHECK_BROKEN=accepted
else
    CHECK_BROKEN=missing
fi
echo "campaign: negative-control check rc=$CHECK_BROKEN_RC ($CHECK_BROKEN)"

RESTORE_S=-1
if [ "$CHECK_V2" != "pass" ]; then
    write_boundary yes "$CHECK_V2" "$CHECK_BROKEN" no no
    echo "campaign: the pre-deploy check REFUSED the v2 job; the campaign does not deploy" >&2
    echo "  past a refusal. Evidence in $OUT_DIR" >&2
    on_host "$OPS_PUB" "touch /qual/progress.json.stop; true" || true
    collect_container_logs "-at-refusal" || true
    python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
        ${LOCAL_MODE:+--local} > "$OUT_DIR/QUAL-11-summary.md" 2>&1 || true
    exit 5
fi

# --- the swap ------------------------------------------------------------------
echo "campaign: cancelling v1 and restoring as v2"
curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
cw=0
while [ "$cw" -lt 120 ]; do
    st=$(job_status "$JOB_ID")
    [ "$st" = "CANCELLED" ] || [ "$st" = "FAILED" ] || [ -z "$st" ] && break
    sleep 5; cw=$(( cw + 5 ))
done

T_SUBMIT=$(date +%s)
submit_job qual11_job_v2.so "$CKPT_DIR_V2" "$SP_PORTABLE" "$SP_ID" \
    > "$OUT_DIR/submit-v2.log" 2>&1 || true
JOB_ID_V2=$(parse_job_id "$OUT_DIR/submit-v2.log")
if [ -z "$JOB_ID_V2" ]; then
    write_boundary yes "$CHECK_V2" "$CHECK_BROKEN" no no
    tail -20 "$OUT_DIR/submit-v2.log" >&2
    verify_fail "the migrated v2 job did not submit"
fi
JOB_ID="$JOB_ID_V2"
dw=0
while [ "$dw" -lt "$DEPLOY_TIMEOUT_S" ]; do
    [ "$(job_status "$JOB_ID")" = "RUNNING" ] && { RESTORE_S=$(( $(date +%s) - T_SUBMIT )); break; }
    sleep 5; dw=$(( dw + 5 ))
done
if [ "$RESTORE_S" -lt 0 ]; then
    write_boundary yes "$CHECK_V2" "$CHECK_BROKEN" no no
    verify_fail "the migrated job never reached RUNNING within ${DEPLOY_TIMEOUT_S}s"
fi
# "Same logical job" here means the restore continued THIS job's identity:
# a new job id is expected from a resubmission, so continuity is judged by
# the oracle (counts carried) rather than by the id - the id is recorded
# for the evidence, and a job that vanished is caught separately.
write_boundary yes "$CHECK_V2" "$CHECK_BROKEN" yes yes
echo "campaign: migrated job $JOB_ID RUNNING in ${RESTORE_S}s"

# --- gate 4's evidence: the migrated STATE, before the battery -----------------
# Read from the savepoints, not the output stream. The sink is
# at-least-once and buffers, so the obvious signal (the first
# post-boundary row showing the seeded sentinels collapse) can be
# destroyed by any unrelated fault - qual11-local-e proved that, losing
# the evidence to a worker kill while the engine behaved perfectly.
# A second savepoint here captures the migrated state while some keys are
# still untouched, which is what makes the migration's exact output
# checkable.
cw=0
while [ "$cw" -lt "$DEPLOY_TIMEOUT_S" ]; do
    [ "$(ckpt_id "$JOB_ID")" -ge 1 ] && break
    sleep 5; cw=$(( cw + 5 ))
done
SP2_OUT=$(on_host "$COORD_PUB" "docker exec clink-coordinator clink savepoint \
    --job-id=$JOB_ID --timeout-s=$SAVEPOINT_TIMEOUT_S" 2>&1 | tr -d '\r') || true
echo "$SP2_OUT" > "$OUT_DIR/savepoint-v2.txt"
SP2_DIR=$(echo "$SP2_OUT" | sed -n 's/.*dir=\([^ ]*\).*/\1/p' | head -1 | tr -d '"')
SP2_ID=$(echo "$SP2_OUT" | sed -n 's/.*id=\([0-9]*\).*/\1/p' | head -1)
if [ -n "$SP2_DIR" ] && [ -n "$SP2_ID" ]; then
    SNAP2=$(on_host "$OPS_PUB" "find '$SP2_DIR' -name 'checkpoint-${SP2_ID}.snap' | head -1" | tr -d '\r')
    for pair in "v1:$SNAP_ONE" "v2:$SNAP2"; do
        tag="${pair%%:*}"; snap="${pair#*:}"
        [ -n "$snap" ] || continue
        on_host "$COORD_PUB" "docker exec clink-coordinator clink state-cat \
            --file='$snap' --json --max-rows=0" > "$OUT_DIR/state-$tag.json" 2>/dev/null || true
    done
    python3 "$HERE/migration_effect.py" --v1-dump "$OUT_DIR/state-v1.json" \
        --v2-dump "$OUT_DIR/state-v2.json" --out "$OUT_DIR/q11-effect.json" \
        > "$OUT_DIR/effect.log" 2>&1 || true
    echo "campaign: migration-effect evidence captured"
else
    echo "campaign: WARNING - no post-restore savepoint; gate 4 has no state evidence" >&2
fi

# --- battery on the MIGRATED job ----------------------------------------------
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
# Flags INLINE, not via a variable: the harness's chaos-interface drift
# test reads the launch line statically, and arguments hidden behind a
# variable are arguments nothing checks (it caught this campaign passing
# --out where chaos.py wants --log).
start_on_host "$OPS_PUB" q11-chaos.log \
    "python3 /qual/chaos.py --inventory /qual/inventory.json --log /qual/q11-chaos.jsonl \
     --coordinator-url http://${COORD_PRIV}:8095 --job-id $JOB_ID --run-id $RUN_ID \
     --profile $PROFILE --seed $SEED --min-gap-s $MIN_GAP_S \
     --recovery-timeout-s $RECOVERY_TIMEOUT_S --duration-s $(( DURATION_S + 600 )) \
     --ensure-coverage"
echo "campaign: battery started for ${DURATION_S}s"

bstart=$(date +%s)
while [ $(( $(date +%s) - bstart )) -lt "$DURATION_S" ]; do
    sleep "$JOB_PROBE_INTERVAL_S"
    st=$(job_status "$JOB_ID")
    if [ -z "$st" ]; then
        echo "job_id=$JOB_ID" > "$OUT_DIR/job-gone.txt"
        verify_fail "the job disappeared from the coordinator"
    fi
    echo "campaign: battery $(( $(date +%s) - bstart ))s: status=$st ckpt=$(ckpt_id "$JOB_ID")"
done
echo "campaign: battery complete, draining"
on_host "$OPS_PUB" "touch /qual/q11-chaos.jsonl.stop; pkill -INT -f '[c]haos.py'; true" || true

# --- drain, catch up, verify ---------------------------------------------------
on_host "$OPS_PUB" "touch /qual/progress.json.stop; pkill -INT -f '[g]enerator.py'; true" || true
sleep 20
PRODUCED=$(on_host "$OPS_PUB" "python3 -c \"
import json
p=json.load(open('/qual/progress.json'))
print(sum(p.get('produced_high',{}).values()))\"" | tr -d '\r')
echo "campaign: generator produced $PRODUCED events"

# Catch-up: the sink's row count must stop moving AND the job must have
# completed a checkpoint after the last event, or the judgement is of a
# pipeline still working.
LAST=0; STILL=0; CAUGHT=no
cstart=$(date +%s)
while [ $(( $(date +%s) - cstart )) -lt "$CATCHUP_TIMEOUT_S" ]; do
    sleep 30
    N=$(rpk_ops "topic describe $OUT_TOPIC -p" 2>/dev/null \
        | awk 'NR>1 {s+=$3} END {print s+0}' | tr -d '\r')
    case "$N" in ''|*[!0-9]*) continue ;; esac
    echo "campaign: catch-up $N rows"
    if [ "$N" = "$LAST" ]; then
        STILL=$(( STILL + 30 ))
        [ "$STILL" -ge 90 ] && { CAUGHT=yes; break; }
    else
        STILL=0
    fi
    LAST="$N"
    [ $(( $(date +%s) - cstart )) -gt "$CATCHUP_STALL_S" ] && break
done
echo "caught_up=$CAUGHT" > "$OUT_DIR/catchup.txt"
echo "produced=$PRODUCED" >> "$OUT_DIR/catchup.txt"

sleep "$FINAL_WAIT_S"
ST=$(job_status "$JOB_ID")
[ "$ST" = "RUNNING" ] && echo "quiesced=yes" > "$OUT_DIR/final-quiesce.txt" \
                      || echo "quiesced=no" > "$OUT_DIR/final-quiesce.txt"

echo "campaign: verifying"
on_host "$OPS_PUB" "python3 /qual/verifier.py --brokers '$BROKER_LIST' --topic $OUT_TOPIC \
    --spec /qual/progress.json.spec --progress /qual/progress.json \
    --key-epoch-ms $KEY_EPOCH_MS --out /qual/q11-verify.json" > "$OUT_DIR/verify.log" 2>&1 || true
on_host "$OPS_PUB" "cat /qual/q11-verify.json" > "$OUT_DIR/q11-verify.json" 2>/dev/null || true

# --- evidence + verdict ---------------------------------------------------------
collect_container_logs || true
for f in q11-chaos.jsonl q11-generator.log q11-chaos.log progress.json progress.json.spec; do
    on_host "$OPS_PUB" "cat /qual/$f" > "$OUT_DIR/$f" 2>/dev/null || true
done
python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    ${LOCAL_MODE:+--local} > "$OUT_DIR/QUAL-11-summary.md" 2>&1 || true
cat "$OUT_DIR/QUAL-11-summary.md"
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh $RIG_RUN_ID --yes && qualification/infra/teardown.sh --check"
