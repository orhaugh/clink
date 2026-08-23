#!/usr/bin/env bash
# QUAL-08: rolling upgrade across engine revisions.
#
# THE CLAIM. A running stateful SQL job upgrades across engine revisions
# with exactly-once continuity: savepoint on vN, restore on vN+1, and an
# oracle that spans the boundary - because the generator never stops -
# counts every event exactly once. The fault battery then runs entirely on
# vN+1, so the restored state is also the state that survives faults.
#
# WHAT CARRIES THE STATE ACROSS. Not binary compatibility: the plugin ABI
# gate governs dlopen only, and a SQL job ships no .so at all. The upgrade
# contract is the savepoint - canonical Arrow snapshots with state-schema
# version stamps, operator identity from the planner's node ids, and
# migrate-at-restore per slot. `clink check-savepoint`, run INSIDE THE NEW
# IMAGE, is the pre-deploy gate: exit 3 names a migration path the new
# engine lacks, and a refused pair publishes nothing and files a finding.
#
# THE OP-ID PRE-FLIGHT. Restore keys on operator identity, and SQL
# operators get planner-emitted node ids - counters in plan order, stable
# by measurement (identical across 6ba73b5 -> bed138c), not by proof. So
# every run of this campaign re-verifies it: the SAME rendered script is
# compiled inside BOTH images and the ids diffed before anything runs. A
# mismatch is an engine finding that blocks the campaign; state restored
# under renumbered ids would be silently orphaned, which is worse than a
# refusal.
#
# MEASURED AT THE BOUNDARY: savepoint duration, restore duration (submit
# to RUNNING), and downtime (savepoint completion to the first vN+1
# checkpoint).
#
# SINGLE-IMAGE SMOKE. With IMAGE_V0 == IMAGE_V1 the run exercises the
# whole savepoint -> swap -> restore machinery at one version. That is the
# required local shakedown; the summariser marks it as a smoke and it is
# never publishable as an upgrade claim.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID, e.g. qual08-20260825a}"
IMAGE_V0="${IMAGE_V0:?set IMAGE_V0 - the revision the job starts on}"
IMAGE_V1="${IMAGE_V1:?set IMAGE_V1 - the revision the job upgrades to}"
DURATION_H="${DURATION_H:-0.75}"
PROFILE="${PROFILE:-aggressive}"
RATE="${RATE:-1000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-5000}"
KEY_EPOCH_MS="${KEY_EPOCH_MS:-60000}"
SEED="${SEED:-20260825}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-15000}"
WM_LAG_MS="${WM_LAG_MS:-2000}"
STATE_TTL_MS="${STATE_TTL_MS:-600000}"

# How long the job runs on vN before the savepoint. Long enough that the
# savepoint carries a real population (DISTINCT holds ~RATE * TTL entries
# at equilibrium and is still filling before that); a trivial savepoint
# would make the restore-size gate meaningless.
FILL_S="${FILL_S:-900}"

SAVEPOINT_TIMEOUT_S="${SAVEPOINT_TIMEOUT_S:-240}"
DEPLOY_TIMEOUT_S="${DEPLOY_TIMEOUT_S:-600}"
# The restored job's first checkpoint must carry at least this percentage
# of the savepoint's bytes, or the "restore" started empty (the silent
# nothing-restored path config_lint warns about) and the run must stop
# before it wastes a battery. Generous because event-time TTL keeps
# releasing state while the job is down.
RESTORE_CARRY_MIN_PCT="${RESTORE_CARRY_MIN_PCT:-30}"

MIN_GAP_S="${MIN_GAP_S:-120}"
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-300}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
RESTART_DRAIN_TIMEOUT_MS="${RESTART_DRAIN_TIMEOUT_MS:-300000}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
PGPASSWORD="${PGPASSWORD:-qual08-$(echo "$RUN_ID" | tr -cd 'a-zA-Z0-9')}"
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"

DURATION_S="${DURATION_S:-$(python3 -c "print(int(float('$DURATION_H') * 3600))")}"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-120}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-30}"
FINAL_WAIT_S="${FINAL_WAIT_S:-600}"
CATCHUP_TIMEOUT_S="${CATCHUP_TIMEOUT_S:-1800}"
CATCHUP_STALL_S="${CATCHUP_STALL_S:-900}"
RECOVER_PROBES="${RECOVER_PROBES:-20}"

OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" "$OUT_DIR/oracle-dirty.txt" \
      "$OUT_DIR/completeness.txt" "$OUT_DIR/catchup.txt" "$OUT_DIR/final-quiesce.txt" \
      "$OUT_DIR/upgrade.txt" "$OUT_DIR/verification.txt" \
      "$OUT_DIR/ids-v0.txt" "$OUT_DIR/ids-v1.txt" "$OUT_DIR/checksave.txt"

[ -x "$SUBMIT_BIN" ] || { echo "campaign: $SUBMIT_BIN is not executable; build it first" >&2; exit 78; }

# The retention premise, inherited from QUAL-05 and still load-bearing: a
# TTL below the epoch truncates keys mid-life, and a TTL inside the replay
# lag double-folds replayed events. Either would fail the oracle for a
# reason that is the workload's fault rather than the upgrade's.
if [ "$STATE_TTL_MS" -le "$KEY_EPOCH_MS" ]; then
    echo "campaign: state_ttl (${STATE_TTL_MS}ms) must exceed the key epoch (${KEY_EPOCH_MS}ms)" >&2
    exit 78
fi
if [ "$STATE_TTL_MS" -le $(( CHECKPOINT_INTERVAL_MS * 4 )) ]; then
    echo "campaign: state_ttl (${STATE_TTL_MS}ms) is not comfortably above the replay lag" >&2
    exit 78
fi

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
        sleep $(( attempt * 5 ))
        attempt=$(( attempt + 1 ))
    done
}
to_host() {
    local host=$1 src=$2 dst=$3 attempt=1 rc=0
    while : ; do
        scp "${SSH_OPTS[@]}" -q "$src" "root@$host:$dst" && rc=0 || rc=$?
        if [ "$rc" -eq 0 ] || [ "$attempt" -ge "${SSH_RETRIES:-4}" ]; then
            return "$rc"
        fi
        echo "campaign: scp to $host failed (attempt $attempt); retrying" >&2
        sleep $(( attempt * 5 ))
        attempt=$(( attempt + 1 ))
    done
}

# The suffix matters here in a way it did not in QUAL-05: the image swap
# force-recreates every engine container, and `docker logs` of a recreated
# container starts empty. The vN logs must be retained BEFORE the swap or
# the upgrade's own before-side evidence is gone.
collect_container_logs() {  # optional suffix, e.g. "-v0"
    local sfx="${1:-}"
    mkdir -p "$OUT_DIR/logs"
    on_host "$COORD_PUB" "docker logs --tail 200000 clink-coordinator 2>&1" \
        > "$OUT_DIR/logs/coordinator$sfx.log" 2>/dev/null \
        || echo "campaign: WARNING - could not retain the coordinator container log" >&2
    local wi=0
    for wp in $WORKER_PUBS; do
        on_host "$wp" "docker logs --tail 200000 clink-worker 2>&1" \
            > "$OUT_DIR/logs/worker-$wi$sfx.log" 2>/dev/null \
            || echo "campaign: WARNING - could not retain worker $wi's container log" >&2
        wi=$(( wi + 1 ))
    done
}

start_on_host() {  # host, log-name, command
    local host=$1 log=$2 cmd=$3
    on_host "$host" "cd /qual && (setsid nohup $cmd </dev/null >/qual/$log 2>&1 &) ; exit 0"
    sleep 3
    local script; script=$(echo "$cmd" | awk '{print $2}')
    local pattern="[${script:0:1}]${script:1}"
    on_host "$host" "pgrep -f '$pattern' >/dev/null" \
        || { echo "campaign: $log did not start - see /qual/$log on $host" >&2
             on_host "$host" "tail -20 /qual/$log" >&2 || true; exit 3; }
}

kill_campaign_processes() {  # host
    local host=$1 sig
    for sig in INT TERM KILL; do
        on_host "$host" "pkill -$sig -f '[g]enerator.py'; pkill -$sig -f '[v]erifier.py'; \
            pkill -$sig -f '[c]haos.py'; true"
        sleep 5
        local left
        left=$(on_host "$host" "pgrep -f '[g]enerator.py|[v]erifier.py|[c]haos.py' | wc -l" | tr -d ' \r')
        [ "${left:-0}" = "0" ] && return 0
    done
    return 1
}

verify_fail() {
    echo "campaign: FUNCTIONAL VERIFICATION FAILED - $1" >&2
    # Chaos first (the QUAL-06 lesson): a controller left running keeps
    # firing at whatever is broken, and the evidence then records faults
    # nothing was judging.
    on_host "$OPS_PUB" "touch /qual/q8-chaos.jsonl.stop; pkill -INT -f '[c]haos.py'; true" || true
    collect_container_logs "-at-failure" || true
    [ -n "${JOB_ID:-}" ] && curl -fsS -X POST \
        "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
    echo "campaign: stopping here. Evidence in $OUT_DIR" >&2
    exit 4
}

SAME_IMAGE=no
[ "$IMAGE_V0" = "$IMAGE_V1" ] && SAME_IMAGE=yes
echo "campaign: QUAL-08 run $RUN_ID: $IMAGE_V0 -> $IMAGE_V1 (same_image=$SAME_IMAGE)"
echo "campaign: fill ${FILL_S}s, battery ${DURATION_S}s on v1, profile=$PROFILE"

# --- rig ------------------------------------------------------------------
RIG_RUN_ID="${RIG_RUN_ID:-$RUN_ID}"
if [ "$RIG_RUN_ID" != "$RUN_ID" ]; then
    echo "campaign: running against the existing rig labelled qual-run=$RIG_RUN_ID"
    SKIP_PROVISION=1
fi
if [ -n "${INVENTORY:-}" ]; then
    cp "$INVENTORY" "$OUT_DIR/inventory.json"
    echo "campaign: using the inventory supplied at $INVENTORY"
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

# Shared checkpoint state. The savepoint written on vN must be readable by
# every vN+1 worker regardless of where the restored subtasks land.
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h - a restore after the" >&2
             echo "  image swap could land a subtask on a host that cannot see the savepoint." >&2; exit 2; }
done
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID/v0 /qual/state/$RUN_ID/v1"
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done
CKPT_DIR_V0="/qual/state/$RUN_ID/v0"
CKPT_DIR_V1="/qual/state/$RUN_ID/v1"
echo "campaign: shared state verified; v0=$CKPT_DIR_V0 v1=$CKPT_DIR_V1"

# Both images land on every engine host BEFORE anything runs: discovering
# a missing vN+1 image with the job already down is self-inflicted
# downtime in the measurement.
if [ "${SKIP_IMAGE_PULL:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" IMAGE="$IMAGE_V0" "$HERE/../infra/pull-image.sh"
    [ "$SAME_IMAGE" = "yes" ] || RUN_ID="$RUN_ID" IMAGE="$IMAGE_V1" "$HERE/../infra/pull-image.sh"
fi
for h in $COORD_PUB $WORKER_PUBS; do
    for img in "$IMAGE_V0" "$IMAGE_V1"; do
        on_host "$h" "docker image inspect '$img' >/dev/null 2>&1" \
            || { echo "campaign: image $img is not present on $h" >&2; exit 2; }
    done
done
DIGEST_V0=$(on_host "$COORD_PUB" "docker image inspect --format '{{.Id}}' '$IMAGE_V0'" | tr -d '\r')
DIGEST_V1=$(on_host "$COORD_PUB" "docker image inspect --format '{{.Id}}' '$IMAGE_V1'" | tr -d '\r')
echo "campaign: v0 $DIGEST_V0"
echo "campaign: v1 $DIGEST_V1"
if [ "$SAME_IMAGE" = "no" ] && [ "$DIGEST_V0" = "$DIGEST_V1" ]; then
    echo "campaign: IMAGE_V0 and IMAGE_V1 are different tags of the SAME image; nothing" >&2
    echo "  would be upgraded. Refusing to spend a run on it." >&2
    exit 78
fi

# Both images must carry fault injection: v1 takes the battery, and a v0
# without it would be a different build premise on the two sides.
for img in "$IMAGE_V0" "$IMAGE_V1"; do
    on_host "$COORD_PUB" "docker run --rm --entrypoint clink '$img' --capabilities-json 2>/dev/null" \
        | python3 -c "
import json,sys
d=json.load(sys.stdin)
sys.exit(0 if d.get('build',{}).get('fault_injection') else 1)
" || { echo "campaign: $img has NO fault injection compiled in" >&2; exit 2; }
done

# --- verification database -------------------------------------------------
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
on_host "$OPS_PUB" "grep -q '^PGPASSWORD=' /qual/.env 2>/dev/null || echo 'PGPASSWORD=$PGPASSWORD' >> /qual/.env"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml down -v >/dev/null 2>&1; true"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml up -d"
pgready=0
until on_host "$OPS_PUB" "docker exec qual08-postgres pg_isready -U qual >/dev/null 2>&1"; do
    pgready=$(( pgready + 3 )); sleep 3
    [ "$pgready" -lt 180 ] || { echo "campaign: postgres never became ready" >&2; exit 2; }
done
CONNINFO="host=${OPS_PRIV} port=5432 dbname=qual user=qual password=${PGPASSWORD}"
DSN="host=127.0.0.1 port=5432 dbname=qual user=qual password=${PGPASSWORD}"
psql_q() { on_host "$OPS_PUB" "docker exec qual08-postgres psql -U qual -d qual -tAc \"$1\""; }
psql_q "DROP TABLE IF EXISTS public.q8_out; CREATE TABLE public.q8_out (k BIGINT PRIMARY KEY, n BIGINT)" >/dev/null

# --- stack on v0 -------------------------------------------------------------
i=0
for bp in $(read_inv broker public_ip); do
    bpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$bp'][0])")
    to_host "$bp" "$HERE/../infra/broker.yml" /qual/broker.yml
    on_host "$bp" "cd /qual && NODE_ID=$i PRIVATE_IP=$bpriv SEEDS='$SEED_LIST' \
        docker compose -f broker.yml up -d"
    i=$(( i + 1 ))
done
to_host "$COORD_PUB" "$HERE/../infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml down >/dev/null 2>&1; \
    rm -rf /qual/ha/jobs /qual/ha/history; mkdir -p /qual/ha; \
    printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nRESTART_DRAIN_TIMEOUT_MS=%s\n' \
    '$IMAGE_V0' '$COORD_PRIV' '$RESTART_DRAIN_TIMEOUT_MS' > /qual/.env && \
    docker compose -f coordinator.yml up -d"
wi=0
for wp in $WORKER_PUBS; do
    wpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$wp'][0])")
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=%s\nWORKER_IP=%s\n' \
        '$IMAGE_V0' '$COORD_PRIV' 'w$wi' '$wpriv' > /qual/.env && \
        docker compose -f worker.yml up -d --force-recreate"
    wi=$(( wi + 1 ))
done
sleep 20

# --- topic + ops-host machinery ----------------------------------------------
kill_campaign_processes "$OPS_PUB" || true
rpk_ops() {
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
rpk_ops "topic delete qual08-in" >/dev/null 2>&1 || true
for _t in $(seq 1 30); do
    rpk_ops "topic list" 2>/dev/null | grep -q "qual08-in" || break
    sleep 2
done
REPL=$(python3 -c "print(min(3, len('$BROKER_PRIVS'.split())))")
rpk_ops "topic create qual08-in -p $PARTITIONS -r $REPL" >/dev/null

on_host "$OPS_PUB" "rm -f /qual/q8-*.stop /qual/q8-progress.json /qual/q8-verdict.json \
    /qual/q8-chaos.jsonl /qual/q8-generator.log /qual/q8-verifier.log /qual/q8-chaos.log"
for f in "$HERE/../qual01/detspec.py" "$HERE/../qual01/generator.py" \
         "$HERE/../qual05/verifier.py" "$HERE/../qual05/endstate.py" \
         "$HERE/../qual05/ckptsize.py" "$HERE/../chaos/chaos.py"; do
    to_host "$OPS_PUB" "$f" "/qual/$(basename "$f")"
done
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
to_host "$OPS_PUB" "$KEY_FILE" /root/.ssh/id_ed25519
on_host "$OPS_PUB" "chmod 600 /root/.ssh/id_ed25519"
on_host "$OPS_PUB" "pip3 install --break-system-packages -q confluent-kafka psycopg2-binary 2>/dev/null || true"

EPS=$(( RATE / PARTITIONS ))
[ "$EPS" -ge 1 ] || { echo "campaign: RATE must be at least one event per partition per second" >&2; exit 78; }
BASE_MS=$(python3 -c "import time; print(int(time.time()*1000))")
echo "$BASE_MS" > "$OUT_DIR/base_ms"

start_on_host "$OPS_PUB" q8-generator.log \
    "python3 /qual/generator.py --brokers '$BROKER_LIST' --topic qual08-in \
     --rate $RATE --partitions $PARTITIONS --keys $KEYS --seed $SEED \
     --base-ms $BASE_MS --max-jitter-ms 0 --window-ms 10000 \
     --key-epoch-ms $KEY_EPOCH_MS --progress /qual/q8-progress.json"

# --- pipeline: rendered ONCE, submitted to both revisions ---------------------
sed -e "s|__BROKERS__|$BROKER_LIST|g" \
    -e "s|__CONNINFO__|$CONNINFO|g" \
    -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
    -e "s|__GROUP__|qual08-$RUN_ID|g" \
    -e "s|__STATE_TTL_MS__|$STATE_TTL_MS|g" \
    "$HERE/pipeline.sql.tmpl" > "$OUT_DIR/pipeline.sql"

# --- op-id pre-flight ----------------------------------------------------------
# clink_submit_sql without --coordinator-host prints the compiled
# JobGraphSpec; the ops[].id fields are the identities restore keys on.
echo "campaign: op-id pre-flight - compiling the script inside both images"
to_host "$COORD_PUB" "$OUT_DIR/pipeline.sql" /qual/q8-pipeline.sql
extract_ids() {  # image -> operator ids, one per line
    on_host "$COORD_PUB" "docker run --rm -v /qual/q8-pipeline.sql:/p.sql:ro \
        --entrypoint clink_submit_sql '$1' --file /p.sql 2>/dev/null" | python3 -c "
import json, sys
ids = []
for line in sys.stdin:
    line = line.strip()
    if not line.startswith('{'):
        continue
    try:
        d = json.loads(line)
    except Exception:
        continue
    for key in ('ops', 'operators', 'nodes'):
        for op in (d.get(key) or []):
            if isinstance(op, dict) and 'id' in op:
                ids.append(str(op['id']))
print('\n'.join(ids))"
}
extract_ids "$IMAGE_V0" > "$OUT_DIR/ids-v0.txt"
extract_ids "$IMAGE_V1" > "$OUT_DIR/ids-v1.txt"
[ -s "$OUT_DIR/ids-v0.txt" ] || { echo "campaign: could not extract operator ids from $IMAGE_V0" >&2; exit 2; }
[ -s "$OUT_DIR/ids-v1.txt" ] || { echo "campaign: could not extract operator ids from $IMAGE_V1" >&2; exit 2; }
OPID_MATCH=yes
if ! diff -u "$OUT_DIR/ids-v0.txt" "$OUT_DIR/ids-v1.txt" > "$OUT_DIR/opid-diff.txt"; then
    OPID_MATCH=no
    { echo "opid_match=no"; echo "savepoint_ok=no"; echo "same_image=$SAME_IMAGE";
      echo "image_v0=$IMAGE_V0"; echo "image_v1=$IMAGE_V1";
    } > "$OUT_DIR/upgrade.txt"
    echo "campaign: OPERATOR IDS DIFFER between the two revisions - restored state" >&2
    echo "  would be silently orphaned. This is an engine finding; the campaign stops" >&2
    echo "  before running anything. Diff retained at $OUT_DIR/opid-diff.txt" >&2
    exit 4
fi
echo "campaign: operator ids identical across the pair ($(wc -l < "$OUT_DIR/ids-v0.txt" | tr -d ' ') ops)"

# --- submit on v0 --------------------------------------------------------------
"$SUBMIT_BIN" --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir "$CKPT_DIR_V0" \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --max-restarts-on-worker-loss "$MAX_RESTARTS" \
    > "$OUT_DIR/submit-v0.log" 2>&1 || true
parse_job_id() {  # submit log -> job id
    python3 -c "
import json
jid=''
for line in open('$1'):
    line=line.strip()
    if line.startswith('{'):
        try:
            d=json.loads(line)
        except Exception:
            continue
        if d.get('ok') and d.get('job_id') is not None:
            jid=str(d['job_id'])
print(jid)"
}
JOB_ID=$(parse_job_id "$OUT_DIR/submit-v0.log")
[ -n "$JOB_ID" ] || { echo "campaign: the v0 job did not submit" >&2
                      tail -20 "$OUT_DIR/submit-v0.log" >&2; exit 1; }
echo "campaign: v0 job $JOB_ID"

job_status() {  # job id
    local body
    body=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/$1" 2>/dev/null) || {
        echo UNREACHABLE; return 0; }
    [ -n "$body" ] || { echo UNREACHABLE; return 0; }
    echo "$body" | python3 -c "
import json,sys
try:
    print(json.load(sys.stdin).get('status') or 'UNKNOWN')
except Exception:
    print('GONE')" 2>/dev/null || echo GONE
}
ckpt_id() {  # job id
    curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/$1" 2>/dev/null | python3 -c "
import json,sys
try:
    print(int(json.load(sys.stdin).get('latest_completed_checkpoint_id') or 0))
except Exception:
    print(0)" 2>/dev/null || echo 0
}
state_bytes() {  # checkpoint dir
    on_host "$OPS_PUB" "python3 /qual/ckptsize.py --dir '$1'" 2>/dev/null | tr -d '\r'
}

# --- functional verification on v0 ---------------------------------------------
echo "campaign: functional verification on v0"
P1=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q8-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 45
P2=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q8-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
[ "${P2:-0}" -gt "${P1:-0}" ] || verify_fail "no input is flowing (progress ${P1} -> ${P2})"

JOB_HEALTHY=no
for _t in $(seq 1 30); do
    if [ "$(job_status "$JOB_ID")" = "RUNNING" ] && [ "$(ckpt_id "$JOB_ID")" -ge 1 ]; then
        JOB_HEALTHY=yes; break
    fi
    sleep 10
done
[ "$JOB_HEALTHY" = "yes" ] \
    || verify_fail "the v0 job never reached RUNNING with a completed checkpoint"

NJOBS=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs" 2>/dev/null \
    | python3 -c "
import json,sys
live = {'RUNNING', 'RESTARTING', 'DEPLOYING', 'PENDING'}
try:
    js = json.load(sys.stdin).get('jobs', [])
except Exception:
    print(-1); sys.exit(0)
print(sum(1 for j in js if (j.get('status') or '').upper() in live))" 2>/dev/null || echo -1)
[ "${NJOBS:-0}" = "1" ] || verify_fail "expected exactly one LIVE job on the cluster, found ${NJOBS}"

SINK_OK=no
for _t in $(seq 1 20); do
    C=$(psql_q "SELECT count(*) FROM public.q8_out" | tr -d '\r')
    [ "${C:-0}" -gt 0 ] && { SINK_OK=yes; break; }
    sleep 15
done
[ "$SINK_OK" = "yes" ] || verify_fail "nothing reached the sink table"

STATE_OK=no
for _t in $(seq 1 20); do
    B=$(state_bytes "$CKPT_DIR_V0" || echo "")
    case "$B" in ''|*[!0-9]*) sleep 15; continue ;; esac
    [ "$B" -gt 0 ] && { STATE_OK=yes; break; }
    sleep 15
done
[ "$STATE_OK" = "yes" ] || verify_fail "the state instrument cannot measure the v0 checkpoints"

# The verifier watches the sink across the WHOLE run, boundary included: a
# per-key count that shrinks at the swap is upgrade-lost state, and it is
# caught here long before the end-state pass.
start_on_host "$OPS_PUB" q8-verifier.log \
    "python3 /qual/verifier.py --dsn '$DSN' --table public.q8_out \
     --progress /qual/q8-progress.json --out /qual/q8-verdict.json --interval-s 20"

# --- fill on v0 -----------------------------------------------------------------
echo "campaign: filling on v0 (${FILL_S}s)"
fstart=$(date +%s)
while [ $(( $(date +%s) - fstart )) -lt "$FILL_S" ]; do
    sleep "$SAMPLE_INTERVAL_S"
    B=$(state_bytes "$CKPT_DIR_V0" || echo "")
    case "$B" in ''|*[!0-9]*) continue ;; esac
    echo "campaign: fill $(( ($(date +%s) - fstart) / 60 ))m: $(( B / 1024 / 1024 )) MiB of v0 state"
done

# =============================================================================
# THE UPGRADE
# =============================================================================
echo "campaign: === upgrade: savepoint on v0 ==="
T_SP0=$(date +%s)
SP_OUT=$(on_host "$COORD_PUB" "docker exec clink-coordinator clink savepoint \
    --job-id=$JOB_ID --timeout-s=$SAVEPOINT_TIMEOUT_S" 2>&1 | tr -d '\r') || true
echo "$SP_OUT" > "$OUT_DIR/savepoint.txt"
SP_OK=$(echo "$SP_OUT" | sed -n 's/.*ok=\([01]\).*/\1/p' | head -1)
# The tool prints dir="..." - the quotes are part of the output, not the
# path. The first local smoke fed the quoted string to find and got "no
# snapshot files" for a savepoint that was sitting right there.
SP_DIR=$(echo "$SP_OUT" | sed -n 's/.*dir=\([^ ]*\).*/\1/p' | head -1 | tr -d '"')
SP_ID=$(echo "$SP_OUT" | sed -n 's/.*id=\([0-9]*\).*/\1/p' | head -1)
SAVEPOINT_S=$(( $(date +%s) - T_SP0 ))
if [ "${SP_OK:-0}" != "1" ] || [ -z "$SP_DIR" ] || [ -z "$SP_ID" ]; then
    { echo "opid_match=$OPID_MATCH"; echo "savepoint_ok=no"; echo "same_image=$SAME_IMAGE";
      echo "image_v0=$IMAGE_V0"; echo "image_v1=$IMAGE_V1";
    } > "$OUT_DIR/upgrade.txt"
    verify_fail "the savepoint did not complete: $SP_OUT"
fi
echo "campaign: savepoint id=$SP_ID dir=$SP_DIR in ${SAVEPOINT_S}s"

# Relocate the savepoint IMMEDIATELY, exactly as the tool's contract asks
# ("physical relocation to a portable path is the operator's
# responsibility"). This is not optional tidiness: snapshot retention for
# some operators keeps only the newest checkpoint, so the savepoint's
# files are garbage-collected as soon as the NEXT checkpoint completes -
# one interval after the handle is printed. The first local smoke lost 4
# of 10 subtask snapshots this way and the restore (correctly) refused.
# Hard links, not a byte copy: instant at any state size, so the window
# stays milliseconds even on a rig-scale savepoint; retention's unlink
# then only drops the original link. Layout preserved relative to the
# checkpoint root: v<gen>/<subtask>/checkpoint-<id>.snap{,.meta} plus the
# _jobs completion markers the restore reads participants from.
SP_PORTABLE="/qual/state/$RUN_ID/savepoint-$SP_ID"
on_host "$OPS_PUB" "rm -rf '$SP_PORTABLE' && mkdir -p '$SP_PORTABLE' && cd '$SP_DIR' && \
    find . -type f \( -name 'checkpoint-${SP_ID}.snap' -o -name 'checkpoint-${SP_ID}.snap.meta' \
             -o -path './_jobs/*' \) -exec cp -l --parents {} '$SP_PORTABLE/' \;" \
    || verify_fail "could not relocate the savepoint to $SP_PORTABLE"
# The relocation itself can lose the race (the window is the ssh round
# trip): every participant directory in the live tree must have
# contributed its snapshot, or the restore would refuse later with a
# message that points at the engine instead of at this copy.
# Relative matching, from inside the tree: the ABSOLUTE path contains the
# campaign's own .../v0 component, so '*/v*/*' would also count _jobs/1.
DIRS_N=$(on_host "$OPS_PUB" "cd '$SP_DIR' && find . -mindepth 2 -maxdepth 2 -type d -path './v*/*' | wc -l" | tr -d ' \r')
SNAPS_N=$(on_host "$OPS_PUB" "find '$SP_PORTABLE' -name 'checkpoint-${SP_ID}.snap' | wc -l" | tr -d ' \r')
[ "${SNAPS_N:-0}" -ge "${DIRS_N:-1}" ] \
    || verify_fail "the savepoint relocation raced retention: ${SNAPS_N} of ${DIRS_N} subtask snapshots survived to the copy"
SP_BYTES=$(state_bytes "$SP_PORTABLE")
case "$SP_BYTES" in ''|*[!0-9]*) SP_BYTES=0 ;; esac
echo "campaign: savepoint relocated to $SP_PORTABLE (${SNAPS_N} subtask snapshots, $(( SP_BYTES / 1024 / 1024 )) MiB)"

# --- the check-savepoint gate, run INSIDE the new image -------------------------
echo "campaign: check-savepoint under $IMAGE_V1"
# Against the PORTABLE copy - the live tree can keep losing files to
# retention while this gate runs. Exact name, not a wildcard:
# checkpoint-1* would also match later checkpoints (10, 11, ...).
SNAPS=$(on_host "$OPS_PUB" "find '$SP_PORTABLE' -name 'checkpoint-${SP_ID}.snap' 2>/dev/null | sort" | tr -d '\r')
[ -n "$SNAPS" ] || verify_fail "no snapshot files found for checkpoint $SP_ID under $SP_DIR"
CHECKSAVE=ok
: > "$OUT_DIR/checksave.txt"
for f in $SNAPS; do
    CS_RC=0
    CS_OUT=$(on_host "$COORD_PUB" "docker run --rm -v /qual/state:/qual/state \
        --entrypoint clink '$IMAGE_V1' check-savepoint --file='$f'" 2>&1 | tr -d '\r') || CS_RC=$?
    { echo "--- $f (rc=$CS_RC)"; echo "$CS_OUT"; } >> "$OUT_DIR/checksave.txt"
    [ "$CS_RC" -eq 0 ] || CHECKSAVE=refused
done
if [ "$CHECKSAVE" != "ok" ]; then
    { echo "opid_match=$OPID_MATCH"; echo "savepoint_ok=yes"; echo "savepoint_id=$SP_ID";
      echo "savepoint_s=$SAVEPOINT_S"; echo "checksave=refused"; echo "same_image=$SAME_IMAGE";
      echo "image_v0=$IMAGE_V0"; echo "image_v1=$IMAGE_V1";
      echo "digest_v0=$DIGEST_V0"; echo "digest_v1=$DIGEST_V1";
    } > "$OUT_DIR/upgrade.txt"
    echo "campaign: the NEW image REFUSED the savepoint - a migration path is missing." >&2
    echo "  This pair does not upgrade; the refusal is the finding. See checksave.txt" >&2
    verify_fail "check-savepoint refused the pair"
fi
echo "campaign: the new image accepts every snapshot of checkpoint $SP_ID"

# --- stop v0, retain its logs, swap the images ----------------------------------
curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
cstop=0
while [ "$cstop" -lt 180 ]; do
    case "$(job_status "$JOB_ID")" in RUNNING|RESTARTING|UNKNOWN) sleep 5; cstop=$(( cstop + 5 )) ;; *) break ;; esac
done
[ "$cstop" -lt 180 ] || echo "campaign: WARNING - the v0 job was still live 180s after cancel; the swap will clear it" >&2
collect_container_logs "-v0"

echo "campaign: === swapping the engine to $IMAGE_V1 ==="
# Coordinator DOWN before the HA wipe (item 69), wipe so the recreated
# coordinator cannot resurrect the cancelled v0 job, then up on v1.
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml down >/dev/null 2>&1; \
    rm -rf /qual/ha/jobs /qual/ha/history; mkdir -p /qual/ha; \
    printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nRESTART_DRAIN_TIMEOUT_MS=%s\n' \
    '$IMAGE_V1' '$COORD_PRIV' '$RESTART_DRAIN_TIMEOUT_MS' > /qual/.env && \
    docker compose -f coordinator.yml up -d"
wi=0
for wp in $WORKER_PUBS; do
    wpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$wp'][0])")
    on_host "$wp" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=%s\nWORKER_IP=%s\n' \
        '$IMAGE_V1' '$COORD_PRIV' 'w$wi' '$wpriv' > /qual/.env && \
        docker compose -f worker.yml up -d --force-recreate"
    wi=$(( wi + 1 ))
done
sleep 20
RUNNING_IMG=$(on_host "$COORD_PUB" "docker inspect --format '{{.Image}}' clink-coordinator" | tr -d '\r')
[ "$RUNNING_IMG" = "$DIGEST_V1" ] \
    || { echo "campaign: the recreated coordinator is not running the v1 image ($RUNNING_IMG)" >&2; exit 2; }

# --- restore on v1 ---------------------------------------------------------------
echo "campaign: resubmitting with --restore-from-dir $SP_PORTABLE id $SP_ID"
T_SUBMIT=$(date +%s)
"$SUBMIT_BIN" --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir "$CKPT_DIR_V1" \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --restore-from-dir "$SP_PORTABLE" \
    --restore-from-checkpoint-id "$SP_ID" \
    --max-restarts-on-worker-loss "$MAX_RESTARTS" \
    > "$OUT_DIR/submit-v1.log" 2>&1 || true
JOB_ID=$(parse_job_id "$OUT_DIR/submit-v1.log")
[ -n "$JOB_ID" ] || { { echo "opid_match=$OPID_MATCH"; echo "savepoint_ok=yes"; echo "savepoint_id=$SP_ID";
      echo "savepoint_s=$SAVEPOINT_S"; echo "checksave=ok"; echo "restore_ok=no";
      echo "same_image=$SAME_IMAGE"; echo "image_v0=$IMAGE_V0"; echo "image_v1=$IMAGE_V1";
    } > "$OUT_DIR/upgrade.txt"
    tail -20 "$OUT_DIR/submit-v1.log" >&2
    verify_fail "the v1 job did not submit"; }
echo "campaign: v1 job $JOB_ID"

RESTORE_S=-1; DOWNTIME_S=-1
dw=0
while [ "$dw" -lt "$DEPLOY_TIMEOUT_S" ]; do
    [ "$(job_status "$JOB_ID")" = "RUNNING" ] && { RESTORE_S=$(( $(date +%s) - T_SUBMIT )); break; }
    sleep 5; dw=$(( dw + 5 ))
done
[ "$RESTORE_S" -ge 0 ] || verify_fail "the restored job never reached RUNNING within ${DEPLOY_TIMEOUT_S}s"
cw=0
while [ "$cw" -lt "$DEPLOY_TIMEOUT_S" ]; do
    [ "$(ckpt_id "$JOB_ID")" -ge 1 ] && { DOWNTIME_S=$(( $(date +%s) - T_SP0 - SAVEPOINT_S )); break; }
    sleep 5; cw=$(( cw + 5 ))
done
[ "$DOWNTIME_S" -ge 0 ] || verify_fail "the restored job never completed a checkpoint on v1"

# The restore must have CARRIED the state, not started clean: the first v1
# checkpoint has to be in the savepoint's ballpark. A nothing-restored job
# checkpoints near-empty and would still look RUNNING and healthy.
V1_BYTES=""
for _t in $(seq 1 10); do
    V1_BYTES=$(state_bytes "$CKPT_DIR_V1" || echo "")
    case "$V1_BYTES" in ''|*[!0-9]*) sleep 10; continue ;; esac
    break
done
case "$V1_BYTES" in ''|*[!0-9]*) V1_BYTES=0 ;; esac
RESTORE_CARRIED=yes
if [ "$SP_BYTES" -gt 0 ] && [ $(( V1_BYTES * 100 )) -lt $(( SP_BYTES * RESTORE_CARRY_MIN_PCT )) ]; then
    RESTORE_CARRIED=no
fi
{ echo "opid_match=$OPID_MATCH"; echo "savepoint_ok=yes"; echo "savepoint_id=$SP_ID";
  echo "savepoint_s=$SAVEPOINT_S"; echo "savepoint_bytes=$SP_BYTES";
  echo "checksave=ok"; echo "restore_ok=yes"; echo "restore_s=$RESTORE_S";
  echo "downtime_s=$DOWNTIME_S"; echo "v1_first_ckpt_bytes=$V1_BYTES";
  echo "restore_carried=$RESTORE_CARRIED"; echo "same_image=$SAME_IMAGE";
  echo "image_v0=$IMAGE_V0"; echo "image_v1=$IMAGE_V1";
  echo "digest_v0=$DIGEST_V0"; echo "digest_v1=$DIGEST_V1";
} > "$OUT_DIR/upgrade.txt"
cat "$OUT_DIR/upgrade.txt"
[ "$RESTORE_CARRIED" = "yes" ] \
    || verify_fail "the restored job's first checkpoint ($V1_BYTES bytes) is below ${RESTORE_CARRY_MIN_PCT}% of the savepoint ($SP_BYTES bytes); the restore did not carry the state"

# Post-restore functional verification: the fold must be advancing again.
S1=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q8_out" | tr -d '\r')
ADVANCED=no
for _t in $(seq 1 20); do
    sleep 15
    S2=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q8_out" | tr -d '\r')
    [ "${S2:-0}" -gt "${S1:-0}" ] && { ADVANCED=yes; break; }
done
[ "$ADVANCED" = "yes" ] || verify_fail "the restored job is not folding new events"

{ echo "campaign=QUAL-08"; echo "run_id=$RUN_ID"; echo "job_id_v1=$JOB_ID";
  echo "state_ttl_ms=$STATE_TTL_MS"; echo "key_epoch_ms=$KEY_EPOCH_MS";
  echo "rate=$RATE"; echo "partitions=$PARTITIONS"; echo "keys_per_epoch=$KEYS";
  echo "checkpoint_interval_ms=$CHECKPOINT_INTERVAL_MS"; echo "fill_s=$FILL_S";
} > "$OUT_DIR/verification.txt"

# --- the battery, entirely on v1 -------------------------------------------------
echo "campaign: === fault battery on v1 (${DURATION_S}s) ==="
Q8_POINTS=$(python3 -c "
import sys; sys.path.insert(0, '$HERE')
import summarise
print(','.join(summarise.TWOPC_POINTS))")
[ -n "$Q8_POINTS" ] || { echo "campaign: no 2PC points from the summariser" >&2; exit 78; }
start_on_host "$OPS_PUB" q8-chaos.log \
    "python3 /qual/chaos.py --inventory /qual/inventory.json --log /qual/q8-chaos.jsonl \
     --coordinator-url http://${COORD_PRIV}:8095 --job-id $JOB_ID --run-id $RUN_ID \
     --profile $PROFILE --seed $SEED --min-gap-s $MIN_GAP_S \
     --twopc-points '$Q8_POINTS' --recovery-timeout-s $RECOVERY_TIMEOUT_S \
     --duration-s $(( DURATION_S + 1800 )) --ensure-coverage"

FAULTED=no
for _t in $(seq 1 60); do
    N=$(on_host "$OPS_PUB" "wc -l < /qual/q8-chaos.jsonl 2>/dev/null || echo 0" | tr -d ' \r')
    [ "${N:-0}" -gt 0 ] && { FAULTED=yes; break; }
    sleep 20
done
[ "$FAULTED" = "yes" ] || verify_fail "the chaos controller applied no fault"

BEFORE=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q8_out" | tr -d '\r')
RECOVERED=no
for _p in $(seq 1 "$RECOVER_PROBES"); do
    sleep 30
    S=$(job_status "$JOB_ID")
    AFTER=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q8_out" | tr -d '\r')
    if [ "$S" = "RUNNING" ] && [ "${AFTER:-0}" -gt "${BEFORE:-0}" ]; then RECOVERED=yes; break; fi
done
[ "$RECOVERED" = "yes" ] || verify_fail "the job did not resume making progress after the first fault"

SOAK_START=$(date +%s)
END=$(( SOAK_START + DURATION_S ))
CHAOS_DIED_AT=""
JOB_GONE_AT=""
WATCH_LOOPS=0
while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$WATCH_MAX_LOOPS" != "0" ] && [ "$WATCH_LOOPS" -ge "$WATCH_MAX_LOOPS" ]; then break; fi
    WATCH_LOOPS=$(( WATCH_LOOPS + 1 ))
    sleep "$SAMPLE_INTERVAL_S"
    for f in q8-verdict.json q8-chaos.jsonl q8-progress.json q8-generator.log q8-verifier.log q8-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    DIRTY=$(python3 - "$OUT_DIR/q8-verdict.json" <<'PY' || echo ""
import json, sys
try:
    v = json.load(open(sys.argv[1]))
except Exception:
    print(""); sys.exit(0)
print("DIRTY" if (v.get("findings") or v.get("stuck")) else "")
PY
)
    if [ "$DIRTY" = "DIRTY" ]; then
        echo "campaign: ORACLE DIRTY - freezing faults and collecting evidence" >&2
        { echo "oracle_dirty=yes"; echo "noticed_at_utc=$(date -u +%H:%M)"; } > "$OUT_DIR/oracle-dirty.txt"
        on_host "$OPS_PUB" "touch /qual/q8-chaos.jsonl.stop"
        on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py' || true"
        collect_container_logs
        break
    fi

    if [ -z "$CHAOS_DIED_AT" ] && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q8-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        echo "campaign: WARNING - the chaos controller is no longer running (${NFAULTS} faults)" >&2
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NFAULTS"; } > "$OUT_DIR/chaos-died.txt"
    fi

    K=$(psql_q "SELECT count(*) FROM public.q8_out" 2>/dev/null | tr -d '\r' || echo '?')
    SM=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q8_out" 2>/dev/null | tr -d '\r' || echo '?')
    echo "campaign: $(date -u +%H:%M) battery $(( $(date +%s) - SOAK_START ))s: $K keys, sum $SM"

    if [ -z "$JOB_GONE_AT" ]; then
        NOT_RUNNING=0; PROBES=""
        for _probe in 1 2 3 4 5 6; do
            ALIVE=$(job_status "$JOB_ID")
            PROBES="${PROBES}${ALIVE} "
            if [ "$ALIVE" = "RUNNING" ]; then NOT_RUNNING=0; break; fi
            [ "$ALIVE" = "UNREACHABLE" ] || NOT_RUNNING=$(( NOT_RUNNING + 1 ))
            [ "$_probe" -lt 6 ] && sleep "$JOB_PROBE_INTERVAL_S"
        done
        if [ "$NOT_RUNNING" -ge 6 ]; then
            JOB_GONE_AT=$(date -u +%H:%M)
            echo "campaign: WARNING - job ${JOB_ID} is no longer RUNNING (probes: ${PROBES})" >&2
            { echo "job_gone=yes"; echo "probes=$PROBES"; } > "$OUT_DIR/job-gone.txt"
        fi
    fi
done

# --- drain and final judgement ------------------------------------------------
echo "campaign: battery complete, draining"
on_host "$OPS_PUB" "touch /qual/q8-chaos.jsonl.stop"
cwaited=0
while [ "$cwaited" -lt 120 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5; cwaited=$(( cwaited + 5 ))
done
[ "$cwaited" -lt 120 ] \
    || echo "campaign: WARNING - the chaos controller is still running 120s after its stop request" >&2

echo "campaign: waiting for the job to settle after the last fault"
settle=0
while [ "$settle" -lt 300 ]; do
    if [ "$(job_status "$JOB_ID")" = "RUNNING" ]; then
        CK1=$(ckpt_id "$JOB_ID")
        sleep 20; settle=$(( settle + 20 ))
        CK2=$(ckpt_id "$JOB_ID")
        [ "${CK2:-0}" -gt "${CK1:-0}" ] && break
    else
        sleep 20; settle=$(( settle + 20 ))
    fi
done
[ "$settle" -lt 300 ] \
    || echo "campaign: WARNING - the job had not completed a fresh checkpoint 300s after the last fault" >&2

on_host "$OPS_PUB" "touch /qual/q8-progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[g]enerator.py'; true"
gwaited=0
while [ "$gwaited" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5; gwaited=$(( gwaited + 5 ))
done

echo "campaign: waiting for the pipeline to catch up with the generator"
CATCHUP=no
PRODUCED_FINAL=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q8-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
cwait=0; LAST_FOLDED=-1; STALL_S=0; FOLDED=0
while [ "$cwait" -lt "$CATCHUP_TIMEOUT_S" ]; do
    FOLDED=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q8_out" | tr -d '\r')
    if [ "${FOLDED:-0}" -ge "${PRODUCED_FINAL:-1}" ]; then CATCHUP=yes; break; fi
    if [ "${FOLDED:-0}" -le "${LAST_FOLDED}" ]; then
        STALL_S=$(( STALL_S + 30 ))
        [ "$STALL_S" -ge "$CATCHUP_STALL_S" ] && break
    else
        STALL_S=0
    fi
    LAST_FOLDED=${FOLDED:-0}
    echo "campaign: catch-up ${FOLDED:-0} / ${PRODUCED_FINAL} events"
    sleep 30; cwait=$(( cwait + 30 ))
done
{ echo "caught_up=$CATCHUP"; echo "produced_final=${PRODUCED_FINAL}";
  echo "folded_at_catchup=${FOLDED:-0}"; echo "catchup_seconds=$cwait";
} > "$OUT_DIR/catchup.txt"

QUIESCED=no
waited=0
prev_sum=-1
while [ "$waited" -lt "$FINAL_WAIT_S" ]; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/q8-verdict.json" "$OUT_DIR/" 2>/dev/null || true
    s=$(python3 - "$OUT_DIR/q8-verdict.json" <<'PY' || echo -1
import json, sys
try:
    v = json.load(open(sys.argv[1]))
    print((v.get("last_stats") or {}).get("sum_n", -1))
except Exception:
    print(-1)
PY
)
    if [ "${s:--1}" -ge 0 ] && [ "$s" = "$prev_sum" ]; then QUIESCED=yes; break; fi
    prev_sum="$s"
    sleep 25; waited=$(( waited + 25 ))
done
echo "quiesced=$QUIESCED" > "$OUT_DIR/final-quiesce.txt"

on_host "$OPS_PUB" "python3 /qual/endstate.py --dsn '$DSN' --table public.q8_out \
    --progress /qual/q8-progress.json --seed $SEED --partitions $PARTITIONS \
    --keys $KEYS --eps $EPS --base-ms $BASE_MS --key-epoch-ms $KEY_EPOCH_MS" \
    > "$OUT_DIR/completeness.txt" \
    || echo "campaign: WARNING - the end-state pass failed; correctness has no evidence" >&2
cat "$OUT_DIR/completeness.txt" 2>/dev/null || true

curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30
kill_campaign_processes "$OPS_PUB" || true
for f in q8-verdict.json q8-chaos.jsonl q8-progress.json q8-generator.log q8-verifier.log q8-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --profile "$PROFILE" > "$OUT_DIR/QUAL-08-summary.md"
cat "$OUT_DIR/QUAL-08-summary.md"

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh <rig-run-id> --yes && qualification/infra/teardown.sh --check"
