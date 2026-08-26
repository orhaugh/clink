#!/usr/bin/env bash
# QUAL-06: large job graphs at high parallelism.
#
# THE CLAIM. Clink deploys and runs a job graph of N operators as S
# network-bridged subtasks - the cluster runs every operator as its own
# subtask, so width multiplies bridge connections - with exactly-once
# output under the fault battery. The claim is made at the LARGEST GREEN
# RUNG of a progressive ladder; a rung that fails to deploy is a measured
# boundary, reported as such, not a campaign failure.
#
# THE LADDER. Each rung generates a wider SQL pipeline (dag-gen.py: B
# branches -> 6B+4 operators (the +4 gained a sink-boundary schema
# binding op with item 78), verified against the planner by
# test_dag_gen.py), deploys it, and must go green on deploy + first
# checkpoint + sink filling within its budget. Only the final green rung
# gets the full battery, the drain and the every-key oracle - exactness at
# the size where the claim is made, deploy evidence at every size below.
#
# THE TRAP THE GENERATOR ENCODES: replicated scans of one table share its
# consumer group, so branches must each read through their own table with
# their own group (B-fold broker read amplification, sized into RATE).
#
# Item-69 discipline: every rung's job is cancelled AND its HA manifest
# removed before the next rung submits - a cancelled job is otherwise
# resurrected by any coordinator restart and competes for slots.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID, e.g. qual06-20260824a}"
PROFILE="${PROFILE:-aggressive}"
RATE="${RATE:-300}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-5000}"
KEY_EPOCH_MS="${KEY_EPOCH_MS:-60000}"
SEED="${SEED:-20260824}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-15000}"
WM_LAG_MS="${WM_LAG_MS:-2000}"
STATE_TTL_MS="${STATE_TTL_MS:-600000}"
# "branches:parallelism" per rung, in ladder order. Subtasks = (6B+4) x par.
LADDER="${LADDER:-8:4 24:4 48:8}"
# Per-rung gate budgets and the final rung's battery length.
DEPLOY_TIMEOUT_S="${DEPLOY_TIMEOUT_S:-300}"
BATTERY_S="${BATTERY_S:-2700}"
MIN_GAP_S="${MIN_GAP_S:-120}"
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-300}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
RESTART_DRAIN_TIMEOUT_MS="${RESTART_DRAIN_TIMEOUT_MS:-300000}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
PGPASSWORD="${PGPASSWORD:-qual06-$(echo "$RUN_ID" | tr -cd 'a-zA-Z0-9')}"
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"

DURATION_S="${DURATION_S:-$BATTERY_S}"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-30}"
FINAL_WAIT_S="${FINAL_WAIT_S:-600}"
CATCHUP_TIMEOUT_S="${CATCHUP_TIMEOUT_S:-1800}"
CATCHUP_STALL_S="${CATCHUP_STALL_S:-900}"
RECOVER_PROBES="${RECOVER_PROBES:-20}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-120}"

OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/rung-*.txt "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" \
      "$OUT_DIR/oracle-dirty.txt" "$OUT_DIR/completeness.txt" "$OUT_DIR/catchup.txt" \
      "$OUT_DIR/final-quiesce.txt" "$OUT_DIR/state-series-steady.csv"

[ -x "$SUBMIT_BIN" ] || { echo "campaign: $SUBMIT_BIN is not executable; build it first" >&2; exit 78; }
if [ "$STATE_TTL_MS" -le "$KEY_EPOCH_MS" ] || [ "$STATE_TTL_MS" -le $(( CHECKPOINT_INTERVAL_MS * 4 )) ]; then
    echo "campaign: state_ttl must exceed the key epoch and 4x the checkpoint interval" >&2
    exit 78
fi
# The worst rung sizes CLINK_SLOTS for the whole run; a rung that still
# does not fit is recorded as the capacity boundary rather than parked.
MAX_SUBTASKS=0
for rung in $LADDER; do
    B=${rung%%:*}; P=${rung##*:}
    S=$(( (6 * B + 3) * P ))
    [ "$S" -gt "$MAX_SUBTASKS" ] && MAX_SUBTASKS=$S
done

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
collect_container_logs() {
    mkdir -p "$OUT_DIR/logs"
    on_host "$COORD_PUB" "docker logs --tail 200000 clink-coordinator 2>&1" \
        > "$OUT_DIR/logs/coordinator.log" 2>/dev/null || true
    local wi=0
    for wp in $WORKER_PUBS; do
        on_host "$wp" "docker logs --tail 200000 clink-worker 2>&1" \
            > "$OUT_DIR/logs/worker-$wi.log" 2>/dev/null || true
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
    # Chaos first: a controller left running keeps firing at whatever is
    # wedged, and the evidence then records faults nothing was judging.
    on_host "$OPS_PUB" "touch /qual/q6-chaos.jsonl.stop; pkill -INT -f '[c]haos.py'; true" || true
    collect_container_logs || true
    for j in "${JOB_ID:-}"; do
        [ -n "$j" ] && curl -fsS -X POST \
            "http://${COORD_PUB}:8095/api/v1/jobs/${j}/cancel" >/dev/null 2>&1 || true
    done
    echo "campaign: not entering the battery. Evidence in $OUT_DIR" >&2
    exit 4
}

echo "campaign: QUAL-06 run $RUN_ID, ladder [$LADDER], max subtasks $MAX_SUBTASKS, battery ${BATTERY_S}s"

# --- rig ----------------------------------------------------------------------
RIG_RUN_ID="${RIG_RUN_ID:-$RUN_ID}"
if [ "$RIG_RUN_ID" != "$RUN_ID" ]; then
    echo "campaign: running against the existing rig labelled qual-run=$RIG_RUN_ID"
    SKIP_PROVISION=1
fi
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
NWORKERS=$(echo "$WORKER_PUBS" | wc -w | tr -d ' ')
# Slots sized for the worst rung, with headroom for the coordinator's own
# placement maths; a rung above this records "capacity" and ends the ladder.
CLINK_SLOTS=$(( (MAX_SUBTASKS + NWORKERS - 1) / NWORKERS + 8 ))
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV slots/worker=$CLINK_SLOTS"

for h in $OPS_PUB $COORD_PUB $WORKER_PUBS $(read_inv broker public_ip); do
    until on_host "$h" "docker info >/dev/null 2>&1"; do
        echo "campaign: waiting for docker on $h"; sleep 15
    done
done
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h" >&2; exit 2; }
done
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID"
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done

if [ "${SKIP_IMAGE_PULL:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" IMAGE="$CLINK_IMAGE" "$HERE/../infra/pull-image.sh"
fi

# --- verification database -----------------------------------------------------
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
on_host "$OPS_PUB" "grep -q '^PGPASSWORD=' /qual/.env 2>/dev/null || echo 'PGPASSWORD=$PGPASSWORD' >> /qual/.env"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml down -v >/dev/null 2>&1; true"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml up -d"
pgready=0
until on_host "$OPS_PUB" "docker exec qual06-postgres pg_isready -U qual >/dev/null 2>&1"; do
    pgready=$(( pgready + 3 )); sleep 3
    [ "$pgready" -lt 180 ] || { echo "campaign: postgres never became ready" >&2; exit 2; }
done
CONNINFO="host=${OPS_PRIV} port=5432 dbname=qual user=qual password=${PGPASSWORD}"
DSN="host=127.0.0.1 port=5432 dbname=qual user=qual password=${PGPASSWORD}"
psql_q() { on_host "$OPS_PUB" "docker exec qual06-postgres psql -U qual -d qual -tAc \"$1\""; }
psql_q "DROP TABLE IF EXISTS public.q6_out; CREATE TABLE public.q6_out (k BIGINT PRIMARY KEY, n BIGINT)" >/dev/null

# --- stack ----------------------------------------------------------------------
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
    '$CLINK_IMAGE' '$COORD_PRIV' '$RESTART_DRAIN_TIMEOUT_MS' > /qual/.env && \
    docker compose -f coordinator.yml up -d"
wi=0
for wp in $WORKER_PUBS; do
    wpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$wp'][0])")
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=%s\nWORKER_IP=%s\nCLINK_SLOTS=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' 'w$wi' '$wpriv' '$CLINK_SLOTS' > /qual/.env && \
        docker compose -f worker.yml up -d --force-recreate"
    wi=$(( wi + 1 ))
done
sleep 20

# --- fault surface ---------------------------------------------------------------
CAPS=""
for _try in 1 2 3 4 5 6 7 8 9 10; do
    CAPS=$(on_host "$COORD_PUB" "docker exec clink-coordinator clink --capabilities-json 2>/dev/null" || echo "")
    [ -n "$CAPS" ] && break
    sleep 6
done
echo "$CAPS" > "$OUT_DIR/capabilities.json"
python3 -c "
import json,sys
try:
    d=json.load(open('$OUT_DIR/capabilities.json'))
except Exception:
    sys.exit('campaign: could not read the image capability manifest')
if not d.get('build',{}).get('fault_injection'):
    sys.exit('campaign: this image has NO fault injection; armed points would be silent no-ops')
" || exit 2

# --- topic + shared oracle machinery ---------------------------------------------
kill_campaign_processes "$OPS_PUB" || true
rpk_ops() {  # rpk against the broker, from the ops host
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
rpk_ops "topic delete qual06-in" >/dev/null 2>&1 || true
for _t in $(seq 1 30); do
    rpk_ops "topic list" 2>/dev/null | grep -q "qual06-in" || break
    sleep 2
done
REPL=$(python3 -c "print(min(3, len('$BROKER_PRIVS'.split())))")
rpk_ops "topic create qual06-in -p $PARTITIONS -r $REPL" >/dev/null

on_host "$OPS_PUB" "rm -f /qual/q6-*.stop /qual/q6-progress.json /qual/q6-verdict.json \
    /qual/q6-chaos.jsonl /qual/q6-generator.log /qual/q6-verifier.log /qual/q6-chaos.log"
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
start_on_host "$OPS_PUB" q6-generator.log \
    "python3 /qual/generator.py --brokers '$BROKER_LIST' --topic qual06-in \
     --rate $RATE --partitions $PARTITIONS --keys $KEYS --seed $SEED \
     --base-ms $BASE_MS --max-jitter-ms 0 --window-ms 10000 \
     --key-epoch-ms $KEY_EPOCH_MS --progress /qual/q6-progress.json"

# Recreate the control plane and workers with a wiped HA store. Needed
# whenever a job wedges (finding 73: RUNNING, uncheckpointing, cancel
# ignored) - it holds its slots and nothing else can deploy. Coordinator
# DOWN before the wipe, or its replacement resurrects the manifests
# (finding 69).
reset_stack() {
    echo "campaign: resetting the control plane (wedged or unkillable job)"
    on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml down >/dev/null 2>&1; \
        rm -rf /qual/ha/jobs /qual/ha/history; mkdir -p /qual/ha; \
        docker compose -f coordinator.yml up -d" >/dev/null 2>&1
    for wp in $WORKER_PUBS; do
        on_host "$wp" "cd /qual && docker compose -f worker.yml up -d --force-recreate" >/dev/null 2>&1
    done
    sleep 20
}

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
graph_ops() {  # job id -> deployed operator count, from the coordinator's graph
    curl -fsS --max-time 30 "http://${COORD_PUB}:8095/api/v1/jobs/$1/graph" 2>/dev/null | python3 -c "
import json,sys
try:
    print(len(json.load(sys.stdin).get('nodes', [])))
except Exception:
    print(-1)" 2>/dev/null || echo -1
}

# --- the ladder -----------------------------------------------------------------
RUNG=0
JOB_ID=""
FINAL_B=""; FINAL_P=""; FINAL_SUBTASKS=""
for rung in $LADDER; do
    B=${rung%%:*}; P=${rung##*:}
    RUNG=$(( RUNG + 1 ))
    # 6 ops per branch, then union tree glue, aggregate, sink - AND the
    # sink-boundary schema bind the planner has emitted in front of every
    # Row-channel sink since item 78 (2dd2e23). The formula lagged that by
    # one and failed a fully-green 150-subtask run for deploying "76 ops
    # against a claim of 75".
    EXPECTED_OPS=$(( 6 * B + 4 ))
    SUBTASKS=$(( EXPECTED_OPS * P ))
    RUNG_FILE="$OUT_DIR/rung-$RUNG.txt"
    echo "campaign: === rung $RUNG: B=$B par=$P -> $EXPECTED_OPS ops, $SUBTASKS subtasks ==="

    # A previous rung's job must be fully gone: cancelled, manifest removed
    # (item 69), and its slots released.
    if [ -n "$JOB_ID" ]; then
        curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
        for _t in $(seq 1 36); do
            case "$(job_status "$JOB_ID")" in RUNNING|RESTARTING|UNREACHABLE) sleep 5 ;; *) break ;; esac
        done
        # A job that ignores cancel (finding 73) still holds its slots; the
        # next rung would then fail on capacity and report the wrong thing.
        case "$(job_status "$JOB_ID")" in
            RUNNING|RESTARTING) reset_stack ;;
            *) on_host "$COORD_PUB" "rm -rf /qual/ha/jobs/${JOB_ID} 2>/dev/null; true" ;;
        esac
        psql_q "TRUNCATE public.q6_out" >/dev/null
    fi

    if [ "$SUBTASKS" -gt $(( CLINK_SLOTS * NWORKERS )) ]; then
        { echo "branches=$B"; echo "expected_ops=$EXPECTED_OPS"; echo "deployed_ops=-1";
          echo "subtasks=$SUBTASKS"; echo "parallelism=$P"; echo "deploy_s=-1";
          echo "first_checkpoint_s=-1"; echo "status=capacity";
          echo "reason=slots $(( CLINK_SLOTS * NWORKERS )) < subtasks $SUBTASKS";
        } > "$RUNG_FILE"
        echo "campaign: rung $RUNG exceeds capacity; ladder ends here"
        JOB_ID=""
        break
    fi

    python3 "$HERE/dag-gen.py" --branches "$B" --run-tag "$RUN_ID-L$RUNG" \
        | sed -e "s|__BROKERS__|$BROKER_LIST|g" \
              -e "s|__CONNINFO__|$CONNINFO|g" \
              -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
              -e "s|__STATE_TTL_MS__|$STATE_TTL_MS|g" \
        > "$OUT_DIR/pipeline-L$RUNG.sql"

    T0=$(date +%s)
    "$SUBMIT_BIN" --file "$OUT_DIR/pipeline-L$RUNG.sql" \
        --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
        --parallelism "$P" \
        --checkpoint-dir "/qual/state/$RUN_ID/L$RUNG" \
        --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
        --max-restarts-on-worker-loss "$MAX_RESTARTS" \
        > "$OUT_DIR/submit-L$RUNG.log" 2>&1 || true
    JOB_ID=$(python3 -c "
import json
jid=''
for line in open('$OUT_DIR/submit-L$RUNG.log'):
    line=line.strip()
    if line.startswith('{'):
        try:
            d=json.loads(line)
        except Exception:
            continue
        if d.get('ok') and d.get('job_id') is not None:
            jid=str(d['job_id'])
print(jid)")
    if [ -z "$JOB_ID" ]; then
        { echo "branches=$B"; echo "expected_ops=$EXPECTED_OPS"; echo "deployed_ops=-1";
          echo "subtasks=$SUBTASKS"; echo "parallelism=$P"; echo "deploy_s=-1";
          echo "first_checkpoint_s=-1"; echo "status=deploy-failed";
          echo "reason=submit refused: $(tail -1 "$OUT_DIR/submit-L$RUNG.log" | tr -d '\"')";
        } > "$RUNG_FILE"
        echo "campaign: rung $RUNG submit failed; ladder ends here"
        break
    fi

    # Deploy gate: RUNNING within the budget; first checkpoint after that.
    DEPLOY_S=-1; FIRST_CKPT_S=-1
    dw=0
    while [ "$dw" -lt "$DEPLOY_TIMEOUT_S" ]; do
        [ "$(job_status "$JOB_ID")" = "RUNNING" ] && { DEPLOY_S=$(( $(date +%s) - T0 )); break; }
        sleep 5; dw=$(( dw + 5 ))
    done
    if [ "$DEPLOY_S" -ge 0 ]; then
        cw=0
        while [ "$cw" -lt "$DEPLOY_TIMEOUT_S" ]; do
            [ "$(ckpt_id "$JOB_ID")" -ge 1 ] && { FIRST_CKPT_S=$(( $(date +%s) - T0 )); break; }
            sleep 5; cw=$(( cw + 5 ))
        done
    fi
    DEPLOYED_OPS=$(graph_ops "$JOB_ID")
    # The deployed TASK count comes from the coordinator, not from ops x
    # par arithmetic: source chains cap their parallelism at the topic's
    # partition count, so the arithmetic overstates width (rung 3 of the
    # first rig run claimed 2,328 by arithmetic and deployed 1,160).
    DEPLOYED_TASKS=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/$JOB_ID" 2>/dev/null | python3 -c "
import json,sys
try:
    print(int(json.load(sys.stdin).get('expected_completion') or 0))
except Exception:
    print(-1)" 2>/dev/null || echo -1)

    if [ "$DEPLOY_S" -lt 0 ] || [ "$FIRST_CKPT_S" -lt 0 ]; then
        { echo "branches=$B"; echo "expected_ops=$EXPECTED_OPS"; echo "deployed_ops=$DEPLOYED_OPS";
          echo "subtasks=$SUBTASKS"; echo "parallelism=$P"; echo "deploy_s=$DEPLOY_S";
          echo "first_checkpoint_s=$FIRST_CKPT_S"; echo "status=deploy-failed";
          echo "reason=no RUNNING or no first checkpoint within ${DEPLOY_TIMEOUT_S}s";
        } > "$RUNG_FILE"
        collect_container_logs
        echo "campaign: rung $RUNG did not go green; ladder ends here"
        curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
        on_host "$COORD_PUB" "rm -rf /qual/ha/jobs/${JOB_ID} 2>/dev/null; true"
        JOB_ID=""
        break
    fi

    # Sink filling: output is flowing through the full width.
    SINK_OK=no
    for _t in $(seq 1 24); do
        C=$(psql_q "SELECT count(*) FROM public.q6_out" | tr -d '\r')
        [ "${C:-0}" -gt 0 ] && { SINK_OK=yes; break; }
        sleep 10
    done
    if [ "$SINK_OK" != "yes" ]; then
        { echo "branches=$B"; echo "expected_ops=$EXPECTED_OPS"; echo "deployed_ops=$DEPLOYED_OPS";
          echo "subtasks=$SUBTASKS"; echo "parallelism=$P"; echo "deploy_s=$DEPLOY_S";
          echo "first_checkpoint_s=$FIRST_CKPT_S"; echo "status=deploy-failed";
          echo "reason=deployed but nothing reached the sink";
        } > "$RUNG_FILE"
        collect_container_logs
        curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
        on_host "$COORD_PUB" "rm -rf /qual/ha/jobs/${JOB_ID} 2>/dev/null; true"
        JOB_ID=""
        break
    fi

    # Recovery gate: the claim is "runs under faults", so a rung is only
    # green if it survives a controlled worker kill - checkpoint id and the
    # folded total must both advance past their pre-kill values. Finding 73
    # is exactly the failure this catches: deploys fine, wedges on loss.
    KILL_WORKER=$(echo "$WORKER_PUBS" | awk '{print $NF}')
    PRE_CKPT=$(ckpt_id "$JOB_ID")
    PRE_SUM=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q6_out" | tr -d '\r')
    T_KILL=$(date +%s)
    on_host "$KILL_WORKER" "docker kill -s SIGKILL clink-worker >/dev/null 2>&1; sleep 8; \
        cd /qual && docker compose -f worker.yml up -d" >/dev/null 2>&1
    RECOVERED=no; RECOVERY_S=-1
    rw=0
    while [ "$rw" -lt "${RECOVERY_GATE_S:-600}" ]; do
        sleep 15; rw=$(( rw + 15 ))
        CK=$(ckpt_id "$JOB_ID")
        SM=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q6_out" | tr -d '\r')
        if [ "$(job_status "$JOB_ID")" = "RUNNING" ] && \
           [ "${CK:-0}" -gt "${PRE_CKPT:-0}" ] && [ "${SM:-0}" -gt "${PRE_SUM:-0}" ]; then
            RECOVERED=yes; RECOVERY_S=$(( $(date +%s) - T_KILL )); break
        fi
    done
    if [ "$RECOVERED" != "yes" ]; then
        { echo "branches=$B"; echo "expected_ops=$EXPECTED_OPS"; echo "deployed_ops=$DEPLOYED_OPS";
          echo "subtasks=${DEPLOYED_TASKS:-$SUBTASKS}"; echo "parallelism=$P"; echo "deploy_s=$DEPLOY_S";
          echo "first_checkpoint_s=$FIRST_CKPT_S"; echo "status=recovery-failed";
          echo "reason=deployed and checkpointed, but did not recover a worker kill within ${RECOVERY_GATE_S:-600}s";
        } > "$RUNG_FILE"
        collect_container_logs
        echo "campaign: rung $RUNG failed RECOVERY; ladder ends here"
        # The failed job is very likely wedged (finding 73): reset rather
        # than trust cancel.
        reset_stack
        JOB_ID=""
        break
    fi
    { echo "branches=$B"; echo "expected_ops=$EXPECTED_OPS"; echo "deployed_ops=$DEPLOYED_OPS";
      echo "subtasks=${DEPLOYED_TASKS:-$SUBTASKS}"; echo "parallelism=$P"; echo "deploy_s=$DEPLOY_S";
      echo "first_checkpoint_s=$FIRST_CKPT_S"; echo "recovery_s=$RECOVERY_S";
      echo "status=green"; echo "reason=";
    } > "$RUNG_FILE"
    echo "campaign: rung $RUNG GREEN (deploy ${DEPLOY_S}s, first ckpt ${FIRST_CKPT_S}s, recovered a worker kill in ${RECOVERY_S}s)"
    FINAL_B=$B; FINAL_P=$P; FINAL_SUBTASKS=${DEPLOYED_TASKS:-$SUBTASKS}
done

# The ladder cancels each green rung before trying the next, so when a
# higher rung fails the largest green one is no longer running. The claim
# is made at the largest GREEN size, so redeploy that configuration for
# the battery - the boundary rung's record stands as the measured limit.
if [ -z "$JOB_ID" ] && [ -n "$FINAL_B" ]; then
    echo "campaign: redeploying the largest green rung (B=$FINAL_B par=$FINAL_P) as the claim job"
    # The boundary rung's job may be wedged and holding slots.
    reset_stack
    psql_q "TRUNCATE public.q6_out" >/dev/null
    python3 "$HERE/dag-gen.py" --branches "$FINAL_B" --run-tag "$RUN_ID-claim" \
        | sed -e "s|__BROKERS__|$BROKER_LIST|g" \
              -e "s|__CONNINFO__|$CONNINFO|g" \
              -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
              -e "s|__STATE_TTL_MS__|$STATE_TTL_MS|g" \
        > "$OUT_DIR/pipeline-claim.sql"
    "$SUBMIT_BIN" --file "$OUT_DIR/pipeline-claim.sql" \
        --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
        --parallelism "$FINAL_P" \
        --checkpoint-dir "/qual/state/$RUN_ID/claim" \
        --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
        --max-restarts-on-worker-loss "$MAX_RESTARTS" \
        > "$OUT_DIR/submit-claim.log" 2>&1 || true
    JOB_ID=$(python3 -c "
import json
jid=''
for line in open('$OUT_DIR/submit-claim.log'):
    line=line.strip()
    if line.startswith('{'):
        try:
            d=json.loads(line)
        except Exception:
            continue
        if d.get('ok') and d.get('job_id') is not None:
            jid=str(d['job_id'])
print(jid)")
    RUNG=claim
    dw=0
    while [ "$dw" -lt "$DEPLOY_TIMEOUT_S" ]; do
        [ "$(job_status "$JOB_ID")" = "RUNNING" ] && [ "$(ckpt_id "$JOB_ID")" -ge 1 ] && break
        sleep 5; dw=$(( dw + 5 ))
    done
    [ "$dw" -lt "$DEPLOY_TIMEOUT_S" ] || { echo "campaign: the claim redeploy never went green" >&2; JOB_ID=""; }
fi

[ -n "$JOB_ID" ] || { echo "campaign: no rung is green and running; summarising what was measured" >&2
    python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
        --profile "$PROFILE" > "$OUT_DIR/QUAL-06-summary.md"
    cat "$OUT_DIR/QUAL-06-summary.md"
    exit 4; }

echo "campaign: claim rung = B=$FINAL_B par=$FINAL_P ($FINAL_SUBTASKS subtasks); battery begins"

# --- the battery at the claim rung ---------------------------------------------
start_on_host "$OPS_PUB" q6-verifier.log \
    "python3 /qual/verifier.py --dsn '$DSN' --table public.q6_out \
     --progress /qual/q6-progress.json --out /qual/q6-verdict.json --interval-s 20"

Q6_POINTS=$(python3 -c "
import sys; sys.path.insert(0, '$HERE')
import summarise
print(','.join(summarise.TWOPC_POINTS))")
[ -n "$Q6_POINTS" ] || { echo "campaign: no 2PC points from the summariser" >&2; exit 78; }
start_on_host "$OPS_PUB" q6-chaos.log \
    "python3 /qual/chaos.py --inventory /qual/inventory.json --log /qual/q6-chaos.jsonl \
     --coordinator-url http://${COORD_PRIV}:8095 --job-id $JOB_ID --run-id $RUN_ID \
     --profile $PROFILE --seed $SEED --min-gap-s $MIN_GAP_S \
     --twopc-points '$Q6_POINTS' --recovery-timeout-s $RECOVERY_TIMEOUT_S \
     --duration-s $(( DURATION_S + 1800 )) --ensure-coverage"

FAULTED=no
for _t in $(seq 1 60); do
    N=$(on_host "$OPS_PUB" "wc -l < /qual/q6-chaos.jsonl 2>/dev/null || echo 0" | tr -d ' \r')
    [ "${N:-0}" -gt 0 ] && { FAULTED=yes; break; }
    sleep 20
done
[ "$FAULTED" = "yes" ] || verify_fail "the chaos controller applied no fault"
LOST=no
for _t in $(seq 1 30); do
    M=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/metrics" 2>/dev/null \
        | awk '/^clink_coordinator_workers_lost_total/{print $2}' | tail -1)
    case "${M:-0}" in ''|*[!0-9.]*) : ;; *) [ "${M%.*}" -ge 1 ] && { LOST=yes; break; } ;; esac
    sleep 5
done
[ "$LOST" = "yes" ] || verify_fail "a fault was recorded but the engine shows no worker loss"
BEFORE=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q6_out" | tr -d '\r')
RECOVERED=no
for _p in $(seq 1 "$RECOVER_PROBES"); do
    sleep 30
    S=$(job_status "$JOB_ID")
    AFTER=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q6_out" | tr -d '\r')
    if [ "$S" = "RUNNING" ] && [ "${AFTER:-0}" -gt "${BEFORE:-0}" ]; then RECOVERED=yes; break; fi
done
[ "$RECOVERED" = "yes" ] || verify_fail "the job did not resume making progress after the first fault"

{ echo "campaign=QUAL-06"; echo "run_id=$RUN_ID"; echo "job_id=$JOB_ID";
  echo "claim_branches=$FINAL_B"; echo "claim_parallelism=$FINAL_P";
  echo "claim_subtasks=$FINAL_SUBTASKS"; echo "clink_slots=$CLINK_SLOTS";
  echo "rate=$RATE"; echo "state_ttl_ms=$STATE_TTL_MS"; echo "key_epoch_ms=$KEY_EPOCH_MS";
} > "$OUT_DIR/verification.txt"

END=$(( $(date +%s) + DURATION_S ))
CHAOS_DIED_AT=""; JOB_GONE_AT=""; WATCH_LOOPS=0
SOAK_START=$(date +%s)
while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$WATCH_MAX_LOOPS" != "0" ] && [ "$WATCH_LOOPS" -ge "$WATCH_MAX_LOOPS" ]; then break; fi
    WATCH_LOOPS=$(( WATCH_LOOPS + 1 ))
    sleep "$SAMPLE_INTERVAL_S"
    B=$(on_host "$OPS_PUB" "python3 /qual/ckptsize.py --dir '/qual/state/$RUN_ID/'"$( [ "$RUNG" = claim ] && echo claim || echo L$RUNG )"" 2>/dev/null | tr -d '\r' || true)
    case "$B" in ''|*[!0-9]*) : ;; *)
        echo "$(( $(date +%s) - SOAK_START )),$B" >> "$OUT_DIR/state-series-steady.csv" ;;
    esac
    for f in q6-verdict.json q6-chaos.jsonl q6-progress.json q6-generator.log q6-verifier.log q6-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done
    DIRTY=$(python3 - "$OUT_DIR/q6-verdict.json" <<'PY' || echo ""
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
        on_host "$OPS_PUB" "touch /qual/q6-chaos.jsonl.stop; pkill -INT -f '[c]haos.py' || true"
        collect_container_logs
        break
    fi
    if [ -z "$CHAOS_DIED_AT" ] && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NF=$(on_host "$OPS_PUB" "wc -l < /qual/q6-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NF"; } > "$OUT_DIR/chaos-died.txt"
        echo "campaign: WARNING - the chaos controller is no longer running (${NF} faults)" >&2
    fi
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
            { echo "job_gone=yes"; echo "probes=$PROBES"; } > "$OUT_DIR/job-gone.txt"
            echo "campaign: WARNING - job ${JOB_ID} is no longer RUNNING (probes: ${PROBES})" >&2
        fi
    fi
    K=$(psql_q "SELECT count(*) FROM public.q6_out" 2>/dev/null | tr -d '\r' || echo '?')
    echo "campaign: $(date -u +%H:%M) battery $(( $(date +%s) - SOAK_START ))s, $K keys in the sink"
done

# --- drain and final judgement ---------------------------------------------------
echo "campaign: battery complete, draining"
on_host "$OPS_PUB" "touch /qual/q6-chaos.jsonl.stop"
cw=0
while [ "$cw" -lt 120 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5; cw=$(( cw + 5 ))
done
[ "$cw" -lt 120 ] || echo "campaign: WARNING - chaos still running 120s after its stop request" >&2

echo "campaign: waiting for the job to settle after the last fault"
settle=0
while [ "$settle" -lt 300 ]; do
    if [ "$(job_status "$JOB_ID")" = "RUNNING" ]; then
        C1=$(ckpt_id "$JOB_ID"); sleep 20; settle=$(( settle + 20 )); C2=$(ckpt_id "$JOB_ID")
        [ "${C2:-0}" -gt "${C1:-0}" ] && break
    else
        sleep 20; settle=$(( settle + 20 ))
    fi
done

on_host "$OPS_PUB" "touch /qual/q6-progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[g]enerator.py'; true"
gw=0
while [ "$gw" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5; gw=$(( gw + 5 ))
done

PRODUCED_FINAL=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q6-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
CATCHUP=no; cw=0; LAST=-1; STALL=0; FOLDED=0
while [ "$cw" -lt "$CATCHUP_TIMEOUT_S" ]; do
    FOLDED=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q6_out" | tr -d '\r')
    if [ "${FOLDED:-0}" -ge "${PRODUCED_FINAL:-1}" ]; then CATCHUP=yes; break; fi
    if [ "${FOLDED:-0}" -le "${LAST}" ]; then
        STALL=$(( STALL + 30 )); [ "$STALL" -ge "$CATCHUP_STALL_S" ] && break
    else STALL=0; fi
    LAST=${FOLDED:-0}
    echo "campaign: catch-up ${FOLDED:-0} / ${PRODUCED_FINAL} events"
    sleep 30; cw=$(( cw + 30 ))
done
{ echo "caught_up=$CATCHUP"; echo "produced_final=${PRODUCED_FINAL}";
  echo "folded_at_catchup=${FOLDED:-0}"; echo "catchup_seconds=$cw"; } > "$OUT_DIR/catchup.txt"

QUIESCED=no; w=0; prev=-1
while [ "$w" -lt "$FINAL_WAIT_S" ]; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/q6-verdict.json" "$OUT_DIR/" 2>/dev/null || true
    s=$(python3 - "$OUT_DIR/q6-verdict.json" <<'PY' || echo -1
import json, sys
try:
    v = json.load(open(sys.argv[1]))
    print((v.get("last_stats") or {}).get("sum_n", -1))
except Exception:
    print(-1)
PY
)
    if [ "${s:--1}" -ge 0 ] && [ "$s" = "$prev" ]; then QUIESCED=yes; break; fi
    prev="$s"; sleep 25; w=$(( w + 25 ))
done
echo "quiesced=$QUIESCED" > "$OUT_DIR/final-quiesce.txt"

on_host "$OPS_PUB" "python3 /qual/endstate.py --dsn '$DSN' --table public.q6_out \
    --progress /qual/q6-progress.json --seed $SEED --partitions $PARTITIONS \
    --keys $KEYS --eps $EPS --base-ms $BASE_MS --key-epoch-ms $KEY_EPOCH_MS" \
    > "$OUT_DIR/completeness.txt" \
    || echo "campaign: WARNING - the end-state pass failed; correctness has no evidence" >&2
cat "$OUT_DIR/completeness.txt" 2>/dev/null || true

curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30
kill_campaign_processes "$OPS_PUB" || true
for f in q6-verdict.json q6-chaos.jsonl q6-progress.json q6-generator.log q6-verifier.log q6-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --profile "$PROFILE" > "$OUT_DIR/QUAL-06-summary.md"
cat "$OUT_DIR/QUAL-06-summary.md"
echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh <rig-run-id> --yes && qualification/infra/teardown.sh --check"
