#!/usr/bin/env bash
# QUAL-05: state TTL steady state.
#
# THE CLAIM. A job whose key space keeps turning over, with a `state_ttl`
# declared, holds BOUNDED state: it reaches a plateau and stays there for
# the rest of the run, while its output stays exactly correct, and it does
# so across the fault battery.
#
# WHY THIS CAMPAIGN NEEDED AN INSTRUMENT BEFORE IT COULD BE WRITTEN. The
# 2026-08-15 feasibility audit refused to schedule it: retention had no
# statistic anyone outside the engine could read, so the campaign could not
# have failed, which is worse than not running it. Two things closed that:
#
#   * the size instrument (ckptsize.py) sizes ONE checkpoint written to the
#     shared mount, so state is measured from OUTSIDE the engine, from the
#     artefacts it writes for its own recovery rather than from anything it
#     says about itself;
#   * the retention gauges the engine now exports corroborate it, and gate
#     the run fast if retention never released anything at all.
#
# THE CONTROL ARM IS THE POINT. A flat line proves nothing on its own: a
# workload that never accumulated state is also flat. So the campaign runs
# the IDENTICAL workload twice - once with retention removed and ALLOW
# UNBOUNDED STATE in its place, once with the TTL declared - and the
# summariser refuses a PASS unless the control arm actually grew. That is
# the difference between measuring retention and measuring nothing.
#
# ORDER: control first and short, subject second and long. The subject
# reads the topic from the beginning, so it folds the control window's
# events too and the exact-accounting check still covers every event the
# generator produced. Separate consumer groups, separate checkpoint
# directories, and the sink table is truncated between them.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID, e.g. qual05-20260823a}"
DURATION_H="${DURATION_H:-1.5}"
PROFILE="${PROFILE:-aggressive}"
RATE="${RATE:-1000}"
PARTITIONS="${PARTITIONS:-4}"
# Keys PER EPOCH. The generator's key space advances with event time, so
# the total number of distinct keys grows without bound; this is the width
# of each epoch's disjoint block.
KEYS="${KEYS:-5000}"
KEY_EPOCH_MS="${KEY_EPOCH_MS:-60000}"
SEED="${SEED:-20260823}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-15000}"
WM_LAG_MS="${WM_LAG_MS:-2000}"

# Retention, in milliseconds (parse_retention_ms reads a bare integer as
# ms). It has to exceed three things, and the campaign asserts the first
# two below rather than trusting them:
#   1. the key epoch, or a key's aggregate would be truncated mid-life and
#      the exact-accounting check would fail for a legitimate reason;
#   2. the worst replay lag (checkpoint interval + recovery), or a fault
#      could replay an event whose id DISTINCT had already released, and it
#      would be folded twice;
#   3. long enough that the plateau is reached well inside the soak.
STATE_TTL_MS="${STATE_TTL_MS:-600000}"

# The steady-state window opens once the population has had time to reach
# equilibrium: one full TTL plus one epoch is when the oldest live key
# first ages out, plus a margin for the initial catch-up.
WARMUP_S="${WARMUP_S:-$(( STATE_TTL_MS / 1000 + KEY_EPOCH_MS / 1000 + 300 ))}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-120}"
# The control arm. Long enough to show unmistakable growth; every second
# of it is spent proving the subject arm's flat line means something.
CONTROL_S="${CONTROL_S:-900}"

MIN_GAP_S="${MIN_GAP_S:-120}"
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-300}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
RESTART_DRAIN_TIMEOUT_MS="${RESTART_DRAIN_TIMEOUT_MS:-300000}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
PGPASSWORD="${PGPASSWORD:-qual05-$(echo "$RUN_ID" | tr -cd 'a-zA-Z0-9')}"
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"

# Simulator hooks: the watch loop is where two defects hid in QUAL-01
# because DURATION_H=0 skipped it entirely.
DURATION_S="${DURATION_S:-$(python3 -c "print(int(float('$DURATION_H') * 3600))")}"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-30}"
FINAL_WAIT_S="${FINAL_WAIT_S:-600}"
CATCHUP_TIMEOUT_S="${CATCHUP_TIMEOUT_S:-1800}"
CATCHUP_STALL_S="${CATCHUP_STALL_S:-900}"
RECOVER_PROBES="${RECOVER_PROBES:-20}"

OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
mkdir -p "$OUT_DIR"
# A reused RUN_ID must not inherit a previous attempt's verdicts.
rm -f "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" "$OUT_DIR/oracle-dirty.txt" \
      "$OUT_DIR/completeness.txt" "$OUT_DIR/catchup.txt" "$OUT_DIR/final-quiesce.txt" \
      "$OUT_DIR/control.txt" "$OUT_DIR/retention.txt" \
      "$OUT_DIR/state-series-steady.csv" "$OUT_DIR/state-series-all.csv"

[ -x "$SUBMIT_BIN" ] || { echo "campaign: $SUBMIT_BIN is not executable; build it first" >&2; exit 78; }

# The retention premise, asserted rather than assumed (see STATE_TTL_MS).
if [ "$STATE_TTL_MS" -le "$KEY_EPOCH_MS" ]; then
    echo "campaign: state_ttl (${STATE_TTL_MS}ms) must exceed the key epoch (${KEY_EPOCH_MS}ms)," >&2
    echo "  or a key's aggregate is truncated mid-life and exact accounting fails for a" >&2
    echo "  reason that is the workload's fault rather than the engine's." >&2
    exit 78
fi
if [ "$STATE_TTL_MS" -le $(( CHECKPOINT_INTERVAL_MS * 4 )) ]; then
    echo "campaign: state_ttl (${STATE_TTL_MS}ms) is not comfortably above the replay lag" >&2
    echo "  (checkpoint interval ${CHECKPOINT_INTERVAL_MS}ms); a replayed event whose id" >&2
    echo "  DISTINCT had already released would be folded twice." >&2
    exit 78
fi

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
          -o ConnectTimeout=15 -o ServerAliveInterval=15 -o ServerAliveCountMax=4
          -i "$KEY_FILE")

# ssh, retried on TRANSPORT failure only. See qual04/campaign.sh: one
# "Connection reset by peer" killed a rig run twenty minutes in.
on_host() {
    local host=$1 cmd=$2 attempt=1 rc=0
    while : ; do
        # `cmd && rc=0 || rc=$?`, not a bare call: under set -e a bare
        # failing command exits before rc can be captured.
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
        > "$OUT_DIR/logs/coordinator.log" 2>/dev/null \
        || echo "campaign: WARNING - could not retain the coordinator container log" >&2
    local wi=0
    for wp in $WORKER_PUBS; do
        on_host "$wp" "docker logs --tail 200000 clink-worker 2>&1" \
            > "$OUT_DIR/logs/worker-$wi.log" 2>/dev/null \
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
    collect_container_logs || true
    # Cancel what this run started. A campaign that exits at a gate and
    # leaves its job behind hands the next run a crashlooping job holding
    # its slots, and the next failure looks like a different defect.
    for j in "${CONTROL_JOB:-}" "${JOB_ID:-}"; do
        [ -n "$j" ] && curl -fsS -X POST \
            "http://${COORD_PUB}:8095/api/v1/jobs/${j}/cancel" >/dev/null 2>&1 || true
    done
    echo "campaign: not entering soak. Evidence in $OUT_DIR" >&2
    exit 4
}

echo "campaign: QUAL-05 run $RUN_ID, ${DURATION_H}h soak, profile=$PROFILE"
echo "campaign: ttl=${STATE_TTL_MS}ms key_epoch=${KEY_EPOCH_MS}ms rate=${RATE}/s warmup=${WARMUP_S}s"

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

# Shared checkpoint state. Keyed state IS the subject here, and it is
# per-subtask on each node's own filesystem: on local disks a killed
# worker's subtask redeployed elsewhere finds nothing, and the campaign
# would measure failed restores rather than retention.
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h - refusing to run a" >&2
             echo "  worker-kill campaign on per-host local state." >&2; exit 2; }
done
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID/control /qual/state/$RUN_ID/subject"
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done
# One directory per ARM. Sharing one would conflate the control's
# unbounded growth with the subject's plateau in the same measurement.
CONTROL_CKPT_DIR="/qual/state/$RUN_ID/control"
SUBJECT_CKPT_DIR="/qual/state/$RUN_ID/subject"
echo "campaign: shared state verified; control=$CONTROL_CKPT_DIR subject=$SUBJECT_CKPT_DIR"

if [ "${SKIP_IMAGE_PULL:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" IMAGE="$CLINK_IMAGE" "$HERE/../infra/pull-image.sh"
fi

# --- verification database -------------------------------------------------
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
on_host "$OPS_PUB" "grep -q '^PGPASSWORD=' /qual/.env 2>/dev/null || echo 'PGPASSWORD=$PGPASSWORD' >> /qual/.env"
# down -v FIRST, and it is not tidiness: POSTGRES_PASSWORD only applies to
# an UNINITIALISED data directory, so a volume left by an earlier run keeps
# that run's password and every sink connection fails authentication. The
# job then crashloops with the coordinator restarting it for ever, the
# state curve stays flat because nothing accumulates, and the campaign
# reads it as "retention worked". Exactly the MinIO trap QUAL-04 hit.
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml down -v >/dev/null 2>&1; true"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml up -d"
pgready=0
until on_host "$OPS_PUB" "docker exec qual05-postgres pg_isready -U qual >/dev/null 2>&1"; do
    pgready=$(( pgready + 3 )); sleep 3
    [ "$pgready" -lt 180 ] || { echo "campaign: postgres never became ready" >&2; exit 2; }
done
CONNINFO="host=${OPS_PRIV} port=5432 dbname=qual user=qual password=${PGPASSWORD}"
DSN="host=127.0.0.1 port=5432 dbname=qual user=qual password=${PGPASSWORD}"
psql_q() { on_host "$OPS_PUB" "docker exec qual05-postgres psql -U qual -d qual -tAc \"$1\""; }
for arm in control subject; do
    psql_q "DROP TABLE IF EXISTS public.q5_out_$arm; \
            CREATE TABLE public.q5_out_$arm (k BIGINT PRIMARY KEY, n BIGINT)" >/dev/null
done

# --- stack ------------------------------------------------------------------
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
# --force-recreate below, and the HA wipe beside it, are what make a run
# independent of the last one. `up -d` leaves an already-running container
# alone, so on a reused rig the previous run's coordinator survives with
# its jobs still in memory: a campaign that exited at a gate leaves its job
# crashlooping, and it holds the slots the next run needs. That is how the
# third local run got "the control arm never completed a checkpoint" - 12
# free slots against the 20 it needed, because a dead run's job still owned
# the rest.
to_host "$COORD_PUB" "$HERE/../infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "rm -rf /qual/ha/jobs /qual/ha/history; mkdir -p /qual/ha"
on_host "$COORD_PUB" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nRESTART_DRAIN_TIMEOUT_MS=%s\n' \
    '$CLINK_IMAGE' '$COORD_PRIV' '$RESTART_DRAIN_TIMEOUT_MS' > /qual/.env && \
    docker compose -f coordinator.yml up -d --force-recreate"
wi=0
for wp in $WORKER_PUBS; do
    wpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$wp'][0])")
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=%s\nWORKER_IP=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' 'w$wi' '$wpriv' > /qual/.env && \
        docker compose -f worker.yml up -d --force-recreate"
    wi=$(( wi + 1 ))
done
sleep 20

# --- fault surface ----------------------------------------------------------
# Without fault injection compiled in, every armed protocol point is a
# silent no-op and the campaign reports surviving faults it never applied.
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

# --- topic ------------------------------------------------------------------
kill_campaign_processes "$OPS_PUB" || true
# --brokers is a PER-COMMAND flag in rpk v24, not a global one: putting it
# before the subcommand gets "unknown flag: --brokers" and a usage dump.
rpk_ops() {  # rpk against the broker, from the ops host
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
rpk_ops "topic delete qual05-in" >/dev/null 2>&1 || true
for _t in $(seq 1 30); do
    rpk_ops "topic list" 2>/dev/null | grep -q "qual05-in" || break
    sleep 2
done
REPL=$(python3 -c "print(min(3, len('$BROKER_PRIVS'.split())))")
rpk_ops "topic create qual05-in -p $PARTITIONS -r $REPL" >/dev/null

# --- ops-host processes ------------------------------------------------------
on_host "$OPS_PUB" "rm -f /qual/q5-*.stop /qual/q5-progress.json /qual/q5-verdict.json"
for f in "$HERE/../qual01/detspec.py" "$HERE/../qual01/generator.py" \
         "$HERE/verifier.py" "$HERE/ckptsize.py" "$HERE/endstate.py" \
         "$HERE/../chaos/chaos.py"; do
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

start_on_host "$OPS_PUB" q5-generator.log \
    "python3 /qual/generator.py --brokers '$BROKER_LIST' --topic qual05-in \
     --rate $RATE --partitions $PARTITIONS --keys $KEYS --seed $SEED \
     --base-ms $BASE_MS --max-jitter-ms 0 --window-ms 10000 \
     --key-epoch-ms $KEY_EPOCH_MS --progress /qual/q5-progress.json"

# --- pipeline rendering -------------------------------------------------------
render_pipeline() {  # arm, out-path
    local arm=$1 out=$2 retention="" unbounded=""
    if [ "$arm" = "subject" ]; then
        retention=", state_ttl='${STATE_TTL_MS}', state_ttl_domain='event_time'"
    else
        unbounded=" ALLOW UNBOUNDED STATE"
    fi
    sed -e "s|__BROKERS__|$BROKER_LIST|g" \
        -e "s|__CONNINFO__|$CONNINFO|g" \
        -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
        -e "s|__GROUP__|qual05-$arm|g" \
        -e "s|__SINK_TABLE__|q5_out_$arm|g" \
        -e "s|__RETENTION__|$retention|g" \
        -e "s|__UNBOUNDED__|$unbounded|g" \
        "$HERE/pipeline.sql.tmpl" > "$out"
}

submit_arm() {  # arm, checkpoint-dir -> echoes the job id
    local arm=$1 ckpt=$2
    render_pipeline "$arm" "$OUT_DIR/pipeline-$arm.sql"
    "$SUBMIT_BIN" --file "$OUT_DIR/pipeline-$arm.sql" \
        --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
        --parallelism "$PARTITIONS" \
        --checkpoint-dir "$ckpt" \
        --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
        --max-restarts-on-worker-loss "$MAX_RESTARTS" \
        > "$OUT_DIR/submit-$arm.log" 2>&1 || true
    python3 -c "
import json,sys
jid=''
for line in open('$OUT_DIR/submit-$arm.log'):
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

# UNREACHABLE is deliberately distinct from GONE. Chaos restarts the
# coordinator on purpose, and while it is down curl fails - which is not
# evidence about the JOB at all. Conflating them latched "the job
# disappeared" on a local run whose only crime was a coordinator kill
# landing inside the probe window, and that latch is a FAIL.
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

state_bytes() {  # checkpoint dir
    on_host "$OPS_PUB" "python3 /qual/ckptsize.py --dir '$1'" 2>/dev/null | tr -d '\r'
}
state_detail() {  # checkpoint dir
    on_host "$OPS_PUB" "python3 /qual/ckptsize.py --dir '$1' --detail" 2>/dev/null | tr -d '\r'
}

# Retention as the ENGINE sees it, summed over the workers. Corroboration
# only: the pass criterion is the external measurement above. What this
# answers that the size curve cannot is whether retention is the reason
# the curve is flat.
retention_totals() {
    # Scraped from each worker's own /metrics over ssh rather than through
    # the coordinator's proxy: the proxy needs a registered http_port and
    # returns nothing for a worker mid-restart, and a retention gate that
    # reads 0 because it could not reach anything is a gate that fails the
    # engine for the harness's mistake.
    local expired=0 tracked=0
    for wp in $WORKER_PUBS; do
        local body e t
        body=$(on_host "$wp" "curl -fsS --max-time 10 http://127.0.0.1:8082/metrics" 2>/dev/null || true)
        e=$(echo "$body" | awk '/^clink_state_ttl_expired_total\{/{s+=$2} END{printf "%d", s+0}')
        t=$(echo "$body" | awk '/^clink_state_ttl_tracked_keys\{/{s+=$2} END{printf "%d", s+0}')
        expired=$(( expired + ${e:-0} ))
        tracked=$(( tracked + ${t:-0} ))
    done
    echo "$expired $tracked"
}

# =============================================================================
# CONTROL ARM - the identical workload with retention removed.
# =============================================================================
echo "campaign: === control arm (${CONTROL_S}s, ALLOW UNBOUNDED STATE) ==="
CONTROL_JOB=$(submit_arm control "$CONTROL_CKPT_DIR")
[ -n "$CONTROL_JOB" ] || { echo "campaign: control arm did not submit" >&2
                           tail -20 "$OUT_DIR/submit-control.log" >&2; exit 1; }
echo "campaign: control job $CONTROL_JOB"

# The control arm used to be measured blind. A job crashlooping on its sink
# is reported RUNNING by the coordinator while restarting for ever, holds
# no state, and produces a perfectly flat curve - which is indistinguishable
# from a control that legitimately did not grow, and wastes the run either
# way. Completed checkpoints, not status, is what says it is working.
echo "campaign: waiting for the control arm to complete a checkpoint"
CONTROL_HEALTHY=no
for _t in $(seq 1 30); do
    CJ=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/${CONTROL_JOB}" 2>/dev/null || echo "")
    # latest_completed_checkpoint_id, NOT completed_count: the latter
    # counts SUBTASKS that have finished, so on a streaming job it is zero
    # for ever and this gate would fail a perfectly healthy run. It did,
    # twice, on the local rig.
    CC=$(echo "$CJ" | python3 -c "
import json,sys
try:
    print(int(json.load(sys.stdin).get('latest_completed_checkpoint_id') or 0))
except Exception:
    print(0)" 2>/dev/null || echo 0)
    [ "${CC:-0}" -ge 1 ] && { CONTROL_HEALTHY=yes; break; }
    sleep 10
done
[ "$CONTROL_HEALTHY" = "yes" ] \
    || verify_fail "the control arm never completed a checkpoint; it is not processing"

CONTROL_FIRST=""
CONTROL_LAST=""
cstart=$(date +%s)
celapsed=0
while [ "$celapsed" -lt "$CONTROL_S" ]; do
    sleep "$SAMPLE_INTERVAL_S"
    celapsed=$(( $(date +%s) - cstart ))
    B=$(state_bytes "$CONTROL_CKPT_DIR" || echo "")
    case "$B" in
        ''|*[!0-9]*) echo "campaign: control sample at ${celapsed}s: state not measurable yet"; continue ;;
    esac
    [ -z "$CONTROL_FIRST" ] && CONTROL_FIRST="$B"
    CONTROL_LAST="$B"
    echo "campaign: control ${celapsed}s: $(( B / 1024 / 1024 )) MiB"
done
CSTATUS=$(job_status "$CONTROL_JOB")
curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${CONTROL_JOB}/cancel" >/dev/null 2>&1 || true
# Wait for it to actually stop, not just to have been asked. A cancelled
# job still draining can still write into the sink the subject is about to
# be judged on, and it still holds slots the subject needs.
cstop=0
while [ "$cstop" -lt 180 ]; do
    CS=$(job_status "$CONTROL_JOB")
    case "$CS" in RUNNING|RESTARTING|UNKNOWN) : ;; *) break ;; esac
    sleep 5; cstop=$(( cstop + 5 ))
done
[ "$cstop" -lt 180 ] \
    || echo "campaign: WARNING - the control arm was still live 180s after cancel" >&2
{ echo "control_first_bytes=${CONTROL_FIRST:-0}"
  echo "control_last_bytes=${CONTROL_LAST:-0}"
  echo "control_window_s=$celapsed"
  echo "control_final_status=$CSTATUS"
  echo "control_job_id=$CONTROL_JOB"
} > "$OUT_DIR/control.txt"
cat "$OUT_DIR/control.txt"
if [ -z "$CONTROL_FIRST" ] || [ "${CONTROL_LAST:-0}" -le "${CONTROL_FIRST:-0}" ]; then
    echo "campaign: WARNING - the control arm did not grow; the subject's plateau will not" >&2
    echo "  be able to prove anything and the summary will read INCONCLUSIVE." >&2
fi
# Remove the control's HA manifest. Cancelling a job does not record the
# cancellation in the HA store, so the next coordinator restart - and this
# campaign's chaos restarts it repeatedly - recovers and re-runs it
# (followups item 69). The separate sink tables mean a resurrected control
# can no longer corrupt the subject's evidence; this stops it competing for
# slots as well.
on_host "$COORD_PUB" "rm -rf /qual/ha/jobs/${CONTROL_JOB} 2>/dev/null; true"

# =============================================================================
# SUBJECT ARM - the same workload with a declared state_ttl.
# =============================================================================
echo "campaign: === subject arm (state_ttl=${STATE_TTL_MS}ms) ==="
JOB_ID=$(submit_arm subject "$SUBJECT_CKPT_DIR")
[ -n "$JOB_ID" ] || { echo "campaign: subject arm did not submit" >&2
                      tail -20 "$OUT_DIR/submit-subject.log" >&2; exit 1; }
echo "campaign: subject job $JOB_ID"
curl -fsS "http://${COORD_PUB}:8095/api/v1/connectors" > "$OUT_DIR/cluster-connectors.json" 2>/dev/null || true

start_on_host "$OPS_PUB" q5-verifier.log \
    "python3 /qual/verifier.py --dsn '$DSN' --table public.q5_out_subject \
     --progress /qual/q5-progress.json --out /qual/q5-verdict.json --interval-s 20"

# --- functional verification: nothing soaks until all of this is proven ------
echo "campaign: functional verification"
P1=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q5-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 45
P2=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q5-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
[ "${P2:-0}" -gt "${P1:-0}" ] || verify_fail "no input is flowing (progress ${P1} -> ${P2})"

# Polled, not single-shot: a job seconds away from its first checkpoint is
# healthy. The field is latest_completed_checkpoint_id; completed_count is
# finished SUBTASKS and stays zero for ever on a streaming job.
JOB_HEALTHY=no
for _t in $(seq 1 30); do
    curl -fsS --max-time 30 "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" \
        > "$OUT_DIR/job-status.json" 2>/dev/null || true
    if python3 -c "
import json,sys
try:
    d=json.load(open('$OUT_DIR/job-status.json'))
except Exception:
    sys.exit(1)
ok = (d.get('status') == 'RUNNING'
      and int(d.get('latest_completed_checkpoint_id') or 0) >= 1
      and not d.get('errors'))
sys.exit(0 if ok else 1)
"; then JOB_HEALTHY=yes; break; fi
    sleep 10
done
[ "$JOB_HEALTHY" = "yes" ] \
    || verify_fail "the job never reached RUNNING with a completed checkpoint"

# Exactly one LIVE job. The zombie-twin check qual01 run e paid for, but
# counting every job would count this campaign's own finished control arm,
# which stays in the listing as CANCELLED. A terminal job writes nothing.
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
    C=$(psql_q "SELECT count(*) FROM public.q5_out_subject" | tr -d '\r')
    [ "${C:-0}" -gt 0 ] && { SINK_OK=yes; break; }
    sleep 15
done
[ "$SINK_OK" = "yes" ] || verify_fail "nothing reached the sink table"

# The instrument itself has to work before the campaign depends on it.
STATE_OK=no
for _t in $(seq 1 20); do
    B=$(state_bytes "$SUBJECT_CKPT_DIR" || echo "")
    case "$B" in
        ''|*[!0-9]*) sleep 15; continue ;;
    esac
    [ "$B" -gt 0 ] && { STATE_OK=yes; break; }
    sleep 15
done
[ "$STATE_OK" = "yes" ] || verify_fail "the state instrument cannot measure the subject's checkpoints"

# Retention must be ENGAGED, not merely declared. Zero released keys after
# a full TTL of event time means the bound the gate accepted is not being
# applied, which is the exact defect this campaign exists to catch.
echo "campaign: waiting for retention to release its first keys"
RET_OK=no
rwait=0
while [ "$rwait" -lt $(( STATE_TTL_MS / 1000 + KEY_EPOCH_MS / 1000 + 600 )) ]; do
    read -r REXP RTRK <<<"$(retention_totals)"
    if [ "${REXP:-0}" -gt 0 ]; then
        RET_OK=yes
        echo "campaign: retention engaged (${REXP} keys released, ${RTRK} tracked)"
        break
    fi
    sleep 30
    rwait=$(( rwait + 30 ))
done
[ "$RET_OK" = "yes" ] || verify_fail "retention released no keys at all; the declared state_ttl is not being applied"

# --- warm-up to the plateau, then chaos --------------------------------------
echo "campaign: warm-up to equilibrium (${WARMUP_S}s from submit)"
wstart=$(date +%s)
: > "$OUT_DIR/state-series-all.csv"
while [ $(( $(date +%s) - wstart )) -lt "$WARMUP_S" ]; do
    sleep "$SAMPLE_INTERVAL_S"
    B=$(state_bytes "$SUBJECT_CKPT_DIR" || echo "")
    case "$B" in ''|*[!0-9]*) continue ;; esac
    echo "$(( $(date +%s) - wstart )),$B" >> "$OUT_DIR/state-series-all.csv"
    echo "campaign: warm-up $(( ($(date +%s) - wstart) / 60 ))m: $(( B / 1024 / 1024 )) MiB"
done

Q5_POINTS=$(python3 -c "
import sys; sys.path.insert(0, '$HERE')
import summarise
print(','.join(summarise.TWOPC_POINTS))")
[ -n "$Q5_POINTS" ] || { echo "campaign: no 2PC points from the summariser" >&2; exit 78; }

start_on_host "$OPS_PUB" q5-chaos.log \
    "python3 /qual/chaos.py --inventory /qual/inventory.json --log /qual/q5-chaos.jsonl \
     --coordinator-url http://${COORD_PRIV}:8095 --job-id $JOB_ID --run-id $RUN_ID \
     --profile $PROFILE --seed $SEED --min-gap-s $MIN_GAP_S \
     --twopc-points '$Q5_POINTS' --recovery-timeout-s $RECOVERY_TIMEOUT_S \
     --duration-s $(( DURATION_S + 1800 )) --ensure-coverage"

FAULTED=no
for _t in $(seq 1 60); do
    N=$(on_host "$OPS_PUB" "wc -l < /qual/q5-chaos.jsonl 2>/dev/null || echo 0" | tr -d ' \r')
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

BEFORE=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q5_out_subject" | tr -d '\r')
RECOVERED=no
for _p in $(seq 1 "$RECOVER_PROBES"); do
    sleep 30
    S=$(job_status "$JOB_ID")
    AFTER=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q5_out_subject" | tr -d '\r')
    if [ "$S" = "RUNNING" ] && [ "${AFTER:-0}" -gt "${BEFORE:-0}" ]; then RECOVERED=yes; break; fi
done
[ "$RECOVERED" = "yes" ] || verify_fail "the job did not resume making progress after the first fault"

read -r REXP RTRK <<<"$(retention_totals)"
{ echo "campaign=QUAL-05"; echo "run_id=$RUN_ID"; echo "job_id=$JOB_ID";
  echo "state_ttl_ms=$STATE_TTL_MS"; echo "key_epoch_ms=$KEY_EPOCH_MS";
  echo "rate=$RATE"; echo "partitions=$PARTITIONS"; echo "keys_per_epoch=$KEYS";
  echo "checkpoint_interval_ms=$CHECKPOINT_INTERVAL_MS";
  echo "warmup_s=$WARMUP_S";
  echo "predicted_live_distinct_entries=$(( RATE * (STATE_TTL_MS + KEY_EPOCH_MS) / 1000 ))";
  echo "predicted_live_groups=$(( KEYS * (STATE_TTL_MS + KEY_EPOCH_MS) / KEY_EPOCH_MS ))";
  echo "retention_expired_at_gate=${REXP:-0}"; echo "retention_tracked_at_gate=${RTRK:-0}";
} > "$OUT_DIR/verification.txt"
cat "$OUT_DIR/verification.txt"

# --- soak: steady-state sampling + the standard watch latches ----------------
echo "campaign: === steady state (${DURATION_S}s) ==="
: > "$OUT_DIR/state-series-steady.csv"
SOAK_START=$(date +%s)
END=$(( SOAK_START + DURATION_S ))
CHAOS_DIED_AT=""
JOB_GONE_AT=""
WATCH_LOOPS=0
while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$WATCH_MAX_LOOPS" != "0" ] && [ "$WATCH_LOOPS" -ge "$WATCH_MAX_LOOPS" ]; then break; fi
    WATCH_LOOPS=$(( WATCH_LOOPS + 1 ))
    sleep "$SAMPLE_INTERVAL_S"
    for f in q5-verdict.json q5-chaos.jsonl q5-progress.json q5-generator.log q5-verifier.log q5-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    DIRTY=$(python3 - "$OUT_DIR/q5-verdict.json" <<'PY' || echo ""
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
        on_host "$OPS_PUB" "touch /qual/q5-chaos.jsonl.stop"
        on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py' || true"
        collect_container_logs
        break
    fi

    if [ -z "$CHAOS_DIED_AT" ] && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q5-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        echo "campaign: WARNING - the chaos controller is no longer running (${NFAULTS} faults)" >&2
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NFAULTS"; } > "$OUT_DIR/chaos-died.txt"
    fi

    # The state sample comes FIRST. It is the campaign's pass criterion, and
    # putting it after the latches meant a loop that took a slow path
    # contributed no sample at all: one run produced a single steady-state
    # point across five minutes and could not be judged.
    B=$(state_bytes "$SUBJECT_CKPT_DIR" || echo "")
    case "$B" in
        ''|*[!0-9]*) echo "campaign: state not measurable this tick" ;;
        *)
            T=$(( $(date +%s) - SOAK_START ))
            echo "$T,$B" >> "$OUT_DIR/state-series-steady.csv"
            echo "$(( T + WARMUP_S )),$B" >> "$OUT_DIR/state-series-all.csv"
            K=$(psql_q "SELECT count(*) FROM public.q5_out_subject" 2>/dev/null | tr -d '\r' || echo '?')
            echo "campaign: $(date -u +%H:%M) steady ${T}s: $(( B / 1024 / 1024 )) MiB state, $K keys"
            ;;
    esac

    if [ -z "$JOB_GONE_AT" ]; then
        NOT_RUNNING=0; PROBES=""
        for _probe in 1 2 3 4 5 6; do
            ALIVE=$(job_status "$JOB_ID")
            PROBES="${PROBES}${ALIVE} "
            if [ "$ALIVE" = "RUNNING" ]; then NOT_RUNNING=0; break; fi
            # An unanswered probe says nothing about the job: the
            # coordinator is being restarted by this campaign's own chaos.
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
echo "campaign: soak complete, draining"
on_host "$OPS_PUB" "touch /qual/q5-chaos.jsonl.stop"
cwaited=0
while [ "$cwaited" -lt 120 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5; cwaited=$(( cwaited + 5 ))
done
[ "$cwaited" -lt 120 ] \
    || echo "campaign: WARNING - the chaos controller is still running 120s after its stop request" >&2

# Retention as the engine saw it, captured BEFORE the job is cancelled.
read -r REXP RTRK <<<"$(retention_totals)"
{ echo "retention_expired_total=${REXP:-0}"; echo "retention_tracked_keys=${RTRK:-0}";
} > "$OUT_DIR/retention.txt"
cat "$OUT_DIR/retention.txt"

# Let the job finish recovering from the LAST fault before the catch-up
# clock starts. Chaos stops between faults, but the job it just restarted
# is still draining, restoring and replaying, and during that it makes no
# progress at all - which the catch-up loop's stall detector reads as
# "caught up as far as it ever will". A local run ended 612 events short
# of 381,570 that way, and an unexplained shortfall is the difference
# between a verdict and an INCONCLUSIVE.
echo "campaign: waiting for the job to settle after the last fault"
settle=0
while [ "$settle" -lt 300 ]; do
    if [ "$(job_status "$JOB_ID")" = "RUNNING" ]; then
        CK1=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" 2>/dev/null \
            | python3 -c "
import json,sys
try:
    print(int(json.load(sys.stdin).get('latest_completed_checkpoint_id') or 0))
except Exception:
    print(0)" 2>/dev/null || echo 0)
        sleep 20; settle=$(( settle + 20 ))
        CK2=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" 2>/dev/null \
            | python3 -c "
import json,sys
try:
    print(int(json.load(sys.stdin).get('latest_completed_checkpoint_id') or 0))
except Exception:
    print(0)" 2>/dev/null || echo 0)
        # A checkpoint completed AFTER the faults stopped: the job is
        # running normally again, not merely reported as RUNNING.
        [ "${CK2:-0}" -gt "${CK1:-0}" ] && break
    else
        sleep 20; settle=$(( settle + 20 ))
    fi
done
[ "$settle" -lt 300 ] \
    || echo "campaign: WARNING - the job had not completed a fresh checkpoint 300s after the last fault" >&2

on_host "$OPS_PUB" "touch /qual/q5-progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[g]enerator.py'; true"
gwaited=0
while [ "$gwaited" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5; gwaited=$(( gwaited + 5 ))
done

echo "campaign: waiting for the pipeline to catch up with the generator"
CATCHUP=no
PRODUCED_FINAL=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q5-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
cwait=0; LAST_FOLDED=-1; STALL_S=0; FOLDED=0
while [ "$cwait" -lt "$CATCHUP_TIMEOUT_S" ]; do
    FOLDED=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q5_out_subject" | tr -d '\r')
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
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/q5-verdict.json" "$OUT_DIR/" 2>/dev/null || true
    s=$(python3 - "$OUT_DIR/q5-verdict.json" <<'PY' || echo -1
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

# The final state measurement, taken while the state still exists.
state_detail "$SUBJECT_CKPT_DIR" > "$OUT_DIR/state-size-final.txt" 2>/dev/null \
    || echo "campaign: WARNING - could not measure the final state size" >&2
cat "$OUT_DIR/state-size-final.txt" 2>/dev/null || true

# The authoritative correctness pass, in a FRESH process against a settled
# table: exact accounting plus every key recomputed from the seed.
on_host "$OPS_PUB" "python3 /qual/endstate.py --dsn '$DSN' --table public.q5_out_subject \
    --progress /qual/q5-progress.json --seed $SEED --partitions $PARTITIONS \
    --keys $KEYS --eps $EPS --base-ms $BASE_MS --key-epoch-ms $KEY_EPOCH_MS" \
    > "$OUT_DIR/completeness.txt" \
    || echo "campaign: WARNING - the end-state pass failed; correctness has no evidence" >&2
cat "$OUT_DIR/completeness.txt" 2>/dev/null || true

curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30
kill_campaign_processes "$OPS_PUB" || true
for f in q5-verdict.json q5-chaos.jsonl q5-progress.json q5-generator.log q5-verifier.log q5-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --duration-h "$DURATION_H" --profile "$PROFILE" > "$OUT_DIR/QUAL-05-summary.md"
cat "$OUT_DIR/QUAL-05-summary.md"

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh <rig-run-id> --yes && qualification/infra/teardown.sh --check"
