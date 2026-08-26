#!/usr/bin/env bash
# QUAL-10: the leak and stability campaign.
#
# The claim: every engine process's footprint stays flat for the length of
# the run - RSS, threads, file descriptors and the on-disk snapshot
# population - while output stays exactly correct, under a fault schedule
# that keeps replacing processes.
#
# THE SHAPE IS THE DESIGN. Two leak classes need opposite conditions, and a
# schedule that serves one is blind to the other:
#
#   - a STEADY-STATE leak (per record, per checkpoint) only shows if a
#     process is left alone long enough to drift, so the run needs a QUIET
#     window with no faults in it;
#   - a PER-RECOVERY leak (sockets not closed, threads not joined, per-job
#     maps never erased) only accumulates across restart CYCLES, so the run
#     needs many faults.
#
# A dry run on synthetic data proved that mattered: with faults every 15
# minutes and nothing else, no process incarnation lived long enough to
# judge drift, and the analyser reported a cheerful pass having never run
# half its criteria. It now FAILS that case rather than passing it.
#
# So the run is phased, and the phases are FRACTIONS of the total duration -
# which is what lets the same driver run a 12-hour campaign on the rig and a
# few-minute functional rehearsal on the local rig with the same shape:
#
#     |-- warm-up --|------ quiet ------|------- faults -------|-quiesce-|
#     0           1/12                4.5/12                11.5/12    12/12
#
# The instrument is qual10/sampler.py, running detached on EVERY host and
# measuring from outside the engine - /proc, the cgroup and the filesystem,
# never clink's own gauges. An engine gauge that under-reports a leak is
# precisely the defect this campaign hunts, so it cannot also be the
# witness.
#
#   RUN_ID=qual10-local-a DURATION_S=600 TARGET_CYCLES=6 \
#     qualification/qual10/campaign.sh
#
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID, e.g. qual10-20260826a}"
DURATION_H="${DURATION_H:-12}"
PROFILE="${PROFILE:-trend}"

# The phase machine, as fractions of the whole run. Held constant so the
# local rehearsal exercises the same shape as the paid run - the rehearsal
# is worth little if it runs a different campaign quickly.
WARMUP_FRAC="${WARMUP_FRAC:-0.0833}"      # 1h of 12
QUIET_END_FRAC="${QUIET_END_FRAC:-0.375}" # 4.5h of 12
FAULT_END_FRAC="${FAULT_END_FRAC:-0.958}" # 11.5h of 12
# Recovery cycles to buy in the fault window. The gap is DERIVED from this
# and the window length, so compressing the run keeps the shape and trades
# cycles rather than silently firing faults faster than recovery completes.
TARGET_CYCLES="${TARGET_CYCLES:-42}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-15}"
RATE="${RATE:-1000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-5000}"
KEY_EPOCH_MS="${KEY_EPOCH_MS:-60000}"
SEED="${SEED:-20260825}"
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-15000}"
WM_LAG_MS="${WM_LAG_MS:-2000}"
STATE_TTL_MS="${STATE_TTL_MS:-600000}"
FILL_S="${FILL_S:-300}"

# Faults this environment cannot apply (comma-separated). The local
# launcher passes disk_pressure,clock_step; the cloud launcher passes
# nothing. Every skip is recorded by the controller and the summariser
# accepts an absence only under --local AND with the record present.
SKIP_FAULTS="${SKIP_FAULTS:-}"
# The bounded sandbox for disk_pressure (cloud only): a loopback image
# mounted at /qual/state before the NFS export, so ENOSPC is ENOSPC of
# a 4 GiB file, never of the ops host's root disk.
STATE_LOOP="${STATE_LOOP:-0}"
STATE_LOOP_SIZE="${STATE_LOOP_SIZE:-4G}"

MIN_GAP_S="${MIN_GAP_S:-120}"
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-300}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
RESTART_DRAIN_TIMEOUT_MS="${RESTART_DRAIN_TIMEOUT_MS:-300000}"
# partition_sustained must outlive the drain deadline or the full
# loss/degraded/heal/re-register cycle never runs; derived, not guessed.
SUSTAINED_PARTITION_S="${SUSTAINED_PARTITION_S:-$(( RESTART_DRAIN_TIMEOUT_MS / 1000 + 120 ))}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
PGPASSWORD="${PGPASSWORD:-qual10-$(echo "$RUN_ID" | tr -cd 'a-zA-Z0-9')}"
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"

DURATION_S="${DURATION_S:-$(python3 -c "print(int(float('$DURATION_H') * 3600))")}"

# Phase boundaries and the fault gap, derived rather than guessed. MIN_GAP_S
# comes out of the window length divided by the cycles asked for, so a
# compressed rehearsal cannot end up firing faults faster than a recovery
# can finish - it just buys fewer cycles, which is the honest trade.
eval "$(python3 - <<PYEOF
d = int("$DURATION_S")
w  = int(d * float("$WARMUP_FRAC"))
qe = int(d * float("$QUIET_END_FRAC"))
fe = int(d * float("$FAULT_END_FRAC"))
cycles = max(1, int("$TARGET_CYCLES"))
gap = max(20, (fe - qe) // cycles)
print(f"WARMUP_S={w}")
print(f"QUIET_END_S={qe}")
print(f"FAULT_END_S={fe}")
print(f"FAULT_WINDOW_S={fe - qe}")
print(f"MIN_GAP_S={gap}")
PYEOF
)"
[ "$FAULT_WINDOW_S" -gt 0 ] || { echo "campaign: the fault window is empty; check the phase fractions" >&2; exit 78; }
echo "campaign: phases - warm-up ${WARMUP_S}s, quiet until ${QUIET_END_S}s, faults until ${FAULT_END_S}s (gap ${MIN_GAP_S}s, ~${TARGET_CYCLES} cycles), total ${DURATION_S}s"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
SAMPLE_INTERVAL_S="${SAMPLE_INTERVAL_S:-120}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-30}"
FINAL_WAIT_S="${FINAL_WAIT_S:-600}"
CATCHUP_TIMEOUT_S="${CATCHUP_TIMEOUT_S:-1800}"
CATCHUP_STALL_S="${CATCHUP_STALL_S:-900}"
RECOVER_PROBES="${RECOVER_PROBES:-20}"

# The judge's minimum incarnation length scales with the run: in a
# compressed rehearsal the quiet window is minutes, and a fixed 1h floor
# would leave drift unjudged - the exact hole the dry run exposed.
eval "$(python3 -c "
d=int('$DURATION_S'); qs=int('$WARMUP_S'); qe=int('$QUIET_END_S')
quiet_h=(qe-qs)/3600.0
print(f'WARMUP_HOURS={qs/3600.0:.4f}')
print(f'MIN_INCARNATION_HOURS={max(quiet_h*0.6, 0.002):.4f}')
")"

OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" "$OUT_DIR/oracle-dirty.txt" \
      "$OUT_DIR/completeness.txt" "$OUT_DIR/catchup.txt" "$OUT_DIR/final-quiesce.txt" \
      "$OUT_DIR/verification.txt" "$OUT_DIR/environment.txt"

[ -x "$SUBMIT_BIN" ] || { echo "campaign: $SUBMIT_BIN is not executable; build it first" >&2; exit 78; }

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
    # Chaos first (the QUAL-06 lesson), and for THIS campaign doubly so:
    # the controller's exit drains its revert registry, so a stepped
    # clock or a filler at ENOSPC is put back before anything else runs.
    on_host "$OPS_PUB" "touch /qual/q10-chaos.jsonl.stop; pkill -INT -f '[c]haos.py'; true" || true
    collect_container_logs || true
    [ -n "${JOB_ID:-}" ] && curl -fsS -X POST \
        "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
    echo "campaign: stopping here. Evidence in $OUT_DIR" >&2
    exit 4
}

echo "campaign: QUAL-10 run $RUN_ID, battery ${DURATION_S}s, profile=$PROFILE"
echo "campaign: skip_faults='${SKIP_FAULTS}' state_loop=$STATE_LOOP sustained_partition=${SUSTAINED_PARTITION_S}s"
{ echo "skip_faults=$SKIP_FAULTS"; echo "state_loop=$STATE_LOOP";
  echo "sustained_partition_s=$SUSTAINED_PARTITION_S";
} > "$OUT_DIR/environment.txt"

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

# --- the bounded state sandbox (cloud only) -----------------------------------
# Mounted BEFORE the NFS export below, so every host shares the same
# 4 GiB filesystem and disk_pressure's ENOSPC is an ENOSPC of a file.
if [ "$STATE_LOOP" = "1" ]; then
    on_host "$OPS_PUB" "mountpoint -q /qual/state && umount /qual/state 2>/dev/null; \
        mkdir -p /qual/state; \
        [ -f /qual/state.img ] || { fallocate -l $STATE_LOOP_SIZE /qual/state.img && \
            mkfs.ext4 -q /qual/state.img; }; \
        mount -o loop /qual/state.img /qual/state && \
        exportfs -ra 2>/dev/null; true"
    on_host "$OPS_PUB" "mountpoint -q /qual/state" \
        || { echo "campaign: the loopback state sandbox did not mount" >&2; exit 2; }
    echo "campaign: /qual/state is a $STATE_LOOP_SIZE loopback sandbox"
fi

# Shared checkpoint state, exactly as every campaign since QUAL-04: a
# killed worker's subtask redeployed elsewhere must find its snapshot.
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
CKPT_DIR="/qual/state/$RUN_ID"
echo "campaign: shared state verified at $CKPT_DIR"

if [ "${SKIP_IMAGE_PULL:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" IMAGE="$CLINK_IMAGE" "$HERE/../infra/pull-image.sh"
fi

# --- verification database -------------------------------------------------
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
on_host "$OPS_PUB" "grep -q '^PGPASSWORD=' /qual/.env 2>/dev/null || echo 'PGPASSWORD=$PGPASSWORD' >> /qual/.env"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml down -v >/dev/null 2>&1; true"
on_host "$OPS_PUB" "cd /qual && PGPASSWORD='$PGPASSWORD' docker compose -f postgres.yml up -d"
pgready=0
until on_host "$OPS_PUB" "docker exec qual10-postgres pg_isready -U qual >/dev/null 2>&1"; do
    pgready=$(( pgready + 3 )); sleep 3
    [ "$pgready" -lt 180 ] || { echo "campaign: postgres never became ready" >&2; exit 2; }
done
CONNINFO="host=${OPS_PRIV} port=5432 dbname=qual user=qual password=${PGPASSWORD}"
DSN="host=127.0.0.1 port=5432 dbname=qual user=qual password=${PGPASSWORD}"
psql_q() { on_host "$OPS_PUB" "docker exec qual10-postgres psql -U qual -d qual -tAc \"$1\""; }
psql_q "DROP TABLE IF EXISTS public.q10_out; CREATE TABLE public.q10_out (k BIGINT PRIMARY KEY, n BIGINT)" >/dev/null

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
to_host "$COORD_PUB" "$HERE/../infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml down >/dev/null 2>&1; \
    rm -rf /qual/ha/jobs /qual/ha/history; mkdir -p /qual/ha; \
    printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nRESTART_DRAIN_TIMEOUT_MS=%s\nMALLOC_ARENA_MAX=%s\nCLINK_LD_PRELOAD=%s\nMALLOC_CONF=%s\n' \
    '$CLINK_IMAGE' '$COORD_PRIV' '$RESTART_DRAIN_TIMEOUT_MS' '${MALLOC_ARENA_MAX:-}' '${CLINK_LD_PRELOAD:-}' '${MALLOC_CONF:-}' > /qual/.env && \
    docker compose -f coordinator.yml up -d"
wi=0
for wp in $WORKER_PUBS; do
    wpriv=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print([h['private_ip'] for h in inv['hosts'] if h['public_ip']=='$wp'][0])")
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    # The allocator knob rides in .env so the chaos controller's own
    # `docker compose up` after a kill recreates the worker WITH it; an env
    # prefix on this one invocation would be lost at the first recovery.
    on_host "$wp" "cd /qual && printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=%s\nWORKER_IP=%s\nMALLOC_ARENA_MAX=%s\nCLINK_LD_PRELOAD=%s\nMALLOC_CONF=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' 'w$wi' '$wpriv' '${MALLOC_ARENA_MAX:-}' '${CLINK_LD_PRELOAD:-}' '${MALLOC_CONF:-}' > /qual/.env && \
        docker compose -f worker.yml up -d --force-recreate"
    wi=$(( wi + 1 ))
done
sleep 20

# --- fault surface ----------------------------------------------------------
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

# --- topic + ops-host machinery ----------------------------------------------
kill_campaign_processes "$OPS_PUB" || true
rpk_ops() {
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
rpk_ops "topic delete qual10-in" >/dev/null 2>&1 || true
for _t in $(seq 1 30); do
    rpk_ops "topic list" 2>/dev/null | grep -q "qual10-in" || break
    sleep 2
done
REPL=$(python3 -c "print(min(3, len('$BROKER_PRIVS'.split())))")
rpk_ops "topic create qual10-in -p $PARTITIONS -r $REPL" >/dev/null

on_host "$OPS_PUB" "rm -f /qual/q10-*.stop /qual/q10-progress.json /qual/q10-verdict.json \
    /qual/q10-chaos.jsonl /qual/q10-generator.log /qual/q10-verifier.log /qual/q10-chaos.log"
for f in "$HERE/../qual01/detspec.py" "$HERE/../qual01/generator.py" \
         "$HERE/../qual05/verifier.py" "$HERE/../qual05/endstate.py" \
         "$HERE/../qual05/ckptsize.py" "$HERE/../chaos/chaos.py"; do
    to_host "$OPS_PUB" "$f" "/qual/$(basename "$f")"
done
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
to_host "$OPS_PUB" "$KEY_FILE" /root/.ssh/id_ed25519
on_host "$OPS_PUB" "chmod 600 /root/.ssh/id_ed25519"
on_host "$OPS_PUB" "pip3 install --break-system-packages -q confluent-kafka psycopg2-binary 2>/dev/null || true"

# --- the instrument: a sampler on every host ------------------------------
# On EVERY host, not just the ops box. A leak lives in a process on the node
# that hosts it, and a coordinator that grows while the workers stay flat is
# a different diagnosis from the reverse - one central sampler polling over
# ssh would blur that, and would stop sampling the moment the ssh path had a
# bad minute.
#
# Detached, with its own stop-file, so the laptop can die without ending the
# measurement. The samples are the campaign's evidence; losing them to a
# closed lid would waste the whole run.
ALL_HOSTS=$(python3 -c "
import json
inv=json.load(open('$OUT_DIR/inventory.json'))
print(' '.join(h['public_ip'] for h in inv['hosts']))")
echo "campaign: starting samplers on: $ALL_HOSTS"
for H in $ALL_HOSTS; do
    to_host "$H" "$HERE/sampler.py" /qual/sampler.py
    # Kill any sampler left over from an earlier run before starting this
    # one, and give each run its OWN file. Both matter: a detached sampler
    # outlives a campaign that stopped early, and two of them appending to
    # one path interleaves two runs into a single series - which reads as a
    # sawtooth nobody scheduled, or hides a real one. Found in the local
    # rehearsal, which is what it is for.
    on_host "$H" "pkill -f '[s]ampler.py' 2>/dev/null; true"
    on_host "$H" "rm -f /qual/q10-metrics.jsonl.stop; mkdir -p /qual/metrics; \
                  rm -f /qual/metrics/$RUN_ID-*.jsonl"
    start_on_host "$H" q10-sampler.log \
        "python3 /qual/sampler.py --out /qual/metrics/$RUN_ID-\$(hostname).jsonl \
         --interval $SAMPLE_INTERVAL_S --state-dir /qual/state \
         --fs / --stop-file /qual/q10-metrics.jsonl.stop"
done
# Prove the instrument is actually recording before the run leans on it: a
# sampler that never started reads exactly like a flat engine.
sleep $(( SAMPLE_INTERVAL_S * 2 + 5 ))
for H in $ALL_HOSTS; do
    N=$(on_host "$H" "cat /qual/metrics/$RUN_ID-*.jsonl 2>/dev/null | wc -l" | tr -d ' \r')
    echo "campaign: sampler on $H has $N sample(s)"
    [ "${N:-0}" -ge 1 ] || { echo "campaign: the sampler on $H recorded nothing; the run cannot be judged" >&2; exit 78; }
done

EPS=$(( RATE / PARTITIONS ))
[ "$EPS" -ge 1 ] || { echo "campaign: RATE must be at least one event per partition per second" >&2; exit 78; }
BASE_MS=$(python3 -c "import time; print(int(time.time()*1000))")
echo "$BASE_MS" > "$OUT_DIR/base_ms"

start_on_host "$OPS_PUB" q10-generator.log \
    "python3 /qual/generator.py --brokers '$BROKER_LIST' --topic qual10-in \
     --rate $RATE --partitions $PARTITIONS --keys $KEYS --seed $SEED \
     --base-ms $BASE_MS --max-jitter-ms 0 --window-ms 10000 \
     --key-epoch-ms $KEY_EPOCH_MS --progress /qual/q10-progress.json"

# --- pipeline + submit -----------------------------------------------------------
sed -e "s|__BROKERS__|$BROKER_LIST|g" \
    -e "s|__CONNINFO__|$CONNINFO|g" \
    -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
    -e "s|__GROUP__|qual10-$RUN_ID|g" \
    -e "s|__STATE_TTL_MS__|$STATE_TTL_MS|g" \
    "$HERE/pipeline.sql.tmpl" > "$OUT_DIR/pipeline.sql"

"$SUBMIT_BIN" --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir "$CKPT_DIR" \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --max-restarts-on-worker-loss "$MAX_RESTARTS" \
    > "$OUT_DIR/submit.log" 2>&1 || true
JOB_ID=$(python3 -c "
import json
jid=''
for line in open('$OUT_DIR/submit.log'):
    line=line.strip()
    if line.startswith('{'):
        try:
            d=json.loads(line)
        except Exception:
            continue
        if d.get('ok') and d.get('job_id') is not None:
            jid=str(d['job_id'])
print(jid)")
[ -n "$JOB_ID" ] || { echo "campaign: the job did not submit" >&2
                      tail -20 "$OUT_DIR/submit.log" >&2; exit 1; }
echo "campaign: job $JOB_ID"

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

# --- functional verification -----------------------------------------------------
echo "campaign: functional verification"
P1=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q10-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 45
P2=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q10-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
[ "${P2:-0}" -gt "${P1:-0}" ] || verify_fail "no input is flowing (progress ${P1} -> ${P2})"

JOB_HEALTHY=no
for _t in $(seq 1 30); do
    if [ "$(job_status "$JOB_ID")" = "RUNNING" ] && [ "$(ckpt_id "$JOB_ID")" -ge 1 ]; then
        JOB_HEALTHY=yes; break
    fi
    sleep 10
done
[ "$JOB_HEALTHY" = "yes" ] \
    || verify_fail "the job never reached RUNNING with a completed checkpoint"

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
    C=$(psql_q "SELECT count(*) FROM public.q10_out" | tr -d '\r')
    [ "${C:-0}" -gt 0 ] && { SINK_OK=yes; break; }
    sleep 15
done
[ "$SINK_OK" = "yes" ] || verify_fail "nothing reached the sink table"

STATE_OK=no
for _t in $(seq 1 20); do
    B=$(state_bytes "$CKPT_DIR" || echo "")
    case "$B" in ''|*[!0-9]*) sleep 15; continue ;; esac
    [ "$B" -gt 0 ] && { STATE_OK=yes; break; }
    sleep 15
done
[ "$STATE_OK" = "yes" ] || verify_fail "the state instrument cannot measure the checkpoints"

start_on_host "$OPS_PUB" q10-verifier.log \
    "python3 /qual/verifier.py --dsn '$DSN' --table public.q10_out \
     --progress /qual/q10-progress.json --out /qual/q10-verdict.json --interval-s 20"

{ echo "campaign=QUAL-10"; echo "run_id=$RUN_ID"; echo "job_id=$JOB_ID";
  echo "state_ttl_ms=$STATE_TTL_MS"; echo "key_epoch_ms=$KEY_EPOCH_MS";
  echo "rate=$RATE"; echo "partitions=$PARTITIONS"; echo "keys_per_epoch=$KEYS";
  echo "checkpoint_interval_ms=$CHECKPOINT_INTERVAL_MS"; echo "fill_s=$FILL_S";
} > "$OUT_DIR/verification.txt"

# --- fill ---------------------------------------------------------------------
echo "campaign: filling (${FILL_S}s) so the infra faults threaten real state"
fstart=$(date +%s)
while [ $(( $(date +%s) - fstart )) -lt "$FILL_S" ]; do
    sleep "$SAMPLE_INTERVAL_S"
    B=$(state_bytes "$CKPT_DIR" || echo "")
    case "$B" in ''|*[!0-9]*) continue ;; esac
    echo "campaign: fill $(( ($(date +%s) - fstart) / 60 ))m: $(( B / 1024 / 1024 )) MiB of state"
done

# --- the quiet window ----------------------------------------------------------
# No faults. This is the half of the campaign that can see a STEADY-STATE
# leak: one long-lived incarnation per process, drifting or not, with
# nothing resetting it. Skipping it would leave the analyser judging only
# across restarts - which passes an engine that leaks per record.
# The judged window opens HERE - after functional verification and after
# the fill, which grows state on purpose. Recorded as a fact rather than
# derived as a fraction of the clock: the local rehearsal judged the fill's
# cold-start climb as an 831%/h leak, which is what a guessed warm-up buys
# you.
JUDGE_FROM_EPOCH=$(date +%s)
echo "campaign: judged window opens at $JUDGE_FROM_EPOCH (after fill)"
QUIET_S=$(( QUIET_END_S - WARMUP_S ))
if [ "$QUIET_S" -gt 0 ]; then
    echo "campaign: === quiet window (${QUIET_S}s, no faults) ==="
    QUIET_DEADLINE=$(( $(date +%s) + QUIET_S ))
    while [ "$(date +%s)" -lt "$QUIET_DEADLINE" ]; do
        sleep 10
        # The job must still be alive, or the quiet window is measuring an
        # idle cluster and every process looks beautifully flat.
        if ! on_host "$COORD_PUB" "curl -fsS http://${COORD_PRIV}:8095/api/v1/jobs/$JOB_ID >/dev/null 2>&1"; then
            echo "campaign: the job vanished during the quiet window" >&2
            echo "job vanished during quiet window" > "$OUT_DIR/job-gone.txt"
            break
        fi
    done
fi

# --- the battery ---------------------------------------------------------------
echo "campaign: === fault battery (${FAULT_WINDOW_S}s, gap ${MIN_GAP_S}s) ==="
Q10_POINTS=$(python3 -c "
import sys; sys.path.insert(0, '$HERE')
import summarise
print(','.join(summarise.TWOPC_POINTS))")
[ -n "$Q10_POINTS" ] || { echo "campaign: no 2PC points from the summariser" >&2; exit 78; }
SKIP_ARG=""
[ -n "$SKIP_FAULTS" ] && SKIP_ARG="--skip-faults $SKIP_FAULTS"
start_on_host "$OPS_PUB" q10-chaos.log \
    "python3 /qual/chaos.py --inventory /qual/inventory.json --log /qual/q10-chaos.jsonl \
     --coordinator-url http://${COORD_PRIV}:8095 --job-id $JOB_ID --run-id $RUN_ID \
     --profile $PROFILE --seed $SEED --min-gap-s $MIN_GAP_S \
     --twopc-points '$Q10_POINTS' --recovery-timeout-s $RECOVERY_TIMEOUT_S \
     --sustained-partition-s $SUSTAINED_PARTITION_S $SKIP_ARG \
     --duration-s $(( FAULT_WINDOW_S + 300 )) --ensure-coverage"

FAULTED=no
for _t in $(seq 1 60); do
    N=$(on_host "$OPS_PUB" "wc -l < /qual/q10-chaos.jsonl 2>/dev/null || echo 0" | tr -d ' \r')
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

BEFORE=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q10_out" | tr -d '\r')
RECOVERED=no
for _p in $(seq 1 "$RECOVER_PROBES"); do
    sleep 30
    S=$(job_status "$JOB_ID")
    AFTER=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q10_out" | tr -d '\r')
    if [ "$S" = "RUNNING" ] && [ "${AFTER:-0}" -gt "${BEFORE:-0}" ]; then RECOVERED=yes; break; fi
done
[ "$RECOVERED" = "yes" ] || verify_fail "the job did not resume making progress after the first fault"

SOAK_START=$(date +%s)
# The battery runs for the FAULT WINDOW, not the whole run. This line was
# inherited from a campaign whose DURATION_S WAS its battery, and here it
# ran the 10-hour cloud battery for ten hours on top of the setup and the
# quiet window - a 14-hour run sold as a 10-hour one.
END=$(( SOAK_START + FAULT_WINDOW_S ))
CHAOS_DIED_AT=""
JOB_GONE_AT=""
WATCH_LOOPS=0
while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$WATCH_MAX_LOOPS" != "0" ] && [ "$WATCH_LOOPS" -ge "$WATCH_MAX_LOOPS" ]; then break; fi
    WATCH_LOOPS=$(( WATCH_LOOPS + 1 ))
    sleep "$SAMPLE_INTERVAL_S"
    for f in q10-verdict.json q10-chaos.jsonl q10-progress.json q10-generator.log q10-verifier.log q10-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    DIRTY=$(python3 - "$OUT_DIR/q10-verdict.json" <<'PY' || echo ""
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
        on_host "$OPS_PUB" "touch /qual/q10-chaos.jsonl.stop"
        on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py' || true"
        collect_container_logs
        break
    fi

    if [ -z "$CHAOS_DIED_AT" ] && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q10-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        echo "campaign: WARNING - the chaos controller is no longer running (${NFAULTS} faults)" >&2
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NFAULTS"; } > "$OUT_DIR/chaos-died.txt"
    fi

    K=$(psql_q "SELECT count(*) FROM public.q10_out" 2>/dev/null | tr -d '\r' || echo '?')
    SM=$(psql_q "SELECT coalesce(sum(n),0) FROM public.q10_out" 2>/dev/null | tr -d '\r' || echo '?')
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
on_host "$OPS_PUB" "touch /qual/q10-chaos.jsonl.stop"
cwaited=0
while [ "$cwaited" -lt 300 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5; cwaited=$(( cwaited + 5 ))
done
# 300s, not 120: a sustained partition held by the controller must run
# its course (and its revert) before the stop lands between faults.
[ "$cwaited" -lt 300 ] \
    || echo "campaign: WARNING - the chaos controller is still running 300s after its stop request" >&2

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

on_host "$OPS_PUB" "touch /qual/q10-progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[g]enerator.py'; true"
gwaited=0
while [ "$gwaited" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5; gwaited=$(( gwaited + 5 ))
done

echo "campaign: waiting for the pipeline to catch up with the generator"
CATCHUP=no
PRODUCED_FINAL=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q10-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
cwait=0; LAST_FOLDED=-1; STALL_S=0; FOLDED=0
while [ "$cwait" -lt "$CATCHUP_TIMEOUT_S" ]; do
    FOLDED=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q10_out" | tr -d '\r')
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
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/q10-verdict.json" "$OUT_DIR/" 2>/dev/null || true
    s=$(python3 - "$OUT_DIR/q10-verdict.json" <<'PY' || echo -1
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

# Retention audit (item 77a's standing gate): the deepest snapshot pile
# across every subtask directory, judged from the DISK. ckptsize sizes
# only the newest snapshot per subtask, which is exactly how unbounded
# retention stayed invisible through five campaigns until a bounded
# volume made it terminal.
MAX_SNAPS=$(on_host "$OPS_PUB" "for d in \$(find '$CKPT_DIR' -mindepth 2 -maxdepth 2 -type d -path '*/v*/*' 2>/dev/null); do ls \$d 2>/dev/null | grep -c 'snap\$'; done | sort -n | tail -1" | tr -d ' \r')
{ echo "max_snaps_per_dir=${MAX_SNAPS:-0}"; echo "retained_configured=3";
} > "$OUT_DIR/retention-audit.txt"
cat "$OUT_DIR/retention-audit.txt"

on_host "$OPS_PUB" "python3 /qual/endstate.py --dsn '$DSN' --table public.q10_out \
    --progress /qual/q10-progress.json --seed $SEED --partitions $PARTITIONS \
    --keys $KEYS --eps $EPS --base-ms $BASE_MS --key-epoch-ms $KEY_EPOCH_MS" \
    > "$OUT_DIR/completeness.txt" \
    || echo "campaign: WARNING - the end-state pass failed; correctness has no evidence" >&2
cat "$OUT_DIR/completeness.txt" 2>/dev/null || true

curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30
kill_campaign_processes "$OPS_PUB" || true
for f in q10-verdict.json q10-chaos.jsonl q10-progress.json q10-generator.log q10-verifier.log q10-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

# --- the leak verdict ----------------------------------------------------------
# Stop the samplers only now: the quiesce and drain above are part of the
# run, and a process that only leaks while shutting down still leaks.
echo "campaign: === collecting metrics ==="
for H in $ALL_HOSTS; do
    on_host "$H" "touch /qual/q10-metrics.jsonl.stop" || true
done
sleep $(( SAMPLE_INTERVAL_S + 3 ))

mkdir -p "$OUT_DIR/metrics"
for H in $ALL_HOSTS; do
    scp "${SSH_OPTS[@]}" -q "root@${H}:/qual/metrics/$RUN_ID-*.jsonl" "$OUT_DIR/metrics/" 2>/dev/null \
        || echo "campaign: WARNING - no metrics retrieved from $H" >&2
done
SAMPLE_FILES=$(ls "$OUT_DIR"/metrics/*.jsonl 2>/dev/null | wc -l | tr -d ' ')
echo "campaign: retrieved metric series from $SAMPLE_FILES host(s)"
[ "${SAMPLE_FILES:-0}" -ge 1 ] || {
    echo "campaign: no metrics at all; the leak claim cannot be judged" >&2
    echo "no metrics retrieved" > "$OUT_DIR/metrics-missing.txt"
}

if [ "${SAMPLE_FILES:-0}" -ge 1 ]; then
    python3 "$HERE/analyse.py" \
        --samples "$OUT_DIR"/metrics/*.jsonl \
        --events "$OUT_DIR/q10-chaos.jsonl" \
        --charts-dir "$OUT_DIR/charts" \
        --out-json "$OUT_DIR/leak-report.json" \
        --judge-from-epoch "$JUDGE_FROM_EPOCH" \
        --plateau-seconds "$(( STATE_TTL_MS / 1000 ))" \
        --min-incarnation-hours "$MIN_INCARNATION_HOURS" \
        > "$OUT_DIR/leak-summary.txt" 2>&1 || true
    cat "$OUT_DIR/leak-summary.txt"
fi

LOCAL_ARG=""
[ -n "$SKIP_FAULTS" ] && LOCAL_ARG="--local"
python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --profile "$PROFILE" $LOCAL_ARG > "$OUT_DIR/QUAL-10-summary.md" || true
cat "$OUT_DIR/QUAL-10-summary.md" 2>/dev/null || true

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh <rig-run-id> --yes && qualification/infra/teardown.sh --check"
