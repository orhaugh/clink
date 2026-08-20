#!/usr/bin/env bash
# QUAL-02: Postgres two-phase-commit sink under worker faults.
#
# QUAL-01 qualifies exactly-once into Kafka, where the transaction is the
# broker's. This campaign qualifies the other half: an EXTERNAL
# transaction manager, where clink stages rows in an open Postgres
# transaction, PREPAREs it at the checkpoint barrier under a deterministic
# global id, and COMMIT PREPAREs it only once the checkpoint is durable.
# The interesting window is between prepare and commit - a worker killed
# there leaves a prepared transaction that outlives the connection, and
# recovery must resolve it in exactly one direction.
#
# The oracle reads Postgres directly and never asks clink anything. See
# verifier.py for the three countable failure modes.
#
#   RUN_ID=qual02-YYYYMMDD [INVENTORY=<path>] ./campaign.sh
#
# INVENTORY seeds this run from an existing rig's inventory, so a second
# campaign reuses provisioned hosts instead of paying to build them again.
# The CLUSTER on that rig is redeployed fresh regardless (HA store wiped,
# containers recreated): reusing a previous campaign's coordinator
# resurrects its jobs from the HA store, and qual01-20260817e measured
# 177k duplicates from exactly that zombie.
#
# This driver carries every harness lesson QUAL-01 paid for: docker and
# broker readiness gates, the NFS shared-state probe, a per-run checkpoint
# directory under the shared mount, the fault-surface gate, the
# exactly-one-job gate, an oracle fail-fast watch at a 2-minute cadence,
# the six-probe job-gone latch, chaos-liveness, a quiesced final
# judgement (the verifier must sample a SETTLED table before its verdict
# is called final - QUAL-01's verifier was killed mid-finalise and a
# 751/751-clean campaign summarised INCONCLUSIVE), an end-state
# completeness capture, and a summary that refuses PASS without coverage
# and stability evidence.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID (e.g. qual02-20260816)}"
DURATION_H="${DURATION_H:-2}"
PROFILE="${PROFILE:-steady}"
RATE="${RATE:-1000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-50000}"
SEED="${SEED:-20260816}"
# The 2PC sink prepares once per checkpoint interval per subtask, so the
# interval sets how often the window under test opens. Shorter than
# QUAL-01's: more prepare/commit cycles per hour of campaign.
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-8000}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
PGPASSWORD="${PGPASSWORD:-qual02-$(echo "$RUN_ID" | tr -dc 'a-z0-9')}"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
# Overridable for the simulator (DURATION_H=0 skips the soak loop
# entirely, which is how QUAL-01's watch-loop defects escaped simulation).
DURATION_S="${DURATION_S:-$(( DURATION_H * 3600 ))}"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-10}"
FINAL_WAIT_S="${FINAL_WAIT_S:-600}"

# Host-side prerequisites before any provisioning spend (same guard as
# qual01: a missing submit binary must fail at second zero, not at submit
# time on a live rig).
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"
if [ ! -x "$SUBMIT_BIN" ]; then
    echo "campaign: SUBMIT_BIN missing or not executable: $SUBMIT_BIN" >&2
    exit 78
fi

mkdir -p "$OUT_DIR"
# A relaunch reusing a RUN_ID must not inherit the previous attempt's
# evidence: the watch loop reads the pulled verdict before the first scp
# is guaranteed to have replaced it, and stale failure markers poison the
# summary. Scoped to the files this campaign writes.
rm -f "$OUT_DIR/q2-verdict.json" "$OUT_DIR/q2-chaos.jsonl" "$OUT_DIR/q2-progress.json" \
      "$OUT_DIR/oracle-dirty.txt" "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" \
      "$OUT_DIR/verification.txt" "$OUT_DIR/completeness.txt" "$OUT_DIR/job-status.json" \
      "$OUT_DIR/QUAL-02-summary.md"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() { ssh -n "${SSH_OPTS[@]}" "root@$1" "$2"; }
to_host()  { scp "${SSH_OPTS[@]}" -q "$2" "root@$1:$3"; }

# Container logs into the evidence before any teardown - the qual01 run f
# lesson: a real engine finding whose cluster logs died with the rig.
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

# See qual01/campaign.sh: ssh holds its channel open until the remote
# command's descriptors are released, so detaching needs setsid, every
# descriptor redirected, and an explicit exit - and the start must then
# be PROVEN rather than assumed.
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

# Bracketed patterns: an unbracketed pkill matches the remote shell
# carrying it, signals itself, and `|| true` hides the miss (qual01 run 3
# collected evidence from under a still-running producer).
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
    # The cluster logs ARE the evidence for a verification failure - the
    # first local QUAL-02 run failed here and left nothing to diagnose
    # with until the still-live rig was dug through by hand.
    collect_container_logs || true
    echo "campaign: not entering soak. Evidence in $OUT_DIR" >&2
    exit 4
}

echo "campaign: QUAL-02 run $RUN_ID, ${DURATION_H}h, profile=$PROFILE"

# --- inventory ----------------------------------------------------------
if [ ! -f "$OUT_DIR/inventory.json" ]; then
    [ -n "${INVENTORY:-}" ] || { echo "campaign: no inventory. Provision a rig, or set INVENTORY=<path>" >&2; exit 2; }
    cp "$INVENTORY" "$OUT_DIR/inventory.json"
    echo "campaign: reusing rig inventory from $INVENTORY"
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
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV ops=$OPS_PUB"

# Docker readiness on EVERY host before anything deploys - qual01 run C's
# deploy raced the slowest broker's docker and failed there.
for h in $OPS_PUB $COORD_PUB $WORKER_PUBS $(read_inv broker public_ip); do
    until on_host "$h" "docker info >/dev/null 2>&1"; do
        echo "campaign: waiting for docker on $h"; sleep 15
    done
done

# Shared checkpoint state. Checkpoint state is per-subtask, resolved on
# each node's own filesystem: on per-host local disks a killed worker's
# subtask redeployed elsewhere finds nothing and the restore refuses, so
# the campaign would measure failed restores rather than recovery. The
# previous revision of this driver pointed the job at /qual/state-q2 - a
# SIBLING of the NFS export, silently per-host - which is exactly that
# defect. Each run gets its own directory INSIDE the shared mount.
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h - refusing to run a" >&2
             echo "  worker-kill campaign on per-host local state." >&2; exit 2; }
done
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID"
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done
CHECKPOINT_DIR="/qual/state/$RUN_ID"
echo "campaign: shared state verified; checkpoint dir $CHECKPOINT_DIR"

# --- sink database ------------------------------------------------------
echo "campaign: starting the sink database"
on_host "$OPS_PUB" "mkdir -p /qual"
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
# Written to the host, not passed inline, so anything that later restarts
# this container reproduces its configuration.
on_host "$OPS_PUB" "printf 'PGPASSWORD=%s\\n' '$PGPASSWORD' >> /qual/.env"
# down -v FIRST: POSTGRES_PASSWORD only takes effect when the data
# directory is initialised, and this campaign derives its password from
# RUN_ID - so a second campaign on a REUSED rig (the INVENTORY= path
# this driver advertises) inherits a volume whose role still carries the
# previous run's password, and every oracle connection fails "password
# authentication failed for user postgres". A fresh volume is also the
# right default on its own terms: the sink table's counts are this
# campaign's evidence, and inheriting another run's rows corrupts them
# (the same reasoning that wipes the HA job store between runs).
on_host "$OPS_PUB" "cd /qual && docker compose -f postgres.yml down -v" >/dev/null 2>&1 || true
on_host "$OPS_PUB" "cd /qual && docker compose -f postgres.yml up -d"

for _ in $(seq 1 30); do
    if on_host "$OPS_PUB" "docker exec qual02-postgres pg_isready -U postgres -q" 2>/dev/null; then
        break
    fi
    sleep 2
done
on_host "$OPS_PUB" "docker exec qual02-postgres pg_isready -U postgres -q" \
    || { echo "campaign: sink database never became ready" >&2; exit 2; }

psql_q() { on_host "$OPS_PUB" "docker exec -e PGPASSWORD='$PGPASSWORD' qual02-postgres psql -U postgres -d qual02 -tAc \"$1\""; }

# The 2PC sink is unusable unless the server allows prepared
# transactions, and the setting defaults to OFF. Read the EFFECTIVE value
# from the server rather than trusting the compose file that asked for it.
MAXPREP=$(psql_q "SHOW max_prepared_transactions" | tr -d '\r')
echo "campaign: server max_prepared_transactions=$MAXPREP"
[ "${MAXPREP:-0}" -gt 0 ] || { echo "campaign: PREPARE TRANSACTION is disabled on the server" >&2; exit 2; }

# Any prepared transaction left by a previous run would be counted as
# this run's orphan at the end.
LEFTOVER=$(psql_q "SELECT count(*) FROM pg_prepared_xacts" | tr -d '\r')
if [ "${LEFTOVER:-0}" -gt 0 ]; then
    echo "campaign: rolling back $LEFTOVER prepared transaction(s) from an earlier run"
    on_host "$OPS_PUB" "docker exec -e PGPASSWORD='$PGPASSWORD' qual02-postgres psql -U postgres -d qual02 -tAc \
        \"SELECT 'ROLLBACK PREPARED ''' || gid || ''';' FROM pg_prepared_xacts\" \
        | docker exec -i -e PGPASSWORD='$PGPASSWORD' qual02-postgres psql -U postgres -d qual02 -q"
fi

# Deliberately NO unique constraint on event_id: a primary key would make
# Postgres reject a duplicate on clink's behalf, hiding the exact defect
# this campaign exists to detect. The oracle must be able to see a
# duplicate land.
psql_q "DROP TABLE IF EXISTS public.q2_out; CREATE TABLE public.q2_out (event_id text, k bigint, amount bigint)" >/dev/null
echo "campaign: sink table created (no unique constraint - duplicates must be visible)"

# The sink database is reached over the private network. Workers connect
# to it; chaos never targets it, because what is under test is clink's
# behaviour around an available transaction manager, not Postgres's own
# availability.
CONNINFO="host=$OPS_PRIV port=5432 user=postgres password=$PGPASSWORD dbname=qual02"
DSN="postgresql://postgres:$PGPASSWORD@localhost:5432/qual02"

# --- stack (fresh, always) -----------------------------------------------
# Brokers, coordinator and workers are recreated even on a reused rig.
# Configuration lives in /qual/.env on each host so the chaos controller's
# container restarts reproduce the deployment instead of guessing at it.
echo "campaign: deploying stack"
i=0
for bp in $(read_inv broker public_ip); do
    i=$((i+1))
    priv=$(echo "$BROKER_PRIVS" | cut -d' ' -f$i)
    on_host "$bp" "mkdir -p /qual"
    to_host "$bp" "$HERE/../infra/broker.yml" /qual/broker.yml
    on_host "$bp" "cd /qual && NODE_ID=$i PRIVATE_IP=$priv SEEDS='$SEED_LIST' docker compose -f broker.yml up -d"
done

# The coordinator's HA directory is wiped before every campaign: within a
# run recover_persisted_jobs() is the recovery under test, across runs it
# is a zombie factory (qual01-20260817e: 177k duplicates from the
# previous run's resurrected job).
on_host "$COORD_PUB" "docker rm -f clink-coordinator >/dev/null 2>&1 || true; \
    rm -rf /qual/ha/jobs /qual/ha/history"
on_host "$COORD_PUB" "mkdir -p /qual /qual/ha"
to_host "$COORD_PUB" "$HERE/../infra/coordinator.yml" /qual/coordinator.yml
on_host "$COORD_PUB" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\n' '$CLINK_IMAGE' '$COORD_PRIV' > /qual/.env"
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml up -d"

wid=0
for wp in $WORKER_PUBS; do
    wid=$((wid+1))
    wpriv=$(read_inv worker private_ip | cut -d' ' -f$wid)
    on_host "$wp" "docker rm -f clink-worker >/dev/null 2>&1 || true"
    on_host "$wp" "mkdir -p /qual /qual/state"
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=w%s\nWORKER_IP=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' '$wid' '$wpriv' > /qual/.env"
    on_host "$wp" "cd /qual && docker compose -f worker.yml up -d"
done

# The deployed build's own capability manifest, captured BEFORE the
# campaign runs. The chaos controller's mandatory coverage arms the 2PC
# crash windows via CLINK_FAULT_INJECT; against a build with no fault
# surface every one of those arms compiles to nothing and the campaign
# would report faults it never injected.
until on_host "$COORD_PUB" "docker exec clink-coordinator clink --capabilities-json" \
        > "$OUT_DIR/capabilities.json" 2>/dev/null; do
    echo "campaign: waiting for the coordinator to answer --capabilities-json"; sleep 10
done
FAULT_SURFACE=$(python3 -c "
import json; d=json.load(open('$OUT_DIR/capabilities.json'))
print('yes' if (d.get('build') or d).get('fault_injection') else 'no')")
[ "$FAULT_SURFACE" = "yes" ] || {
    echo "campaign: the deployed image has no fault-injection surface; the mandatory" >&2
    echo "  2PC-window faults would be silently skipped. Use the -faultinj image." >&2
    exit 2
}
echo "campaign: fault-injection surface confirmed"

# --- topics -------------------------------------------------------------
BROKER_ONE=$(echo "$BROKER_PRIVS" | awk '{print $1}')
# --entrypoint rpk, not a leading rpk arg: the image's entrypoint already
# invokes rpk on some arches, and the doubled token made rpk parse its own
# name as a subcommand (the local rig caught it; the flag error printed
# rpk's help and killed the campaign at topic creation). Replication
# follows the rig's actual broker count, capped at the cloud shape's 3 -
# a hardcoded 3 cannot be satisfied by a smaller rig and says nothing
# more on a bigger one.
BROKER_COUNT=$(read_inv broker private_ip | wc -w | tr -d ' ')
REPLICATION=$(( BROKER_COUNT < 3 ? BROKER_COUNT : 3 ))
# Silence the PREVIOUS attempt's ops processes BEFORE touching topics: a
# stale generator keeps producing, and with broker auto-creation on, the
# topic delete below "succeeds" and the topic reincarnates on the very
# next produce - with the broker-default single partition (round 5 spun
# on exactly that for 30 delete attempts). The full ops reset later
# still runs; this early sweep only guarantees a quiet broker here.
on_host "$OPS_PUB" "pkill -f '[g]enerator.py'; pkill -f '[v]erifier.py'; pkill -f '[c]haos.py'; true"

rpk_ops() {  # rpk against the broker, from the ops host
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
rpk_ops "topic delete qual02-in" >/dev/null 2>&1 || true
# Deletion is asynchronous: wait for the broker to actually forget the
# topic, re-issuing the delete, instead of racing the create into
# TOPIC_ALREADY_EXISTS - qual01's identical lesson (a topic this run does
# not own carries a previous run's events, and reading those as this
# run's once produced 161,111 phantom missing results there).
gone=0
for _ in $(seq 1 30); do
    if ! rpk_ops "topic list" 2>/dev/null | awk '{print $1}' | grep -qx qual02-in; then
        gone=1; break
    fi
    rpk_ops "topic delete qual02-in" >/dev/null 2>&1 || true
    sleep 2
done
[ "$gone" = "1" ] || { echo "campaign: topic qual02-in still exists after 30 delete attempts" >&2; exit 2; }
rpk_ops "topic create qual02-in -p $PARTITIONS -r $REPLICATION"
echo "campaign: topic recreated ($PARTITIONS partitions, replication $REPLICATION)"

# --- ops host: generator + verifier + chaos -----------------------------
# Stop requests are FILES (see qual01/verifier.py: the spawn discipline
# starts these processes with SIGINT ignored, so a polite pkill -INT never
# reaches them). Stale stop files on a reused rig would make fresh
# processes exit on their first loop iteration.
on_host "$OPS_PUB" "rm -f /qual/q2-progress.json.stop /qual/q2-chaos.jsonl.stop"
for f in ../qual01/detspec.py ../qual01/generator.py verifier.py; do
    to_host "$OPS_PUB" "$HERE/$f" "/qual/$(basename "$f")"
done
to_host "$OPS_PUB" "$HERE/../chaos/chaos.py" /qual/chaos.py
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
to_host "$OPS_PUB" "$KEY_FILE" /root/.ssh/id_ed25519
on_host "$OPS_PUB" "chmod 600 /root/.ssh/id_ed25519 && \
    (pip3 install --break-system-packages -q confluent-kafka psycopg2-binary \
     || pip3 install -q confluent-kafka psycopg2-binary)"

on_host "$OPS_PUB" "pkill -f '[g]enerator.py'; pkill -f '[v]erifier.py'; pkill -f '[c]haos.py'; \
    rm -f /qual/q2-progress.json* /qual/q2-verdict.json /qual/q2-chaos.jsonl; true"

BASE_MS=$(( $(date +%s) * 1000 ))
echo "$BASE_MS" > "$OUT_DIR/base_ms"

start_on_host "$OPS_PUB" q2-generator.log "python3 generator.py --brokers '$BROKER_LIST' \
    --topic qual02-in --rate $RATE --partitions $PARTITIONS --keys $KEYS \
    --seed $SEED --base-ms $BASE_MS --max-jitter-ms 0 \
    --window-ms 10000 --progress /qual/q2-progress.json"
echo "campaign: generator started"

# --- pipeline -----------------------------------------------------------
sed -e "s|__BROKERS__|$BROKER_LIST|g" -e "s|__CONNINFO__|$CONNINFO|g" \
    "$HERE/pipeline.sql.tmpl" > "$OUT_DIR/pipeline.sql"

SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"
"$SUBMIT_BIN" \
    --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir "$CHECKPOINT_DIR" \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --max-restarts-on-worker-loss "$MAX_RESTARTS" \
    | tee "$OUT_DIR/submit.log"
JOB_ID=$(python3 - "$OUT_DIR/submit.log" <<'PYEOF'
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
PYEOF
)
[ -n "$JOB_ID" ] || { echo "campaign: could not determine job id - see $OUT_DIR/submit.log" >&2; exit 1; }
echo "campaign: job $JOB_ID submitted"

# The connector manifest that actually describes what is under test: the
# coordinator's registry, not the CLI binary's (which does not link the
# impl libraries and lists six connectors where the cluster lists
# thirty-three).
curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/connectors" \
    > "$OUT_DIR/cluster-connectors.json" 2>/dev/null \
    || echo "campaign: WARNING - could not retain the cluster's connector manifest" >&2

start_on_host "$OPS_PUB" q2-verifier.log "python3 verifier.py --dsn '$DSN' \
    --progress /qual/q2-progress.json --out /qual/q2-verdict.json --interval-s 20"
echo "campaign: verifier started"

# --- functional verification --------------------------------------------
# Nothing soaks until every link in the chain is proven to carry traffic.
echo "campaign: functional verification (nothing soaks until this passes)"

# 1. Input is flowing.
sleep 45
P1=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q2-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 30
P2=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q2-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
[ "${P2:-0}" -gt "${P1:-0}" ] || verify_fail "the generator is not producing (progress $P1 -> $P2)"
echo "campaign: input flowing ($P1 -> $P2 events)"

# 2. The job is running, checkpointing, and error-free.
JOB_JSON=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "coordinator unreachable"
echo "$JOB_JSON" > "$OUT_DIR/job-status.json"
python3 -c "
import json,sys
j = json.loads(sys.argv[1])
if j.get('status') != 'RUNNING':
    print('job status ' + str(j.get('status'))); sys.exit(1)
if int(j.get('checkpoints_completed') or j.get('latest_completed_checkpoint_id') or 0) < 1:
    print('no completed checkpoints'); sys.exit(1)
if j.get('errors'):
    print('job carries errors: ' + str(j.get('errors'))); sys.exit(1)
" "$JOB_JSON" || verify_fail "job not running cleanly with completed checkpoints: $JOB_JSON"
echo "campaign: job RUNNING with completed checkpoints"

# 2b. EXACTLY ONE job. A second active job means something else is
# producing into this run's sink - qual01 run e's zombie twin.
JOBS_JSON=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs") || verify_fail "cannot list jobs"
ACTIVE_JOBS=$(python3 -c "
import json,sys
doc = json.loads(sys.argv[1])
jobs = doc.get('jobs', doc) if isinstance(doc, dict) else doc
print(len(jobs))" "$JOBS_JSON")
[ "$ACTIVE_JOBS" = "1" ] || verify_fail "expected exactly one job on the coordinator, found ${ACTIVE_JOBS}: $JOBS_JSON"
echo "campaign: exactly one job on the coordinator"

# 3. Rows are being COMMITTED to Postgres. A 2PC sink that never commits
#    looks identical to a healthy one from clink's side, so this is read
#    from the database.
COMMITTED=0
for _ in $(seq 1 20); do
    COMMITTED=$(psql_q "SELECT count(*) FROM public.q2_out" | tr -d '\r')
    [ "${COMMITTED:-0}" -gt 0 ] && break
    sleep 15
done
[ "${COMMITTED:-0}" -gt 0 ] || verify_fail "no rows have been committed to Postgres"
echo "campaign: rows committed to the sink database ($COMMITTED)"

# 4. The mechanism UNDER TEST is actually engaged.
#
#    Rows landing in Postgres proves the sink works; it does NOT prove
#    they arrived through two-phase commit. A plain autocommit insert
#    would look the same in the table. So require evidence that prepared
#    transactions are really being created under clink's global-id scheme
#    - otherwise the campaign would qualify a protocol it never exercised.
#    Sampling pg_prepared_xacts for a live prepared transaction is a
#    race against the prepare-to-commit window, which is MILLISECONDS
#    on a healthy cluster - the local rig polled 120s and never caught
#    one while 2PC ran perfectly underneath. Deterministic evidence
#    instead: a worker hosting the sink logs the 2PC family name at
#    open(), and a plain-insert or upsert deployment logs a different
#    family. The prepared-xacts sample stays as recorded corroboration
#    (the slower cloud rig can catch one), never as the verdict; the
#    SOAK's armed 2PC faults - fired-proofed and recovery-proofed - are
#    what genuinely exercise the windows this campaign qualifies.
SINK_FAMILY_SEEN=0
for wp in $WORKER_PUBS; do
    if on_host "$wp" "docker logs clink-worker 2>&1 | grep -q postgres_json_sink_2pc"; then
        SINK_FAMILY_SEEN=1
    fi
done
[ "$SINK_FAMILY_SEEN" = "1" ] || verify_fail "no worker's sink opened as postgres_json_sink_2pc.
  Rows are committing, but not through the two-phase-commit family this
  campaign exists to qualify."
echo "campaign: 2PC sink family confirmed deployed (postgres_json_sink_2pc on a worker)"
N=$(psql_q "SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'clink!_%' ESCAPE '!'" | tr -d '\r')
echo "prepared_xacts_sampled=${N:-0}" > "$OUT_DIR/gid-sample.txt"
if [ "${N:-0}" -gt 0 ]; then
    GID_SAMPLE=$(psql_q "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'clink!_%' ESCAPE '!' LIMIT 1" | tr -d '\r')
    echo "campaign: prepared transaction observed in flight (gid example: $GID_SAMPLE)"
    echo "$GID_SAMPLE" >> "$OUT_DIR/gid-sample.txt"
fi

# 5. Faults land, and land where clink can see them. Coverage-first: the
#    mandatory faults (including every 2PC crash window) are scheduled
#    before the weighted random tail, and each is fired-proofed by the
#    controller. No --verdict here: chaos.py's oracle fail-fast parses
#    QUAL-01's counter-shaped verdict, and this campaign's verdict is
#    finding-shaped; the watch loop below owns fail-fast instead.
# Padded past the soak deadline (the controller's clock starts before the
# soak's does); the drain stops it via its stop file, orderly.
# The fireable 2PC point set comes from summarise.py's TWOPC_POINTS -
# one source of truth, so the chaos schedule can never require a point
# the summariser does not, nor the reverse (QUAL-02's original wiring
# imported the full Kafka list and PASS was structurally unreachable:
# sink.between_commit_and_receipt exists only in the Kafka sink).
Q2_POINTS=$(cd "$(dirname "$0")" && python3 -c "import summarise; print(','.join(summarise.TWOPC_POINTS))")
[ -n "$Q2_POINTS" ] || { echo "campaign: could not derive the 2PC point set from summarise.py" >&2; exit 78; }
start_on_host "$OPS_PUB" q2-chaos.log "python3 chaos.py --inventory /qual/inventory.json \
    --log /qual/q2-chaos.jsonl --coordinator-url http://${COORD_PRIV}:8095 \
    --job-id $JOB_ID --run-id $RUN_ID --profile $PROFILE --seed $SEED \
    --min-gap-s ${MIN_GAP_S:-120} --twopc-points $Q2_POINTS \
    --extra-faults pg_unavailable \
    --duration-s $(( DURATION_S + 1800 )) --ensure-coverage"
echo "campaign: chaos started (${DURATION_H}h, profile=$PROFILE, coverage-first)"

FAULTS=0
for _ in $(seq 1 60); do
    FAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q2-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
    [ "${FAULTS:-0}" -gt 0 ] && break
    sleep 20
done
[ "${FAULTS:-0}" -gt 0 ] || verify_fail "the chaos controller applied no fault in 20 minutes"
echo "campaign: chaos landing faults ($FAULTS recorded)"

# A recorded fault is not a landed fault. Read the engine's own count,
# and distinguish "the metric says zero" from "the metric could not be
# read".
# The counter is read on a POLL, not once: the chaos log records the kill
# the moment it is issued, while the coordinator only declares the loss
# when its heartbeat lease expires seconds later. A one-shot read raced
# that window and failed a campaign whose engine was behaving perfectly
# (the local rig hit it on the first fault - being FASTER than the cloud
# rig, whose ssh round-trips had been masking the race). Still a real
# gate: if no worker loss is recorded within the window, the fault left
# no trace in the engine and did not happen.
LOST=0
LOST_SEEN=0
for _ in $(seq 1 30); do
    METRICS=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/metrics" 2>/dev/null) || METRICS=""
    if [ -n "$METRICS" ]; then
        LOST=$(echo "$METRICS" | awk '/^clink_coordinator_workers_lost_total /{print $2}')
        if [ -n "$LOST" ] && [ "${LOST%%.*}" -ge 1 ]; then LOST_SEEN=1; break; fi
    fi
    sleep 5
done
[ -n "$METRICS" ] || verify_fail "cannot read the coordinator's metrics, so a fault cannot be confirmed"
[ -n "$LOST" ] || verify_fail "the coordinator exports no clink_coordinator_workers_lost_total"
if [ "$LOST_SEEN" != "1" ]; then
    verify_fail "the chaos controller recorded $FAULTS fault(s) but the coordinator has lost
  no worker (clink_coordinator_workers_lost_total=$LOST). A fault that leaves no trace
  in the engine did not happen, whatever the chaos log says."
fi
echo "campaign: fault confirmed by the engine (workers lost: $LOST)"

# 6. The job survives the fault AND keeps committing through it.
BEFORE=$(psql_q "SELECT count(*) FROM public.q2_out" | tr -d '\r')
sleep 90
POST=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "coordinator unreachable after the first fault"
python3 -c "
import json,sys
j = json.loads(sys.argv[1])
sys.exit(0 if j.get('status') == 'RUNNING' else 1)" "$POST" \
    || verify_fail "the job did not recover from the first fault: $POST"
AFTER=$(psql_q "SELECT count(*) FROM public.q2_out" | tr -d '\r')
[ "${AFTER:-0}" -gt "${BEFORE:-0}" ] \
    || verify_fail "the job reports RUNNING after the fault but has committed no further rows
  ($BEFORE -> $AFTER). A recovered job that cannot commit is not recovered."
echo "campaign: recovered and still committing ($BEFORE -> $AFTER rows) - VERIFICATION PASSED, entering soak"

{ echo "campaign=QUAL-02"; echo "run_id=$RUN_ID"; echo "job_id=$JOB_ID";
  echo "input_events_observed=$P2"; echo "rows_committed_at_gate=$AFTER";
  echo "max_prepared_transactions=$MAXPREP"; echo "gid_sample=${GID_SAMPLE:-none-caught-in-flight}";
  echo "faults_recorded=$FAULTS"; echo "workers_lost_observed_by_coordinator=$LOST";
  echo "recovered_after_first_fault=yes";
} > "$OUT_DIR/verification.txt"

# --- soak (2-minute cadence, oracle fail-fast) ----------------------------
# QUAL-01 run C's 10-minute cadence let the cluster keep mutating for most
# of an hour after the first bad window. Two minutes, and a dirty oracle
# freezes the faults and stops the soak while the state that produced the
# defect still exists.
END=$(( $(date +%s) + DURATION_S ))
CHAOS_DIED_AT=""
JOB_GONE_AT=""
WATCH_LOOPS=0
while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$WATCH_MAX_LOOPS" != "0" ] && [ "$WATCH_LOOPS" -ge "$WATCH_MAX_LOOPS" ]; then
        break
    fi
    WATCH_LOOPS=$(( WATCH_LOOPS + 1 ))
    sleep 120
    for f in q2-verdict.json q2-chaos.jsonl q2-progress.json q2-generator.log q2-verifier.log q2-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    DIRTY=$(python3 - "$OUT_DIR/q2-verdict.json" <<'PY' || echo ""
import json, sys
try:
    v = json.load(open(sys.argv[1]))
except Exception:
    print("")
    sys.exit(0)
print("DIRTY" if (v.get("findings") or v.get("stuck")) else "")
PY
)
    if [ "$DIRTY" = "DIRTY" ]; then
        echo "campaign: ORACLE DIRTY - freezing faults and collecting evidence" >&2
        { echo "oracle_dirty=yes"; echo "noticed_at_utc=$(date -u +%H:%M)"; } > "$OUT_DIR/oracle-dirty.txt"
        on_host "$OPS_PUB" "touch /qual/q2-chaos.jsonl.stop"
        on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py' || true"
        collect_container_logs
        break
    fi

    # Is anything still applying faults? A soak with no fault generator
    # alive looks exactly like a soak that survived the faults.
    if [ -z "$CHAOS_DIED_AT" ] \
       && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q2-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        echo "campaign: WARNING - the chaos controller is no longer running (noticed ${CHAOS_DIED_AT}," \
             "${NFAULTS} fault record(s) written)." >&2
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NFAULTS";
          echo "tail:"; on_host "$OPS_PUB" "tail -20 /qual/q2-chaos.log" 2>/dev/null || true;
        } > "$OUT_DIR/chaos-died.txt"
    fi

    # Is the SUBJECT still alive? Six consecutive non-RUNNING probes
    # across a full minute - a coordinator mid-armed-fault answers
    # nothing and a job mid-recovery answers RESTARTING; both are healthy
    # chaos, and a single-shot probe latching on them poisons the summary.
    if [ -z "$JOB_GONE_AT" ]; then
        NOT_RUNNING=0
        PROBES=""
        for _probe in 1 2 3 4 5 6; do
            ALIVE=$(curl -fsS --max-time 20 \
                    "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" 2>/dev/null \
                    | python3 -c "
import json,sys
try:
    print(json.load(sys.stdin).get('status') or 'UNKNOWN')
except Exception:
    print('GONE')" 2>/dev/null || echo GONE)
            PROBES="${PROBES}${ALIVE} "
            if [ "$ALIVE" = "RUNNING" ]; then
                NOT_RUNNING=0
                break
            fi
            NOT_RUNNING=$(( NOT_RUNNING + 1 ))
            [ "$_probe" -lt 6 ] && sleep "$JOB_PROBE_INTERVAL_S"
        done
        if [ "$NOT_RUNNING" -ge 6 ]; then
            JOB_GONE_AT=$(date -u +%H:%M)
            echo "campaign: WARNING - job ${JOB_ID} is no longer RUNNING (probes: ${PROBES})." >&2
            { echo "job_gone=yes"; echo "probes=$PROBES"; echo "noticed_at_utc=$JOB_GONE_AT";
            } > "$OUT_DIR/job-gone.txt"
        fi
    fi

    psql_q "SELECT count(*), count(DISTINCT event_id) FROM public.q2_out" > "$OUT_DIR/pg-counts.txt" 2>/dev/null || true
    echo "campaign: $(date -u +%H:%M) soak, $(tail -1 "$OUT_DIR/pg-counts.txt" 2>/dev/null || echo '?') rows/distinct"
done

# --- drain and final judgement ------------------------------------------
echo "campaign: soak complete, draining"
# Stop requests are FILES; the pkill -INT is a courtesy for interactive
# runs only (the spawn discipline starts these with SIGINT ignored - see
# qual01/verifier.py). Chaos first and ORDERLY: mid-fault it holds tc
# rules and armed points, so it exits between faults, clearing them.
on_host "$OPS_PUB" "touch /qual/q2-chaos.jsonl.stop"
cwaited=0
while [ "$cwaited" -lt 120 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5
    cwaited=$(( cwaited + 5 ))
done
[ "$cwaited" -lt 120 ] \
    || echo "campaign: WARNING - the chaos controller is still running 120s after its stop request; a fault may straddle the drain" >&2
on_host "$OPS_PUB" "touch /qual/q2-progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py'; pkill -INT -f '[g]enerator.py'; true"
gwaited=0
while [ "$gwaited" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5
    gwaited=$(( gwaited + 5 ))
done
[ "$gwaited" -lt 60 ] \
    || echo "campaign: WARNING - the generator is still producing 60s after its stop request" >&2
sleep 120   # let the pipeline commit what it has already read

# The verdict is final only over a SETTLED table. The verifier re-judges
# the whole table each pass, so "final" here means: at least one fresh
# sample was taken after the drain AND two consecutive samples saw the
# same row count. Killing the verifier before that reads a moving table -
# QUAL-01's version of this mistake summarised a 751/751-clean campaign
# INCONCLUSIVE.
QUIESCED=no
waited=0
prev_rows=-1
while [ "$waited" -lt "$FINAL_WAIT_S" ]; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/q2-verdict.json" "$OUT_DIR/" 2>/dev/null || true
    rows=$(python3 - "$OUT_DIR/q2-verdict.json" <<'PY' || echo -1
import json, sys
try:
    v = json.load(open(sys.argv[1]))
    print((v.get("last_stats") or {}).get("rows_total", -1))
except Exception:
    print(-1)
PY
)
    if [ "${rows:--1}" -ge 0 ] && [ "$rows" = "$prev_rows" ]; then
        QUIESCED=yes
        break
    fi
    prev_rows="$rows"
    sleep 25
    waited=$(( waited + 25 ))
done
if [ "$QUIESCED" = "yes" ]; then
    echo "campaign: verdict quiesced after ${waited}s (rows_total=$prev_rows across two samples)"
else
    echo "campaign: WARNING - the verdict never quiesced within ${FINAL_WAIT_S}s; the summary will read this run as incomplete" >&2
fi
echo "quiesced=$QUIESCED" > "$OUT_DIR/final-quiesce.txt"

# End-state completeness: everything produced must be committed once the
# pipeline drained. Prefix contiguity alone cannot see a silently dropped
# TAIL - a sink that lost its last intervals still reads contiguous.
PRODUCED_TOTAL=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q2-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo -1)
COMMITTED_DISTINCT=$(psql_q "SELECT count(DISTINCT event_id) FROM public.q2_out" | tr -d '\r')
{ echo "produced_total=$PRODUCED_TOTAL"; echo "committed_distinct=${COMMITTED_DISTINCT:--1}";
} > "$OUT_DIR/completeness.txt"
echo "campaign: completeness produced=$PRODUCED_TOTAL committed_distinct=$COMMITTED_DISTINCT"

# The AUTHORITATIVE foreign check. The generator has stopped and its final
# progress file is now an upper bound, so a committed sequence at or above
# it is a record this campaign never produced. Mid-flight the verifier
# deliberately cannot make this call (its snapshot is only a lower bound,
# and treating it as an upper one made the oracle invent 32 findings on a
# provably complete run); here it can, and it is cheap.
AHEAD_PAIRS=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q2-progress.json'));print(' '.join('%s:%s' % (k,v) for k,v in d['produced_high'].items()))\"" 2>/dev/null || echo "")
FOREIGN_AHEAD=0
for pair in $AHEAD_PAIRS; do
    part=${pair%%:*}
    high=${pair##*:}
    MAXSEQ=$(psql_q "SELECT coalesce(max(split_part(event_id,'-',2)::bigint),-1) FROM public.q2_out WHERE event_id LIKE 'p${part}-%'" | tr -d '\r')
    if [ "${MAXSEQ:--1}" -ge "${high:-0}" ]; then
        echo "campaign: FOREIGN - partition $part committed up to seq $MAXSEQ but the generator" >&2
        echo "  finished at $high; the sink holds records that were never produced" >&2
        FOREIGN_AHEAD=$(( FOREIGN_AHEAD + 1 ))
    fi
done
echo "foreign_ahead_partitions=$FOREIGN_AHEAD" >> "$OUT_DIR/completeness.txt"
echo "campaign: post-drain foreign check: $FOREIGN_AHEAD partition(s) ahead of the generator"

# An orphaned prepared transaction holds locks and blocks vacuum forever.
# Sampled BEFORE the job is cancelled: at most one in flight per subtask
# is normal, a pile is not.
psql_q "SELECT count(*) FROM pg_prepared_xacts" > "$OUT_DIR/prepared-before-cancel.txt" || true
curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30
psql_q "SELECT coalesce(string_agg(gid, ','), '') FROM pg_prepared_xacts" > "$OUT_DIR/prepared-after-cancel.txt" || true

kill_campaign_processes "$OPS_PUB" || true
STILL=$(on_host "$OPS_PUB" "pgrep -f '[g]enerator.py|[v]erifier.py|[c]haos.py' | wc -l" | tr -d ' \r')
[ "${STILL:-0}" = "0" ] || echo "campaign: WARNING - $STILL campaign process(es) survived the drain" >&2

for f in q2-verdict.json q2-chaos.jsonl q2-progress.json q2-generator.log q2-verifier.log q2-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs
psql_q "SELECT count(*) FROM public.q2_out" > "$OUT_DIR/pg-rows-final.txt" || true
psql_q "SELECT count(DISTINCT event_id) FROM public.q2_out" > "$OUT_DIR/pg-distinct-final.txt" || true
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --duration-h "$DURATION_H" --profile "$PROFILE" > "$OUT_DIR/QUAL-02-summary.md"
cat "$OUT_DIR/QUAL-02-summary.md"

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh <rig-run-id> --yes && qualification/infra/teardown.sh --check"
