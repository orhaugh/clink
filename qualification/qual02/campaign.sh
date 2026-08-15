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
DURATION_S=$(( DURATION_H * 3600 ))

mkdir -p "$OUT_DIR"
SSH_OPTS=(-o StrictHostKeyChecking=no -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() { ssh -n "${SSH_OPTS[@]}" "root@$1" "$2"; }
to_host()  { scp "${SSH_OPTS[@]}" -q "$2" "root@$1:$3"; }

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

verify_fail() {
    echo "campaign: FUNCTIONAL VERIFICATION FAILED - $1" >&2
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
BROKER_PRIVS=$(read_inv broker private_ip)
BROKER_LIST=$(for ip in $BROKER_PRIVS; do printf "%s:9092," "$ip"; done | sed 's/,$//')
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV ops=$OPS_PUB"

# The sink database lives on the ops host and is reached over the private
# network. Workers connect to it; chaos never targets it, because what is
# under test is clink's behaviour around an available transaction
# manager, not Postgres's own availability.
CONNINFO="host=$OPS_PRIV port=5432 user=postgres password=$PGPASSWORD dbname=qual02"
DSN="postgresql://postgres:$PGPASSWORD@localhost:5432/qual02"

# --- sink database ------------------------------------------------------
echo "campaign: starting the sink database"
on_host "$OPS_PUB" "mkdir -p /qual"
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
# Written to the host, not passed inline, so anything that later restarts
# this container reproduces its configuration. See qual01/campaign.sh: a
# restart that lost its environment brought a worker back on the wrong
# image with an empty coordinator address.
on_host "$OPS_PUB" "printf 'PGPASSWORD=%s\\n' '$PGPASSWORD' >> /qual/.env"
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

# --- topics -------------------------------------------------------------
BROKER_ONE=$(echo "$BROKER_PRIVS" | awk '{print $1}')
on_host "$OPS_PUB" "docker run --rm docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
    rpk topic delete qual02-in --brokers $BROKER_ONE:9092" >/dev/null 2>&1 || true
on_host "$OPS_PUB" "docker run --rm docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
    rpk topic create qual02-in -p $PARTITIONS -r 3 --brokers $BROKER_ONE:9092"
echo "campaign: topic recreated ($PARTITIONS partitions, replication 3)"

# --- ops host: generator + verifier + chaos -----------------------------
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

"$REPO_ROOT/build/clink_submit_sql" \
    --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir /qual/state-q2 \
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

# The connector manifest that actually describes what is under test.
#
# `clink --capabilities-json` reports the CLI binary's registry, and the CLI
# does not link the impl libraries: it lists six connectors where the running
# coordinator lists thirty-three, including the ones the campaign depends on.
# Retaining only that would mean the provenance record omits the connector
# being qualified. The coordinator's own registry is the authoritative answer,
# and it is the same rule the deployment gate follows - the target cluster
# decides what is available, not whatever submitted the job.
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

# 2. The job is running and taking checkpoints.
JOB=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "coordinator unreachable"
python3 -c "
import json,sys
j = json.loads(sys.argv[1])
if j.get('status') != 'RUNNING':
    print('job status ' + str(j.get('status'))); sys.exit(1)
if int(j.get('checkpoints_completed') or 0) < 1:
    print('no completed checkpoints'); sys.exit(1)
" "$JOB" || verify_fail "job not running with completed checkpoints: $JOB"
echo "campaign: job RUNNING with completed checkpoints"

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
#    This is QUAL-02's version of the fabricated-fault trap. Rows landing
#    in Postgres proves the sink works; it does NOT prove they arrived
#    through two-phase commit. A plain autocommit insert would look the
#    same in the table. So require evidence that prepared transactions
#    are really being created under clink's global-id scheme - otherwise
#    the campaign would qualify a protocol it never exercised.
PREPARED_SEEN=0
for _ in $(seq 1 40); do
    N=$(psql_q "SELECT count(*) FROM pg_prepared_xacts WHERE gid LIKE 'clink!_%' ESCAPE '!'" | tr -d '\r')
    if [ "${N:-0}" -gt 0 ]; then PREPARED_SEEN=$N; break; fi
    sleep 3
done
[ "${PREPARED_SEEN:-0}" -gt 0 ] || verify_fail "no clink-prefixed prepared transaction was ever observed.
  Rows are committing, but not through PREPARE TRANSACTION - the two-phase-commit
  protocol this campaign exists to qualify is not being exercised."
GID_SAMPLE=$(psql_q "SELECT gid FROM pg_prepared_xacts WHERE gid LIKE 'clink!_%' ESCAPE '!' LIMIT 1" | tr -d '\r')
echo "campaign: two-phase commit confirmed in the database (gid example: $GID_SAMPLE)"
echo "$GID_SAMPLE" > "$OUT_DIR/gid-sample.txt"

# 5. Faults land, and land where clink can see them.
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
start_on_host "$OPS_PUB" q2-chaos.log "python3 chaos.py --inventory /qual/inventory.json \
    --coordinator ${COORD_PRIV}:8095 --profile $PROFILE --duration-s $DURATION_S \
    --out /qual/q2-chaos.jsonl --run-id $RUN_ID"
echo "campaign: chaos started (${DURATION_H}h, profile=$PROFILE)"

FAULTS=0
for _ in $(seq 1 60); do
    FAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q2-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
    [ "${FAULTS:-0}" -gt 0 ] && break
    sleep 20
done
[ "${FAULTS:-0}" -gt 0 ] || verify_fail "the chaos controller applied no fault in 20 minutes"
echo "campaign: chaos landing faults ($FAULTS recorded)"

# A recorded fault is not a landed fault - see qual01. Read the engine's
# own count, and distinguish "the metric says zero" from "the metric
# could not be read", because treating an unreachable coordinator as
# zero is the same silent assumption in a different coat.
METRICS=$(curl -fsS --max-time 20 "http://${COORD_PUB}:8095/metrics" 2>/dev/null) \
    || verify_fail "cannot read the coordinator's metrics, so a fault cannot be confirmed"
LOST=$(echo "$METRICS" | awk '/^clink_coordinator_workers_lost_total /{print $2}')
[ -n "$LOST" ] || verify_fail "the coordinator exports no clink_coordinator_workers_lost_total"
if [ "${LOST%%.*}" -lt 1 ]; then
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
echo "campaign: recovered and still committing ($BEFORE -> $AFTER rows) - VERIFICATION PASSED"

{ echo "campaign=QUAL-02"; echo "run_id=$RUN_ID"; echo "job_id=$JOB_ID";
  echo "input_events_observed=$P2"; echo "rows_committed_at_gate=$AFTER";
  echo "max_prepared_transactions=$MAXPREP"; echo "gid_sample=$GID_SAMPLE";
  echo "faults_recorded=$FAULTS"; echo "workers_lost_observed_by_coordinator=$LOST";
  echo "recovered_after_first_fault=yes";
} > "$OUT_DIR/verification.txt"

# --- soak ---------------------------------------------------------------
END=$(( $(date +%s) + DURATION_S ))
CHAOS_DIED_AT=""
while [ "$(date +%s)" -lt "$END" ]; do
    sleep 600
    for f in q2-verdict.json q2-chaos.jsonl q2-progress.json q2-generator.log q2-verifier.log q2-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    # The gate proves faults land at the start of the run; this proves they
    # are still landing. QUAL-01's controller died four minutes into an hour
    # and the campaign reported healthy for the remaining fifty-six, because
    # a soak with no faults in it looks exactly like a soak that survived
    # them.
    if [ -z "$CHAOS_DIED_AT" ] \
       && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q2-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        echo "campaign: WARNING - the chaos controller is no longer running (noticed ${CHAOS_DIED_AT}," \
             "${NFAULTS} fault record(s) written). Everything after this point is an" \
             "undisturbed soak, not a fault campaign, and must be reported as such." >&2
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NFAULTS";
          echo "tail:"; on_host "$OPS_PUB" "tail -20 /qual/q2-chaos.log" 2>/dev/null || true;
        } > "$OUT_DIR/chaos-died.txt"
    fi
    psql_q "SELECT count(*), count(DISTINCT event_id) FROM public.q2_out" > "$OUT_DIR/pg-counts.txt" 2>/dev/null || true
    echo "campaign: $(date -u +%H:%M) soak, $(tail -1 "$OUT_DIR/pg-counts.txt" 2>/dev/null || echo '?') rows/distinct"
done

# --- drain and final judgement ------------------------------------------
echo "campaign: soak complete, draining"
on_host "$OPS_PUB" "pkill -f '[c]haos.py'; pkill -f '[g]enerator.py'; true"
sleep 120   # let the pipeline commit what it has already read

# An orphaned prepared transaction holds locks and blocks vacuum forever.
# Sampled BEFORE the job is cancelled: at most one in flight per subtask
# is normal, a pile is not.
psql_q "SELECT count(*) FROM pg_prepared_xacts" > "$OUT_DIR/prepared-before-cancel.txt" || true
curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30
psql_q "SELECT coalesce(string_agg(gid, ','), '') FROM pg_prepared_xacts" > "$OUT_DIR/prepared-after-cancel.txt" || true

for f in q2-verdict.json q2-chaos.jsonl q2-progress.json q2-generator.log q2-verifier.log q2-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
done
psql_q "SELECT count(*) FROM public.q2_out" > "$OUT_DIR/pg-rows-final.txt" || true
psql_q "SELECT count(DISTINCT event_id) FROM public.q2_out" > "$OUT_DIR/pg-distinct-final.txt" || true
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

echo "campaign: done. Evidence in $OUT_DIR"
