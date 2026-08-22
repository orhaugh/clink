#!/usr/bin/env bash
# QUAL-04: large keyed state under faults.
#
# QUAL-01 to QUAL-03 qualify DELIVERY semantics - exactly-once into a
# broker, an external transaction manager, and an object store. This
# campaign qualifies something different: that keyed state of a stated
# SIZE survives the same fault battery with its contents intact.
#
# The workload makes state large and output small. Each event is padded
# to BLOB_BYTES inside the engine, so the per-key accumulator is fat and
# state size is set by (distinct keys x blob size) rather than by event
# count - which is what puts 100 GB about an hour of fill away instead of
# a day. An unwindowed GROUP BY never closes, so state accumulates for
# the whole run. Only length() and the count reach the sink.
#
# State rides a DEFERRING backend (remote-read://): on a non-deferring
# backend the SQL aggregate keeps its accumulators in an in-memory map,
# so 100 GB would simply be an out-of-memory kill rather than a campaign.
# AggregateRowOp switches to the async KeyedState path automatically when
# the bound backend reports supports_async_get(), which remote-read://
# always does.
#
# Two instruments the delivery campaigns did not need:
#
#   state size      measured from OUTSIDE the engine, by summing the
#                   object bytes the backend occupies in the store. The
#                   engine has no live keyed-state gauge for any
#                   deferring backend, and a self-reported number would
#                   break the rule that the oracle never asks clink about
#                   clink. The summariser REFUSES to pass a run that did
#                   not reach its target: a clean run at 2 GB is evidence
#                   about a small job, not a large one.
#
#   restore time    clink_ckpt_restore_ns, which grows with state size.
#                   The recovery deadline and the job-gone window are set
#                   from a calibration run's measurement, never guessed -
#                   at size, a fixed ten-minute deadline records "the job
#                   did not recover" about a job that was recovering
#                   normally.
#
# The verification table is an upsert sink keyed by k, so Postgres sees
# traffic proportional to KEYS rather than events and the table holds the
# engine's current answer per key. That is effectively-once by key, not
# two-phase commit, and it is the verification CHANNEL rather than the
# subject.
#
#   RUN_ID=qual04-YYYYMMDD [INVENTORY=<path>] ./campaign.sh
#
# Carries every harness lesson QUAL-01 to QUAL-03 paid for: docker and
# broker readiness gates, the shared-state probe, the fault-surface gate,
# the exactly-one-job gate, an oracle fail-fast watch, the six-probe
# job-gone latch, chaos-liveness, a quiesced final judgement, an
# authoritative end-state pass in a fresh process, and a summary that
# refuses PASS without coverage, stability AND size evidence.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID (e.g. qual04-20260822)}"
DURATION_H="${DURATION_H:-2}"
PROFILE="${PROFILE:-steady}"
RATE="${RATE:-1000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-50000}"
SEED="${SEED:-20260823}"
# The staged-commit sink prepares once per checkpoint interval per
# subtask, so the interval sets how often the window under test opens -
# and here it also sets the object count (one object per subtask per
# interval), which the oracle's incremental reads keep cheap.
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-8000}"
# State sizing. STATE_TARGET_GIB is the campaign's whole point: the
# summariser refuses PASS if the run never reached it, because a clean
# run at a small size is evidence about a small job. BLOB_BYTES is the
# per-key accumulator width and KEYS the key space, so the reachable
# ceiling is about (KEYS x BLOB_BYTES); the fill needs roughly 3x KEYS
# events to touch ~95% of the space (uniform keys, coupon-collector), so
# raise RATE rather than hope.
BLOB_BYTES="${BLOB_BYTES:-20480}"
STATE_TARGET_GIB="${STATE_TARGET_GIB:-1}"
FILL_TIMEOUT_S="${FILL_TIMEOUT_S:-7200}"
# Per-subtask hot-tier bound for the deferring backend. Set explicitly and
# small on purpose: the working set must genuinely exceed RAM, or the
# campaign measures a cache rather than disaggregated state. Absent, the
# backend defaults to 25% of physical RAM PER INSTANCE.
HOT_MAX_BYTES="${HOT_MAX_BYTES:-268435456}"
# Scales with state size - see the header. 600 is the delivery campaigns'
# value and is only right for small state; a calibration run sets this.
RECOVERY_TIMEOUT_S="${RECOVERY_TIMEOUT_S:-600}"
PGPASSWORD="${PGPASSWORD:-qual04-$(echo "$RUN_ID" | tr -dc 'a-z0-9')}"
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
# MinIO requires a secret of at least 8 characters; derived from RUN_ID
# so a reused rig cannot silently serve a previous run's store.
S3_ACCESS_KEY="qual04"
S3_SECRET_KEY="${S3_SECRET_KEY:-qual04-$(echo "$RUN_ID" | tr -dc 'a-z0-9')}"
S3_BUCKET="qual04state"
S3_PREFIX="state"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
# Overridable for the simulator (DURATION_H=0 skips the soak loop
# entirely, which is how QUAL-01's watch-loop defects escaped simulation).
DURATION_S="${DURATION_S:-$(( DURATION_H * 3600 ))}"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
# Six probes at this interval latch "job gone". At size a restore
# legitimately outlasts the delivery campaigns' 60s, so this is
# raised and, like the recovery deadline, set from calibration.
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-30}"
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
rm -f "$OUT_DIR/q4-verdict.json" "$OUT_DIR/q4-chaos.jsonl" "$OUT_DIR/q4-progress.json" \
      "$OUT_DIR/oracle-dirty.txt" "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" \
      "$OUT_DIR/verification.txt" "$OUT_DIR/completeness.txt" "$OUT_DIR/job-status.json" \
      "$OUT_DIR/QUAL-04-summary.md"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
# ssh, retried on TRANSPORT failure only.
#
# ssh exits 255 for its own failures and passes the remote command's exit
# code through otherwise, so retrying only on 255 leaves every legitimate
# non-zero answer alone - `pgrep` finding nothing must still report
# nothing, and the gates that branch on a remote command failing must
# still see it fail.
#
# A multi-hour campaign makes hundreds of these calls against an ops host
# that is simultaneously running the generator, the oracle, the chaos
# controller and the state store. One "Connection reset by peer" killed a
# QUAL-04 rig run at 6.3 GiB of a 10 GiB fill, twenty minutes in, with
# `set -e` turning a transient blip into a total loss of the run and its
# spend. The engine was fine; the driver simply could not survive a
# dropped connection.
on_host() {
    local host=$1 cmd=$2 attempt=1 rc=0
    while : ; do
        # `cmd && rc=0 || rc=$?`, not a bare call: under set -e a bare
        # failing command exits the script before rc can be captured, so
        # the retry would only ever engage in a tested context.
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

echo "campaign: QUAL-04 run $RUN_ID, ${DURATION_H}h, profile=$PROFILE"

# --- rig ----------------------------------------------------------------
# Every campaign is STANDALONE: it provisions the rig it needs, captures
# the inventory, and puts the image under test on the hosts. RIG_RUN_ID
# points at hosts that already exist (a re-run proving a fix, or a second
# campaign sharing one paid rig): evidence still lands in this run's own
# directory while the infrastructure is paid for once. INVENTORY remains
# supported for a rig this tooling did not create - the local rig under
# qualification/test/local-rig uses it.
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
        echo "campaign: waiting for cloud-init on every host"
    fi
    # API-derived, never hand-maintained. provision.sh writes it at rig
    # creation; this refresh covers a rig reused under a new run id.
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
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV ops=$OPS_PUB"

# The store's two addresses: workers dial the ops host's private IP; the
# oracle and the campaign's own probes run ON the ops host and use
# localhost. Both are the same MinIO.
S3_ENDPOINT_WORKERS="http://${OPS_PRIV}:9000"
S3_ENDPOINT_OPS="http://127.0.0.1:9000"

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
# the campaign would measure failed restores rather than recovery. Each
# run gets its own directory INSIDE the shared mount.
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

# --- image under test ---------------------------------------------------
# Part of being standalone: pull-image.sh verifies every host resolved
# the SAME digest for the tag and writes the image's provenance and
# capability manifest into the evidence. Skippable for a rig whose images
# are already loaded - the local rig seeds them directly.
if [ "${SKIP_IMAGE_PULL:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" IMAGE="$CLINK_IMAGE" "$HERE/../infra/pull-image.sh"
fi
# --- state store + verification database ---------------------------------
# MinIO holds the job's KEYED STATE (remote-read:// backend), so its data
# directory goes on the attached volume rather than the ops host's root
# disk - that is what STATE_VOLUME_GB exists for.
echo "campaign: starting the state store"
on_host "$OPS_PUB" "mkdir -p /qual"
to_host "$OPS_PUB" "$HERE/minio.yml" /qual/minio.yml
# Written to the host, not passed inline, so anything that later restarts
# this container reproduces its configuration.
on_host "$OPS_PUB" "printf 'S3_ACCESS_KEY=%s\\nS3_SECRET_KEY=%s\\nMINIO_DATA_DIR=%s\\n' \
    '$S3_ACCESS_KEY' '$S3_SECRET_KEY' '${MINIO_DATA_DIR:-qual04-minio-data}' >> /qual/.env"
# down -v, then wipe the data dir: MinIO's root credentials only take
# effect on an uninitialised data directory, and this campaign derives
# its secret from RUN_ID, so a reused rig would otherwise serve a store
# keyed to the previous run's secret and every call would fail
# InvalidAccessKeyId. Wiping is also correct on its own terms - the state
# in that store is this campaign's subject, and inheriting another run's
# is the same class of mistake as inheriting its job store.
on_host "$OPS_PUB" "cd /qual && docker compose -f minio.yml down -v" >/dev/null 2>&1 || true
if [ -n "${MINIO_DATA_DIR:-}" ]; then
    on_host "$OPS_PUB" "rm -rf $MINIO_DATA_DIR && mkdir -p $MINIO_DATA_DIR"
fi
on_host "$OPS_PUB" "cd /qual && docker compose -f minio.yml up -d"

for _ in $(seq 1 45); do
    if on_host "$OPS_PUB" "curl -fsS $S3_ENDPOINT_OPS/minio/health/ready >/dev/null 2>&1"; then
        break
    fi
    sleep 2
done
on_host "$OPS_PUB" "curl -fsS $S3_ENDPOINT_OPS/minio/health/ready >/dev/null 2>&1" \
    || { echo "campaign: state store never became ready" >&2; exit 2; }

# The state store must have ROOM for the target. A campaign that fills
# the volume mid-fill reports an engine failure that is really a sizing
# mistake, so refuse up front rather than discover it at 80 GB.
# Check the filesystem the STORE actually writes to, which is the root
# disk unless MINIO_DATA_DIR was pointed at the attached volume.
STORE_PATH="${MINIO_DATA_DIR:-/var/lib/docker}"
AVAIL_KB=$(on_host "$OPS_PUB" "df -Pk $STORE_PATH | awk 'NR==2{print \$4}'" | tr -d ' \r')
AVAIL_GIB=$(( ${AVAIL_KB:-0} / 1048576 ))
NEED_GIB=$(python3 -c "print(int(${STATE_TARGET_GIB} * 1.4) + 1)")
echo "campaign: the state store's filesystem ($STORE_PATH) has ${AVAIL_GIB} GiB free, target needs ~${NEED_GIB} GiB"
[ "$AVAIL_GIB" -ge "$NEED_GIB" ] || {
    echo "campaign: $STORE_PATH has ${AVAIL_GIB} GiB free but this run targets" >&2
    echo "  ${STATE_TARGET_GIB} GiB of state (~${NEED_GIB} GiB with overhead)." >&2
    echo "  Provision with STATE_VOLUME_GB=<n> and set MINIO_DATA_DIR=/qual/state/minio;" >&2
    echo "  note the attached volume is far slower for small objects than the root disk." >&2
    exit 2
}

# --- store write-rate gate ------------------------------------------------
# Measure what the store's filesystem can actually absorb, and refuse a
# workload that exceeds it. This is the gate three earlier smokes needed
# and did not have.
#
# The pipeline writes ONE object per distinct key touched per checkpoint
# interval, so the store's small-file create rate is a hard ceiling on
# sustainable events per second. Exceed it and persists take longer than
# the interval they are supposed to fit in; the backlog grows without
# bound, checkpoints stop completing, and a coordinator kill landing on
# an overrunning persist wedges the worker's teardown. That chain is not
# a subtle failure - it looks like an engine defect from every angle
# except this measurement.
#
# Measured with dd, MinIO deliberately out of the picture, because the
# ceiling belongs to the filesystem: a Hetzner network volume managed
# ~300 creates/s where the local NVMe root disk is far faster.
echo "campaign: measuring the state store's object write rate"
# Measure what the STORE serves, at the concurrency the engine actually
# drives - not what the disk underneath can do.
#
# A first version of this timed dd creating files on the store's
# filesystem and reported ~435/s. That number was process-spawn cost: the
# same disk does 14,337 files/s from a single process. It also led to the
# wrong conclusion that a Hetzner volume was the bottleneck. The real
# ceiling is MinIO's own per-request overhead - 171 PUT/s serially,
# ~495 PUT/s from 8 concurrent writers on this host shape, CPU-bound
# rather than disk-bound - so the probe must go through the S3 API, and
# concurrently, or it measures the wrong layer twice over.
to_host "$OPS_PUB" "$HERE/putprobe.py" /qual/putprobe.py
STORE_PUTS=$(on_host "$OPS_PUB" "python3 /qual/putprobe.py --endpoint $S3_ENDPOINT_OPS \
    --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY' \
    --object-bytes $BLOB_BYTES --concurrency ${STORE_PROBE_CONCURRENCY:-8}" | tr -d ' \r')
echo "campaign: the store serves ~${STORE_PUTS:-?} object writes/s at concurrency ${STORE_PROBE_CONCURRENCY:-8}"

# Headroom, because the live mix also carries state READS and MinIO's own
# metadata traffic, and the probe is a clean write-only best case.
SUSTAINABLE=$(( ${STORE_PUTS:-0} * 60 / 100 ))
{ echo "store_puts_per_sec=${STORE_PUTS:-0}"; echo "sustainable_events_per_sec=$SUSTAINABLE";
  echo "configured_rate=$RATE"; echo "probe_concurrency=${STORE_PROBE_CONCURRENCY:-8}"; } \
  > "$OUT_DIR/store-rate.txt"
if [ "${SUSTAINABLE:-0}" -lt 1 ]; then
    echo "campaign: could not measure the store's write rate; refusing to guess" >&2
    exit 2
fi
if [ "$RATE" -gt "$SUSTAINABLE" ]; then
    echo "campaign: RATE=$RATE exceeds what this store can absorb (~$SUSTAINABLE events/s at" >&2
    echo "  60% of a measured ${STORE_PUTS} object writes/s). Persists would outrun their" >&2
    echo "  interval and the run would measure the store, not the engine. Lower RATE, raise" >&2
    echo "  PARTITIONS so the engine drives more concurrent writes, or give the store more CPU." >&2
    exit 2
fi
echo "campaign: RATE=$RATE is within the store's sustainable ~$SUSTAINABLE events/s"

# The checkpoint interval must also exceed the time a persist takes, or
# each persist overlaps the next. Persist writes one object per distinct
# key touched since the last checkpoint, so at RATE events/s the interval
# must satisfy: (RATE * interval) / STORE_FPS < interval, which is just
# RATE < STORE_FPS - already gated above. What remains is the FIRST
# checkpoint after a large fill, which writes everything at once; the
# interval is reported here so the evidence records the margin.
echo "campaign: checkpoint interval ${CHECKPOINT_INTERVAL_MS}ms; a full-state persist of" \
     "$(python3 -c "print(int(${STATE_TARGET_GIB} * 1024**3 / ${BLOB_BYTES}))") objects would take" \
     "~$(python3 -c "print(int(${STATE_TARGET_GIB} * 1024**3 / ${BLOB_BYTES} / max(${STORE_FPS:-1},1)))")s"

on_host "$OPS_PUB" "curl -fsS -X PUT --aws-sigv4 'aws:amz:us-east-1:s3' \
    --user '$S3_ACCESS_KEY:$S3_SECRET_KEY' $S3_ENDPOINT_OPS/$S3_BUCKET >/dev/null" \
    || { echo "campaign: could not create the state bucket" >&2; exit 2; }
echo "campaign: state bucket created (store data in $STORE_PATH)"

# The verification database. NOT the subject: it is how an external
# oracle reads the engine's per-key answers. mode='upsert' keyed by k
# means one row per key and traffic proportional to KEYS, not events.
echo "campaign: starting the verification database"
to_host "$OPS_PUB" "$HERE/postgres.yml" /qual/postgres.yml
on_host "$OPS_PUB" "printf 'PGPASSWORD=%s\\n' '$PGPASSWORD' >> /qual/.env"
on_host "$OPS_PUB" "cd /qual && docker compose -f postgres.yml down -v" >/dev/null 2>&1 || true
on_host "$OPS_PUB" "cd /qual && docker compose -f postgres.yml up -d"
for _ in $(seq 1 30); do
    if on_host "$OPS_PUB" "docker exec qual04-postgres pg_isready -U postgres -q" 2>/dev/null; then
        break
    fi
    sleep 2
done
on_host "$OPS_PUB" "docker exec qual04-postgres pg_isready -U postgres -q" \
    || { echo "campaign: verification database never became ready" >&2; exit 2; }

psql_q() { on_host "$OPS_PUB" "docker exec -e PGPASSWORD='$PGPASSWORD' qual04-postgres psql -U postgres -d qual04 -tAc \"$1\""; }

# A PRIMARY KEY here, unlike QUAL-02's table, and for the opposite
# reason: the upsert sink REQUIRES one (it is the conflict target), and
# one row per key is what makes the table a readable picture of state.
psql_q "DROP TABLE IF EXISTS public.q4_out; CREATE TABLE public.q4_out (k BIGINT PRIMARY KEY, blob_len BIGINT, n BIGINT)" >/dev/null
echo "campaign: verification table created (one row per key, PRIMARY KEY k)"

CONNINFO="host=$OPS_PRIV port=5432 user=postgres password=$PGPASSWORD dbname=qual04"
DSN="postgresql://postgres:$PGPASSWORD@localhost:5432/qual04"
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
# RESTART_DRAIN_TIMEOUT_MS scales the coordinator's restart-drain
# deadline with the state backend: a subtask whose every key access is a
# store round trip winds down far more slowly than one holding state in
# memory, and the 120s default fails the job outright when it overruns.
on_host "$COORD_PUB" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nRESTART_DRAIN_TIMEOUT_MS=%s\n' \
    '$CLINK_IMAGE' '$COORD_PRIV' '${RESTART_DRAIN_TIMEOUT_MS:-600000}' > /qual/.env"
on_host "$COORD_PUB" "cd /qual && docker compose -f coordinator.yml up -d"

wid=0
for wp in $WORKER_PUBS; do
    wid=$((wid+1))
    wpriv=$(read_inv worker private_ip | cut -d' ' -f$wid)
    on_host "$wp" "docker rm -f clink-worker >/dev/null 2>&1 || true"
    on_host "$wp" "mkdir -p /qual /qual/state"
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    # The store credentials ride the worker's .env: worker.yml passes
    # AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY through to the container,
    # and the chaos controller's compose restarts reproduce them.
    on_host "$wp" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=w%s\nWORKER_IP=%s\nAWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' '$wid' '$wpriv' '$S3_ACCESS_KEY' '$S3_SECRET_KEY' > /qual/.env"
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
# name as a subcommand. Replication follows the rig's actual broker
# count, capped at the cloud shape's 3.
BROKER_COUNT=$(read_inv broker private_ip | wc -w | tr -d ' ')
REPLICATION=$(( BROKER_COUNT < 3 ? BROKER_COUNT : 3 ))
# Silence the PREVIOUS attempt's ops processes BEFORE touching topics: a
# stale generator keeps producing, and with broker auto-creation on, the
# topic delete below "succeeds" and the topic reincarnates on the very
# next produce - with the broker-default single partition.
on_host "$OPS_PUB" "pkill -f '[g]enerator.py'; pkill -f '[v]erifier.py'; pkill -f '[c]haos.py'; true"

rpk_ops() {  # rpk against the broker, from the ops host
    on_host "$OPS_PUB" "docker run --rm --entrypoint rpk \
        docker.redpanda.com/redpandadata/redpanda:v24.2.7 \
        $1 --brokers $BROKER_ONE:9092"
}
rpk_ops "topic delete qual04-in" >/dev/null 2>&1 || true
# Deletion is asynchronous: wait for the broker to actually forget the
# topic, re-issuing the delete, instead of racing the create into
# TOPIC_ALREADY_EXISTS - a topic this run does not own carries a previous
# run's events, and reading those as this run's once produced 161,111
# phantom missing results on qual01.
gone=0
for _ in $(seq 1 30); do
    if ! rpk_ops "topic list" 2>/dev/null | awk '{print $1}' | grep -qx qual04-in; then
        gone=1; break
    fi
    rpk_ops "topic delete qual04-in" >/dev/null 2>&1 || true
    sleep 2
done
[ "$gone" = "1" ] || { echo "campaign: topic qual04-in still exists after 30 delete attempts" >&2; exit 2; }
rpk_ops "topic create qual04-in -p $PARTITIONS -r $REPLICATION"
echo "campaign: topic recreated ($PARTITIONS partitions, replication $REPLICATION)"

# --- ops host: generator + verifier + chaos -----------------------------
# Stop requests are FILES (see qual01/verifier.py: the spawn discipline
# starts these processes with SIGINT ignored, so a polite pkill -INT never
# reaches them). Stale stop files on a reused rig would make fresh
# processes exit on their first loop iteration.
on_host "$OPS_PUB" "rm -f /qual/q4-progress.json.stop /qual/q4-chaos.jsonl.stop"
for f in ../qual01/detspec.py ../qual01/generator.py verifier.py statesize.py endstate.py; do
    to_host "$OPS_PUB" "$HERE/$f" "/qual/$(basename "$f")"
done
to_host "$OPS_PUB" "$HERE/../chaos/chaos.py" /qual/chaos.py
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
to_host "$OPS_PUB" "$KEY_FILE" /root/.ssh/id_ed25519
on_host "$OPS_PUB" "chmod 600 /root/.ssh/id_ed25519 && \
    (pip3 install --break-system-packages -q confluent-kafka boto3 psycopg2-binary \
     || pip3 install -q confluent-kafka boto3 psycopg2-binary)"

on_host "$OPS_PUB" "pkill -f '[g]enerator.py'; pkill -f '[v]erifier.py'; pkill -f '[c]haos.py'; \
    rm -f /qual/q4-progress.json* /qual/q4-verdict.json /qual/q4-chaos.jsonl; true"

# The state-size instrument, measured from outside the engine.
# state_bytes reports LIVE keyed state (the newest manifest's referenced
# objects, deduplicated by content hash), NOT the store footprint. The
# store is append-only within a run - every update writes a new value
# object and nothing reclaims the old one - so the footprint grows with
# update volume and would let this campaign "reach" its target on
# garbage. state_detail records both, and their ratio.
state_bytes() { on_host "$OPS_PUB" "python3 /qual/statesize.py --endpoint $S3_ENDPOINT_OPS \
    --bucket $S3_BUCKET --prefix $S3_PREFIX --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY'"; }
state_detail() { on_host "$OPS_PUB" "python3 /qual/statesize.py --detail --endpoint $S3_ENDPOINT_OPS \
    --bucket $S3_BUCKET --prefix $S3_PREFIX --access-key '$S3_ACCESS_KEY' --secret-key '$S3_SECRET_KEY'"; }

# The state store must start EMPTY - what accumulates in it is this run's
# evidence, and a previous run's objects would inflate the size the
# summariser gates on.
PRE=$(state_bytes | tr -d '\r')
[ "${PRE:-1}" = "0" ] || { echo "campaign: the state bucket already holds $PRE byte(s)" >&2; exit 2; }

# The deferring backend. remote-read:// always reports
# supports_async_get(), which is what makes AggregateRowOp put its
# per-key accumulators in the backend instead of an in-memory map.
STATE_BACKEND_URI="remote-read://${S3_BUCKET}/${S3_PREFIX}?endpoint=${S3_ENDPOINT_WORKERS}&region=us-east-1&hot_max_bytes=${HOT_MAX_BYTES}"

BASE_MS=$(( $(date +%s) * 1000 ))
echo "$BASE_MS" > "$OUT_DIR/base_ms"

start_on_host "$OPS_PUB" q4-generator.log "python3 generator.py --brokers '$BROKER_LIST' \
    --topic qual04-in --rate $RATE --partitions $PARTITIONS --keys $KEYS \
    --seed $SEED --base-ms $BASE_MS --max-jitter-ms 0 \
    --window-ms 10000 --progress /qual/q4-progress.json"
echo "campaign: generator started"
# --- pipeline -----------------------------------------------------------
sed -e "s|__BROKERS__|$BROKER_LIST|g" -e "s|__CONNINFO__|$CONNINFO|g" \
    -e "s|__BLOB_BYTES__|$BLOB_BYTES|g" \
    "$HERE/pipeline.sql.tmpl" > "$OUT_DIR/pipeline.sql"

# --state-backend is what makes this a large-state campaign rather than
# an out-of-memory kill: on the default in-memory backend the SQL
# aggregate holds every accumulator in RAM. No campaign before this one
# passed the flag, so its plumbing is exercised here for the first time.
echo "campaign: state backend $STATE_BACKEND_URI"
"$SUBMIT_BIN" \
    --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --state-backend "$STATE_BACKEND_URI" \
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
# coordinator's registry, not the CLI binary's.
curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/connectors" \
    > "$OUT_DIR/cluster-connectors.json" 2>/dev/null \
    || echo "campaign: WARNING - could not retain the cluster's connector manifest" >&2

start_on_host "$OPS_PUB" q4-verifier.log "python3 verifier.py --dsn '$DSN' \
    --progress /qual/q4-progress.json --blob-bytes $BLOB_BYTES \
    --out /qual/q4-verdict.json --interval-s 20"
echo "campaign: verifier started"
# --- functional verification --------------------------------------------
# Nothing soaks until every link in the chain is proven to carry traffic.
echo "campaign: functional verification (nothing soaks until this passes)"

# 1. Input is flowing.
sleep 45
P1=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q4-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 30
P2=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q4-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
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

# 3. Rows are being maintained in the verification table. An aggregate
#    that never emits looks identical to a healthy one from clink's side,
#    so this is read from Postgres.
ROWS=0
for _ in $(seq 1 20); do
    ROWS=$(psql_q "SELECT count(*) FROM public.q4_out" | tr -d '\r')
    [ "${ROWS:-0}" -gt 0 ] && break
    sleep 15
done
[ "${ROWS:-0}" -gt 0 ] || verify_fail "no rows reached the verification table"
echo "campaign: verification table filling ($ROWS keys)"

# 4. The mechanism UNDER TEST is actually engaged, and this campaign's
#    is not the sink - it is where the KEYED STATE lives.
#
#    Rows appearing proves the pipeline runs; it does NOT prove the
#    accumulators are in the deferring backend rather than an in-memory
#    map, and that distinction is the entire campaign. On a non-deferring
#    backend this job would look identical right up to the point it was
#    killed for memory. So require state to be MEASURABLY present in the
#    store, read from outside the engine.
STATE_SEEN=0
for _ in $(seq 1 20); do
    STATE_SEEN=$(state_bytes | tr -d '\r')
    [ "${STATE_SEEN:-0}" -gt 0 ] && break
    sleep 15
done
[ "${STATE_SEEN:-0}" -gt 0 ] || verify_fail "the state store holds nothing.
  Rows are being produced, but the keyed state is not in the deferring
  backend - the aggregate is holding it in memory, which is the one
  configuration this campaign cannot be run on."
echo "campaign: keyed state confirmed in the deferring backend ($STATE_SEEN live bytes)"

# The accumulator must be the FULL configured width from the first rows
# on. A short blob here means the pipeline is not building the fat value
# the state target is computed from, and every size number afterwards
# would be measuring something else.
# IS DISTINCT FROM, not <>: a NULL blob_len makes `blob_len <> 4096`
# evaluate to NULL rather than true, so the count came back 0 and this
# gate passed against a column that was NULL in every row.
BAD_LEN=$(psql_q "SELECT count(*) FROM public.q4_out WHERE blob_len IS DISTINCT FROM $BLOB_BYTES" | tr -d '\r')
[ "${BAD_LEN:-1}" = "0" ] || verify_fail "$BAD_LEN row(s) carry an accumulator that is
  not $BLOB_BYTES bytes. The fat value is not being built as configured, so the
  state target would be measuring a different workload."
echo "campaign: accumulators are the full $BLOB_BYTES bytes"

# --- fill to the state target (no chaos yet) ------------------------------
# Faults must land AT size, or the campaign proves something about a
# small job. So the state is grown first, unperturbed, and only then does
# chaos start - which also makes the published claim unambiguous: N GiB
# of state, THEN the fault battery.
#
# Progress is polled from outside the engine. A fill that stalls is a
# finding in its own right (the aggregate stopped absorbing keys), so a
# stalled poll fails the run rather than soaking on regardless.
TARGET_BYTES=$(python3 -c "print(int(${STATE_TARGET_GIB} * 1024 ** 3))")
echo "campaign: filling to ${STATE_TARGET_GIB} GiB (${TARGET_BYTES} bytes) of keyed state"
FILL_START=$(date +%s)
FILL_LAST=0
FILL_STALLED_SINCE=$(date +%s)
FILL_REACHED=no
while [ $(( $(date +%s) - FILL_START )) -lt "$FILL_TIMEOUT_S" ]; do
    CUR=$(state_bytes | tr -d '\r')
    CUR=${CUR:-0}
    if [ "$CUR" -ge "$TARGET_BYTES" ]; then
        FILL_REACHED=yes
        break
    fi
    if [ "$CUR" -gt "$FILL_LAST" ]; then
        FILL_LAST=$CUR
        FILL_STALLED_SINCE=$(date +%s)
    elif [ $(( $(date +%s) - FILL_STALLED_SINCE )) -ge 600 ]; then
        verify_fail "keyed state stopped growing at $CUR bytes for 10 minutes while
  filling towards $TARGET_BYTES. The job is running but no longer absorbing
  state, which is a finding rather than a reason to soak."
    fi
    echo "campaign: $(date -u +%H:%M) fill $(python3 -c "print('%.2f' % ($CUR/1024**3))") / ${STATE_TARGET_GIB} GiB"
    sleep 60
done
if [ "$FILL_REACHED" != "yes" ]; then
    echo "campaign: WARNING - the fill did not reach ${STATE_TARGET_GIB} GiB within" >&2
    echo "  ${FILL_TIMEOUT_S}s (reached $(python3 -c "print('%.2f' % (${FILL_LAST:-0}/1024**3))") GiB). Soaking anyway;" >&2
    echo "  the summary will read this run as INCONCLUSIVE on its size gate." >&2
fi
FILL_BYTES=$(state_bytes | tr -d '\r')
FILL_KEYS=$(psql_q "SELECT count(*) FROM public.q4_out" | tr -d '\r')
echo "campaign: fill complete - $(python3 -c "print('%.2f' % (${FILL_BYTES:-0}/1024**3))") GiB across ${FILL_KEYS} keys"
{ echo "fill_bytes=${FILL_BYTES:-0}"; echo "fill_keys=${FILL_KEYS:-0}";
  echo "fill_reached_target=$FILL_REACHED";
  echo "fill_seconds=$(( $(date +%s) - FILL_START ))"; } > "$OUT_DIR/fill.txt"

# 5. Faults land, and land where clink can see them. Coverage-first: the
#    mandatory faults (including every 2PC crash window) are scheduled
#    before the weighted random tail, and each is fired-proofed by the
#    controller. The fireable 2PC point set comes from summarise.py's
#    TWOPC_POINTS - one source of truth, so the chaos schedule can never
#    require a point the summariser does not, nor the reverse.
Q4_POINTS=$(cd "$(dirname "$0")" && python3 -c "import summarise; print(','.join(summarise.TWOPC_POINTS))")
[ -n "$Q4_POINTS" ] || { echo "campaign: could not derive the 2PC point set from summarise.py" >&2; exit 78; }
start_on_host "$OPS_PUB" q4-chaos.log "python3 chaos.py --inventory /qual/inventory.json \
    --log /qual/q4-chaos.jsonl --coordinator-url http://${COORD_PRIV}:8095 \
    --job-id $JOB_ID --run-id $RUN_ID --profile $PROFILE --seed $SEED \
    --min-gap-s ${MIN_GAP_S:-120} --twopc-points $Q4_POINTS \
    --recovery-timeout-s $RECOVERY_TIMEOUT_S \
    --duration-s $(( DURATION_S + 1800 )) --ensure-coverage"
echo "campaign: chaos started (${DURATION_H}h, profile=$PROFILE, coverage-first)"

FAULTS=0
for _ in $(seq 1 60); do
    FAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q4-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
    [ "${FAULTS:-0}" -gt 0 ] && break
    sleep 20
done
[ "${FAULTS:-0}" -gt 0 ] || verify_fail "the chaos controller applied no fault in 20 minutes"
echo "campaign: chaos landing faults ($FAULTS recorded)"

# A recorded fault is not a landed fault. Read the engine's own count,
# on a POLL: the chaos log records the kill the moment it is issued,
# while the coordinator only declares the loss when its heartbeat lease
# expires seconds later (the local rig hit the race on its first fault).
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

# 6. The job survives the fault AND keeps folding events into state.
#
# Polled, not slept. Two things make a single 90-second look wrong at
# size: a restart drain followed by a restore takes longer than it does
# for a small job, and the replay from the last checkpoint re-folds
# events the accumulators had already counted, so the total sits FLAT
# until the job catches back up past the point it died. A one-shot probe
# reads both as "the engine stopped".
BEFORE=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q4_out" | tr -d '\r')
AFTER=$BEFORE
RECOVERED=no
for _ in $(seq 1 "${RECOVER_PROBES:-20}"); do
    sleep 30
    POST=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" 2>/dev/null) || continue
    python3 -c "
import json,sys
j = json.loads(sys.argv[1])
sys.exit(0 if j.get('status') == 'RUNNING' else 1)" "$POST" || continue
    AFTER=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q4_out" | tr -d '\r')
    if [ "${AFTER:-0}" -gt "${BEFORE:-0}" ]; then RECOVERED=yes; break; fi
done
[ "$RECOVERED" = "yes" ] || verify_fail "the job did not resume folding events within
  $(( ${RECOVER_PROBES:-20} * 30 ))s of the first fault ($BEFORE -> $AFTER). Either the
  restart never completed or the job is not making progress; the container logs
  collected alongside this message say which."
echo "campaign: recovered and still folding events ($BEFORE -> $AFTER) - VERIFICATION PASSED, entering soak"

{ echo "campaign=QUAL-04"; echo "run_id=$RUN_ID"; echo "job_id=$JOB_ID";
  echo "input_events_observed=$P2"; echo "events_folded_at_gate=$AFTER";
  echo "blob_bytes=$BLOB_BYTES"; echo "keys_configured=$KEYS";
  echo "state_backend=$STATE_BACKEND_URI";
  echo "hot_max_bytes=$HOT_MAX_BYTES";
  echo "state_target_gib=$STATE_TARGET_GIB";
  echo "recovery_timeout_s=$RECOVERY_TIMEOUT_S";
  echo "faults_recorded=$FAULTS"; echo "workers_lost_observed_by_coordinator=$LOST";
  echo "recovered_after_first_fault=yes";
} > "$OUT_DIR/verification.txt"
# --- soak (2-minute cadence, oracle fail-fast) ----------------------------
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
    for f in q4-verdict.json q4-chaos.jsonl q4-progress.json q4-generator.log q4-verifier.log q4-chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    DIRTY=$(python3 - "$OUT_DIR/q4-verdict.json" <<'PY' || echo ""
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
        on_host "$OPS_PUB" "touch /qual/q4-chaos.jsonl.stop"
        on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py' || true"
        collect_container_logs
        break
    fi

    # Is anything still applying faults? A soak with no fault generator
    # alive looks exactly like a soak that survived the faults.
    if [ -z "$CHAOS_DIED_AT" ] \
       && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/q4-chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        echo "campaign: WARNING - the chaos controller is no longer running (noticed ${CHAOS_DIED_AT}," \
             "${NFAULTS} fault record(s) written)." >&2
        { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
          echo "fault_records_at_death=$NFAULTS";
          echo "tail:"; on_host "$OPS_PUB" "tail -20 /qual/q4-chaos.log" 2>/dev/null || true;
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

    SD=$(state_detail 2>/dev/null | tr -d '\r' || true)
    SLIVE=$(echo "$SD" | awk -F= '/^state_gib=/{print $2}')
    SFOOT=$(echo "$SD" | awk -F= '/^state_footprint_gib=/{print $2}')
    SK=$(psql_q "SELECT count(*) FROM public.q4_out" 2>/dev/null | tr -d '\r' || echo '?')
    echo "campaign: $(date -u +%H:%M) soak, ${SLIVE:-?} GiB live state (${SFOOT:-?} GiB store footprint), $SK keys"
done
# --- drain and final judgement ------------------------------------------
echo "campaign: soak complete, draining"
# Stop requests are FILES; the pkill -INT is a courtesy for interactive
# runs only. Chaos first and ORDERLY: mid-fault it holds tc rules, armed
# points, and possibly a PAUSED store container, so it exits between
# faults, clearing them.
on_host "$OPS_PUB" "touch /qual/q4-chaos.jsonl.stop"
cwaited=0
while [ "$cwaited" -lt 120 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5
    cwaited=$(( cwaited + 5 ))
done
[ "$cwaited" -lt 120 ] \
    || echo "campaign: WARNING - the chaos controller is still running 120s after its stop request; a fault may straddle the drain" >&2
# Belt and braces: a chaos controller killed MID-s3_unavailable leaves the
# store paused forever, and everything downstream of here reads as loss.
on_host "$OPS_PUB" "docker unpause qual04-minio >/dev/null 2>&1; true"
on_host "$OPS_PUB" "touch /qual/q4-progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py'; pkill -INT -f '[g]enerator.py'; true"
gwaited=0
while [ "$gwaited" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5
    gwaited=$(( gwaited + 5 ))
done
[ "$gwaited" -lt 60 ] \
    || echo "campaign: WARNING - the generator is still producing 60s after its stop request" >&2
echo "campaign: waiting for the pipeline to catch up with the generator"
CATCHUP=no
PRODUCED_FINAL=$(on_host "$OPS_PUB" "python3 -c \"
import json;d=json.load(open('/qual/q4-progress.json'));print(sum(d['produced_high'].values()))\"" 2>/dev/null || echo 0)
cwait=0
LAST_FOLDED=-1
STALL_S=0
while [ "$cwait" -lt "${CATCHUP_TIMEOUT_S:-1800}" ]; do
    FOLDED=$(psql_q "SELECT coalesce(sum(n), 0) FROM public.q4_out" | tr -d '\r')
    if [ "${FOLDED:-0}" -ge "${PRODUCED_FINAL:-1}" ]; then CATCHUP=yes; break; fi
    if [ "${FOLDED:-0}" -le "${LAST_FOLDED}" ]; then
        STALL_S=$(( STALL_S + 30 ))
        # Caught up as far as it is ever going to: stop waiting out a
        # deadline that will not change the answer.
        # A mid-recovery pipeline legitimately makes no progress for
        # minutes: a restart drain plus a restore plus the replay back to
        # where it died. 300s cut the wait short during exactly that, and
        # the run was then judged on a partial read.
        [ "$STALL_S" -ge "${CATCHUP_STALL_S:-900}" ] && break
    else
        STALL_S=0
    fi
    LAST_FOLDED=${FOLDED:-0}
    echo "campaign: catch-up ${FOLDED:-0} / ${PRODUCED_FINAL} events"
    sleep 30
    cwait=$(( cwait + 30 ))
done
{ echo "caught_up=$CATCHUP"; echo "produced_final=${PRODUCED_FINAL}";
  echo "folded_at_catchup=${FOLDED:-0}"; echo "catchup_seconds=$cwait";
} > "$OUT_DIR/catchup.txt"
if [ "$CATCHUP" = "yes" ]; then
    echo "campaign: pipeline caught up (${FOLDED} events folded)"
else
    echo "campaign: WARNING - the pipeline did not catch up with the generator" >&2
    echo "  (${FOLDED:-0} of ${PRODUCED_FINAL}). The exact-accounting check cannot" >&2
    echo "  distinguish lost events from unread ones, so the summary will read this" >&2
    echo "  run as INCONCLUSIVE rather than judge correctness on a partial read." >&2
fi

# The verdict is final only over a SETTLED bucket: at least one fresh
# sample after the drain AND two consecutive samples seeing the same line
# count. Killing the verifier before that reads a moving store.
QUIESCED=no
waited=0
prev_lines=-1
while [ "$waited" -lt "$FINAL_WAIT_S" ]; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/q4-verdict.json" "$OUT_DIR/" 2>/dev/null || true
    lines=$(python3 - "$OUT_DIR/q4-verdict.json" <<'PY' || echo -1
import json, sys
try:
    v = json.load(open(sys.argv[1]))
    print((v.get("last_stats") or {}).get("sum_n", -1))
except Exception:
    print(-1)
PY
)
    if [ "${lines:--1}" -ge 0 ] && [ "$lines" = "$prev_lines" ]; then
        QUIESCED=yes
        break
    fi
    prev_lines="$lines"
    sleep 25
    waited=$(( waited + 25 ))
done
if [ "$QUIESCED" = "yes" ]; then
    echo "campaign: verdict quiesced after ${waited}s (lines_total=$prev_lines across two samples)"
else
    echo "campaign: WARNING - the verdict never quiesced within ${FINAL_WAIT_S}s; the summary will read this run as incomplete" >&2
fi
echo "quiesced=$QUIESCED" > "$OUT_DIR/final-quiesce.txt"

# The state size that the summary's size gate reads, captured BEFORE the
# job is cancelled: this is the campaign's headline number and it must be
# measured while the state still exists.
state_detail > "$OUT_DIR/state-size-final.txt" 2>/dev/null \
    || echo "campaign: WARNING - could not measure the final state size" >&2
cat "$OUT_DIR/state-size-final.txt" 2>/dev/null || true

# The authoritative correctness pass, in a FRESH process (endstate.py):
# exact accounting (every produced event folded in exactly once) and
# per-key verification of a seeded sample against detspec. The
# incremental oracle rides its own running totals, and a campaign must
# not let the oracle certify itself. Both judgements need a settled
# table and a stopped generator, which is why they wait until here.
on_host "$OPS_PUB" "python3 /qual/endstate.py --dsn '$DSN' \
    --progress /qual/q4-progress.json --seed $SEED --partitions $PARTITIONS \
    --keys $KEYS --blob-bytes $BLOB_BYTES --sample-keys ${SAMPLE_KEYS:-2000}" \
    > "$OUT_DIR/completeness.txt" \
    || echo "campaign: WARNING - the end-state pass failed; correctness has no evidence" >&2
cat "$OUT_DIR/completeness.txt" 2>/dev/null || true

curl -fsS -X POST "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}/cancel" >/dev/null 2>&1 || true
sleep 30

kill_campaign_processes "$OPS_PUB" || true
STILL=$(on_host "$OPS_PUB" "pgrep -f '[g]enerator.py|[v]erifier.py|[c]haos.py' | wc -l" | tr -d ' \r')
[ "${STILL:-0}" = "0" ] || echo "campaign: WARNING - $STILL campaign process(es) survived the drain" >&2

for f in q4-verdict.json q4-chaos.jsonl q4-progress.json q4-generator.log q4-verifier.log q4-chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs
curl -fsS "http://${COORD_PUB}:8095/metrics" > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null || true

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --duration-h "$DURATION_H" --profile "$PROFILE" \
    --state-target-gib "$STATE_TARGET_GIB" > "$OUT_DIR/QUAL-04-summary.md"
cat "$OUT_DIR/QUAL-04-summary.md"

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh <rig-run-id> --yes && qualification/infra/teardown.sh --check"
