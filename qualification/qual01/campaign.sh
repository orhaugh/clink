#!/usr/bin/env bash
# QUAL-01: Kafka exactly-once campaign driver.
#
# Brings up the rig, deploys the stack, starts the generator and the
# independent verifier on the ops host, submits the pipeline, runs the
# chaos controller for the campaign duration, then collects evidence and
# writes the standard summary. Teardown is the caller's responsibility
# (scripts/qualification/destroy.sh <run-id> --yes) so a FAILED campaign
# can be inspected before its rig disappears - but the run id is printed
# on every exit path so nothing is left unfindable.
#
#   RUN_ID=qual01-20260815 DURATION_H=168 ./campaign.sh
#   RUN_ID=... SKIP_PROVISION=1 ./campaign.sh     # reuse a live rig
#   RUN_ID=... DURATION_H=2 PROFILE=aggressive ./campaign.sh   # shakedown
#
# The shakedown (DURATION_H=2) is not optional before a multi-day run: it
# proves the oracle, the chaos controller and the evidence pipeline on the
# real rig for the price of two hours, and every long campaign that
# skipped it has instead discovered its harness bugs on day three.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

RUN_ID="${RUN_ID:?set RUN_ID (e.g. qual01-20260815)}"
DURATION_H="${DURATION_H:-168}"
PROFILE="${PROFILE:-steady}"
RATE="${RATE:-2000}"
PARTITIONS="${PARTITIONS:-4}"
KEYS="${KEYS:-100000}"
SEED="${SEED:-20260815}"
WINDOW_MS="${WINDOW_MS:-10000}"
MAX_JITTER_MS="${MAX_JITTER_MS:-1500}"
# The watermark lag must STRICTLY exceed the generator's jitter or
# in-window records are legitimately late and the "expected" set the
# oracle computes is not the set the engine is obliged to produce. This
# is the campaign's central premise, so it is asserted, not assumed.
WM_LAG_MS="${WM_LAG_MS:-4000}"
if [ "$WM_LAG_MS" -le "$MAX_JITTER_MS" ]; then
    echo "campaign: WM_LAG_MS ($WM_LAG_MS) must exceed MAX_JITTER_MS ($MAX_JITTER_MS)" >&2
    exit 2
fi
CHECKPOINT_INTERVAL_MS="${CHECKPOINT_INTERVAL_MS:-10000}"
# The worker-loss restart budget is a LIFETIME cap that is never reset,
# and its default resolves to 10. A campaign that kills a worker every
# few minutes for days would spend it in the first hour and then measure
# a stopped job, so it is set deliberately and generously here.
MAX_RESTARTS="${MAX_RESTARTS:-100000}"
CLINK_IMAGE="${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"
KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/clink-qual-ed25519}"
OUT_DIR="$REPO_ROOT/qualification-results/$RUN_ID"
# The submit CLI. Overridable so the driver can be exercised against a
# stub - see qualification/test/test_campaign.sh - and so a build in a
# non-default directory does not need the script edited.
SUBMIT_BIN="${SUBMIT_BIN:-$REPO_ROOT/build/clink_submit_sql}"
# Host-side prerequisites are validated BEFORE any server is provisioned:
# a missing submit binary discovered at submit time has already paid for
# a full provision + image pull (qual01-smoke-d died exactly there, seven
# minutes and four servers in).
if [ ! -x "$SUBMIT_BIN" ]; then
    echo "campaign: SUBMIT_BIN missing or not executable: $SUBMIT_BIN" >&2
    echo "campaign: build clink_submit_sql in $REPO_ROOT/build or set SUBMIT_BIN" >&2
    exit 78
fi
mkdir -p "$OUT_DIR"
# A relaunch reusing a RUN_ID must not inherit the previous attempt's
# evidence: the watch loop reads $OUT_DIR/verdict.json BEFORE the first
# scp is guaranteed to have replaced it, so a stale dirty verdict would
# abort a healthy run, and a stale job-gone.txt or failure.txt poisons
# the summary. Scoped to the files this campaign writes; archives from
# prior runs under other RUN_IDs are untouched.
rm -f "$OUT_DIR/verdict.json" "$OUT_DIR/job-gone.txt" "$OUT_DIR/chaos-died.txt" \
      "$OUT_DIR/chaos-completed.txt" \
      "$OUT_DIR/failure.txt" "$OUT_DIR/job-status.json" "$OUT_DIR/verification.txt" \
      "$OUT_DIR/chaos.jsonl" "$OUT_DIR/progress.json"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 -o BatchMode=yes -i "$KEY_FILE")
on_host() { ssh -n "${SSH_OPTS[@]}" "root@$1" "$2"; }
to_host()  { scp "${SSH_OPTS[@]}" -q "$2" "root@$1:$3"; }

# Retain the coordinator's and every worker's CONTAINER logs in the
# evidence. qual01-20260819f failed with a real engine finding and its
# cluster logs were destroyed with the rig - nothing here captured them,
# and the teardown-always discipline (correct for cost) means nobody gets
# a second chance. Called on the oracle-dirty fail-fast AND in the final
# collection; last call wins, bounded via --tail. Best-effort per host,
# loud per miss.
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

# Start a long-running process on a host and RETURN.
#
# ssh holds its channel open until every process holding the remote
# command's stdout and stderr has exited, so a plain `cmd &` leaves the
# driver blocked on a step it believes it finished - which is exactly
# how this campaign hung twice, once with the generator already happily
# producing 90,000 events while the driver waited. Detaching needs all
# three: a new session (setsid), every descriptor redirected, and the
# remote shell exiting explicitly.
start_on_host() {  # host, log-name, command
    local host=$1 log=$2 cmd=$3
    on_host "$host" "cd /qual && (setsid nohup $cmd </dev/null >/qual/$log 2>&1 &) ; exit 0"
    # Prove it actually started, rather than trusting a backgrounded
    # command that may have died on its first line.
    sleep 3
    local script; script=$(echo "$cmd" | awk '{print $2}')
    # Bracketed for the same reason as above: an unbracketed pattern also
    # matches the ssh command carrying it, so the check would pass even
    # for a process that never started.
    local pattern="[${script:0:1}]${script:1}"
    on_host "$host" "pgrep -f '$pattern' >/dev/null" \
        || { echo "campaign: $log did not start - see /qual/$log on $host" >&2
             on_host "$host" "tail -20 /qual/$log" >&2 || true; exit 3; }
}


# Kill the previous run's processes and PROVE they are gone.
#
# SIGINT alone does not do it. Run b's drain sent one, the campaign warned
# that two processes had survived, and thirty minutes later its generator
# was still producing - into a topic the next run had just deleted, which
# it promptly recreated with default settings and thereby aborted that run
# before it started. So the signal escalates and the result is verified,
# because "asked it to stop" and "it stopped" are different facts.
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
    echo "campaign: could not stop the previous run's processes on $host" >&2
    return 1
}

echo "campaign: QUAL-01 run $RUN_ID, ${DURATION_H}h, profile=$PROFILE"

# Which rig to run against. Defaults to this run's own, which is what a
# fresh campaign wants. RIG_RUN_ID points a second campaign at hosts that
# already exist - a re-run proving a fix, or a different campaign reusing
# the rig - so the evidence still lands in its own directory while the
# infrastructure is paid for once. The inventory stays API-derived either
# way; this changes which label is queried, not where the answer comes from.
RIG_RUN_ID="${RIG_RUN_ID:-$RUN_ID}"
if [ "$RIG_RUN_ID" != "$RUN_ID" ]; then
    echo "campaign: running against the existing rig labelled qual-run=$RIG_RUN_ID"
    SKIP_PROVISION=1
fi

if [ "${SKIP_PROVISION:-0}" != "1" ]; then
    RUN_ID="$RUN_ID" "$REPO_ROOT/qualification/infra/provision.sh"
    echo "campaign: waiting for cloud-init on every host"
fi

# Inventory, read from the API by label - never hand-maintained.
# One inventory implementation for every tool (provision.sh writes it at
# rig creation; this refresh covers a rig reused under a new run id).
RUN_ID="$RUN_ID" "$HERE/../infra/inventory.sh" "$RIG_RUN_ID" "$OUT_DIR"

read_inv() { python3 -c "
import json
inv = json.load(open('${OUT_DIR}/inventory.json'))
print(*[h['$2'] for h in inv['hosts'] if h['role'] == '$1'])
"; }

OPS_PUB=$(read_inv ops public_ip)
COORD_PUB=$(read_inv coordinator public_ip)
COORD_PRIV=$(read_inv coordinator private_ip)
WORKER_PUBS=$(read_inv worker public_ip)
BROKER_PRIVS=$(read_inv broker private_ip)
BROKER_LIST=$(for ip in $BROKER_PRIVS; do printf "%s:9092," "$ip"; done | sed 's/,$//')
# Redpanda's --seeds takes RPC addresses (33145), not Kafka ones. Seeding
# a cluster with the Kafka port leaves each node forming its own
# single-node cluster, which looks healthy per-node and silently gives
# the campaign no replication to lose when a broker is restarted.
SEED_LIST=$(for ip in $BROKER_PRIVS; do printf "%s:33145," "$ip"; done | sed 's/,$//')
echo "campaign: brokers=$BROKER_LIST coordinator=$COORD_PRIV ops=$OPS_PUB"

# Brokers included: run C's deploy raced broker docker readiness because
# only ops/coordinator/workers were gated here, and the broker compose
# below then failed on the slowest host.
for h in $OPS_PUB $COORD_PUB $WORKER_PUBS $(read_inv broker public_ip); do
    until on_host "$h" "docker info >/dev/null 2>&1"; do
        echo "campaign: waiting for docker on $h"; sleep 15
    done
done

# Shared checkpoint state, mounted from the ops host's NFS export.
# Checkpoint state is per-subtask (<dir>/v1/<subtask_idx>/), resolved on
# each node's own filesystem, so a killed worker's subtask redeployed to
# a different host would find nothing and the restore would refuse. On
# per-host local disks this campaign would measure failed restores
# rather than recovery.
OPS_PRIV=$(read_inv ops private_ip)
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "mkdir -p /qual/state && (mountpoint -q /qual/state || \
        mount -t nfs -o vers=4,hard,timeo=100 ${OPS_PRIV}:/qual/state /qual/state)"
    on_host "$h" "mountpoint -q /qual/state" \
        || { echo "campaign: /qual/state is not a shared mount on $h - refusing to run a" >&2
             echo "  worker-kill campaign on per-host local state; recovery would fail by" >&2
             echo "  construction and the result would be meaningless." >&2; exit 2; }
done
# Prove it is genuinely shared, not just mounted.
# Each run gets its OWN checkpoint directory under the shared mount.
#
# Runs used to share /qual/state, and CONFIRMED-N markers live under
# <checkpoint_dir>/_jobs/<job_id>/ while job ids restart at 1 with every
# coordinator. A new run's job id 1 therefore inherited the previous run's
# job id 1 confirmed checkpoint, and its first restart restored from a
# checkpoint belonging to a different run - re-emitting output it had
# already committed. clink no longer seeds a fresh job that way, but a
# campaign should not depend on that to keep its runs apart: two runs
# sharing a checkpoint directory are not independent experiments.
on_host "$OPS_PUB" "mkdir -p /qual/state/$RUN_ID"
on_host "$OPS_PUB" "echo shared-$$ > /qual/state/.probe"
for h in $COORD_PUB $WORKER_PUBS; do
    on_host "$h" "grep -q shared-$$ /qual/state/.probe" \
        || { echo "campaign: /qual/state on $h does not see the ops host's writes" >&2; exit 2; }
done
echo "campaign: shared state verified on the coordinator and every worker"

# --- stack -------------------------------------------------------------
echo "campaign: deploying stack"
i=0
for bp in $(read_inv broker public_ip); do
    i=$((i+1))
    priv=$(echo "$BROKER_PRIVS" | cut -d' ' -f$i)
    on_host "$bp" "mkdir -p /qual"
    to_host "$bp" "$HERE/../infra/broker.yml" /qual/broker.yml
    on_host "$bp" "cd /qual && NODE_ID=$i PRIVATE_IP=$priv SEEDS='$SEED_LIST' docker compose -f broker.yml up -d"
done

# Every host gets a /qual/.env, which docker compose reads automatically
# from its project directory. The deploy below then passes nothing on the
# command line that is not also in that file.
#
# The reason is the chaos controller. It restarts a killed container with
# `cd /qual && docker compose -f worker.yml up -d` and no environment,
# because it cannot know what the campaign deployed. With the settings
# passed only as inline environment on the original command, that restart
# silently fell back to the compose defaults: the worker came back as
# ghcr.io/orhaugh/clink-runtime:main - a different build entirely from the
# image under test - with an empty --coordinator-host, and exited 1. The
# controller's own assertion then failed, which killed it, and QUAL-01
# soaked for an hour with one fault applied and no fault generator alive.
# Had that container merely STARTED, the campaign would have gone on
# measuring a cluster running two versions of clink at once.
#
# So the configuration lives on the host, and anything that restarts a
# container reproduces the deployment rather than guessing at it.
# The coordinator's HA directory is wiped before every campaign. It
# persists on the host, and recover_persisted_jobs() resurrects whatever
# job manifests it finds - which is exactly right WITHIN a run (that is
# the recovery under test) and exactly wrong ACROSS runs: reusing the rig
# for qual01-20260817e restarted the coordinator over run d's HA dir, run
# d's job came back as a zombie twin consuming the recreated input topic
# (its restored offsets were past the fresh topic's end, so earliest
# rewound it to zero), and every window landed on the output topic twice
# under two different transactional ids. The verifier counted 177k
# identical duplicates against an engine that was doing precisely what
# its HA contract says. A campaign never wants another campaign's jobs.
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
    # Same reuse rule as the coordinator: a surviving worker container from
    # a previous run still hosts that run's task processes, and `up -d`
    # with an unchanged environment would leave it untouched. Every
    # campaign starts its workers fresh.
    on_host "$wp" "docker rm -f clink-worker >/dev/null 2>&1 || true"
    on_host "$wp" "mkdir -p /qual /qual/state"
    to_host "$wp" "$HERE/../infra/worker.yml" /qual/worker.yml
    on_host "$wp" "printf 'CLINK_IMAGE=%s\nCONTROL_IP=%s\nWORKER_ID=w%s\nWORKER_IP=%s\n' \
        '$CLINK_IMAGE' '$COORD_PRIV' '$wid' '$wpriv' > /qual/.env"
    on_host "$wp" "cd /qual && docker compose -f worker.yml up -d"
done

# The deployed build's own capability manifest, captured as evidence
# BEFORE the campaign runs: it is what the verdict's claims are scoped
# to, and it is where the fault-injection surface is asserted rather
# than assumed. A `twopc` profile against a build with no fault points
# would arm CLINK_FAULT_INJECT into a binary that compiles every fault
# point to nothing - reporting faults it never injected.
until on_host "$COORD_PUB" "docker exec clink-coordinator clink --capabilities-json" \
        > "$OUT_DIR/capabilities.json" 2>/dev/null; do
    echo "campaign: waiting for the coordinator to answer --capabilities-json"; sleep 10
done
FAULT_SURFACE=$(python3 -c "
import json; d=json.load(open('$OUT_DIR/capabilities.json'))
print('yes' if (d.get('build') or d).get('fault_injection') else 'no')")
echo "campaign: fault-injection surface: $FAULT_SURFACE"
if [ "$PROFILE" = "twopc" ] && [ "$FAULT_SURFACE" != "yes" ]; then
    echo "campaign: profile 'twopc' needs a build with CLINK_ENABLE_FAULT_INJECTION=ON;" >&2
    echo "  the deployed image has no fault surface, so the targeted 2PC-window faults" >&2
    echo "  would be silently skipped. Build a qualification image:" >&2
    echo "  docker build --build-arg CLINK_ENABLE_FAULT_INJECTION=ON -f docker/Dockerfile.runtime ." >&2
    exit 2
fi

# Topics: partition count is load-bearing. Source parallelism equals it
# (the configuration the per-partition watermark work proved exact), and
# the oracle's per-partition sequences assume exactly these partitions.
# DELETED and recreated, not created-if-absent.
#
# The oracle recomputes expectations from ONE generator spec, so a topic
# still holding a previous run's events is a stream the oracle does not
# know about: on the first cloud run the input topic carried two
# generations with different event-time bases and the verifier reported
# 161,111 missing results for output that was never its run's to produce.
# A campaign must own its topics outright.
# Stop the previous run's processes BEFORE recreating topics, not after.
# A leftover generator auto-creates the topic the moment this campaign
# deletes it - with the broker's default one partition, no replication -
# and the campaign then refuses to run against a topic it does not own.
# That is the right refusal, arriving after the avoidable cause.
kill_campaign_processes "$OPS_PUB" || exit 2
on_host "$OPS_PUB" "rm -f /qual/progress.json* /qual/verdict.json /qual/chaos.jsonl; true"

BROKER1_PUB=$(read_inv broker public_ip | cut -d' ' -f1)
for t in qual01-in qual01-out; do
    on_host "$BROKER1_PUB" "docker exec redpanda rpk topic delete $t >/dev/null 2>&1 || true"
done
# Deletion is asynchronous, so wait for the broker to actually forget the
# topic instead of sleeping and hoping. A fixed sleep raced it and the
# create came back TOPIC_ALREADY_EXISTS, which the campaign correctly
# refused to continue past - a topic it does not own carries a previous
# run's events, and reading those as this run's once produced 161,111
# phantom missing results.
for t in qual01-in qual01-out; do
    gone=0
    for _ in $(seq 1 30); do
        if ! on_host "$BROKER1_PUB" \
                "docker exec redpanda rpk topic list 2>/dev/null | awk '{print \$1}' | grep -qx $t"; then
            gone=1; break
        fi
        on_host "$BROKER1_PUB" "docker exec redpanda rpk topic delete $t >/dev/null 2>&1 || true"
        sleep 2
    done
    [ "$gone" = "1" ] || { echo "campaign: topic $t still exists after 30 delete attempts;" >&2
                           echo "  refusing to run against a topic this campaign does not own" >&2
                           exit 2; }
done
for t in qual01-in qual01-out; do
    on_host "$BROKER1_PUB" "docker exec redpanda rpk topic create $t -p $PARTITIONS -r 3" \
        || { echo "campaign: could not create topic $t" >&2; exit 2; }
done
echo "campaign: topics recreated ($PARTITIONS partitions, replication 3)"

# --- ops host: generator + verifier + chaos -----------------------------
on_host "$OPS_PUB" "mkdir -p /qual"
# A stale stop file from a previous run on a reused rig would make the
# fresh generator and verifier exit on their first loop iteration - the
# stop protocol is files (see the drain phase), so the files must start
# absent.
on_host "$OPS_PUB" "rm -f /qual/progress.json.stop /qual/verdict.json.stop /qual/chaos.jsonl.stop"
for f in detspec.py generator.py verifier.py; do to_host "$OPS_PUB" "$HERE/$f" "/qual/$f"; done
to_host "$OPS_PUB" "$HERE/../chaos/chaos.py" /qual/chaos.py
to_host "$OPS_PUB" "$OUT_DIR/inventory.json" /qual/inventory.json
to_host "$OPS_PUB" "$KEY_FILE" /root/.ssh/id_ed25519
on_host "$OPS_PUB" "chmod 600 /root/.ssh/id_ed25519 && pip3 install --break-system-packages -q confluent-kafka || pip3 install -q confluent-kafka"
# A fixed event-time base makes the whole campaign reproducible: the
# oracle's window boundaries do not depend on when it was started.
BASE_MS=$(( $(date +%s) * 1000 ))
echo "$BASE_MS" > "$OUT_DIR/base_ms"

start_on_host "$OPS_PUB" generator.log "python3 generator.py --brokers '$BROKER_LIST' \
    --topic qual01-in --rate $RATE --partitions $PARTITIONS --keys $KEYS \
    --seed $SEED --base-ms $BASE_MS --max-jitter-ms $MAX_JITTER_MS \
    --window-ms $WINDOW_MS --progress /qual/progress.json"
echo "campaign: generator started"

# --- pipeline -----------------------------------------------------------
sed -e "s|__BROKERS__|$BROKER_LIST|g" -e "s|__WM_LAG_MS__|$WM_LAG_MS|g" \
    -e "s|__TXN_ID__|qual01-${RUN_ID}|g" "$HERE/pipeline.sql.tmpl" > "$OUT_DIR/pipeline.sql"

# clink_submit_sql POSTs to the coordinator's HTTP API, so the port here
# is the HTTP one (8095), not the binary control plane (6123).
"$SUBMIT_BIN" \
    --file "$OUT_DIR/pipeline.sql" \
    --coordinator-host "$COORD_PUB" --coordinator-port 8095 \
    --parallelism "$PARTITIONS" \
    --checkpoint-dir "/qual/state/$RUN_ID" \
    --checkpoint-interval-ms "$CHECKPOINT_INTERVAL_MS" \
    --max-restarts-on-worker-loss "$MAX_RESTARTS" \
    | tee "$OUT_DIR/submit.log"
# The submit response is one JSON object per compiled statement:
# {"ok":true,"job_id":N,"name":"..."}. Parsed directly rather than
# scraped, so a changed message cannot silently yield an empty id.
JOB_ID=$(python3 - "$OUT_DIR/submit.log" <<'PYEOF'
import json, re, sys
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

# The connector manifest the CLUSTER actually has. `clink --capabilities-json`
# above reports the CLI binary's registry, and the CLI does not link the
# connector implementations - it lists six where the coordinator lists
# thirty-three, including the Kafka connector this campaign is about. The
# build flags it reports are right; its connector list is not the evidence it
# looks like.
curl -fsS --max-time 20 "http://${COORD_PUB}:8095/api/v1/connectors" \
    > "$OUT_DIR/cluster-connectors.json" 2>/dev/null \
    || echo "campaign: WARNING - could not retain the cluster's connector manifest" >&2

start_on_host "$OPS_PUB" verifier.log "python3 verifier.py --brokers '$BROKER_LIST' \
    --topic qual01-out --spec /qual/progress.json.spec --progress /qual/progress.json \
    --verdict /qual/verdict.json"
echo "campaign: verifier started"

# --- functional verification --------------------------------------------
#
# Prove the campaign is actually WORKING before leaving it to soak.
#
# Every failure this harness has had so far was silent: a chaos gate that
# could never fire, a verifier that read a partial topic, a job that
# could not checkpoint. All of them would have run for days and produced
# a confident-looking page of nulls. So nothing proceeds to the soak
# until each link in the chain is observed doing its job, and any link
# that cannot be observed aborts the run while the rig is still cheap to
# inspect.
echo "campaign: functional verification (nothing soaks until this passes)"
verify_fail() { echo "campaign: VERIFICATION FAILED - $1" >&2
                # The cluster logs ARE the evidence for a verification
                # failure (QUAL-02's first local run failed here and left
                # nothing to diagnose with until the still-live rig was
                # dug through by hand).
                collect_container_logs || true
                echo "  rig is still up for inspection; tear down with:" >&2
                echo "  scripts/qualification/destroy.sh $RUN_ID --yes" >&2
                exit 3; }

# 1. Input is flowing: the generator's progress must advance.
sleep 30
P1=$(on_host "$OPS_PUB" "python3 -c \"import json;print(sum(json.load(open('/qual/progress.json'))['produced_high'].values()))\"" 2>/dev/null || echo 0)
sleep 30
P2=$(on_host "$OPS_PUB" "python3 -c \"import json;print(sum(json.load(open('/qual/progress.json'))['produced_high'].values()))\"" 2>/dev/null || echo 0)
[ "${P2:-0}" -gt "${P1:-0}" ] || verify_fail "the generator is not producing (progress stuck at ${P1:-unknown})"
echo "campaign: input flowing ($P1 -> $P2 events)"

# 2. The job is RUNNING and CHECKPOINTING. Without completed checkpoints
#    the transactional sink never commits, so the campaign would measure
#    a pipeline that cannot be exactly-once by construction.
JOB_JSON=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "cannot read job status"
echo "$JOB_JSON" > "$OUT_DIR/job-status.json"
JOB_OK=$(python3 -c "
import json,sys
j = json.loads(sys.argv[1])
print('yes' if j.get('status') == 'RUNNING' and j.get('latest_completed_checkpoint_id', 0) > 0
      and not j.get('errors') else 'no')" "$JOB_JSON")
[ "$JOB_OK" = "yes" ] || verify_fail "the job is not running with completed checkpoints: $JOB_JSON"
echo "campaign: job RUNNING with completed checkpoints"

# 2b. EXACTLY ONE job. The campaign submitted one pipeline; a second
# active job means something else is producing into this run's topics -
# run e's coordinator resurrected the previous run's job from a stale HA
# store and every window was committed twice by two individually-correct
# jobs. The HA wipe above prevents that path, and this gate refuses any
# other source of a second job before the soak can spend on it.
JOBS_JSON=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs") || verify_fail "cannot list jobs"
ACTIVE_JOBS=$(python3 -c "
import json,sys
doc = json.loads(sys.argv[1])
jobs = doc.get('jobs', doc) if isinstance(doc, dict) else doc
print(len(jobs))" "$JOBS_JSON")
[ "$ACTIVE_JOBS" = "1" ] || verify_fail "expected exactly one job on the coordinator, found ${ACTIVE_JOBS}: $JOBS_JSON"
echo "campaign: exactly one job on the coordinator"

# 3. Output is reaching the sink topic, committed.
OUT_N=$(on_host "$OPS_PUB" "python3 -c \"import json;print(json.load(open('/qual/verdict.json'))['output_records'])\"" 2>/dev/null || echo 0)
[ "${OUT_N:-0}" -gt 0 ] || verify_fail "the verifier has seen no committed output records"
echo "campaign: verifier observing committed output ($OUT_N records)"

# 4. The verifier is JUDGING, not merely consuming - windows must retire.
for _ in $(seq 1 20); do
    JUDGED=$(on_host "$OPS_PUB" "python3 -c \"import json;print(json.load(open('/qual/verdict.json'))['evaluated_windows'])\"" 2>/dev/null || echo 0)
    [ "${JUDGED:-0}" -gt 0 ] && break
    sleep 30
done
[ "${JUDGED:-0}" -gt 0 ] || verify_fail "the verifier has judged no windows; its verdict would be vacuous"
echo "campaign: verifier judging windows ($JUDGED evaluated)"

# --- chaos --------------------------------------------------------------
# Overridable so the simulator can drive the watch loop directly
# (DURATION_H=0 skips it entirely, which is how the job-gone probe's
# false positive escaped simulation). WATCH_MAX_LOOPS bounds the loop by
# iteration count for the same reason; 0 = unbounded (production).
DURATION_S="${DURATION_S:-$(( DURATION_H * 3600 ))}"
WATCH_MAX_LOOPS="${WATCH_MAX_LOOPS:-0}"
JOB_PROBE_INTERVAL_S="${JOB_PROBE_INTERVAL_S:-10}"
# --verdict: the controller stops injecting the moment the oracle counts
# any error, freezing the cluster for diagnosis instead of mutating it
# for the rest of the soak. --ensure-coverage: every mandatory fault -
# including each named 2PC point, verified to have FIRED - lands once
# before the weighted-random phase, so a PASS never depends on the dice.
# The controller's clock starts now, but the soak deadline starts only
# after the first fault is confirmed recovered - so an unpadded duration
# has the controller retiring minutes BEFORE the soak ends (run e: orderly
# "32 faults applied" exit ~13 minutes early, recorded as a death). Pad it
# past any plausible verification time; the drain's kill sweep is what
# actually ends it.
# MIN_GAP_S: the floor between injected faults. The default paces a long
# soak; a SMOKE run (short DURATION_S validating the whole lifecycle on a
# real rig before a paid multi-hour campaign) sets ~30. Size the smoke
# soak from the MEASURED pre-pass cost, not hope: a 2PC point costs
# 90-150s end to end (arm, a commit passing the point, a process death, a
# full recovery) and recovery dominates, so the gap barely matters. The
# 13-fault mandatory set needs ~40 minutes: qual01-smoke-a (900s) and
# qual01-smoke-c (1500s) both expired with coverage incomplete while
# their correctness was spotless. DURATION_S=2700 is the working smoke
# floor.
start_on_host "$OPS_PUB" chaos.log "python3 chaos.py --inventory /qual/inventory.json \
    --log /qual/chaos.jsonl --coordinator-url http://${COORD_PRIV}:8095 \
    --job-id $JOB_ID --run-id $RUN_ID --profile $PROFILE --seed $SEED \
    --min-gap-s ${MIN_GAP_S:-120} \
    --duration-s $(( DURATION_S + 1800 )) --verdict /qual/verdict.json --ensure-coverage"
echo "campaign: chaos started (${DURATION_H}h, profile=$PROFILE, coverage-first, oracle fail-fast)"

# 5. Chaos must actually land a fault. A controller that applies none is
#    the failure this harness has already had once, and it looks exactly
#    like a quiet campaign.
echo "campaign: waiting for the first fault to land"
FAULTS=0
for _ in $(seq 1 40); do
    FAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/chaos.jsonl 2>/dev/null || echo 0" | tr -d ' ')
    [ "${FAULTS:-0}" -gt 0 ] && break
    sleep 30
done
if [ "${FAULTS:-0}" -eq 0 ]; then
    on_host "$OPS_PUB" "tail -20 /qual/chaos.log" >&2 || true
    verify_fail "the chaos controller applied no fault within 20 minutes"
fi
echo "campaign: chaos landing faults ($FAULTS recorded)"

# 5b. The fault must have had an OBSERVABLE EFFECT on the cluster.
#
# A recorded fault is an intention, not an event. On the first cloud run
# every chaos command timed out against an address the rig's own
# firewall blocked; the controller logged two faults, the job never
# missed a checkpoint, and the campaign was minutes from publishing a
# fault-tolerance result for a cluster nothing had touched. So the gate
# now reads the coordinator's own counter of workers it has lost.
#
# "The metric says zero" and "the metric could not be read" are different
# facts, and defaulting an unreachable coordinator to zero would be the same
# silent assumption in a different coat.
# Polled, not read once: the chaos log records the kill the moment it is
# issued, while the coordinator only declares the loss when its heartbeat
# lease expires seconds later. A one-shot read raced that window and
# failed a campaign whose engine was behaving perfectly (QUAL-02 hit it
# on the local rig, which is FASTER than the cloud rig whose ssh
# round-trips had been masking the race here). Still a real gate: no
# worker loss inside the window means the fault left no trace.
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
[ -n "$LOST" ] || verify_fail "the coordinator exports no clink_coordinator_workers_lost_total,
  so there is no way to confirm a fault reached the engine"
if [ "$LOST_SEEN" != "1" ]; then
    verify_fail "the chaos controller recorded faults but the coordinator has lost no worker
  (clink_coordinator_workers_lost_total=${LOST}). A fault that leaves no trace in the
  engine did not happen, whatever the chaos log says."
fi
echo "campaign: fault confirmed by the engine (workers lost: $LOST)"

# 6. And the job must SURVIVE that fault - recover and keep checkpointing.
sleep 90
POST=$(curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}") || verify_fail "coordinator unreachable after the first fault"
POST_OK=$(python3 -c "
import json,sys
j = json.loads(sys.argv[1])
print('yes' if j.get('status') == 'RUNNING' else 'no')" "$POST")
[ "$POST_OK" = "yes" ] || verify_fail "the job did not recover from the first fault: $POST"
echo "campaign: job recovered from the first fault - VERIFICATION PASSED, entering soak"
echo "campaign: verification summary" > "$OUT_DIR/verification.txt"
{ echo "input_events_observed=$P2"; echo "output_records=$OUT_N";
  echo "windows_judged=$JUDGED"; echo "faults_recorded=$FAULTS";
  echo "workers_lost_observed_by_coordinator=$LOST";
  echo "recovered_after_first_fault=yes"; } >> "$OUT_DIR/verification.txt"

# --- watch --------------------------------------------------------------
# Poll evidence back to the laptop every 2 minutes - both so a campaign
# that dies at hour 90 still has its evidence locally, and so a dirty
# oracle is acted on within minutes. Run C's 10-minute cadence let the
# cluster keep mutating for most of an hour after the first bad window.
END=$(( $(date +%s) + DURATION_S ))
CHAOS_DIED_AT=""
JOB_GONE_AT=""
ORACLE_DIRTY=""
WATCH_LOOPS=0
while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$WATCH_MAX_LOOPS" != "0" ] && [ "$WATCH_LOOPS" -ge "$WATCH_MAX_LOOPS" ]; then
        break
    fi
    WATCH_LOOPS=$(( WATCH_LOOPS + 1 ))
    sleep 120
    for f in verdict.json chaos.jsonl progress.json generator.log verifier.log chaos.log; do
        scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null || true
    done

    # FAIL FAST on the oracle. One non-zero error counter means the
    # campaign has already failed; the only useful thing left is to freeze
    # and collect the evidence while the state that produced the defect
    # still exists. The chaos controller watches the same file and stops
    # injecting on its own; this stops the SOAK.
    ORACLE_DIRTY=$(python3 - "$OUT_DIR/verdict.json" <<'PY' || echo ""
import json, sys
try:
    v = json.load(open(sys.argv[1]))
except Exception:
    print("")
    sys.exit(0)
bad = sum(int(v.get(k) or 0)
          for k in ("missing", "duplicate", "conflicting", "incorrect", "foreign"))
print(bad if bad else "")
PY
)
    if [ -n "$ORACLE_DIRTY" ]; then
        echo "campaign: ORACLE DIRTY ($ORACLE_DIRTY errors) - stopping the soak now and" >&2
        echo "  collecting evidence; every further minute of faults would mutate the" >&2
        echo "  state a diagnosis needs frozen." >&2
        { echo "oracle_dirty=yes"; echo "error_total=$ORACLE_DIRTY";
          echo "noticed_at_utc=$(date -u +%H:%M)"; } > "$OUT_DIR/oracle-dirty.txt"
        on_host "$OPS_PUB" "touch /qual/chaos.jsonl.stop"
        on_host "$OPS_PUB" "pkill -INT -f '[c]haos.py' || true"
        # The state a diagnosis needs, captured NOW while it is freshest;
        # the final collection refreshes it after the drain.
        collect_container_logs
        break
    fi

    # Is anything still applying faults?
    #
    # On the run that found the source-offset defect, the chaos controller
    # raised on its first worker restart and died four minutes in. The
    # campaign then soaked for a full hour, reported healthy every ten
    # minutes, and finished - having applied exactly one fault, with no
    # fault generator alive for 93% of the run. Nothing in the harness
    # noticed, because a soak with no faults looks exactly like a soak that
    # survived them. The gate proves faults land at the START; this proves
    # they are still landing, and says so the moment they stop.
    if [ -z "$CHAOS_DIED_AT" ] \
       && ! on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null"; then
        CHAOS_DIED_AT=$(date -u +%H:%M)
        NFAULTS=$(on_host "$OPS_PUB" "wc -l < /qual/chaos.jsonl 2>/dev/null || echo 0" | tr -d '\r')
        # An orderly duration-elapsed exit prints its fault count as its
        # last act; anything else is a death. The distinction matters:
        # with the padded duration above, a completion mid-soak should be
        # impossible, and a real death must never be softened into one.
        if on_host "$OPS_PUB" "tail -1 /qual/chaos.log 2>/dev/null" | grep -q "faults applied"; then
            echo "campaign: NOTE - the chaos controller completed its padded duration early" \
                 "(noticed ${CHAOS_DIED_AT}, ${NFAULTS} fault record(s)); the remaining soak is" \
                 "undisturbed. This should not happen with the +1800s pad - investigate." >&2
            { echo "chaos_controller_completed=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
              echo "fault_records_at_completion=$NFAULTS";
              echo "tail:"; on_host "$OPS_PUB" "tail -20 /qual/chaos.log" 2>/dev/null || true;
            } > "$OUT_DIR/chaos-completed.txt"
        else
            echo "campaign: WARNING - the chaos controller is no longer running (noticed ${CHAOS_DIED_AT}," \
                 "${NFAULTS} fault record(s) written). Everything after this point is an" \
                 "undisturbed soak, not a fault campaign, and must be reported as such." >&2
            { echo "chaos_controller_died=yes"; echo "noticed_at_utc=$CHAOS_DIED_AT";
              echo "fault_records_at_death=$NFAULTS";
              echo "tail:"; on_host "$OPS_PUB" "tail -20 /qual/chaos.log" 2>/dev/null || true;
            } > "$OUT_DIR/chaos-died.txt"
        fi
    fi

    # And is the SUBJECT of the test still alive?
    #
    # The check above watches the apparatus. This one watches the thing being
    # measured, which is a different failure and produces a far more
    # misleading number. On the re-run a 2PC fault crashed the coordinator,
    # which came back with no jobs because it had no manifest to recover
    # from, and the pipeline stopped for good. The oracle kept doing its job
    # perfectly: it reported every subsequent expected result as missing, and
    # by the end that was 2.9 million of them. Read cold, that looks like
    # catastrophic data loss. It was a job that had ceased to exist.
    #
    # So the campaign says the moment it happens, and the summary can
    # separate "the engine lost data" from "there was no engine".
    if [ -z "$JOB_GONE_AT" ]; then
        # A coordinator mid-armed-fault answers nothing, and a job
        # mid-recovery is briefly not RUNNING: both are healthy chaos, not
        # a dead pipeline. A single probe here used to latch job_gone
        # permanently, and the latch poisons the summary's read of every
        # later window in BOTH directions (a false latch discounts real
        # loss; a real latch mislabelled as loss reads as catastrophe). So
        # the latch needs six consecutive non-RUNNING observations across
        # a full minute - longer than any recovery window this campaign
        # arms - and any single RUNNING answer clears the streak.
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
            echo "campaign: WARNING - job ${JOB_ID} is no longer RUNNING (probes: ${PROBES}," \
                 "noticed ${JOB_GONE_AT}). Every result expected after this point will be" \
                 "counted missing because nothing is producing, which is NOT data loss." >&2
            { echo "job_gone=yes"; echo "probes=$PROBES"; echo "noticed_at_utc=$JOB_GONE_AT";
            } > "$OUT_DIR/job-gone.txt"
        fi
    fi
    curl -fsS "http://${COORD_PUB}:8095/api/v1/jobs/${JOB_ID}" \
        > "$OUT_DIR/job-status.json" 2>/dev/null || true
    python3 - "$OUT_DIR/verdict.json" <<'PY' || true
import json, sys
try:
    v = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
print("campaign: windows=%s missing=%s dup=%s conflicting=%s incorrect=%s foreign=%s"
      % (v.get("correct_windows"), v.get("missing"), v.get("duplicate"),
         v.get("conflicting"), v.get("incorrect"), v.get("foreign")), flush=True)
PY
done

echo "campaign: duration reached; stopping generator and collecting evidence"
# Bracketed, for the reason given at the top of the run: an unbracketed
# pattern also matches the remote shell carrying the pkill, so the command
# signals itself and dies before reaching its target. `|| true` then hides
# it completely. That is not theoretical - it is why run 3's generator and
# verifier were still running nearly two hours after the campaign reported
# that it had stopped them and collected its evidence.
# Stop requests are FILES, not signals. start_on_host backgrounds these
# processes with `&` under a non-interactive shell, which POSIX starts
# with SIGINT set to SIG_IGN - and Python does not install its
# KeyboardInterrupt handler over an inherited ignore, so every polite
# pkill -INT this drain ever sent was silently dropped. qual01-20260818e
# spent its full finalisation grace waiting on a verifier that never saw
# the request: perfect oracle, 235,668 tail pairs, final=false,
# INCONCLUSIVE. Both scripts now poll for their stop file; the pkill -INT
# stays as a courtesy for interactively-run instances only.
# Chaos first, and ORDERLY: with the padded duration it is still injecting
# when the soak ends, and faults during the drain would delay tail commits
# past the oracle's grace. A hard kill is no answer - mid-fault it leaves
# tc rules installed or points armed - so the controller checks its stop
# file BETWEEN faults and always clears what it applied before exiting.
on_host "$OPS_PUB" "touch /qual/chaos.jsonl.stop"
cwaited=0
while [ "$cwaited" -lt 120 ]; do
    on_host "$OPS_PUB" "pgrep -f '[c]haos.py' >/dev/null" || break
    sleep 5
    cwaited=$(( cwaited + 5 ))
done
[ "$cwaited" -lt 120 ] \
    || echo "campaign: WARNING - the chaos controller is still running 120s after its stop request; a fault may straddle the drain" >&2
on_host "$OPS_PUB" "touch /qual/progress.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[g]enerator.py' || true"
# The generator must actually stop before the drain sleep means anything:
# a producer still writing during the "drain" manufactures tail windows
# faster than the pipeline can retire them.
gwaited=0
while [ "$gwaited" -lt 60 ]; do
    on_host "$OPS_PUB" "pgrep -f '[g]enerator.py' >/dev/null" || break
    sleep 5
    gwaited=$(( gwaited + 5 ))
done
[ "$gwaited" -lt 60 ] \
    || echo "campaign: WARNING - the generator is still producing 60s after its stop request" >&2
sleep 120   # let the pipeline drain the tail and the verifier judge it
on_host "$OPS_PUB" "touch /qual/verdict.json.stop"
on_host "$OPS_PUB" "pkill -INT -f '[v]erifier.py' || true"
# The verifier's stop path runs ONE FULL evaluation over every pending
# window before writing final=true - minutes, not seconds, at multi-hour
# scale. Wait for the final verdict with a PROGRESS-AWARE bound: keep
# waiting while pending pairs fall or judged windows rise, give up only
# on a genuine stall or the hard cap, and say so loudly - the summary
# then correctly refuses to call the run complete.
FINAL_WAIT_S="${FINAL_WAIT_S:-1200}"
FINAL_STALL_S="${FINAL_STALL_S:-180}"
waited=0
stalled=0
last_state=""
finalised=0
while [ "$waited" -lt "$FINAL_WAIT_S" ]; do
    state=$(on_host "$OPS_PUB" "python3 -c \"import json;d=json.load(open('/qual/verdict.json'));print(int(bool(d.get('final'))), d.get('pending_pairs',-1), d.get('evaluated_windows',-1))\"" 2>/dev/null \
        || echo "unreadable")
    case "$state" in
        1\ *) echo "campaign: verifier finalised after ${waited}s"; finalised=1; break ;;
    esac
    if [ "$state" = "$last_state" ]; then
        stalled=$(( stalled + 10 ))
        if [ "$stalled" -ge "$FINAL_STALL_S" ]; then
            echo "campaign: WARNING - the verifier made no finalisation progress for ${FINAL_STALL_S}s" \
                 "(state: '$state'); giving up on the final verdict" >&2
            break
        fi
    else
        stalled=0
        last_state="$state"
    fi
    sleep 10
    waited=$(( waited + 10 ))
done
[ "$finalised" = "1" ] \
    || echo "campaign: WARNING - the verifier did not finalise (waited ${waited}s of ${FINAL_WAIT_S}s); the summary will read this run as incomplete" >&2
# SIGINT is the polite request; this makes sure it happened.
kill_campaign_processes "$OPS_PUB" || true
# Say so if either is somehow still alive, rather than collecting evidence
# from underneath a still-running producer and calling the verdict final.
STILL=$(on_host "$OPS_PUB" "pgrep -f '[g]enerator.py|[v]erifier.py' | wc -l" | tr -d ' \r')
[ "${STILL:-0}" = "0" ] || echo "campaign: WARNING - $STILL generator/verifier process(es) survived the drain" >&2
# Loud per-file: run e's verifier.log silently failed to collect under
# the old `|| true`, and the one file that could explain a finalisation
# failure was the one file missing from the evidence.
for f in verdict.json chaos.jsonl progress.json progress.json.spec generator.log verifier.log chaos.log; do
    scp "${SSH_OPTS[@]}" -q "root@${OPS_PUB}:/qual/$f" "$OUT_DIR/" 2>/dev/null \
        || echo "campaign: WARNING - could not retain /qual/$f in the evidence" >&2
done
collect_container_logs

# The coordinator's final counters, retained rather than read from a live
# cluster that is about to be destroyed. Run 3's restart and worker-loss
# figures had to be quoted from a terminal session because nothing kept them.
curl -fsS --max-time 20 "http://${COORD_PUB}:8095/metrics" \
    > "$OUT_DIR/coordinator-metrics-final.txt" 2>/dev/null \
    || echo "campaign: WARNING - could not retain the coordinator's final metrics" >&2

python3 "$HERE/summarise.py" --out-dir "$OUT_DIR" --run-id "$RUN_ID" \
    --duration-h "$DURATION_H" --profile "$PROFILE" > "$OUT_DIR/QUAL-01-summary.md"
cat "$OUT_DIR/QUAL-01-summary.md"

echo
echo "campaign: evidence in $OUT_DIR"
echo "campaign: rig STILL RUNNING and billing. Tear down with:"
echo "  scripts/qualification/destroy.sh $RUN_ID --yes && qualification/infra/teardown.sh --check"
if [ -n "${ORACLE_DIRTY:-}" ]; then
    echo "campaign: RESULT: FAILED FAST on a dirty oracle ($ORACLE_DIRTY errors); the" >&2
    echo "  configured duration was NOT completed and this run is a FAILURE, not a" >&2
    echo "  short pass. Evidence above; do not publish." >&2
    exit 4
fi
