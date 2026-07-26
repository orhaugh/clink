#!/usr/bin/env bash
# Cross-engine nexmark run on the SPLIT cloud rig: engine node + broker node.
#
# WHAT THIS ANSWERS THAT THE LAPTOP CANNOT. On the single-box harness the broker,
# both engines and every container shared the same cores, and the run-to-run spread
# on identical work was wide enough to swamp the effect being measured (clink's q0
# was recorded at both 1.06M and 571k rec/s for the same configuration). Here the
# broker has its own node, the engine has four dedicated vCPUs, and the two engines
# take the SAME node one after the other.
#
# Runs from the LAPTOP and drives the engine node over ssh:
#   - the clink spec is compiled locally by clink_submit_sql and POSTed to the
#     coordinator, because the runtime image ships clink_node and the client CLI
#     but not the SQL submit tool.
#   - the SAMPLER runs on the engine node, not here. Polling a counter across the
#     public internet at a 100ms interval would fold tens of ms of RTT into a
#     500ms slope window and quietly distort every rate it reported.
#
# Each measured variant gets a FRESHLY COMPOSED stack (see the nexmark README:
# chained runs on one warm cluster drift monotonically in CPU, 2.6x by the sixth
# job, which erases the very delta being looked for) and a FRESH consumer group,
# so every run re-reads the topic from the beginning instead of resuming at a
# committed offset and measuring nothing.
#
#   ENGINE_IP=... BROKER_PRIVATE_IP=10.10.1.2 ./split-run.sh
#
# Knobs: QUERIES (default "q0 q12"), PAR (4), EVENTS (topic depth), ENGINES,
# REPEATS (trials per engine/query), TAG (result dir suffix).
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"

# No apostrophe in this message: an APOSTROPHE INSIDE ${var:?...} opens a quoted
# section as far as bash's parser is concerned, even within double quotes, and it
# desynchronises the quote state for the whole rest of the file - the script then
# failed to parse ~150 lines later with a syntax error pointing at innocent code.
ENGINE_IP="${ENGINE_IP:?set ENGINE_IP to the public address of the engine node}"
BROKER_PRIVATE_IP="${BROKER_PRIVATE_IP:?set BROKER_PRIVATE_IP}"
BROKER="${BROKER_PRIVATE_IP}:9092"
TOPIC="${TOPIC:-nx-bid}"
QUERIES="${QUERIES:-q0 q12}"
PAR="${PAR:-4}"
EVENTS="${EVENTS:-9200000}"
ENGINES="${ENGINES:-clink flink}"
REPEATS="${REPEATS:-1}"
TAG="${TAG:-}"
KEY="${KEY:-$HOME/.ssh/clink-bench-ed25519}"
RESULTS="$HERE/results-split${TAG:+-$TAG}"
REMOTE=/root/clink/benchmarks/nexmark_compare
PROJECT=nxsplit

mkdir -p "$RESULTS"

# Explicit inline options, not a quoted $SSHO variable: zsh does not word-split
# unquoted parameters, so a collected option string arrives as ONE argument and
# ssh rejects it.
sshx() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o LogLevel=ERROR root@"$ENGINE_IP" "$@"; }
scpx() { scp -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o LogLevel=ERROR "$@"; }
dc() { sshx "cd $REMOTE/cloud && BROKER=$BROKER docker compose -p $PROJECT -f split-engine.yml $*"; }
teardown_stack() { dc "--profile clink --profile flink down -v" >/dev/null 2>&1; }

step() { echo; echo "=== $* ==="; }
want() { case " $ENGINES " in *" $1 "*) return 0;; esac; return 1; }
now_s() { python3 -c 'import time;print(time.time())'; }
rcpu() { sshx "cd $REMOTE && python3 driver/cpu.py read-flink $*"; }

step "0. Rig"
echo "engine  : $ENGINE_IP (4 dedicated vCPU, hosts whichever engine is measured)"
echo "broker  : $BROKER (own node, 4 dedicated vCPU)"
echo "topic   : $TOPIC, target $EVENTS events, par=$PAR, engines: $ENGINES"
sshx "cd $REMOTE && git rev-parse --short HEAD" | sed 's/^/repo    : /'
scpx "$HERE/split-engine.yml" "$HERE/record.py" root@"$ENGINE_IP":"$REMOTE/cloud/" || exit 1

# ---------------------------------------------------------------- clink -------
run_clink() {  # query trial
    local q=$1 trial=$2
    local gid="cl-$q-$$-$trial"
    local sql="/tmp/$q-clink-cloud.sql"
    # Rewrite the broker AND the consumer group. The template pins a fixed
    # group_id, so a second run of the same query would resume at the committed
    # offset and drain nothing.
    sed -e "s#__BROKERS__#$BROKER#" -e "s#__OUT__#nx-out-$q#" \
        -e "s#group_id='[^']*'#group_id='$gid'#" \
        "$ROOT/queries/clink/${q}_bh.tmpl.sql" > "$sql" || return 1

    teardown_stack
    dc "--profile clink up -d" >/dev/null 2>&1
    local ok=0 i
    for i in $(seq 1 40); do
        sshx "curl -fsS http://127.0.0.1:8095/api/v1/health >/dev/null 2>&1" && ok=1 && break
        sleep 2
    done
    [ "$ok" = 1 ] || { echo "  clink coordinator never came up"; return 1; }
    # Wait for the worker to register, else the submit is rejected for want of slots.
    for i in $(seq 1 30); do
        sshx "curl -fsS http://127.0.0.1:8095/api/v1/workers 2>/dev/null | grep -q worker-1" && break
        sleep 1
    done

    local ctrs="$PROJECT-clink-coordinator-1 $PROJECT-clink-worker1-1"
    # clink's per-operator counters are cumulative across job submissions on a live
    # worker, so the run's progress is a DELTA from whatever they already read.
    # A freshly composed stack makes this 0, but it is read rather than assumed.
    local base0
    base0=$(sshx "cd $REMOTE/driver && python3 -c \"import sample_rate; print(sample_rate.clink_prior_total('http://127.0.0.1:8095'))\"" 2>/dev/null)
    [ -z "$base0" ] && base0=0
    local cpu_pre wall_pre
    cpu_pre=$(rcpu "$ctrs")
    wall_pre=$(now_s)

    # clink job ids are small INTEGERS, so read the id out of the submit tool's
    # JSON line rather than pattern-matching a hex token. Grepping for 8+ hex
    # characters (Flink's shape) found nothing, reported "submit failed" for a
    # submit that had in fact succeeded, and left the job running and holding all
    # 16 slots - the next submit then failed for real, with a misleading message.
    local jid
    jid=$("$CLINK_ROOT/build/clink_submit_sql" --file "$sql" \
            --coordinator-host "$ENGINE_IP" --coordinator-port 8095 \
            --parallelism "$PAR" --name "$q-clink" 2>/dev/null \
          | python3 "$HERE/job_id.py")
    [ -z "$jid" ] && { echo "  clink submit failed"; teardown_stack; return 1; }
    echo "  clink  job $jid"

    local s
    s=$(sshx "cd $REMOTE && python3 driver/sample_rate.py clink --base http://127.0.0.1:8095 --job $jid --target $EVENTS --baseline $base0 --max-runtime 240")
    local cpu_post wall_post mem
    cpu_post=$(rcpu "$ctrs")
    wall_post=$(now_s)
    mem=$(sshx "cd $REMOTE && python3 driver/mem.py read $ctrs")

    printf '%s' "$s" | python3 "$HERE/record.py" --out "$RESULTS/$q-clink-$trial.json" \
        --engine clink --query "$q" --trial "$trial" --par "$PAR" \
        --cpu-pre "$cpu_pre" --cpu-post "$cpu_post" \
        --wall-pre "$wall_pre" --wall-post "$wall_post" --input-events "$EVENTS"
    python3 "$ROOT/driver/mem.py" merge "$RESULTS/$q-clink-$trial.json" --mem "$mem" \
        --fresh-stack 2>/dev/null | sed 's/^/  clink  mem /'
    teardown_stack
}

# ---------------------------------------------------------------- flink -------
run_flink() {  # query trial
    local q=$1 trial=$2
    local gid="fl-$q-$$-$trial"
    local sql="/tmp/$q-flink-cloud.sql"
    sed -e "s#kafka:29092#$BROKER#" -e "s#__OUT__#nx-out-$q#" \
        -e "s#'properties.group.id' = '[^']*'#'properties.group.id' = '$gid'#" \
        "$ROOT/flink-job/queries/${q}_bh.tmpl.sql" > "$sql" || return 1

    teardown_stack
    dc "--profile flink up -d" >/dev/null 2>&1
    local jm="$PROJECT-flink-jobmanager-1"
    local ok=0 i
    for i in $(seq 1 45); do
        sshx "docker exec $jm flink list >/dev/null 2>&1" && ok=1 && break
        sleep 2
    done
    [ "$ok" = 1 ] || { echo "  flink jobmanager never came up"; return 1; }

    scpx "$sql" root@"$ENGINE_IP":/tmp/ >/dev/null 2>&1
    sshx "docker cp /root/nexmark-sql.jar $jm:/tmp/nexmark-sql.jar && docker cp $sql $jm:$sql" >/dev/null 2>&1

    local ctrs="$jm $PROJECT-flink-taskmanager-1"
    local cpu_pre wall_pre
    cpu_pre=$(rcpu "$ctrs")
    wall_pre=$(now_s)

    local jid
    jid=$(sshx "docker exec $jm flink run -d -p $PAR /tmp/nexmark-sql.jar $sql 2>&1" \
          | grep -oE 'JobID [0-9a-f]+' | awk '{print $2}' | tail -1)
    [ -z "$jid" ] && { echo "  flink submit failed"; return 1; }
    echo "  flink  job $jid"

    local s
    s=$(sshx "cd $REMOTE && python3 driver/sample_rate.py flink --base http://127.0.0.1:8081 --job $jid --target $EVENTS --max-runtime 240")
    local cpu_post wall_post mem
    cpu_post=$(rcpu "$ctrs")
    wall_post=$(now_s)
    mem=$(sshx "cd $REMOTE && python3 driver/mem.py read $ctrs")

    printf '%s' "$s" | python3 "$HERE/record.py" --out "$RESULTS/$q-flink-$trial.json" \
        --engine flink --query "$q" --trial "$trial" --par "$PAR" \
        --cpu-pre "$cpu_pre" --cpu-post "$cpu_post" \
        --wall-pre "$wall_pre" --wall-post "$wall_post" --input-events "$EVENTS"
    python3 "$ROOT/driver/mem.py" merge "$RESULTS/$q-flink-$trial.json" --mem "$mem" \
        --fresh-stack 2>/dev/null | sed 's/^/  flink  mem /'
    sshx "docker exec $jm flink cancel $jid" >/dev/null 2>&1
    teardown_stack
}

if want flink; then
    scpx "$ROOT/flink-job/target/nexmark-sql.jar" root@"$ENGINE_IP":/root/ >/dev/null 2>&1
fi

for q in $QUERIES; do
    for t in $(seq 1 "$REPEATS"); do
        if want clink; then step "$q clink (trial $t)"; run_clink "$q" "$t"; fi
        if want flink; then step "$q flink (trial $t)"; run_flink "$q" "$t"; fi
    done
done

step "Summary"
python3 "$HERE/summarize-split.py" "$RESULTS"
echo
echo "results in $RESULTS"
