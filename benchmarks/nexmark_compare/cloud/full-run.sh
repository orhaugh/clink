#!/usr/bin/env bash
# Cross-engine nexmark run on the MULTI-NODE rig, sweeping PARALLELISM.
#
# WHAT THIS ANSWERS THAT THE SPLIT RIG CANNOT. The split rig ran one engine node against an
# isolated broker, so every shuffle was intra-node and the results contained no cross-host
# data-plane cost for either engine. Two questions were left explicitly open by it:
#
#   1. Does a per-event efficiency figure measured at one parallelism extrapolate? A
#      clink-only sweep showed CPU-per-event flat on a stateless query (1.05x from parallelism
#      1 to 8) but 2.1x WORSE on a keyed one, all of it in the shuffle. Whether the RATIO
#      against another engine survives that was unmeasured, because only one engine was swept.
#   2. What does a shuffle cost once it crosses hosts? Three worker nodes make it cross.
#
# So this sweeps parallelism for BOTH engines on the same hardware. Beyond parallelism 4 the
# job spans worker hosts and the keyed shuffle is genuinely remote.
#
# CPU is summed across EVERY engine node (control plus all three workers), not just one. A
# per-node figure would silently exclude the coordinator and two thirds of the workers, and
# would flatter whichever engine pushes more work off the sampled node.
#
#   CONTROL_IP=... CONTROL_PRIV=... BROKER_PRIV=... WORKERS="ip:priv:id ..." ./full-run.sh
#
# Knobs: QUERIES (default "q0 q12"), PARS (default "4 8 12"), EVENTS, ENGINES, TAG.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"

CONTROL_IP="${CONTROL_IP:?public address of the control node}"
CONTROL_PRIV="${CONTROL_PRIV:?private address of the control node}"
BROKER_PRIV="${BROKER_PRIV:?private address of the broker node}"
# "public:private:id" per worker, space separated.
WORKERS="${WORKERS:?worker list as public:private:id triples}"
BROKER="${BROKER_PRIV}:9092"
QUERIES="${QUERIES:-q0 q12}"
PARS="${PARS:-4 8 12}"
EVENTS="${EVENTS:-9200000}"
ENGINES="${ENGINES:-clink flink}"
# Trials per (query, parallelism, engine). Two samples cannot adjudicate a spread
# (clink q11/q19 read 1.42x/1.72x apart on two trials in the 2026-07-28 sweep).
REPEATS="${REPEATS:-1}"
# Sampler give-up after the frontier stalls; default matches every published
# number. Raise for the q18/q19 stall probe (see split-run.sh for the history).
QUIET_TIMEOUT="${QUIET_TIMEOUT:-6}"
# clink worker slots and Flink TM process size, forwarded to every node's
# compose. See full-worker.yml for what each changes and what it does NOT.
CLINK_SLOTS="${CLINK_SLOTS:-16}"
FLINK_TM_MEM="${FLINK_TM_MEM:-1728m}"
TAG="${TAG:-}"
KEY="${KEY:-$HOME/.ssh/clink-bench-ed25519}"
RESULTS="$HERE/results-full${TAG:+-$TAG}"
REMOTE=/root/clink/benchmarks/nexmark_compare
PROJECT=nxfull

mkdir -p "$RESULTS"

sshx() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o LogLevel=ERROR -o ConnectTimeout=10 root@"$1" "${@:2}"; }
scpx() { scp -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o LogLevel=ERROR "$@"; }
step() { echo; echo "=== $* ==="; }
want() { case " $ENGINES " in *" $1 "*) return 0;; esac; return 1; }
now_s() { python3 -c 'import time;print(time.time())'; }

# Every engine node, so CPU is summed over all of them.
ALL_NODES="$CONTROL_IP"
for w in $WORKERS; do ALL_NODES="$ALL_NODES ${w%%:*}"; done

# Cumulative CPU seconds across every engine container on every engine node.
cpu_all() {  # profile: clink|flink
    local profile=$1 total=0
    for node in $ALL_NODES; do
        local v
        v=$(sshx "$node" "docker ps --format '{{.Names}}' | grep -E '$profile' | while read -r c; do
                 docker inspect -f '{{.State.Pid}}' \$c 2>/dev/null; done | while read -r p; do
                 [ -n \"\$p\" ] && awk '{print (\$14+\$15)/100}' /proc/\$p/stat 2>/dev/null; done |
                 awk '{s+=\$1} END {printf \"%.2f\", s+0}'" 2>/dev/null)
        total=$(python3 -c "print($total + ${v:-0})")
    done
    echo "$total"
}

# Anonymous memory across every engine container on every engine node.
mem_all() {  # profile
    local profile=$1 total=0
    for node in $ALL_NODES; do
        local v
        v=$(sshx "$node" "docker ps --format '{{.Names}}' | grep -E '$profile' | while read -r c; do
                 docker exec \$c cat /sys/fs/cgroup/memory.stat 2>/dev/null | awk '/^anon /{print \$2}'; done |
                 awk '{s+=\$1} END {printf \"%.0f\", s+0}'" 2>/dev/null)
        total=$(python3 -c "print($total + ${v:-0})")
    done
    echo "$total"
}

up() {    # profile
    local profile=$1
    sshx "$CONTROL_IP" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV CLINK_IMAGE=${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main} \
        docker compose -p $PROJECT -f full-control.yml --profile $profile up -d" >/dev/null 2>&1
    for w in $WORKERS; do
        local pub=${w%%:*} rest=${w#*:} priv id
        priv=${rest%%:*}; id=${rest#*:}
        sshx "$pub" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV WORKER_IP=$priv WORKER_ID=$id \
            CLINK_IMAGE=${CLINK_IMAGE:-ghcr.io/orhaugh/clink-runtime:main} \
            CLINK_SLOTS=$CLINK_SLOTS FLINK_TM_MEM=$FLINK_TM_MEM \
            docker compose -p $PROJECT -f full-worker.yml --profile $profile up -d" >/dev/null 2>&1
    done
}

down() {
    sshx "$CONTROL_IP" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV docker compose -p $PROJECT \
        -f full-control.yml --profile clink --profile flink down -v" >/dev/null 2>&1
    for w in $WORKERS; do
        local pub=${w%%:*}
        sshx "$pub" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV WORKER_IP=x WORKER_ID=x \
            docker compose -p $PROJECT -f full-worker.yml --profile clink --profile flink down -v" >/dev/null 2>&1
    done
}

step "0. Rig"
echo "control : $CONTROL_IP ($CONTROL_PRIV)"
echo "workers : $WORKERS"
echo "broker  : $BROKER"
echo "sweep   : queries='$QUERIES' parallelism='$PARS' engines='$ENGINES' events=$EVENTS"
scpx "$HERE/full-control.yml" "$HERE/full-worker.yml" "$HERE/record.py" \
     root@"$CONTROL_IP":"$REMOTE/cloud/" >/dev/null || exit 1
for w in $WORKERS; do
    scpx "$HERE/full-worker.yml" root@"${w%%:*}":"$REMOTE/cloud/" >/dev/null || exit 1
done

# REFUSE TO MEASURE AN EMPTY TOPIC - the same guard split-run.sh grew after
# producing two full scoreboards of zeros (no topic; then an advertised-listener
# with an empty host). This script does not load the topic; full-load-canonical.sh
# does.
step "0b. Broker depth"
depth=$(sshx "$CONTROL_IP" "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
    kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $BROKER --topic nx-bid 2>/dev/null \
    | awk -F: '{sum += \$3} END {print sum+0}'" 2>/dev/null | tr -d '\r')
echo "topic nx-bid holds ${depth:-unknown} records (target $EVENTS)"
if [ -z "$depth" ] || [ "$depth" = "0" ]; then
    echo "REFUSING TO RUN: nx-bid on $BROKER holds no records. Load it first" >&2
    echo "  (full-load-canonical.sh), and check the broker came up WITH" >&2
    echo "  BROKER_PRIVATE_IP set - without it Kafka advertises an empty host." >&2
    exit 1
fi

run_one() {  # engine query par trial
    local engine=$1 q=$2 par=$3 trial=${4:-1}
    # The consumer group must be unique PER TRIAL, not just per (engine, query,
    # par): offsets commit to the broker, so a repeated trial under the same group
    # resumes at the end of the topic and reads NOTHING - the first baseline
    # attempt measured trial 1 at 11.2M rec/s and trials 2-3 at exactly 0 for this
    # reason. $$ alone cannot distinguish trials inside one invocation.
    local gid="f-$engine-$q-$par-t$trial-$$"
    down
    up "$engine"

    local ok=0 i
    if [ "$engine" = clink ]; then
        for i in $(seq 1 45); do
            sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8095/api/v1/health >/dev/null 2>&1" && ok=1 && break
            sleep 2
        done
        [ "$ok" = 1 ] || { echo "  coordinator never came up"; return 1; }
        # Every worker must register, or the submit is rejected for want of slots and the run
        # would silently measure a smaller cluster than intended.
        local want_w
        want_w=$(echo "$WORKERS" | wc -w | tr -d ' ')
        for i in $(seq 1 60); do
            local n
            n=$(sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8095/api/v1/workers 2>/dev/null | grep -o worker- | wc -l" 2>/dev/null | tr -d ' ')
            [ "${n:-0}" -ge "$want_w" ] && break
            sleep 2
        done
        n=$(sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8095/api/v1/workers 2>/dev/null | grep -o worker- | wc -l" 2>/dev/null | tr -d ' ')
        echo "  workers registered: ${n:-0}/$want_w"
        [ "${n:-0}" -ge "$want_w" ] || { echo "  SKIP: cluster short of workers"; return 1; }
    else
        for i in $(seq 1 60); do
            sshx "$CONTROL_IP" "docker exec ${PROJECT}-flink-jobmanager-1 flink list >/dev/null 2>&1" && ok=1 && break
            sleep 2
        done
        [ "$ok" = 1 ] || { echo "  jobmanager never came up"; return 1; }
        local slots
        for i in $(seq 1 60); do
            slots=$(sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8081/overview 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get(\"slots-total\",0))'" 2>/dev/null)
            [ "${slots:-0}" -ge "$par" ] && break
            sleep 2
        done
        echo "  task slots available: ${slots:-0} (need $par)"
        [ "${slots:-0}" -ge "$par" ] || { echo "  SKIP: fewer slots than parallelism"; return 1; }
    fi

    local cpu_pre wall_pre
    cpu_pre=$(cpu_all "$engine")
    wall_pre=$(now_s)

    local s jid
    if [ "$engine" = clink ]; then
        sed -e "s#__BROKERS__#$BROKER#" -e "s#__OUT__#nx-out-$q#" \
            -e "s#group_id='[^']*'#group_id='$gid'#" \
            "$ROOT/queries/clink/${q}_bh.tmpl.sql" > /tmp/fr.sql || return 1
        jid=$("$CLINK_ROOT/build/clink_submit_sql" --file /tmp/fr.sql \
                --coordinator-host "$CONTROL_IP" --coordinator-port 8095 \
                --parallelism "$par" --name "$q-p$par" 2>/dev/null | python3 "$HERE/job_id.py")
        [ -z "$jid" ] && { echo "  submit failed"; down; return 1; }
        # --baseline 0 explicitly. Every measured run gets a freshly composed stack, so the
        # counters start at zero; WITHOUT this the sampler anchors on its first strictly
        # positive sample and subtracts the run's own head start, which on a fast drain scores
        # a fully-completed run as incomplete (verified: the job reaches 9.2M of 9.2M, while
        # the sampler reported reached=False).
        s=$(sshx "$CONTROL_IP" "cd $REMOTE && python3 driver/sample_rate.py clink --base http://127.0.0.1:8095 --job $jid --target $EVENTS --baseline 0 --max-runtime 300 --quiet-timeout $QUIET_TIMEOUT")
    else
        sed -e "s#kafka:29092#$BROKER#" -e "s#__OUT__#nx-out-$q#" \
            -e "s#'properties.group.id' = '[^']*'#'properties.group.id' = '$gid'#" \
            "$ROOT/flink-job/queries/${q}_bh.tmpl.sql" > /tmp/fr.sql || return 1
        scpx /tmp/fr.sql root@"$CONTROL_IP":/tmp/fr.sql >/dev/null 2>&1
        sshx "$CONTROL_IP" "docker cp /root/nexmark-sql.jar ${PROJECT}-flink-jobmanager-1:/tmp/n.jar && docker cp /tmp/fr.sql ${PROJECT}-flink-jobmanager-1:/tmp/fr.sql" >/dev/null 2>&1
        jid=$(sshx "$CONTROL_IP" "docker exec ${PROJECT}-flink-jobmanager-1 flink run -d -p $par /tmp/n.jar /tmp/fr.sql 2>&1" \
              | grep -oE 'JobID [0-9a-f]+' | awk '{print $2}' | tail -1)
        [ -z "$jid" ] && { echo "  submit failed"; down; return 1; }
        s=$(sshx "$CONTROL_IP" "cd $REMOTE && python3 driver/sample_rate.py flink --base http://127.0.0.1:8081 --job $jid --target $EVENTS --max-runtime 300 --quiet-timeout $QUIET_TIMEOUT")
    fi

    local cpu_post wall_post mem
    cpu_post=$(cpu_all "$engine")
    wall_post=$(now_s)
    mem=$(mem_all "$engine")

    printf '%s' "$s" | python3 "$HERE/record.py" --out "$RESULTS/$q-$engine-p$par-t$trial.json" \
        --engine "$engine" --query "$q" --trial "$trial" --par "$par" \
        --cpu-pre "$cpu_pre" --cpu-post "$cpu_post" \
        --wall-pre "$wall_pre" --wall-post "$wall_post" --input-events "$EVENTS"
    python3 - "$RESULTS/$q-$engine-p$par-t$trial.json" "$mem" <<'PY'
import json, sys
p, anon = sys.argv[1], float(sys.argv[2] or 0)
d = json.load(open(p))
d['anon_mb'] = round(anon / 1e6, 1)
json.dump(d, open(p, 'w'))
print(f"  {d['engine']:<6} anon {d['anon_mb']:,.0f} MB across all engine nodes")
PY
    down
}

for q in $QUERIES; do
    for par in $PARS; do
        for eng in clink flink; do
            want "$eng" || continue
            for trial in $(seq 1 "$REPEATS"); do
                step "$q  parallelism=$par  $eng  (trial $trial/$REPEATS)"
                run_one "$eng" "$q" "$par" "$trial"
            done
        done
    done
done

step "Summary"
python3 - "$RESULTS" <<'PY'
import glob, json, os, sys, collections
rows = []
for p in sorted(glob.glob(os.path.join(sys.argv[1], '*.json'))):
    try: rows.append(json.load(open(p)))
    except Exception: pass
if not rows:
    print('no results'); raise SystemExit
print(f"{'query':<5} {'par':>4} {'engine':<6} {'sustained':>11} {'ev/cpu-s':>10} {'cores':>6} {'anon MB':>8} {'reached':>8}")
print('-'*66)
for r in sorted(rows, key=lambda x: (x.get('query',''), x.get('par',0), x.get('engine',''))):
    print(f"{r.get('query',''):<5} {r.get('par',0):>4} {r.get('engine',''):<6} "
          f"{(r.get('sustained_slope') or 0):>11,.0f} {(r.get('events_per_cpu_sec') or 0):>10,.0f} "
          f"{(r.get('cores') or 0):>6.2f} {(r.get('anon_mb') or 0):>8.0f} {str(r.get('reached_target')):>8}")
# The question this rig exists for: does the RATIO hold as parallelism rises?
# Aggregate over trials: mean efficiency per (query, par, engine), with the
# max/min spread kept so a noisy pair is visible rather than averaged away.
import statistics
groups = collections.defaultdict(list)
for r in rows:
    groups[(r.get('query'), r.get('par'), r.get('engine'))].append(r)
by = {}
for k, rs in groups.items():
    eff = [r.get('events_per_cpu_sec') for r in rs if r.get('events_per_cpu_sec')]
    agg = dict(rs[0])
    agg['events_per_cpu_sec'] = statistics.mean(eff) if eff else None
    agg['eff_spread'] = (max(eff) / min(eff)) if len(eff) > 1 and min(eff) else None
    by[k] = agg
qs = sorted({r.get('query') for r in rows}); ps = sorted({r.get('par') for r in rows})
print()
print("clink/flink efficiency ratio by parallelism (the question this rig exists to answer):")
for q in qs:
    line = f"  {q}: "
    for p in ps:
        c, f = by.get((q,p,'clink')), by.get((q,p,'flink'))
        if c and f and (f.get('events_per_cpu_sec') or 0) > 0:
            line += f"par{p}={c['events_per_cpu_sec']/f['events_per_cpu_sec']:.2f}x  "
        else:
            line += f"par{p}=-  "
    print(line)
print()
print("per-engine CPU-per-event across parallelism (is each engine's own efficiency flat?):")
for q in qs:
    for eng in ('clink','flink'):
        vals = [(p, by[(q,p,eng)].get('events_per_cpu_sec')) for p in ps if (q,p,eng) in by]
        vals = [(p,v) for p,v in vals if v]
        if len(vals) > 1:
            first, last = vals[0][1], vals[-1][1]
            print(f"  {q} {eng:<6} " + "  ".join(f"par{p}={v:,.0f}" for p,v in vals) +
                  f"   -> {first/last:.2f}x change")
PY
echo
echo "results in $RESULTS"
