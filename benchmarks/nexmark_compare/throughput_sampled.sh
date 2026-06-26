#!/usr/bin/env bash
# Sustained-throughput run via ENGINE-SIDE metrics sampling.
#
# Both engines containerized (clink Release+LTO, Flink production JVM), same
# pre-loaded Kafka input. Instead of timing broker append (burst-fooled) or a
# downstream consumer (consumer-capped), we poll EACH ENGINE'S OWN
# records-processed counter over time and take the sustained max slope:
#   clink  -> GET /api/v1/jobs/<id>/operators  (max records_in across operators)
#   Flink  -> GET /jobs/<jid>                    (max read/write-records across vertices)
# The slope excludes the deploy/JVM-warmup startup gap and the end taper, so it
# is the engine's true steady-state processing rate - not bounded by a consumer,
# not inflated by sink flush.
#
# Input is large by default (5M events) so the FAST engine runs for a measurable
# window (clink ~2-3s, Flink ~12-20s). Only q0/q12 are sampled: their input is
# the full bid stream (~4.6M events). q8/q6 have tiny join inputs (<=400k) that
# drain in well under a second on clink - no measurable sustained window - so
# they stay in the correctness-gate harness, not here.
#
#   ./throughput_sampled.sh                          # q0 q12, par 4, 5M events
#   EVENTS=10000000 PARALLELISM=4 ./throughput_sampled.sh
#   QUERIES="q0" KEEP_UP=1 ./throughput_sampled.sh   # leave cluster up after
#   SINK=blackhole EVENTS=10000000 ./throughput_sampled.sh  # discard output to
#       remove the shared Kafka broker as a write ceiling (engine-rate only)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
PROJECT=nxcompare
PY="$ROOT/../flink_compare/.venv/bin/python"
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
GETOFF="docker exec ${PROJECT}-kafka-1 kafka-get-offsets --bootstrap-server localhost:9092"
CLINK_JM_HTTP=8095
FLINK_REST=8081
FLINK_JM=${PROJECT}-flink-jobmanager-1
CLINK_CTRS="${PROJECT}-clink-jm-1 ${PROJECT}-clink-tm1-1 ${PROJECT}-clink-tm2-1 ${PROJECT}-clink-tm3-1 ${PROJECT}-clink-tm4-1"
FLINK_CTRS="${PROJECT}-flink-jobmanager-1 ${PROJECT}-flink-taskmanager-1"

EVENTS="${EVENTS:-5000000}"
TPS="${TPS:-1000}"
PAR="${PARALLELISM:-4}"
QUERIES="${QUERIES:-q0 q12}"
# SINK=kafka (default): write output to a Kafka topic + gate on row count.
# SINK=blackhole: discard output (clink connector='blackhole' / Flink built-in
# 'blackhole') so the engine's read+process rate is measured WITHOUT the shared
# Kafka broker as a write ceiling. No output topic, no row-count gate; the
# completeness check is that each engine's own counter drained the full input
# (reached_target). Uses the q*_bh.tmpl.sql variants.
SINK="${SINK:-kafka}"
DATA_DIR="${DATA_DIR:-/tmp/nxcompare-sampled}"
RESULTS="$ROOT/results-sampled"
QSUFFIX=""; [ "$SINK" = "blackhole" ] && QSUFFIX="_bh"

now_s() { python3 -c 'import time;print(time.time())'; }
step() { printf '\n=== %s ===\n' "$*"; }
recreate_topic() {  # name partitions [append]
    $KEX --delete --topic "$1" >/dev/null 2>&1
    while $KEX --list 2>/dev/null | grep -qx "$1"; do sleep 1; done
    if [[ "${3:-}" == "append" ]]; then
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 \
            --config message.timestamp.type=LogAppendTime >/dev/null 2>&1
    else
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null 2>&1
    fi
}
end_offset_sum() {  # topic -> total records (fresh topic from offset 0)
    $GETOFF --topic "$1" --time -1 2>/dev/null | awk -F: '{s+=$3} END {print s+0}'
}
wait_stable_offset() {  # topic -> total records once the sink stops growing
    # The sampler stops when the SOURCE drains; the sink may still be flushing
    # (and windowed queries emit panes near end-of-stream). Poll until the output
    # offset is unchanged across two reads so the correctness gate is honest.
    local prev=-1 cur; for i in $(seq 1 15); do
        cur=$(end_offset_sum "$1"); [ "$cur" = "$prev" ] && { echo "$cur"; return; }
        prev=$cur; sleep 1.5
    done; echo "$cur"
}
# clink's per-op counters are cumulative per op_id on the persistent TM process,
# so a repeat run re-appears at its prior total. The sampler anchors its baseline
# at the first strictly-positive reading (= that prior total) and measures only
# the delta, so no TM restart is needed - each run's drain rate is clean.

mkdir -p "$RESULTS" "$DATA_DIR"; rm -f "$RESULTS"/*.json

step "1. Build host deps + Flink jar, check clink-runtime image"
cmake --build "$CLINK_ROOT/build" --target nexmark_dump clink_submit_sql --parallel 10 >/dev/null 2>&1 \
    || { echo "host build failed"; exit 1; }
docker image inspect clink-runtime:latest >/dev/null 2>&1 || { echo "clink-runtime:latest missing - run verify_distributed.sh first"; exit 1; }
( cd "$ROOT/flink-job" && mvn -q -o -DskipTests package 2>/dev/null || mvn -q -DskipTests package ) \
    || { echo "flink jar build failed"; exit 1; }

step "2. Bring up Kafka + clink + Flink (containers)"
docker compose -p "$PROJECT" --profile clink --profile flink up -d >/dev/null 2>&1
for i in $(seq 1 45); do docker exec ${PROJECT}-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && break; sleep 2; done
for i in $(seq 1 60); do curl -fsS "http://127.0.0.1:${CLINK_JM_HTTP}/api/v1/health" >/dev/null 2>&1 && break; sleep 2; done
for i in $(seq 1 30); do docker exec "$FLINK_JM" flink list >/dev/null 2>&1 && break; sleep 2; done
docker cp "$ROOT/flink-job/target/nexmark-sql.jar" "$FLINK_JM:/tmp/nexmark-sql.jar" >/dev/null 2>&1

step "3. Generate dataset ($EVENTS events) + load nx-{person,auction,bid} ($PAR partitions)"
dump=$("$CLINK_ROOT/build/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR")
echo "$dump" | tail -1
BIDS=$(echo "$dump" | python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l)["bid"]); break
        except: pass')
[ -z "$BIDS" ] && { echo "could not read bid count"; exit 1; }
echo "bid stream = $BIDS events (the sustained-throughput input for q0/q12)"
for t in nx-person nx-auction nx-bid; do recreate_topic "$t" "$PAR"; done
"$PY" "$ROOT/driver/load_ndjson.py" --dir "$DATA_DIR" --bootstrap localhost:9092 --prefix nx- 2>/dev/null | tail -1

extract_job_id() { python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except: pass'; }

run_clink() {  # query
    local q=$1 out="nx-out-$q-clink"
    [ "$SINK" = "kafka" ] && recreate_topic "$out" "$PAR" append
    sed -e "s#__OUT__#$out#" -e "s#__BROKERS__#kafka:29092#" "$ROOT/queries/clink/$q$QSUFFIX.tmpl.sql" > "$DATA_DIR/$q-clink.sql"
    local cpu_pre wall_pre; cpu_pre=$("$PY" "$ROOT/driver/cpu.py" read-flink $CLINK_CTRS); wall_pre=$(now_s)
    local jid; jid=$(../../build/clink_submit_sql --file "$DATA_DIR/$q-clink.sql" \
        --jm-host 127.0.0.1 --jm-port "$CLINK_JM_HTTP" --name "ts_$q" --parallelism "$PAR" 2>/dev/null | extract_job_id)
    [ -z "$jid" ] && { echo "  clink submit failed"; return 1; }
    # Generous quiet-timeout/max-runtime (like the Flink side) so a momentary
    # metric-refresh plateau does not trip an early "drained" before the full
    # input is consumed - otherwise the run is INCOMPLETE and not comparable.
    local s; s=$("$PY" "$ROOT/driver/sample_rate.py" clink --base "http://127.0.0.1:$CLINK_JM_HTTP" \
        --job "$jid" --target "$BIDS" --interval 0.08 --window 0.5 --quiet-timeout 20 --max-runtime 300)
    local cpu_post wall_post; cpu_post=$("$PY" "$ROOT/driver/cpu.py" read-flink $CLINK_CTRS); wall_post=$(now_s)
    local off=-1; [ "$SINK" = "kafka" ] && off=$(wait_stable_offset "$out")
    echo "$s" | python3 -c "import json,sys
d=json.load(sys.stdin); d.update({'query':'$q','sink':'$SINK','cpu_seconds':round($cpu_post-$cpu_pre,2),'wall_seconds':round($wall_post-$wall_pre,2),'out_rows':$off}); open('$RESULTS/$q-clink.json','w').write(json.dumps(d)); print('  clink drain',d['drain_rate'],'rec/s (',d.get('drain_seconds'),'s), reached',d['reached_target'],', out_rows',($off if $off>=0 else 'n/a(blackhole)'))"
    curl -fsS -X POST "http://127.0.0.1:$CLINK_JM_HTTP/api/v1/jobs/$jid/cancel" >/dev/null 2>&1
    sleep 2
}

run_flink() {  # query
    local q=$1 out="nx-out-$q-flink"
    [ "$SINK" = "kafka" ] && recreate_topic "$out" "$PAR" append
    sed "s#__OUT__#$out#" "$ROOT/flink-job/queries/$q$QSUFFIX.tmpl.sql" > "$DATA_DIR/$q-flink.sql"
    docker cp "$DATA_DIR/$q-flink.sql" "$FLINK_JM:/tmp/$q-flink.sql" >/dev/null 2>&1
    local cpu_pre wall_pre; cpu_pre=$("$PY" "$ROOT/driver/cpu.py" read-flink $FLINK_CTRS); wall_pre=$(now_s)
    local jid; jid=$(docker exec "$FLINK_JM" flink run -d -p "$PAR" /tmp/nexmark-sql.jar "/tmp/$q-flink.sql" 2>&1 \
        | grep -oE '[0-9a-f]{32}' | head -1)
    [ -z "$jid" ] && { echo "  flink submit failed"; return 1; }
    # Flink REST metrics refresh ~every 2s and can lag under load, so use a longer
    # quiet-timeout so a refresh gap is not mistaken for "drained". A cold JVM can
    # be slow, so a generous max-runtime lets it drain the full input (else the
    # output is partial and not comparable).
    local s; s=$("$PY" "$ROOT/driver/sample_rate.py" flink --base "http://127.0.0.1:$FLINK_REST" \
        --job "$jid" --target "$BIDS" --interval 0.2 --window 2.5 --quiet-timeout 18 --max-runtime 360)
    local cpu_post wall_post; cpu_post=$("$PY" "$ROOT/driver/cpu.py" read-flink $FLINK_CTRS); wall_post=$(now_s)
    local off=-1; [ "$SINK" = "kafka" ] && off=$(wait_stable_offset "$out")
    echo "$s" | python3 -c "import json,sys
d=json.load(sys.stdin); d.update({'query':'$q','sink':'$SINK','cpu_seconds':round($cpu_post-$cpu_pre,2),'wall_seconds':round($wall_post-$wall_pre,2),'out_rows':$off}); open('$RESULTS/$q-flink.json','w').write(json.dumps(d)); print('  flink drain',d['drain_rate'],'rec/s (',d.get('drain_seconds'),'s), reached',d['reached_target'],', out_rows',($off if $off>=0 else 'n/a(blackhole)'))"
    docker exec "$FLINK_JM" flink cancel "$jid" >/dev/null 2>&1
    sleep 2
}

for q in $QUERIES; do
    step "4. $q on clink (containers, par=$PAR)"; run_clink "$q"
    step "5. $q on Flink (containers, par=$PAR)"; run_flink "$q"
done

step "6. Sustained-throughput summary (engine-side metrics, par=$PAR, $EVENTS events, sink=$SINK)"
"$PY" "$ROOT/driver/summarize_sampled.py" --results-dir "$RESULTS" --par "$PAR" --events "$EVENTS"
RC=$?

if [[ "${KEEP_UP:-}" != "1" ]]; then
    step "7. Teardown"; docker compose -p "$PROJECT" --profile clink --profile flink down -v >/dev/null 2>&1
fi
exit $RC
