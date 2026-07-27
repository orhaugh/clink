#!/usr/bin/env bash
# Why does Flink draw ~7 cores on nexmark q0 where clink draws ~2?
#
# Cores drawn = drain rate / events-per-CPU-second, and on q0 that is 2.1 for
# clink against 7.0 for Flink. The larger number is NOT automatically better use
# of the machine: it counts CPU consumed, not CPU consumed usefully, and two
# unrelated things inflate it.
#
#   * Work that is not the query. A JVM's GC and JIT-compiler threads, Netty event
#     loops, checkpoint RPC. Burned, never converted into throughput - which is
#     consistent with Flink's events-per-CPU-second being 1.8x WORSE than clink's
#     on this same query.
#   * Real pipelining. Flink's KafkaSource runs a fetcher on its own thread with a
#     handover queue, so broker wait overlaps processing. clink's planner fuses
#     q0's forward-edge chain into ONE operator, so one thread per pipeline
#     instance does the fetch and all the processing in series - and at par 4 that
#     caps it at 4 cores, of which it draws 2.1, meaning roughly half idle.
#
# Those two point at opposite conclusions - "Flink is wasteful" versus "clink
# lacks a fetcher thread" - so the split has to be measured rather than argued.
# Threads self-name, so this samples per-thread CPU inside each engine's container
# while it runs the same query.
#
#   ./why_cores.sh                 # q0 at par 4, 8M events
#   QUERY=q12 ./why_cores.sh       # a query clink WINS, for contrast
set -uo pipefail

cd "$(dirname "$0")"
CLINK_ROOT=../..
EVENTS="${EVENTS:-8000000}"
PAR="${PAR:-4}"
QUERY="${QUERY:-q0}"
SAMPLE="${SAMPLE:-6}"
JM_HTTP=8095
FLINK_JM=nxcompare-flink-jobmanager-1
FLINK_TM=nxcompare-flink-taskmanager-1

cleanup() {
    docker compose -p nxcompare --profile clink --profile flink down -v \
        --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "=== Bring up BOTH engines + load $EVENTS events ==="
EVENTS="$EVENTS" PARALLELISM="$PAR" SINK=blackhole ENGINES="clink flink" \
    QUERIES="$QUERY" KEEP_UP=1 ./throughput_sampled.sh 2>&1 \
    | grep -E "bid stream|broker:|drain|RATIO|DRAIN" | sed 's/^/  /'

# A fresh consumer group per probe, or a re-submit resumes at the committed offset
# and reads nothing.
probe_sql() {  # engine_dir template_prefix out_file tag
    sed -e "s#__BROKERS__#kafka:29092#" -e "s#__OUT__#nx-probe#" "$1" > "$2"
    sed -i.bak -E "s#group_id='[^']*'#group_id='probe-$3'#" "$2"
    rm -f "$2.bak"
}

echo
echo "=== clink: per-thread CPU while running $QUERY ==="
probe_sql "queries/clink/${QUERY}_bh.tmpl.sql" /tmp/nxq-why-clink.sql "clink-$$"
CJID=$("$CLINK_ROOT/build/clink_submit_sql" --file /tmp/nxq-why-clink.sql \
        --coordinator-host 127.0.0.1 --coordinator-port "$JM_HTTP" \
        --name why_clink --parallelism "$PAR" 2>/dev/null \
      | python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except: pass')
if [ -n "$CJID" ]; then
    for w in 1 2 3 4; do
        c="nxcompare-clink-worker${w}-1"
        docker inspect "$c" >/dev/null 2>&1 || continue
        python3 driver/thread_split.py --container "$c" --seconds 2 --windows 6 --top 10 \
            | sed 's/^/  /'
        echo
        break  # one worker is representative: each runs one pipeline instance
    done
    curl -fsS -X POST "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$CJID/cancel" >/dev/null 2>&1 || true
else
    echo "  clink submit failed"
fi

echo "=== Flink: per-thread CPU while running $QUERY ==="
probe_sql "flink-job/queries/${QUERY}_bh.tmpl.sql" /tmp/nxq-why-flink.sql "flink-$$"
docker cp /tmp/nxq-why-flink.sql "$FLINK_JM:/tmp/why.sql" >/dev/null 2>&1
FJID=$(docker exec "$FLINK_JM" flink run -d -p "$PAR" /tmp/nexmark-sql.jar /tmp/why.sql 2>&1 \
       | grep -oE '[0-9a-f]{32}' | head -1)
if [ -n "$FJID" ]; then
    python3 driver/thread_split.py --container "$FLINK_TM" --seconds 2 --windows 6 --top 14 \
        | sed 's/^/  /'
    docker exec "$FLINK_JM" flink cancel "$FJID" >/dev/null 2>&1 || true
else
    echo "  flink submit failed"
fi

echo
echo "Read it as: how much of each engine's CPU is the QUERY, and how much is"
echo "everything else. A large 'GC / memory management' or 'JIT compilation' share"
echo "means cores burned without throughput. A meaningful 'Kafka fetch' share on"
echo "one engine and none on the other means that engine overlaps broker wait with"
echo "processing and the other does not."
