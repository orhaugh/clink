#!/usr/bin/env bash
# Reproducible clink-vs-Flink Nexmark comparison (parallelism 1, single-partition,
# hot-path). One command: build, bring up Kafka + Flink, generate ONE canonical
# dataset, load it to single-partition topics both engines read, run each query on
# both engines, measure steady-state by broker append-time, gate on identical
# output-row counts, and print the scoreboard. See pipeline.md for the premise.
#
#   ./run.sh                 # q0 + q12, default 500k events
#   EVENTS=1000000 ./run.sh  # more events
#   QUERIES="q0" ./run.sh    # a subset
#
# Single-partition topics are the correct config at parallelism 1 (one subtask,
# one ordered partition); multi-partition needs parallelism>1 (a follow-on, and
# the start-of-stream watermark refinement noted in the README).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
PROJECT=nxcompare
BROKERS_HOST=localhost:9092
PY="$ROOT/../flink_compare/.venv/bin/python"
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
JM_CONTAINER="${PROJECT}-flink-jobmanager-1"

EVENTS="${EVENTS:-500000}"
TPS="${TPS:-1000}"        # low tps -> datetime spans many 10s windows (windowed queries fire mid-stream)
QUERIES="${QUERIES:-q0 q12}"
DATA_DIR="${DATA_DIR:-/tmp/nxcompare-data}"
RESULTS="$ROOT/results"

# Per-query EXPECTED output-row count over this dataset (EVENTS=500k, TPS=1000).
# q0 = every bid passes through; q12 = distinct (window,bidder) over the
# watermark-closed windows (data-derived; Flink matches it exactly). A case, not
# an associative array - macOS ships bash 3.2 which has none.
expected_for() {
    case "$1" in
        q0) echo 460000 ;;
        q12) echo 184767 ;;
        *) echo 0 ;;
    esac
}

step() { printf '\n=== %s ===\n' "$*"; }
recreate_topic() {  # name partitions [log-append-time]
    $KEX --delete --topic "$1" >/dev/null 2>&1
    while $KEX --list 2>/dev/null | grep -qx "$1"; do sleep 1; done
    if [[ "${3:-}" == "append" ]]; then
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 \
            --config message.timestamp.type=LogAppendTime >/dev/null 2>&1
    else
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null 2>&1
    fi
}

mkdir -p "$RESULTS" "$DATA_DIR"
rm -f "$RESULTS"/*.json

step "1. Build clink (node, submit_sql, nexmark_dump)"
cmake --build "$CLINK_ROOT/build" --target clink_node clink_submit_sql nexmark_dump --parallel 10 \
    >/dev/null 2>&1 || { echo "clink build failed"; exit 1; }

step "2. Build Flink SQL jar"
( cd "$ROOT/flink-job" && mvn -q -o -DskipTests package 2>/dev/null || mvn -q -DskipTests package ) \
    || { echo "flink jar build failed"; exit 1; }

step "3. Bring up Kafka + Flink"
docker compose -p "$PROJECT" --profile flink up -d >/dev/null 2>&1
for i in $(seq 1 45); do
    docker exec ${PROJECT}-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 \
        >/dev/null 2>&1 && break
    sleep 2
done
for i in $(seq 1 30); do docker exec "$JM_CONTAINER" flink list >/dev/null 2>&1 && break; sleep 2; done
docker cp "$ROOT/flink-job/target/nexmark-sql.jar" "$JM_CONTAINER:/tmp/nexmark-sql.jar" >/dev/null 2>&1

step "4. Generate ONE canonical dataset ($EVENTS events, tps=$TPS) + load nx-bid (1 partition)"
"$CLINK_ROOT/build/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR" | tail -1
recreate_topic nx-bid 1
"$PY" "$ROOT/driver/load_ndjson.py" --dir "$DATA_DIR" --bootstrap "$BROKERS_HOST" --prefix nx- \
    2>/dev/null | tail -1

run_clink() {  # query
    local q=$1 out="nx-out-$1-clink"
    recreate_topic "$out" 1 append
    sed "s#__OUT__#$out#" "$ROOT/queries/clink/$q.tmpl.sql" > "$DATA_DIR/$q-clink.sql"
    "$CLINK_ROOT/build/clink_node" --role=jm --port=7100 --http-port=8081 >"$RESULTS/clink-jm.log" 2>&1 &
    local jm=$!; sleep 2
    local tms=(); for i in 1 2 3 4; do
        "$CLINK_ROOT/build/clink_node" --role=tm --jm-host=127.0.0.1 --jm-port=7100 --id=tm-$i --slots=8 \
            >"$RESULTS/clink-tm-$i.log" 2>&1 & tms+=($!)
    done
    sleep 3
    "$CLINK_ROOT/build/clink_submit_sql" --file "$DATA_DIR/$q-clink.sql" \
        --jm-host 127.0.0.1 --jm-port 8081 --name "nx_$q" >/dev/null 2>&1
    "$PY" "$ROOT/driver/measure_steady.py" --brokers "$BROKERS_HOST" --topic "$out" \
        --expected "$(expected_for "$q")" --query "$q" --engine clink --out "$RESULTS/$q-clink.json" \
        --quiet-timeout 12 2>/dev/null | tail -1
    kill "${tms[@]}" "$jm" 2>/dev/null; sleep 1; kill -9 "${tms[@]}" "$jm" 2>/dev/null; sleep 1
}

run_flink() {  # query
    local q=$1 out="nx-out-$1-flink"
    recreate_topic "$out" 1 append
    sed "s#__OUT__#$out#" "$ROOT/flink-job/queries/$q.tmpl.sql" > "$DATA_DIR/$q-flink.sql"
    docker cp "$DATA_DIR/$q-flink.sql" "$JM_CONTAINER:/tmp/$q-flink.sql" >/dev/null 2>&1
    docker exec "$JM_CONTAINER" flink run -d -p 1 /tmp/nexmark-sql.jar "/tmp/$q-flink.sql" >/dev/null 2>&1
    "$PY" "$ROOT/driver/measure_steady.py" --brokers "$BROKERS_HOST" --topic "$out" \
        --expected "$(expected_for "$q")" --query "$q" --engine flink --out "$RESULTS/$q-flink.json" \
        --quiet-timeout 15 2>/dev/null | tail -1
    local jid; jid=$(docker exec "$JM_CONTAINER" flink list 2>/dev/null | grep -i running | grep -oE '[0-9a-f]{32}' | head -1)
    [ -n "$jid" ] && docker exec "$JM_CONTAINER" flink cancel "$jid" >/dev/null 2>&1
}

for q in $QUERIES; do
    step "5. $q on clink"; run_clink "$q"
    step "6. $q on Flink"; run_flink "$q"
done

step "7. Scoreboard"
"$PY" "$ROOT/driver/scoreboard.py" --results-dir "$RESULTS" \
    --premise "par=1, 1-partition, hot-path (ckpt off, in-mem state), $EVENTS events tps=$TPS"
RC=$?

if [[ "${KEEP_UP:-}" != "1" ]]; then
    step "8. Teardown"
    docker compose -p "$PROJECT" --profile flink down -v >/dev/null 2>&1
fi
exit $RC
