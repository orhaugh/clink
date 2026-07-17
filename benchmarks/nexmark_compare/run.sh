#!/usr/bin/env bash
# Reproducible clink-vs-Flink Nexmark comparison (hot-path). One command: build,
# bring up Kafka + Flink, generate ONE canonical dataset, load it to PAR-partition
# topics both engines read, run each query on both engines at parallelism PAR,
# measure steady-state by broker append-time, gate on identical output-row counts,
# and print the scoreboard. See pipeline.md for the premise.
#
#   ./run.sh                      # q0 + q12, par 1, 500k events
#   EVENTS=1000000 ./run.sh       # more events
#   QUERIES="q0" ./run.sh         # a subset
#   PARALLELISM=4 ./run.sh        # par 4: PAR-partition topics, both engines at -p 4
#
# Each source subtask reads one ordered partition (PAR partitions / PAR subtasks),
# so the runtime min-merges per-partition watermarks across the keyed shuffle and
# windowed queries stay gate-exact at any PAR. Single-box, so par>1 measures
# scale-out coordination, not true distributed scaling.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
# Which clink build to run. Default is the dev build/ (RelWithDebInfo). For the
# fully-optimised comparison, point at a Release+LTO tree:
#   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release \
#         -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=ON -DCLINK_BUILD_SQL=ON -DCLINK_BUILD_BENCH=ON
#   cmake --build build-release --target clink_node clink_submit_sql nexmark_dump --parallel 10
#   BUILD_DIR="$PWD/build-release" PARALLELISM=4 ./run.sh
BUILD_DIR="${BUILD_DIR:-$CLINK_ROOT/build}"
PROJECT=nxcompare
BROKERS_HOST=localhost:9092
PY="$ROOT/../flink_compare/.venv/bin/python"
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
JM_CONTAINER="${PROJECT}-flink-jobmanager-1"
TM_CONTAINER="${PROJECT}-flink-taskmanager-1"

EVENTS="${EVENTS:-500000}"
TPS="${TPS:-1000}"        # low tps -> datetime spans many 10s windows (windowed queries fire mid-stream)
PAR="${PARALLELISM:-1}"  # uniform parallelism on both engines; input topics get PAR partitions
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
        q8) echo 1056 ;;
        *) echo 0 ;;
    esac
}

# INPUT Nexmark events a query processes - the denominator for CPU normalisation
# (events_per_cpu_sec), independent of how many output rows it emits. q0/q12 read
# the 460k bids; q8 reads person + auction (10k + 30k).
input_events_for() {
    case "$1" in
        q0|q12) echo 460000 ;;
        q8) echo 40000 ;;
        *) echo 0 ;;
    esac
}
now_s() { python3 -c 'import time;print(time.time())'; }

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
cmake --build "$BUILD_DIR" --target clink_node clink_submit_sql nexmark_dump --parallel 10 \
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

step "4. Generate dataset ($EVENTS events, tps=$TPS) + load nx-{person,auction,bid} ($PAR partition(s) each)"
"$BUILD_DIR/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR" | tail -1
# PAR partitions each: one ordered partition per source subtask at parallelism
# PAR (the loader keys by id/auction so partitions span the full time range; the
# runtime min-merges per-partition watermarks across the shuffle -> gate-exact).
# Create them explicitly so the loader does not auto-create at the broker default.
recreate_topic nx-person "$PAR"
recreate_topic nx-auction "$PAR"
recreate_topic nx-bid "$PAR"
"$PY" "$ROOT/driver/load_ndjson.py" --dir "$DATA_DIR" --bootstrap "$BROKERS_HOST" --prefix nx- \
    2>/dev/null | tail -1

run_clink() {  # query
    local q=$1 out="nx-out-$1-clink"
    recreate_topic "$out" "$PAR" append
    sed -e "s#__OUT__#$out#" -e "s#__BROKERS__#localhost:9092#" \
        "$ROOT/queries/clink/$q.tmpl.sql" > "$DATA_DIR/$q-clink.sql"
    "$BUILD_DIR/clink_node" --role=coordinator --port=7100 --http-port=8081 >"$RESULTS/clink-coordinator.log" 2>&1 &
    local coordinator=$!; sleep 2
    # clink needs ONE slot per subtask (no slot-sharing like Flink), so total
    # slots must cover (#ops * PAR). Scale slots-per-worker with PAR (>=8) so deeper
    # plans (q8's 9 ops -> 36 subtasks at PAR=4) still deploy across 4 workers.
    local slots=$(( PAR * 12 )); [ "$slots" -lt 8 ] && slots=8
    local workers=(); for i in 1 2 3 4; do
        "$BUILD_DIR/clink_node" --role=worker --coordinator-host=127.0.0.1 --coordinator-port=7100 --id=worker-$i --slots="$slots" \
            >"$RESULTS/clink-worker-$i.log" 2>&1 & workers+=($!)
    done
    sleep 3
    # CPU + wall sampled tight around the active window: just before submit (coordinator/worker
    # idle-settled) and right after the drain (measure_steady returns at the last
    # output record for a gate-passing query, so no quiet-wait pollutes the delta).
    local cpu_pre wall_pre
    cpu_pre=$("$PY" "$ROOT/driver/cpu.py" read-clink "$coordinator" "${workers[@]}")
    wall_pre=$(now_s)
    "$BUILD_DIR/clink_submit_sql" --file "$DATA_DIR/$q-clink.sql" \
        --coordinator-host 127.0.0.1 --coordinator-port 8081 --name "nx_$q" --parallelism "$PAR" >/dev/null 2>&1
    "$PY" "$ROOT/driver/measure_steady.py" --brokers "$BROKERS_HOST" --topic "$out" \
        --expected "$(expected_for "$q")" --query "$q" --engine clink --out "$RESULTS/$q-clink.json" \
        --quiet-timeout 12 2>/dev/null | tail -1
    local cpu_post wall_post
    cpu_post=$("$PY" "$ROOT/driver/cpu.py" read-clink "$coordinator" "${workers[@]}")
    wall_post=$(now_s)
    "$PY" "$ROOT/driver/cpu.py" merge "$RESULTS/$q-clink.json" --cpu-pre "$cpu_pre" \
        --cpu-post "$cpu_post" --wall-pre "$wall_pre" --wall-post "$wall_post" \
        --input-events "$(input_events_for "$q")" >/dev/null
    kill "${workers[@]}" "$coordinator" 2>/dev/null; sleep 1; kill -9 "${workers[@]}" "$coordinator" 2>/dev/null; sleep 1
}

run_flink() {  # query
    local q=$1 out="nx-out-$1-flink"
    recreate_topic "$out" "$PAR" append
    sed "s#__OUT__#$out#" "$ROOT/flink-job/queries/$q.tmpl.sql" > "$DATA_DIR/$q-flink.sql"
    docker cp "$DATA_DIR/$q-flink.sql" "$JM_CONTAINER:/tmp/$q-flink.sql" >/dev/null 2>&1
    local cpu_pre wall_pre
    cpu_pre=$("$PY" "$ROOT/driver/cpu.py" read-flink "$JM_CONTAINER" "$TM_CONTAINER")
    wall_pre=$(now_s)
    docker exec "$JM_CONTAINER" flink run -d -p "$PAR" /tmp/nexmark-sql.jar "/tmp/$q-flink.sql" >/dev/null 2>&1
    "$PY" "$ROOT/driver/measure_steady.py" --brokers "$BROKERS_HOST" --topic "$out" \
        --expected "$(expected_for "$q")" --query "$q" --engine flink --out "$RESULTS/$q-flink.json" \
        --quiet-timeout 15 2>/dev/null | tail -1
    local cpu_post wall_post
    cpu_post=$("$PY" "$ROOT/driver/cpu.py" read-flink "$JM_CONTAINER" "$TM_CONTAINER")
    wall_post=$(now_s)
    "$PY" "$ROOT/driver/cpu.py" merge "$RESULTS/$q-flink.json" --cpu-pre "$cpu_pre" \
        --cpu-post "$cpu_post" --wall-pre "$wall_pre" --wall-post "$wall_post" \
        --input-events "$(input_events_for "$q")" >/dev/null
    local jid; jid=$(docker exec "$JM_CONTAINER" flink list 2>/dev/null | grep -i running | grep -oE '[0-9a-f]{32}' | head -1)
    [ -n "$jid" ] && docker exec "$JM_CONTAINER" flink cancel "$jid" >/dev/null 2>&1
}

for q in $QUERIES; do
    step "5. $q on clink"; run_clink "$q"
    step "6. $q on Flink"; run_flink "$q"
done

step "7. Scoreboard"
# q0/q12 outputs are input-scale, so their rate is a throughput proxy and counts
# toward the geomean. q8 (windowed join) emits few rows relative to input, so its
# rate is emission-bound (indicative only) - it still gates on row count.
"$PY" "$ROOT/driver/scoreboard.py" --results-dir "$RESULTS" \
    --geomean-queries "q0 q12" \
    --premise "par=$PAR, $PAR-partition, hot-path (ckpt off, in-mem state), $EVENTS events tps=$TPS"
RC=$?

if [[ "${KEEP_UP:-}" != "1" ]]; then
    step "8. Teardown"
    docker compose -p "$PROJECT" --profile flink down -v >/dev/null 2>&1
fi
exit $RC
