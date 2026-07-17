#!/usr/bin/env bash
# Reproducible clink-vs-Flink q0 per-record LATENCY comparison. One command:
# build, bring up Kafka + Flink, generate the canonical dataset, then per engine
# start the (idle) job on an EMPTY 1-partition LogAppendTime topic, replay the
# dataset at a paced rate, and measure output-append minus input-append per
# record. Gates: count, positional content, pacer rate. See pipeline.md,
# "Latency axis" - a number printed with any gate failed is not quotable.
#
#   ./latency.sh                      # 4M events (~3.68M bids) at 50k ev/s
#   RATE=100000 ./latency.sh          # higher sustained load
#   EVENTS=2000000 ./latency.sh       # shorter run
#   BUILD_DIR=$PWD/../../build-release ./latency.sh   # explicit clink build
#
# v1 scope is q0 at par=1 (positional join; see the premise for why). The paced
# rate must stay well below BOTH engines' gated q0 throughput at par=1
# (clink ~452k/s, Flink ~211k/s).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$CLINK_ROOT/build}"
PROJECT=nxcompare
BROKERS_HOST=localhost:9092
PY="$ROOT/../flink_compare/.venv/bin/python"
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
JM_CONTAINER="${PROJECT}-flink-jobmanager-1"

EVENTS="${EVENTS:-4000000}"
TPS="${TPS:-1000}"                # generator EVENT-TIME spacing (dataset identity), not the pace
RATE="${RATE:-50000}"             # paced wall-clock input rate, events/s
DATA_DIR="${DATA_DIR:-/tmp/nxcompare-data}"
RESULTS="$ROOT/results-latency"

# Bids are exactly 46/50 of the logical events (1:3:46 Person:Auction:Bid).
EXPECTED=$(( EVENTS * 46 / 50 ))

step() { printf '\n=== %s ===\n' "$*"; }
recreate_topic() {  # name partitions
    $KEX --delete --topic "$1" >/dev/null 2>&1
    while $KEX --list 2>/dev/null | grep -qx "$1"; do sleep 1; done
    $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 \
        --config message.timestamp.type=LogAppendTime >/dev/null 2>&1
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

step "4. Dataset ($EVENTS events -> $EXPECTED bids, deterministic)"
if [[ ! -f "$DATA_DIR/bid.ndjson" ]] || \
   [[ "$(wc -l < "$DATA_DIR/bid.ndjson" | tr -d ' ')" != "$EXPECTED" ]]; then
    "$BUILD_DIR/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR" | tail -1
else
    echo "reusing $DATA_DIR/bid.ndjson ($EXPECTED lines)"
fi

pace_and_measure() {  # engine in_topic out_topic
    local engine=$1 in=$2 out=$3
    "$PY" "$ROOT/driver/pace_ndjson.py" --file "$DATA_DIR/bid.ndjson" --topic "$in" \
        --bootstrap "$BROKERS_HOST" --rate "$RATE" --limit "$EXPECTED" \
        --report "$RESULTS/$engine-pace.json" >/dev/null 2>&1 &
    local pacer=$!
    "$PY" "$ROOT/driver/measure_latency.py" --brokers "$BROKERS_HOST" \
        --in-topic "$in" --out-topic "$out" --expected "$EXPECTED" --rate "$RATE" \
        --query q0_lat --engine "$engine" --out "$RESULTS/$engine-lat.json" \
        --quiet-timeout 30 2>/dev/null | tail -1
    wait "$pacer" 2>/dev/null
}

run_clink() {
    local in="nx-bid-lat-clink" out="nx-out-q0lat-clink"
    recreate_topic "$in" 1
    recreate_topic "$out" 1
    sed -e "s#__IN__#$in#" -e "s#__OUT__#$out#" -e "s#__BROKERS__#localhost:9092#" \
        "$ROOT/queries/clink/q0_lat.tmpl.sql" > "$DATA_DIR/q0_lat-clink.sql"
    "$BUILD_DIR/clink_node" --role=coordinator --port=7100 --http-port=8081 >"$RESULTS/clink-coordinator.log" 2>&1 &
    local coordinator=$!; sleep 2
    "$BUILD_DIR/clink_node" --role=worker --coordinator-host=127.0.0.1 --coordinator-port=7100 --id=worker-1 --slots=12 \
        >"$RESULTS/clink-worker-1.log" 2>&1 &
    local worker=$!; sleep 3
    "$BUILD_DIR/clink_submit_sql" --file "$DATA_DIR/q0_lat-clink.sql" \
        --coordinator-host 127.0.0.1 --coordinator-port 8081 --name nx_q0_lat --parallelism 1 >/dev/null 2>&1
    sleep 3   # job deployed + consumer subscribed on the (empty) topic
    pace_and_measure clink "$in" "$out"
    kill "$worker" "$coordinator" 2>/dev/null; sleep 1; kill -9 "$worker" "$coordinator" 2>/dev/null; sleep 1
}

run_flink() {
    local in="nx-bid-lat-flink" out="nx-out-q0lat-flink"
    recreate_topic "$in" 1
    recreate_topic "$out" 1
    sed -e "s#__IN__#$in#" -e "s#__OUT__#$out#" \
        "$ROOT/flink-job/queries/q0_lat.tmpl.sql" > "$DATA_DIR/q0_lat-flink.sql"
    docker cp "$DATA_DIR/q0_lat-flink.sql" "$JM_CONTAINER:/tmp/q0_lat-flink.sql" >/dev/null 2>&1
    docker exec "$JM_CONTAINER" flink run -d -p 1 /tmp/nexmark-sql.jar "/tmp/q0_lat-flink.sql" >/dev/null 2>&1
    # Wait for RUNNING, then a settle for source-consumer assignment.
    for i in $(seq 1 30); do
        docker exec "$JM_CONTAINER" flink list 2>/dev/null | grep -qi running && break
        sleep 2
    done
    sleep 5
    pace_and_measure flink "$in" "$out"
    local jid; jid=$(docker exec "$JM_CONTAINER" flink list 2>/dev/null | grep -i running | grep -oE '[0-9a-f]{32}' | head -1)
    [ -n "$jid" ] && docker exec "$JM_CONTAINER" flink cancel "$jid" >/dev/null 2>&1
}

step "5. q0 latency on clink (paced $RATE ev/s)"
run_clink
step "6. q0 latency on Flink (paced $RATE ev/s)"
run_flink

step "7. Latency scoreboard"
"$PY" "$ROOT/driver/latency_scoreboard.py" --results-dir "$RESULTS" \
    --premise "q0_lat par=1 1-partition hot-path, paced $RATE ev/s ($EXPECTED bids), linger.ms=0 both sinks, single box, ms resolution"
RC=$?

if [[ "${KEEP_UP:-}" != "1" ]]; then
    step "8. Teardown"
    docker compose -p "$PROJECT" --profile flink down -v >/dev/null 2>&1
fi
exit $RC
