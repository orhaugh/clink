#!/usr/bin/env bash
# Both-engines-containerized throughput run: clink (1 JM + 4 TM containers,
# Release+LTO image) vs Flink (containers, production JVM), same pre-generated
# Kafka input, hot-path, steady-state by broker append-time, CPU via each
# engine's container cgroups. This is the most apples-to-apples setup: BOTH
# engines run in containers (separate network namespaces, shuffle over the
# container network), both optimised, and the host clink_submit_sql only compiles
# the SQL to a spec (execution is 100% in the containers), so there is no host
# build-flag asymmetry.
#
#   ./throughput_containers.sh                 # q0 q12, par 4
#   PARALLELISM=1 QUERIES="q0 q12" ./throughput_containers.sh
#
# Requires clink-runtime:latest (built by verify_distributed.sh / Dockerfile.runtime)
# and the Flink SQL jar. Reuses the driver/{measure_steady,cpu,scoreboard}.py.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
PROJECT=nxcompare
PY="$ROOT/../flink_compare/.venv/bin/python"
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
CLINK_JM_HTTP=8095
FLINK_JM=${PROJECT}-flink-jobmanager-1
CLINK_CTRS="${PROJECT}-clink-jm-1 ${PROJECT}-clink-tm1-1 ${PROJECT}-clink-tm2-1 ${PROJECT}-clink-tm3-1 ${PROJECT}-clink-tm4-1"
FLINK_CTRS="${PROJECT}-flink-jobmanager-1 ${PROJECT}-flink-taskmanager-1"

EVENTS="${EVENTS:-500000}"
TPS="${TPS:-1000}"
PAR="${PARALLELISM:-4}"
QUERIES="${QUERIES:-q0 q12}"
DATA_DIR="${DATA_DIR:-/tmp/nxcompare-data}"
RESULTS="$ROOT/results-containers"

expected_for() { case "$1" in q0) echo 460000;; q12) echo 184767;; q8) echo 1056;; *) echo 0;; esac; }
input_events_for() { case "$1" in q0|q12) echo 460000;; q8) echo 40000;; *) echo 0;; esac; }
now_s() { python3 -c 'import time;print(time.time())'; }
step() { printf '\n=== %s ===\n' "$*"; }
recreate_topic() {
    $KEX --delete --topic "$1" >/dev/null 2>&1
    while $KEX --list 2>/dev/null | grep -qx "$1"; do sleep 1; done
    if [[ "${3:-}" == "append" ]]; then
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 \
            --config message.timestamp.type=LogAppendTime >/dev/null 2>&1
    else
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null 2>&1
    fi
}

mkdir -p "$RESULTS" "$DATA_DIR"; rm -f "$RESULTS"/*.json

step "1. Build deps (host nexmark_dump + Flink jar) and check clink-runtime image"
cmake --build "$CLINK_ROOT/build" --target nexmark_dump clink_submit_sql --parallel 10 >/dev/null 2>&1 \
    || { echo "host build failed"; exit 1; }
docker image inspect clink-runtime:latest >/dev/null 2>&1 || { echo "clink-runtime:latest missing - run verify_distributed.sh first"; exit 1; }
( cd "$ROOT/flink-job" && mvn -q -o -DskipTests package 2>/dev/null || mvn -q -DskipTests package ) \
    || { echo "flink jar build failed"; exit 1; }

step "2. Bring up Kafka + clink (containers) + Flink (containers)"
docker compose -p "$PROJECT" --profile clink --profile flink up -d >/dev/null 2>&1
for i in $(seq 1 45); do docker exec ${PROJECT}-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && break; sleep 2; done
for i in $(seq 1 60); do curl -fsS "http://127.0.0.1:${CLINK_JM_HTTP}/api/v1/health" >/dev/null 2>&1 && break; sleep 2; done
for i in $(seq 1 30); do docker exec "$FLINK_JM" flink list >/dev/null 2>&1 && break; sleep 2; done
docker cp "$ROOT/flink-job/target/nexmark-sql.jar" "$FLINK_JM:/tmp/nexmark-sql.jar" >/dev/null 2>&1

step "3. Generate dataset ($EVENTS events tps=$TPS) + load nx-{person,auction,bid} ($PAR partitions)"
"$CLINK_ROOT/build/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR" | tail -1
for t in nx-person nx-auction nx-bid; do recreate_topic "$t" "$PAR"; done
"$PY" "$ROOT/driver/load_ndjson.py" --dir "$DATA_DIR" --bootstrap localhost:9092 --prefix nx- 2>/dev/null | tail -1

run_clink() {  # query
    local q=$1 out="nx-out-$q-clink"
    recreate_topic "$out" "$PAR" append
    sed -e "s#__OUT__#$out#" -e "s#__BROKERS__#kafka:29092#" "$ROOT/queries/clink/$q.tmpl.sql" > "$DATA_DIR/$q-clink-c.sql"
    local cpu_pre wall_pre
    cpu_pre=$("$PY" "$ROOT/driver/cpu.py" read-flink $CLINK_CTRS); wall_pre=$(now_s)
    "$CLINK_ROOT/build/clink_submit_sql" --file "$DATA_DIR/$q-clink-c.sql" \
        --jm-host 127.0.0.1 --jm-port "$CLINK_JM_HTTP" --name "tc_$q" --parallelism "$PAR" >/dev/null 2>&1
    "$PY" "$ROOT/driver/measure_steady.py" --brokers localhost:9092 --topic "$out" \
        --expected "$(expected_for "$q")" --query "$q" --engine clink --out "$RESULTS/$q-clink.json" \
        --quiet-timeout 15 2>/dev/null | tail -1
    local cpu_post wall_post; cpu_post=$("$PY" "$ROOT/driver/cpu.py" read-flink $CLINK_CTRS); wall_post=$(now_s)
    "$PY" "$ROOT/driver/cpu.py" merge "$RESULTS/$q-clink.json" --cpu-pre "$cpu_pre" --cpu-post "$cpu_post" \
        --wall-pre "$wall_pre" --wall-post "$wall_post" --input-events "$(input_events_for "$q")" >/dev/null
}

run_flink() {  # query
    local q=$1 out="nx-out-$q-flink"
    recreate_topic "$out" "$PAR" append
    sed "s#__OUT__#$out#" "$ROOT/flink-job/queries/$q.tmpl.sql" > "$DATA_DIR/$q-flink-c.sql"
    docker cp "$DATA_DIR/$q-flink-c.sql" "$FLINK_JM:/tmp/$q-flink-c.sql" >/dev/null 2>&1
    local cpu_pre wall_pre; cpu_pre=$("$PY" "$ROOT/driver/cpu.py" read-flink $FLINK_CTRS); wall_pre=$(now_s)
    docker exec "$FLINK_JM" flink run -d -p "$PAR" /tmp/nexmark-sql.jar "/tmp/$q-flink-c.sql" >/dev/null 2>&1
    "$PY" "$ROOT/driver/measure_steady.py" --brokers localhost:9092 --topic "$out" \
        --expected "$(expected_for "$q")" --query "$q" --engine flink --out "$RESULTS/$q-flink.json" \
        --quiet-timeout 18 2>/dev/null | tail -1
    local cpu_post wall_post; cpu_post=$("$PY" "$ROOT/driver/cpu.py" read-flink $FLINK_CTRS); wall_post=$(now_s)
    "$PY" "$ROOT/driver/cpu.py" merge "$RESULTS/$q-flink.json" --cpu-pre "$cpu_pre" --cpu-post "$cpu_post" \
        --wall-pre "$wall_pre" --wall-post "$wall_post" --input-events "$(input_events_for "$q")" >/dev/null
    local jid; jid=$(docker exec "$FLINK_JM" flink list 2>/dev/null | grep -i running | grep -oE '[0-9a-f]{32}' | head -1)
    [ -n "$jid" ] && docker exec "$FLINK_JM" flink cancel "$jid" >/dev/null 2>&1
}

for q in $QUERIES; do
    step "4. $q on clink (containers, par=$PAR)"; run_clink "$q"
    step "5. $q on Flink (containers, par=$PAR)"; run_flink "$q"
done

step "6. Scoreboard (both engines containerized, par=$PAR)"
"$PY" "$ROOT/driver/scoreboard.py" --results-dir "$RESULTS" --geomean-queries "q0 q12" \
    --premise "BOTH CONTAINERIZED, par=$PAR, $PAR-partition, hot-path, $EVENTS events tps=$TPS, clink Release+LTO"
RC=$?
cat <<'CAVEAT'

  !! MEASUREMENT CAVEAT - read before quoting any number above !!
  At this scale the RATE column is NOT a valid sustained-throughput ratio:
    - clink burst-drains the pre-loaded topic (engine finishes in well under 1s),
      so its broker-append-time rate measures the sink flush, not steady processing.
    - Flink is JVM-warmup-dominated on a job this small (tens of seconds, much of
      it warmup), so its rate is understated.
    - events/wall is capped by the Python consumer (~50-60k rec/s), which bounds
      the FAST engine - so wall-rate understates clink.
  The least-confounded signal is the CPU column (events / measured CPU-second):
  clink consumes several-x less CPU for the same work - but at this scale that too
  is inflated by Flink's JVM warmup/baseline over its longer run.
  A credible SUSTAINED-throughput ratio needs engine-side metrics sampling
  (records-out/sec from each engine, mid-run) + a much larger or rate-limited
  input so warmup amortizes and no downstream consumer is the bottleneck.
  What IS solid here: the correctness gate (identical output-row counts) and that
  both engines run containerized at par>1.
CAVEAT
if [[ "${KEEP_UP:-}" != "1" ]]; then
    step "7. Teardown"; docker compose -p "$PROJECT" --profile clink --profile flink down -v >/dev/null 2>&1
fi
exit $RC
