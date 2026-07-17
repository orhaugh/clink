#!/usr/bin/env bash
# Head-to-head clink-vs-Flink benchmark driver.
#
# Steps:
#   1. Build the Flink JAR (mvn package) and the clink job .so (cmake).
#   2. Bring up Kafka.
#   3. Pre-populate input topic with N records.
#   4. For each engine:
#        - reset output topic
#        - start the engine pipeline
#        - run the consumer (which times wall + counts records)
#        - tear down the engine
#   5. Print scoreboard.
#
# Requires on the host:
#   - docker + docker compose
#   - mvn + Java 21
#   - python3 with confluent-kafka (pip install -r driver/requirements.txt)
#   - clink built with -DCLINK_BUILD_BENCH=ON

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"

# Use the local venv's python3 if it exists; otherwise fall back to PATH.
if [[ -x "$ROOT/.venv/bin/python3" ]]; then
    PYTHON="${PYTHON:-$ROOT/.venv/bin/python3}"
else
    PYTHON="${PYTHON:-python3}"
fi

RECORDS="${RECORDS:-10000000}"
KEYS="${KEYS:-1000}"
WINDOWS="${WINDOWS:-100}"
PARALLELISM="${PARALLELISM:-4}"
EXPECTED=$(( KEYS * WINDOWS ))

INPUT_TOPIC="${INPUT_TOPIC:-bench-in}"
OUTPUT_TOPIC="${OUTPUT_TOPIC:-bench-out}"
HOST_BROKERS="${HOST_BROKERS:-localhost:9092}"
CONTAINER_BROKERS="${CONTAINER_BROKERS:-kafka:29092}"

step() { printf '\n=== %s ===\n' "$*"; }

# Recreate the output topic using kafka-topics inside the broker. Called
# before each engine submission so the consumer's wall_start anchor
# (engine submission time) is meaningful and no stale records from the
# previous run leak in.
reset_output_topic() {
    docker compose -f "$ROOT/docker-compose.yml" exec -T kafka \
        kafka-topics --bootstrap-server localhost:9092 \
        --delete --if-exists --topic "$OUTPUT_TOPIC" >/dev/null 2>&1 || true
    # Poll until the topic is gone from --list. Kafka's delete is async;
    # the partition data cleanup can take a long time after a prior run
    # wrote 100K+ records, and create-while-pending-delete fails with
    # TopicExistsException.
    for _ in {1..120}; do
        if ! docker compose -f "$ROOT/docker-compose.yml" exec -T kafka \
                kafka-topics --bootstrap-server localhost:9092 \
                --list 2>/dev/null | grep -qx "$OUTPUT_TOPIC"; then
            break
        fi
        sleep 1
    done
    # Then create fresh, retrying briefly in case cleanup is still
    # finalizing under the hood.
    for _ in {1..30}; do
        if docker compose -f "$ROOT/docker-compose.yml" exec -T kafka \
                kafka-topics --bootstrap-server localhost:9092 \
                --create --topic "$OUTPUT_TOPIC" \
                --partitions "$PARALLELISM" \
                --replication-factor 1 >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "reset_output_topic: failed to create $OUTPUT_TOPIC after retries" >&2
    return 1
}

# Capture monotonic-ish epoch right before submitting an engine job.
# Passed to the consumer as --engine-start-epoch so wall_seconds is
# anchored at "engine got the job" rather than "consumer received first
# record" - the latter only captures the tail emit burst.
engine_start_now() {
    "$PYTHON" -c 'import time; print(time.time())'
}

step "1. Build Flink JAR"
( cd "$ROOT/flink-job" && mvn -B -q package )
JAR="$ROOT/flink-job/target/flink-canonical-pipeline.jar"
[[ -f "$JAR" ]] || { echo "missing $JAR"; exit 1; }

step "2. Build clink .so + node + submit"
cmake -S "$CLINK_ROOT" -B "$CLINK_ROOT/build" -DCLINK_BUILD_BENCH=ON >/dev/null
# Rebuild all three together. The plugin embeds a git-derived ABI
# hash; clink_node also embeds the same hash and rejects plugins from
# a different commit. Rebuild both whenever either's commit changes.
cmake --build "$CLINK_ROOT/build" --parallel 10 \
    --target clink_bench_canonical_pipeline clink_node clink_submit_job >/dev/null
SO="$CLINK_ROOT/build/benchmarks/bench_canonical_pipeline.so"
[[ -f "$SO" ]] || { echo "missing $SO"; exit 1; }

step "3. Bring up Kafka"
docker compose -f "$ROOT/docker-compose.yml" up -d zookeeper kafka

# Wait for Kafka to be reachable from the host.
for _ in {1..60}; do
    if docker compose -f "$ROOT/docker-compose.yml" exec -T kafka \
            kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

step "4. Populate input topic ($RECORDS records, $KEYS keys, $WINDOWS windows)"
"$PYTHON" "$ROOT/driver/producer.py" \
    --brokers "$HOST_BROKERS" \
    --topic "$INPUT_TOPIC" \
    --records "$RECORDS" \
    --keys "$KEYS" \
    --windows "$WINDOWS" \
    --partitions "$PARALLELISM"

run_flink() {
    step "5a. Flink run"
    docker compose -f "$ROOT/docker-compose.yml" --profile flink up -d \
        flink-jobmanager flink-taskmanager
    # Wait for coordinator REST.
    for _ in {1..60}; do
        if curl -sf http://localhost:8081/overview >/dev/null; then break; fi
        sleep 1
    done

    docker cp "$JAR" "$(docker compose -f "$ROOT/docker-compose.yml" \
        ps -q flink-jobmanager)":/flink-canonical-pipeline.jar

    reset_output_topic
    local engine_start
    engine_start=$(engine_start_now)

    docker compose -f "$ROOT/docker-compose.yml" exec -T \
        -e KAFKA_BOOTSTRAP="$CONTAINER_BROKERS" \
        -e INPUT_TOPIC="$INPUT_TOPIC" \
        -e OUTPUT_TOPIC="$OUTPUT_TOPIC" \
        -e PARALLELISM="$PARALLELISM" \
        flink-jobmanager flink run -d /flink-canonical-pipeline.jar &
    FLINK_SUBMIT_PID=$!

    "$PYTHON" "$ROOT/driver/consumer.py" \
        --brokers "$HOST_BROKERS" \
        --topic "$OUTPUT_TOPIC" \
        --engine flink \
        --expected "$EXPECTED" \
        --partitions "$PARALLELISM" \
        --engine-start-epoch "$engine_start" \
        --out "$RESULTS/flink.json"

    wait $FLINK_SUBMIT_PID || true
    # Stop Flink only; leave Kafka + ZK running for the clink phase.
    # `docker compose ... down` is project-wide regardless of --profile,
    # so use stop + rm targeted at the Flink containers explicitly.
    docker compose -f "$ROOT/docker-compose.yml" stop \
        flink-taskmanager flink-jobmanager
    docker compose -f "$ROOT/docker-compose.yml" rm -f \
        flink-taskmanager flink-jobmanager
}

run_clink() {
    step "5b. clink run"
    # Start coordinator + workers on host, pointing at host Kafka.
    "$CLINK_ROOT/build/clink_node" \
        --role=coordinator --port=7100 \
        >"$RESULTS/clink_coordinator.log" 2>&1 &
    JM_PID=$!
    sleep 2

    # 4 workers * 8 slots = 32 slots. Apples-to-apples with Flink:
    # parallelism=4 across ~6 operators = ~24 subtasks total, all fit
    # across this slot pool. Each Flink worker here has 4 slots too.
    declare -a TM_PIDS=()
    for i in 1 2 3 4; do
        "$CLINK_ROOT/build/clink_node" \
            --role=worker \
            --coordinator-host=127.0.0.1 --coordinator-port=7100 \
            --id="worker-$i" \
            --slots=8 \
            >"$RESULTS/clink_worker_$i.log" 2>&1 &
        TM_PIDS+=($!)
    done
    sleep 2

    reset_output_topic
    local engine_start
    engine_start=$(engine_start_now)

    BENCH_KAFKA_BROKERS="$HOST_BROKERS" \
    BENCH_INPUT_TOPIC="$INPUT_TOPIC" \
    BENCH_OUTPUT_TOPIC="$OUTPUT_TOPIC" \
    BENCH_PARALLELISM="$PARALLELISM" \
        "$CLINK_ROOT/build/clink_submit_job" \
            --job="$SO" \
            --coordinator-host=127.0.0.1 --coordinator-port=7100 \
            --state-backend=memory \
            >"$RESULTS/clink_submit.log" 2>&1 &
    SUBMIT_PID=$!

    "$PYTHON" "$ROOT/driver/consumer.py" \
        --brokers "$HOST_BROKERS" \
        --topic "$OUTPUT_TOPIC" \
        --engine clink \
        --expected "$EXPECTED" \
        --partitions "$PARALLELISM" \
        --engine-start-epoch "$engine_start" \
        --out "$RESULTS/clink.json"

    # Graceful kill first; some clink_node builds have been observed
    # not exiting on SIGTERM during teardown, so follow up with SIGKILL
    # and skip `wait` (it'd block forever on stuck children).
    kill $SUBMIT_PID 2>/dev/null || true
    for pid in "${TM_PIDS[@]}"; do kill $pid 2>/dev/null || true; done
    kill $JM_PID 2>/dev/null || true
    sleep 2
    kill -9 $SUBMIT_PID 2>/dev/null || true
    for pid in "${TM_PIDS[@]}"; do kill -9 $pid 2>/dev/null || true; done
    kill -9 $JM_PID 2>/dev/null || true
}

# Run each engine, but don't let cleanup quirks abort the scoreboard.
run_flink || echo "warning: run_flink returned non-zero"
run_clink || echo "warning: run_clink returned non-zero"

step "6. Scoreboard"
"$PYTHON" "$ROOT/driver/score.py" \
    --flink-result "$RESULTS/flink.json" \
    --clink-result "$RESULTS/clink.json"
