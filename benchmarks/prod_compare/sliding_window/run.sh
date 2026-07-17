#!/usr/bin/env bash
# Sliding-window clink-vs-Flink bench driver.
#
# Pipeline: bounded synthetic source -> keyBy -> sliding_window(size=1s,
# slide=250ms) -> aggregate -> counting sink. Each record fans out into
# size/slide = 4 windows, so the per-record state work is 4x the
# tumbling-window shape used in inproc_compare/. Default workload spec
# matches the README.

set -euo pipefail

# Robust cleanup: every bench launches coordinator + worker in background. If
# submit_job fails (reject, timeout, signal) the script would exit
# under `set -e` before the inline kills run, leaving zombies on
# the coordinator port that silently corrupt the next iteration. Trap EXIT
# instead so the kills fire on every exit path.
_CLINK_BENCH_PIDS=()
_clink_bench_cleanup() {
    for pid in "${_CLINK_BENCH_PIDS[@]:-}"; do
        [ -z "$pid" ] && continue
        kill -TERM "$pid" 2>/dev/null || true
    done
    sleep 1
    for pid in "${_CLINK_BENCH_PIDS[@]:-}"; do
        [ -z "$pid" ] && continue
        kill -KILL "$pid" 2>/dev/null || true
    done
}
trap _clink_bench_cleanup EXIT INT TERM

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../../.." && pwd)"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"

RECORDS="${RECORDS:-10000000}"
KEYS="${KEYS:-1000}"
WINDOWS="${WINDOWS:-60}"
SIZE_MS="${SIZE_MS:-1000}"
SLIDE_MS="${SLIDE_MS:-250}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-1500}"
CLINK_CKPT_DIR="${CLINK_CKPT_DIR:-/tmp/clink-sliding-ckpts}"
FLINK_CKPT_DIR="${FLINK_CKPT_DIR:-/tmp/flink-sliding-ckpts}"
CLINK_STATE_BACKEND="${CLINK_STATE_BACKEND:-rocksdb://$CLINK_CKPT_DIR}"

step() { printf '\n=== %s ===\n' "$*"; }

step "1. Build Flink JAR"
( cd "$ROOT/flink-job" && mvn -B -q package )
JAR="$ROOT/flink-job/target/flink-sliding-pipeline.jar"
[[ -f "$JAR" ]] || { echo "missing $JAR"; exit 1; }

step "2. Build clink .so + clink_node + clink_submit_job"
cmake -S "$CLINK_ROOT" -B "$CLINK_ROOT/build" -DCLINK_BUILD_BENCH=ON >/dev/null
cmake --build "$CLINK_ROOT/build" --parallel 10 \
    --target clink_sliding_bench_pipeline clink_node clink_submit_job >/dev/null
SO="$CLINK_ROOT/build/benchmarks/sliding_bench_pipeline.so"
[[ -f "$SO" ]] || { echo "missing $SO"; exit 1; }

rm -rf "$CLINK_CKPT_DIR" "$FLINK_CKPT_DIR"
mkdir -p "$CLINK_CKPT_DIR" "$FLINK_CKPT_DIR"

step "3. Flink run"
FLINK_START=$(date +%s.%N)
BENCH_RECORDS="$RECORDS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
BENCH_PAYLOAD_BYTES="$PAYLOAD_BYTES" \
BENCH_CHECKPOINT_DIR="$FLINK_CKPT_DIR" \
    java -cp "$JAR" com.clink.bench.SlidingPipeline >"$RESULTS/flink.log" 2>&1
FLINK_END=$(date +%s.%N)
FLINK_WALL=$(awk "BEGIN { printf \"%.3f\", $FLINK_END - $FLINK_START }")
echo "flink wall: ${FLINK_WALL}s"

step "4. clink run (coordinator + 1 worker in process)"
CLINK_PLAN_FUSE_PAR1=1 \
    "$CLINK_ROOT/build/clink_node" --role=coordinator --port=7151 \
        >"$RESULTS/clink_coordinator.log" 2>&1 &
JM_PID=$!
_CLINK_BENCH_PIDS+=("$JM_PID")
sleep 1

BENCH_RECORDS="$RECORDS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
BENCH_PAYLOAD_BYTES="$PAYLOAD_BYTES" \
CLINK_PLAN_FUSE_PAR1=1 \
CLINK_WB_STATE_CACHE="${CLINK_WB_STATE_CACHE:-0}" \
    "$CLINK_ROOT/build/clink_node" --role=worker \
        --coordinator-host=127.0.0.1 --coordinator-port=7151 \
        --id=worker-sliding --slots=16 \
        >"$RESULTS/clink_worker.log" 2>&1 &
TM_PID=$!
_CLINK_BENCH_PIDS+=("$TM_PID")
sleep 1

CLINK_START=$(date +%s.%N)
BENCH_RECORDS="$RECORDS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
BENCH_PAYLOAD_BYTES="$PAYLOAD_BYTES" \
    "$CLINK_ROOT/build/clink_submit_job" \
        --job="$SO" \
        --coordinator-host=127.0.0.1 --coordinator-port=7151 \
        --state-backend="$CLINK_STATE_BACKEND" \
        --checkpoint-interval-ms=5000 \
        --wait-timeout-s=300 \
        >"$RESULTS/clink_submit.log" 2>&1
CLINK_END=$(date +%s.%N)
CLINK_WALL=$(awk "BEGIN { printf \"%.3f\", $CLINK_END - $CLINK_START }")
echo "clink wall: ${CLINK_WALL}s"

kill $TM_PID 2>/dev/null || true
kill $JM_PID 2>/dev/null || true
sleep 1
kill -9 $TM_PID 2>/dev/null || true
kill -9 $JM_PID 2>/dev/null || true

step "5. Scoreboard"

# Durability-mode banner: scoreboard mode line MUST appear in every
# run so the durability shape is visible alongside the wall time.
# Both modes are durable through the last completed checkpoint
# barrier (same contract as Flink with setDisableWAL=true); they
# differ in how the state reaches RocksDB. Strict writes through
# per-record (CLINK_WB_STATE_CACHE=0); WB-cache holds the
# authoritative state in mem_ and batches the flush at on_barrier
# (CLINK_WB_STATE_CACHE=1).
MODE_VAR="${CLINK_WB_STATE_CACHE:-0}"
if [[ "$MODE_VAR" == "0" ]]; then
    MODE_LABEL="strict (per-record RocksDB; matches Flink)"
else
    MODE_LABEL="WB-cache (in-memory + on_barrier flush; durable through last barrier, matches Flink)"
fi
echo "clink durability mode: $MODE_LABEL"

# Sliding-window expected pane count.
# Event-time span is WINDOWS * SIZE ms. A sliding window starts at
# every multiple of SLIDE; a record at ts=0 falls into the (size/slide)
# windows ending at 0..size-slide, so the earliest start is
# -(SIZE - SLIDE). The latest start is the largest multiple of SLIDE
# strictly less than the event span. Total distinct window starts =
# (event_span + SIZE - SLIDE) / SLIDE. Multiply by KEYS.
#
# Defaults (KEYS=1000, WINDOWS=60, SIZE=1000, SLIDE=250): 1000 *
# (60000 + 750) / 250 = 243_000 panes.
EXPECTED_PANES=$(awk "BEGIN { print $KEYS * (($WINDOWS * $SIZE_MS + $SIZE_MS - $SLIDE_MS) / $SLIDE_MS) }")
FLINK_PANES=$(grep -oE 'FLINK_SINK_FINISH panes=[0-9]+' "$RESULTS/flink.log" 2>/dev/null | head -1 | awk -F= '{print $2}')
CLINK_PANES=$(grep -oE 'sink final count: [0-9]+' "$RESULTS/clink_worker.log" 2>/dev/null | head -1 | awk -F': ' '{print $2}')
FLINK_PANES=${FLINK_PANES:-0}
CLINK_PANES=${CLINK_PANES:-0}

printf '\n%-12s %12s %12s\n' "engine" "wall (s)" "panes"
printf -- '-------------------------------------\n'
printf '%-12s %12s %12s\n' "flink" "$FLINK_WALL" "$FLINK_PANES"
printf '%-12s %12s %12s\n' "clink" "$CLINK_WALL" "$CLINK_PANES"
printf 'expected panes: %s\n' "$EXPECTED_PANES"

if [[ "$FLINK_PANES" != "$EXPECTED_PANES" ]]; then
    echo "WARNING: flink emitted $FLINK_PANES panes, expected $EXPECTED_PANES - flink wall is suspect"
fi
if [[ "$CLINK_PANES" != "$EXPECTED_PANES" ]]; then
    echo "WARNING: clink emitted $CLINK_PANES panes, expected $EXPECTED_PANES - clink wall is suspect"
fi

RATIO=$(awk "BEGIN { printf \"%.2f\", $CLINK_WALL / $FLINK_WALL }")
echo ""
echo "clink / flink = ${RATIO}x"
