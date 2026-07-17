#!/usr/bin/env bash
# In-process clink-vs-Flink bench driver.
#
# Both engines run a bounded pipeline in-process (no Kafka, no Docker)
# and finish naturally when the synthetic source is drained.
# Wall-clock measured from job submission to job completion.
#
# Requires:
#   - mvn + Java 21 (for the Flink JAR)
#   - clink built with -DCLINK_BUILD_BENCH=ON
#   - Flink 2.2 installed locally (we use embedded execution, no cluster)

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
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"

RECORDS="${RECORDS:-10000000}"
KEYS="${KEYS:-1000}"
WINDOWS="${WINDOWS:-100}"
PAYLOAD_BYTES="${PAYLOAD_BYTES:-1500}"
CLINK_CKPT_DIR="${CLINK_CKPT_DIR:-/tmp/clink-inproc-ckpts}"
FLINK_CKPT_DIR="${FLINK_CKPT_DIR:-/tmp/flink-inproc-ckpts}"
# Override the clink state-backend URI to compare e.g. memory:// vs
# rocksdb://. Default is RocksDB to match the Flink config.
CLINK_STATE_BACKEND="${CLINK_STATE_BACKEND:-rocksdb://$CLINK_CKPT_DIR}"

step() { printf '\n=== %s ===\n' "$*"; }

step "1. Build Flink JAR (embedded run, no cluster)"
( cd "$ROOT/flink-job" && mvn -B -q package )
JAR="$ROOT/flink-job/target/flink-inproc-pipeline.jar"
[[ -f "$JAR" ]] || { echo "missing $JAR"; exit 1; }

step "2. Build clink .so + clink_node + clink_submit_job"
cmake -S "$CLINK_ROOT" -B "$CLINK_ROOT/build" -DCLINK_BUILD_BENCH=ON >/dev/null
cmake --build "$CLINK_ROOT/build" --parallel 10 \
    --target clink_inproc_bench_pipeline clink_node clink_submit_job >/dev/null
SO="$CLINK_ROOT/build/benchmarks/inproc_bench_pipeline.so"
[[ -f "$SO" ]] || { echo "missing $SO"; exit 1; }

# Wipe any previous checkpoint dirs so each run starts clean. RocksDB
# requires the parent dir to exist (it only mkdirs the per-subtask
# leaf), so recreate after the rm.
rm -rf "$CLINK_CKPT_DIR" "$FLINK_CKPT_DIR"
mkdir -p "$CLINK_CKPT_DIR" "$FLINK_CKPT_DIR"

step "3. Flink run (embedded, foreground)"
FLINK_START=$(date +%s.%N)
BENCH_RECORDS="$RECORDS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
BENCH_PAYLOAD_BYTES="$PAYLOAD_BYTES" \
BENCH_CHECKPOINT_DIR="$FLINK_CKPT_DIR" \
    java -cp "$JAR" com.clink.bench.InProcPipeline >"$RESULTS/flink.log" 2>&1
FLINK_END=$(date +%s.%N)
FLINK_WALL=$(awk "BEGIN { printf \"%.3f\", $FLINK_END - $FLINK_START }")
echo "flink wall: ${FLINK_WALL}s"

step "4. clink run (coordinator + 1 worker in process, foreground)"
CLINK_PLAN_FUSE_PAR1=1 \
    "$CLINK_ROOT/build/clink_node" --role=coordinator --port=7150 \
        >"$RESULTS/clink_coordinator.log" 2>&1 &
JM_PID=$!
_CLINK_BENCH_PIDS+=("$JM_PID")
sleep 1

# The .so reads BENCH_* from getenv() inside define_job, which runs
# in the worker process — not the submit client — so the worker must see them.
BENCH_RECORDS="$RECORDS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
BENCH_PAYLOAD_BYTES="$PAYLOAD_BYTES" \
CLINK_PLAN_FUSE_PAR1=1 \
CLINK_WB_STATE_CACHE="${CLINK_WB_STATE_CACHE:-0}" \
    "$CLINK_ROOT/build/clink_node" --role=worker \
        --coordinator-host=127.0.0.1 --coordinator-port=7150 \
        --id=worker-inproc --slots=16 \
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
        --coordinator-host=127.0.0.1 --coordinator-port=7150 \
        --state-backend="$CLINK_STATE_BACKEND" \
        --checkpoint-interval-ms=5000 \
        --wait-timeout-s=300 \
        >"$RESULTS/clink_submit.log" 2>&1
CLINK_END=$(date +%s.%N)
CLINK_WALL=$(awk "BEGIN { printf \"%.3f\", $CLINK_END - $CLINK_START }")
echo "clink wall: ${CLINK_WALL}s"

# Tear down.
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

# Sanity check pane counts from both sides. Both pipelines should
# emit KEYS * WINDOWS panes; if either side reports something else
# the wall-time comparison is bogus. Prior to 2026-06-01 the Flink
# source emitted zero panes due to a broken SplitSerializer and the
# 3.5s "Flink wall" we'd been quoting was JVM startup time.
EXPECTED_PANES=$(awk "BEGIN { print $KEYS * $WINDOWS }")
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
