#!/usr/bin/env bash
# Interval-join clink-vs-Flink bench driver (v1: latest-order ValueState).
#
# Pipeline: orders + payments connect -> KeyedCoProcessFunction with
# latest-order ValueState -> Joined sink. Expected emit count: roughly
# BENCH_PAYMENTS (every Payment with a prior Order on the same key).

set -euo pipefail

# Robust cleanup: every bench launches JM + TM in background. If
# submit_job fails (reject, timeout, signal) the script would exit
# under `set -e` before the inline kills run, leaving zombies on
# the JM port that silently corrupt the next iteration. Trap EXIT
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

ORDERS="${ORDERS:-5000000}"
PAYMENTS="${PAYMENTS:-5000000}"
KEYS="${KEYS:-1000}"
WINDOWS="${WINDOWS:-100}"
CLINK_CKPT_DIR="${CLINK_CKPT_DIR:-/tmp/clink-join-ckpts}"
FLINK_CKPT_DIR="${FLINK_CKPT_DIR:-/tmp/flink-join-ckpts}"
CLINK_STATE_BACKEND="${CLINK_STATE_BACKEND:-rocksdb://$CLINK_CKPT_DIR}"

step() { printf '\n=== %s ===\n' "$*"; }

step "1. Build Flink JAR"
( cd "$ROOT/flink-job" && mvn -B -q package )
JAR="$ROOT/flink-job/target/flink-join-pipeline.jar"
[[ -f "$JAR" ]] || { echo "missing $JAR"; exit 1; }

step "2. Build clink .so + clink_node + clink_submit_job"
cmake -S "$CLINK_ROOT" -B "$CLINK_ROOT/build" -DCLINK_BUILD_BENCH=ON >/dev/null
cmake --build "$CLINK_ROOT/build" --parallel 10 \
    --target clink_join_bench_pipeline clink_node clink_submit_job >/dev/null
SO="$CLINK_ROOT/build/benchmarks/join_bench_pipeline.so"
[[ -f "$SO" ]] || { echo "missing $SO"; exit 1; }

rm -rf "$CLINK_CKPT_DIR" "$FLINK_CKPT_DIR"
mkdir -p "$CLINK_CKPT_DIR" "$FLINK_CKPT_DIR"

step "3. Flink run"
FLINK_START=$(date +%s.%N)
BENCH_ORDERS="$ORDERS" \
BENCH_PAYMENTS="$PAYMENTS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
BENCH_CHECKPOINT_DIR="$FLINK_CKPT_DIR" \
    java -cp "$JAR" com.clink.bench.JoinPipeline >"$RESULTS/flink.log" 2>&1
FLINK_END=$(date +%s.%N)
FLINK_WALL=$(awk "BEGIN { printf \"%.3f\", $FLINK_END - $FLINK_START }")
echo "flink wall: ${FLINK_WALL}s"

step "4. clink run (JM + 1 TM in process)"
    "$CLINK_ROOT/build/clink_node" --role=jm --port=7253 \
        >"$RESULTS/clink_jm.log" 2>&1 &
JM_PID=$!
_CLINK_BENCH_PIDS+=("$JM_PID")
sleep 1

BENCH_ORDERS="$ORDERS" \
BENCH_PAYMENTS="$PAYMENTS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
CLINK_WB_STATE_CACHE="${CLINK_WB_STATE_CACHE:-0}" \
    "$CLINK_ROOT/build/clink_node" --role=tm \
        --jm-host=127.0.0.1 --jm-port=7253 \
        --id=tm-join --slots=16 \
        >"$RESULTS/clink_tm.log" 2>&1 &
TM_PID=$!
_CLINK_BENCH_PIDS+=("$TM_PID")
sleep 1

CLINK_START=$(date +%s.%N)
BENCH_ORDERS="$ORDERS" \
BENCH_PAYMENTS="$PAYMENTS" \
BENCH_KEYS="$KEYS" \
BENCH_WINDOWS="$WINDOWS" \
    "$CLINK_ROOT/build/clink_submit_job" \
        --job="$SO" \
        --jm-host=127.0.0.1 --jm-port=7253 \
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

# Expected join output: every Payment whose key already saw an Order.
# With orders and payments emitted in the same key-cycle and Payments
# offset by +50ms, every Payment after the first BENCH_KEYS records
# (i.e. once all keys have seen their first order) has a match. So
# expected ~= PAYMENTS - KEYS. Approximate; allow some slack in the
# warning.
EXPECTED_PANES=$(awk "BEGIN { print $PAYMENTS - $KEYS }")
FLINK_PANES=$(grep -oE 'FLINK_SINK_FINISH panes=[0-9]+' "$RESULTS/flink.log" 2>/dev/null | head -1 | awk -F= '{print $2}')
CLINK_PANES=$(grep -oE 'sink final count: [0-9]+' "$RESULTS/clink_tm.log" 2>/dev/null | head -1 | awk -F': ' '{print $2}')
FLINK_PANES=${FLINK_PANES:-0}
CLINK_PANES=${CLINK_PANES:-0}

printf '\n%-12s %12s %12s\n' "engine" "wall (s)" "joined"
printf -- '-------------------------------------\n'
printf '%-12s %12s %12s\n' "flink" "$FLINK_WALL" "$FLINK_PANES"
printf '%-12s %12s %12s\n' "clink" "$CLINK_WALL" "$CLINK_PANES"
printf 'expected: ~%s (PAYMENTS - KEYS, approximate)\n' "$EXPECTED_PANES"

# Sanity check: both engines should agree to within KEYS records.
DIFF=$(awk "BEGIN { d = $FLINK_PANES - $CLINK_PANES; print (d < 0 ? -d : d) }")
if [[ "$DIFF" -gt "$KEYS" ]]; then
    echo "WARNING: flink/clink join counts differ by $DIFF (> KEYS=$KEYS); something is off"
fi

RATIO=$(awk "BEGIN { printf \"%.2f\", $CLINK_WALL / $FLINK_WALL }")
echo ""
echo "clink / flink = ${RATIO}x"
