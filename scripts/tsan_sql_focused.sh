#!/usr/bin/env bash
# Focused TSan pass for the new async/scheduler concurrency (SQL on). Builds
# clink_sql_tests under ThreadSanitizer and runs the ML_PREDICT (async / batched /
# on_error) + materialized-view scheduler tests serially, so the race detector sees the
# async-ML coroutine + ThreadPoolCompletionExecutor and the RefreshScheduler thread.
# Intended to run inside the clink-build Docker image.
set -euo pipefail
export CLINK_DEPS_PREFIX=/usr/local
BD=build-tsan-sql
cmake -S . -B "$BD" -DCMAKE_BUILD_TYPE=Debug -DCLINK_BUILD_SQL=ON \
  -DCMAKE_CXX_FLAGS="-fsanitize=thread" \
  -DCMAKE_C_FLAGS="-fsanitize=thread" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=thread"
cmake --build "$BD" --target clink_sql_tests -- -j6
echo "=== BUILD OK; running focused TSan tests (serial) ==="
cd "$BD"
TSAN_OPTIONS="halt_on_error=1:suppressions=/workspace/tsan-suppressions.txt" \
  ./tests/clink_sql_tests \
  --gtest_filter='*MlPredict*:*MaterializedView*:*Refresh*' 2>&1
echo "=== FOCUSED TSAN RUN COMPLETE (rc=$?) ==="
