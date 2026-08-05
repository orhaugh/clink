#!/usr/bin/env bash
# Run a test repeatedly until it fails, and keep everything from the run that did.
#
# For the failures that only appear in a full sweep. Two of those are on record
# (follow-up items 29 and 31): each was seen once in a serial run of the whole
# integration label and each passes 3/3 in isolation, which is precisely why
# neither has been diagnosed - there is nothing to attach a debugger to.
#
# The gap between "in a sweep" and "alone" is load: a sweep runs on a warm, busy
# machine with other tests' processes still winding down. --load emulates that
# with background CPU pressure, which is the cheapest way to reproduce the
# condition without paying 20 minutes per sweep.
#
# Usage:
#   scripts/repeat-until-fail.sh --filter 'HaFailoverTest.ExactlyOnce*' [options]
#
#   --filter F     gtest filter (required)
#   --runs N       maximum iterations (default 50)
#   --load N       background CPU hogs during each run (default 0)
#   --binary PATH  test binary (default build-it/tests/clink_integration_tests)
#   --out DIR      where to keep the failing run's output (default ./repeat-fail-out)
#
# Exits 1 on the first failure, having written the run's stdout and named the
# iteration. Exits 0 if every run passed - which is a result too, and the script
# prints the count so it can be quoted honestly rather than as "could not
# reproduce".
set -uo pipefail

cd "$(dirname "$0")/.."

FILTER=""
RUNS=50
LOAD=0
BINARY="build-it/tests/clink_integration_tests"
OUT="repeat-fail-out"

while [ $# -gt 0 ]; do
    case "$1" in
        --filter) FILTER="$2"; shift 2 ;;
        --runs)   RUNS="$2";   shift 2 ;;
        --load)   LOAD="$2";   shift 2 ;;
        --binary) BINARY="$2"; shift 2 ;;
        --out)    OUT="$2";    shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

if [ -z "$FILTER" ]; then
    echo "repeat-until-fail: --filter is required" >&2
    exit 2
fi
if [ ! -x "$BINARY" ]; then
    echo "repeat-until-fail: $BINARY is not executable; build it first" >&2
    exit 2
fi

mkdir -p "$OUT"
LOAD_PIDS=()
cleanup() {
    for p in "${LOAD_PIDS[@]:-}"; do
        kill "$p" 2>/dev/null || true
    done
}
trap cleanup EXIT

if [ "$LOAD" -gt 0 ]; then
    echo "repeat-until-fail: starting $LOAD background CPU hogs"
    for _ in $(seq 1 "$LOAD"); do
        ( while :; do :; done ) &
        LOAD_PIDS+=($!)
    done
fi

echo "repeat-until-fail: '$FILTER' up to $RUNS times, load=$LOAD"
for i in $(seq 1 "$RUNS"); do
    log="$OUT/run-$i.log"
    if ! "$BINARY" --gtest_filter="$FILTER" > "$log" 2>&1; then
        echo ""
        echo "repeat-until-fail: FAILED on iteration $i of $RUNS"
        echo "  output: $log"
        grep -aE "\[  FAILED  \]|Failure$" "$log" | head -10
        # ScopedDiagnostics keeps the cluster's artefacts on failure and prints
        # where; surface that so the next step has somewhere to look.
        grep -a "artifacts kept" "$log" | head -3
        exit 1
    fi
    rm -f "$log"
    printf '\rrepeat-until-fail: %d/%d passed' "$i" "$RUNS"
done

echo ""
echo "repeat-until-fail: $RUNS/$RUNS passed under load=$LOAD."
echo "That bounds the rate, it does not prove absence - say so when quoting it."
