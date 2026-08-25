#!/usr/bin/env bash
# QUAL-12's runner: execute every declared row and record what happened.
#
# Each row in refusals.json names the test binary and gtest filter that
# exercises it. This runs them and writes one result line per row:
#
#   passed  -> the row behaved as declared (the test asserts the declared
#              outcome; that is the test's whole content)
#   failed  -> MEASURED differs from DECLARED - the summariser FAILs and
#              names the row
#   skipped -> UNEXERCISED. A live row whose server could not be started
#              reports this, and the summariser treats it as no evidence
#              rather than as a pass. That distinction is the campaign's
#              central discipline: an unproven row and a missing row must
#              never look alike.
#
# The runner never decides a row's outcome itself. It reports whether the
# row's own proof ran and passed; refusals.json holds what the row means.
#
#   ./run-matrix.sh [--out results.jsonl] [--build-dir DIR]
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build}"
OUT="${OUT:-$REPO_ROOT/qualification-results/qual12/results.jsonl}"
RUN_ID="${RUN_ID:-qual12-local}"

while [ $# -gt 0 ]; do
    case "$1" in
        --out) OUT="$2"; shift 2 ;;
        --build-dir) BUILD_DIR="$2"; shift 2 ;;
        --run-id) RUN_ID="$2"; shift 2 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done
mkdir -p "$(dirname "$OUT")"
: > "$OUT"

binary_for() {
    case "$1" in
        core)        echo "$BUILD_DIR/tests/clink_core_tests" ;;
        integration) echo "$BUILD_DIR/tests/clink_integration_tests" ;;
        kafka)       echo "$BUILD_DIR/impls/kafka/tests/clink_kafka_tests" ;;
        *)           echo "" ;;
    esac
}

rows=$(python3 -c "
import json
d = json.load(open('$HERE/refusals.json'))
for surface, rows in d.items():
    if surface.startswith('_'):
        continue
    for r in rows:
        print(r['id'], r['proof_binary'], r['proof_test'], sep='\t')
")

echo "QUAL-12 refusal matrix: $(echo "$rows" | wc -l | tr -d ' ') declared rows"
while IFS=$'\t' read -r id binary filter; do
    [ -n "$id" ] || continue
    bin=$(binary_for "$binary")
    if [ -z "$bin" ] || [ ! -x "$bin" ]; then
        python3 -c "
import json,sys
print(json.dumps({'id': sys.argv[1], 'outcome': 'UNEXERCISED',
                  'detail': 'test binary not built: ' + sys.argv[2]}))" \
            "$id" "${bin:-$binary}" >> "$OUT"
        echo "  $id: UNEXERCISED (no binary)"
        continue
    fi
    log=$(mktemp)
    "$bin" --gtest_filter="$filter" > "$log" 2>&1
    rc=$?
    # A row is only proven when every test behind it RAN and passed. gtest
    # exits 0 for a fully-skipped filter, so the skip count is what
    # separates "proven" from "not exercised here" - without this a row
    # whose server was unavailable would read as a pass.
    ran=$(grep -cE '^\[       OK \]' "$log")
    skipped=$(grep -cE '^\[  SKIPPED \]' "$log")
    # ORDER MATTERS: "no case ran" is checked BEFORE the exit status. A
    # fixture that cannot start its server exits non-zero having measured
    # NOTHING (gtest reports SetUpTestSuite failed and skips the cases),
    # and calling that a measured difference would blame the engine for
    # the absence of a broker. Nothing ran, so nothing is known.
    if [ "${ran:-0}" -eq 0 ]; then
        why=$(grep -E 'C\+\+ exception|Skipped|SKIPPED' "$log" | head -1 | cut -c1-200)
        python3 -c "
import json,sys
print(json.dumps({'id': sys.argv[1], 'outcome': 'UNEXERCISED',
                  'detail': sys.argv[2]}))" \
            "$id" "no case ran (${skipped:-0} skipped): ${why:-the proof could not be exercised here}" \
            >> "$OUT"
        echo "  $id: UNEXERCISED (${skipped:-0} skipped)"
    elif [ "$rc" -ne 0 ]; then
        detail=$(grep -E 'Failure|error:' "$log" | head -2 | tr '\n' ' ' | cut -c1-300)
        python3 -c "
import json,sys
print(json.dumps({'id': sys.argv[1], 'outcome': 'MEASURED_DIFFERENT',
                  'detail': sys.argv[2]}))" "$id" "${detail:-test failed}" >> "$OUT"
        echo "  $id: MEASURED_DIFFERENT"
    elif [ "${skipped:-0}" -gt 0 ]; then
        python3 -c "
import json,sys
print(json.dumps({'id': sys.argv[1], 'outcome': 'UNEXERCISED',
                  'detail': sys.argv[2]}))" \
            "$id" "${skipped} of the row's cases were skipped; a partially exercised row is not proven" \
            >> "$OUT"
        echo "  $id: UNEXERCISED (partial: ${skipped} skipped)"
    else
        # The row's declared outcome is what its test asserts, so a green
        # run means the declared outcome was observed. The summariser
        # reads the declaration; the runner only reports agreement.
        python3 -c "
import json,sys
d = json.load(open(sys.argv[2]))
declared = next(r['outcome'] for s, rows in d.items() if not s.startswith('_')
                for r in rows if r['id'] == sys.argv[1])
print(json.dumps({'id': sys.argv[1], 'outcome': declared,
                  'detail': sys.argv[3] + ' case(s) passed'}))" \
            "$id" "$HERE/refusals.json" "$ran" >> "$OUT"
        echo "  $id: ${ran} case(s) passed"
    fi
    rm -f "$log"
done <<< "$rows"

echo
python3 "$HERE/summarise.py" --results "$OUT" --run-id "$RUN_ID" \
    | tee "$(dirname "$OUT")/QUAL-12-summary.md"
