#!/usr/bin/env bash
# Cross-engine output gate: do both engines emit the SAME rows from the same input?
#
# WHY THIS EXISTS SEPARATELY FROM THE THROUGHPUT SWEEP. The sweep runs a
# blackhole sink so the measurement is engine read-and-process rate rather than
# the output connector's ceiling. That also means nothing checks the output, and
# a throughput win from an engine doing LESS work looks identical to a real one.
# So each query is also run with a Kafka sink and the two engines' output row
# counts compared. No oracle is needed: the engines check each other.
#
# The gate is deliberately run at a SMALL event count. Row counts must agree
# exactly whatever the scale, so scale buys nothing here and costs wall-clock.
#
#   ./gate.sh                     # every append-only query
#   QUERIES="q15 q16" ./gate.sh
#   EVENTS=2000000 ./gate.sh
#
# WHAT IS NOT GATED, AND WHY. A plain Kafka sink is append-only. Six of the
# queries emit a CHANGELOG - a retracting stream that revises rows it already
# emitted:
#
#   q5           top-1 per sliding window: a new leader retracts the old one
#   q18, q19     dedup / ranking per key: same
#   q3, q4, q20  regular (non-time-windowed) joins, and an aggregate over one
#
# For those, a row count is not the right comparison even in principle - clink
# emits delete+insert pairs, and Flink refuses an append-only sink outright - so
# they are reported as ungated rather than quietly counted. Gating them needs an
# upsert sink on both sides and a comparison of the COMPACTED result, which is a
# different harness than this one.
set -uo pipefail

cd "$(dirname "$0")"

EVENTS="${EVENTS:-1000000}"
PARALLELISM="${PARALLELISM:-4}"
BATCH="${BATCH:-3}"
OUT="${OUT:-results-gate}"

# Append-only queries: one output row per input row, or one per closed window.
# A Kafka sink can carry these, so their row counts are directly comparable.
APPEND_ONLY="q0 q1 q2 q7 q11 q12 q14 q15 q16 q17 q21 q22"
# Changelog-emitting queries, listed so the report can name what it did not gate
# instead of leaving a silent hole.
CHANGELOG="q5 q18 q19 q3 q4 q20"

QUERIES="${QUERIES:-$APPEND_ONLY}"

mkdir -p "$OUT"
rm -f "$OUT"/*.json

read -r -a QLIST <<< "$QUERIES"
total=${#QLIST[@]}
echo "gate: $total append-only queries, $EVENTS events, par=$PARALLELISM, Kafka sink"
echo "      comparing clink vs Flink output row counts; no oracle, the engines check each other"
echo

i=0
batch_no=0
while [ "$i" -lt "$total" ]; do
    batch=("${QLIST[@]:i:BATCH}")
    batch_no=$((batch_no + 1))
    echo "================ gate batch $batch_no: ${batch[*]} ================"
    EVENTS="$EVENTS" PARALLELISM="$PARALLELISM" SINK=kafka ENGINES="clink flink" \
        QUERIES="${batch[*]}" ./throughput_sampled.sh 2>&1 | sed 's/^/  /'
    for f in results-sampled/*.json; do
        [ -e "$f" ] || continue
        cp "$f" "$OUT/"
    done
    i=$((i + BATCH))
    echo
done

echo "================ GATE RESULT ================"
python3 - "$OUT" "$CHANGELOG" <<'PY'
import glob, json, os, sys

out_dir, changelog = sys.argv[1], sys.argv[2].split()
by_q = {}
for f in glob.glob(os.path.join(out_dir, "*.json")):
    d = json.load(open(f))
    q, eng = d.get("query"), d.get("engine")
    if q and eng:
        by_q.setdefault(q, {})[eng] = d

def qnum(q):
    return int(q[1:]) if q[1:].isdigit() else 999

passed, failed, partial = [], [], []
print(f"  {'query':6} {'clink rows':>12} {'flink rows':>12}  verdict")
print("  " + "-" * 50)
for q in sorted(by_q, key=qnum):
    c, fl = by_q[q].get("clink"), by_q[q].get("flink")
    if not c or not fl:
        have = " + ".join(sorted(by_q[q]))
        partial.append(f"{q} (only {have} produced a result)")
        print(f"  {q:6} {'-':>12} {'-':>12}  NOT RUN on both engines ({have} only)")
        continue
    cr, fr = c.get("out_rows", -1), fl.get("out_rows", -1)
    if cr < 0 or fr < 0:
        partial.append(f"{q} (no output count recorded)")
        verdict = "NO COUNT (blackhole sink?)"
    elif cr == fr:
        passed.append(q)
        verdict = "MATCH"
    else:
        failed.append(f"{q}: clink={cr} flink={fr} (delta {cr - fr:+d})")
        verdict = f"MISMATCH  delta {cr - fr:+d}"
    print(f"  {q:6} {cr:>12} {fr:>12}  {verdict}")

print()
print(f"  gated and matching : {len(passed)}  ({' '.join(passed) if passed else 'none'})")
if failed:
    print(f"  MISMATCHED         : {len(failed)}")
    for f in failed:
        print(f"      {f}")
if partial:
    print(f"  incomplete         : {len(partial)}")
    for p in partial:
        print(f"      {p}")
print(f"  not gated here     : {len(changelog)}  ({' '.join(changelog)})")
print("      changelog-emitting; a plain append-only Kafka sink cannot carry them,")
print("      so their row counts are not comparable. See the header of gate.sh.")
sys.exit(1 if failed else 0)
PY
