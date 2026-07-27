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
# WHAT IS NOT GATED HERE, AND WHY. A plain Kafka sink is append-only. THREE of the
# queries emit a CHANGELOG - each revises rows it already emitted, so a row count
# is not the right comparison even in principle:
#
#   q5        top-1 per sliding window: a new leader retracts the old one
#   q18, q19  dedup / ranking per key: same
#
# Gating those needs an upsert sink on both sides and a comparison of the compacted
# result, which is upsert_gate.sh.
#
# q3, q4 and q20 were also on that list, on the assumption that a multi-stream
# query must produce a changelog. It does not: an inner join of two append-only
# inputs cannot retract. Checked rather than assumed - each was compiled against a
# plain Kafka sink and accepted - so all three are gated normally here.
set -uo pipefail

cd "$(dirname "$0")"

EVENTS="${EVENTS:-1000000}"
PARALLELISM="${PARALLELISM:-4}"
BATCH="${BATCH:-3}"
OUT="${OUT:-results-gate}"

# Append-only queries: one output row per input row, or one per closed window.
# A Kafka sink can carry these, so their row counts are directly comparable.
# q3, q4 and q20 join two streams. Both inputs are append-only and an inner join
# of two append-only inputs cannot retract, so clink accepts a plain Kafka sink for
# all three - verified by compiling each against one. They belong here, not in the
# ungated list where they sat on the assumption that "multi-stream" meant
# "changelog".
APPEND_ONLY="q0 q1 q2 q3 q4 q7 q11 q12 q14 q15 q16 q17 q20 q21 q22"
# Changelog-emitting, so the report can name what it did not gate rather than leave
# a silent hole. Determined by compiling each against a plain sink and reading which
# are refused.
CHANGELOG="q5 q18 q19"

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
print("      so their row counts are not comparable. Gated by upsert_gate.sh.")
sys.exit(1 if failed else 0)
PY
