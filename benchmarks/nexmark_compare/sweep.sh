#!/usr/bin/env bash
# Sweep the nexmark query suite across both engines, a FEW QUERIES PER STACK.
#
# throughput_sampled.sh takes a query list and runs it on one composed stack.
# That is right for two queries and wrong for thirteen: chained jobs on a warm
# cluster drift monotonically in CPU (measured at 2.6x by the sixth job), so the
# later queries in a long list are scored against a different machine than the
# earlier ones. This driver keeps each invocation to a small batch - the same
# premise the canonical q0/q12 run is measured under - and accumulates the
# per-query results out of the way of the next batch's startup wipe.
#
#   ./sweep.sh                          # every bid-only query, both engines
#   QUERIES="q18 q19" ./sweep.sh        # just these
#   EVENTS=3000000 PARALLELISM=4 ./sweep.sh
#   ENGINES=clink ./sweep.sh            # clink-only, for a clink-vs-clink A/B
#
# Results land in results-sweep/ and are summarised at the end. Nothing here
# changes what is measured; it only bounds the drift between measurements.
set -uo pipefail

cd "$(dirname "$0")"

EVENTS="${EVENTS:-5000000}"
PARALLELISM="${PARALLELISM:-4}"
SINK="${SINK:-blackhole}"
ENGINES="${ENGINES:-clink flink}"
BATCH="${BATCH:-2}"
OUT="${OUT:-results-sweep}"

# The bid-only queries: all read the same large stream, so all are directly
# comparable and all reach the same target. The multi-stream queries (q3, q4,
# q20) read auction / person, which are a few percent of the bid stream and
# drain in under a second, so a sustained-rate figure for them is noise - they
# belong to the correctness gate (run.sh) instead.
DEFAULT_QUERIES="q0 q1 q2 q5 q7 q11 q12 q14 q15 q16 q17 q18 q19 q21 q22"
QUERIES="${QUERIES:-$DEFAULT_QUERIES}"

mkdir -p "$OUT"
rm -f "$OUT"/*.json

read -r -a QLIST <<< "$QUERIES"
total=${#QLIST[@]}
echo "sweep: $total queries, $EVENTS events, par=$PARALLELISM, sink=$SINK, engines='$ENGINES'"
echo "       batches of $BATCH per freshly composed stack"
echo

i=0
batch_no=0
while [ "$i" -lt "$total" ]; do
    batch=("${QLIST[@]:i:BATCH}")
    batch_no=$((batch_no + 1))
    echo "================ batch $batch_no: ${batch[*]} ================"
    if EVENTS="$EVENTS" PARALLELISM="$PARALLELISM" SINK="$SINK" ENGINES="$ENGINES" \
       QUERIES="${batch[*]}" ./throughput_sampled.sh 2>&1 | sed 's/^/  /'; then
        :
    else
        echo "  batch $batch_no returned non-zero; keeping whatever results it wrote"
    fi
    # Move the batch's results aside before the next invocation's startup wipe.
    for f in results-sampled/*.json; do
        [ -e "$f" ] || continue
        cp "$f" "$OUT/"
    done
    i=$((i + BATCH))
    echo
done

echo "================ sweep summary ================"
python3 driver/summarize_sampled.py --results-dir "$OUT" --par "$PARALLELISM" --events "$EVENTS"
