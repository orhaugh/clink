#!/usr/bin/env bash
# Is clink's stateless-query throughput limited by SOURCE BATCH SIZE?
#
# THE EVIDENCE THAT PROMPTS THIS. A per-thread CPU sample of a clink worker running
# nexmark q0 (driver/thread_split.py) found NOTHING saturated:
#
#     0.125 cores  kafka_text_sour     <- the busiest thread, 12.5% of one core
#     0.101 cores  json_string_to_
#     0.082 cores  rdk:broker1         <- librdkafka's own network thread
#     0.024 cores  project_row
#     0.019 cores  blackhole_sink_
#
# Five stages, each about a tenth of a core busy, delivering 320k rec/s per worker.
# Not CPU-bound, not fetch-bound - librdkafka's broker thread is idle too - and the
# bottleneck sampler found no queue backed up. Stages that do not overlap and never
# fill are the signature of per-batch cost dominating: too few records per handoff.
#
# THE KNOB. KafkaSource fills a batch up to max_batch_size (256) but stops early
# once batch_max_wait (5ms) elapses, emitting a partial batch. That bound exists to
# keep latency bounded on a trickling input, and its own comment says it "costs a
# saturated consumer nothing (a full local queue fills max_batch_size well inside
# the bound)". This tests that claim, because if it is wrong the source is handing
# downstream small batches and paying per-batch cost per handful of records.
#
# Both are TABLE OPTIONS, so this needs no rebuild - only a DDL edit.
#
#   ./batch_ab.sh                    # q0, three variants
#   QUERY=q12 ./batch_ab.sh
#
# Each variant gets a FRESHLY COMPOSED stack: chained runs on one warm cluster
# drift monotonically, which would swamp the effect being measured.
set -uo pipefail

cd "$(dirname "$0")"

EVENTS="${EVENTS:-8000000}"
PAR="${PAR:-4}"
QUERY="${QUERY:-q0}"
OUT="${OUT:-results-batch-ab}"

cleanup() {
    docker compose -p nxcompare --profile clink --profile flink down -v \
        --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup INT TERM

mkdir -p "$OUT"
rm -f "$OUT"/*.json

TEMPLATE="queries/clink/${QUERY}_bh.tmpl.sql"
BACKUP="/tmp/nxq-batchab-$(basename "$TEMPLATE").orig"
cp "$TEMPLATE" "$BACKUP"
restore() { cp "$BACKUP" "$TEMPLATE"; }
trap 'restore; cleanup' EXIT INT TERM

# variant | extra source options
run_variant() {  # tag, opts
    local tag=$1 opts=$2
    restore
    if [ -n "$opts" ]; then
        # Append the options to the bid table's WITH clause.
        python3 - "$TEMPLATE" "$opts" <<'PY'
import sys
path, opts = sys.argv[1], sys.argv[2]
s = open(path).read()
# Append to the FIRST kafka source's WITH(...). Anchoring on a specific option
# (watermark_lag_ms) fails on q0, which declares no event-time column at all.
i = s.index("connector='kafka'")
j = s.index(");", i)
s = s[:j] + ", " + opts + s[j:]
open(path, "w").write(s)
PY
    fi
    echo "================ $tag ================"
    echo "  source options: ${opts:-(defaults: max_batch_size=256, batch_max_wait=5ms)}"
    EVENTS="$EVENTS" PARALLELISM="$PAR" SINK=blackhole ENGINES=clink \
        SLOPE_WINDOW=1.0 QUERIES="$QUERY" ./throughput_sampled.sh 2>&1 \
        | grep -E "clink drain|clink mem|DRAIN rec" | sed 's/^/  /'
    for f in results-sampled/*.json; do
        [ -e "$f" ] || continue
        cp "$f" "$OUT/$(basename "${f%.json}")-$tag.json"
    done
    echo
}

VARIANTS="${VARIANTS:-default nowait nowait-big}"
for v in $VARIANTS; do
    case "$v" in
        default)    run_variant "default" "" ;;
        nowait)     run_variant "nowait" "batch_max_wait='0'" ;;
        nowait-big) run_variant "nowait-big" "batch_max_wait='0', max_batch_size='2048'" ;;
        big)        run_variant "big" "max_batch_size='2048'" ;;
        *)          echo "unknown variant: $v" ;;
    esac
done

echo "================ RESULT ================"
python3 - "$OUT" <<'PY'
import glob, json, os, re, sys
out_dir = sys.argv[1]
rows = {}
for f in glob.glob(os.path.join(out_dir, "*.json")):
    m = re.search(r"-(default|nowait|nowait-big)\.json$", f)
    if not m:
        continue
    d = json.load(open(f))
    rows[m.group(1)] = d
order = ["default", "nowait", "nowait-big"]
print(f"  {'variant':12} {'drain rec/s':>12} {'ev/CPU-s':>10} {'cores':>7} {'vs default':>11}")
print("  " + "-" * 56)
base = rows.get("default", {}).get("drain_rate") or 0
for k in order:
    d = rows.get(k)
    if not d:
        continue
    cpu = d.get("cpu_seconds") or 0
    ev = (d.get("final_count", 0) / cpu) if cpu else 0
    dr = d.get("drain_rate", 0) or 0
    cores = (dr / ev) if ev else 0
    rel = f"{dr / base:.2f}x" if base else "-"
    print(f"  {k:12} {dr / 1e6:11.2f}M {ev / 1000:9.0f}k {cores:7.2f} {rel:>11}")
print()
print("  A large gain from 'nowait' means the 5ms batch-formation bound was firing on")
print("  a saturated consumer and the source was emitting partial batches - the bound's")
print("  own comment says that costs a saturated consumer nothing. No gain means batch")
print("  size was never the constraint and the latency is elsewhere.")
PY
