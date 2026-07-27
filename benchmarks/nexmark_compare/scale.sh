#!/usr/bin/env bash
# Does clink's throughput scale with parallelism, and how much CPU does it draw?
#
# WHY. At a nominal parallelism of 4 on a 12-core box, the symmetric-window sweep
# measured clink at 2.04M rec/s on nexmark q0 using 3.3 cores, and Flink at 4.67M
# using 13.3. Dividing throughput by events-per-CPU-second gives cores actually
# consumed, and the two engines were not consuming the same machine: Flink took
# roughly four times the CPU and turned it into twice the throughput, while clink
# did 2.64x more work per CPU-second and drew a quarter of the hardware.
#
# So "Flink is faster at par 4" and "clink is more efficient at par 4" were both
# true and neither answered the question that matters: given the same machine, can
# clink go faster? That needs clink measured across parallelism, with CORES DRAWN
# reported alongside throughput - a throughput curve that flattens while cores
# keep climbing means contention; one that flattens while cores also flatten means
# the engine has stopped being able to use the box.
#
#   ./scale.sh                        # q0 at 4, 8, 12
#   QUERIES="q0 q12" LEVELS="4 8 16" ./scale.sh
#
# clink only: this is a scaling curve for one engine, not a comparison.
set -uo pipefail

cd "$(dirname "$0")"

EVENTS="${EVENTS:-8000000}"
QUERIES="${QUERIES:-q0}"
LEVELS="${LEVELS:-4 8 12}"
SLOPE_WINDOW="${SLOPE_WINDOW:-1.0}"
OUT="${OUT:-results-scale}"

cleanup() {
    docker compose -p nxcompare --profile clink --profile flink down -v \
        --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup INT TERM

mkdir -p "$OUT"
rm -f "$OUT"/*.json

echo "scale: queries='$QUERIES' levels='$LEVELS' events=$EVENTS window=${SLOPE_WINDOW}s"
echo

for p in $LEVELS; do
    echo "================ parallelism $p ================"
    EVENTS="$EVENTS" PARALLELISM="$p" SINK=blackhole ENGINES=clink \
        SLOPE_WINDOW="$SLOPE_WINDOW" QUERIES="$QUERIES" \
        ./throughput_sampled.sh 2>&1 | sed 's/^/  /'
    for f in results-sampled/*.json; do
        [ -e "$f" ] || continue
        cp "$f" "$OUT/$(basename "${f%.json}")-par$p.json"
    done
    echo
done

echo "================ SCALING CURVE ================"
python3 - "$OUT" <<'PY'
import glob, json, os, re, sys
out_dir = sys.argv[1]
rows = {}
for f in glob.glob(os.path.join(out_dir, "*.json")):
    m = re.search(r"-par(\d+)\.json$", f)
    if not m:
        continue
    d = json.load(open(f))
    rows.setdefault(d.get("query"), {})[int(m.group(1))] = d

for q in sorted(rows):
    print(f"  {q}:")
    print(f"    {'par':>4} {'sustained':>11} {'ev/CPU-s':>10} {'cores drawn':>12} "
          f"{'anon MB':>8} {'vs par4':>8}")
    print("    " + "-" * 60)
    base = None
    for p in sorted(rows[q]):
        d = rows[q][p]
        cpu = d.get("cpu_seconds") or 0
        ev = (d.get("final_count", 0) / cpu) if cpu else 0
        sust = d.get("sustained_slope", 0) or 0
        # Cores drawn = throughput / per-core rate. The honest measure of how much
        # of the machine the engine actually used.
        cores = (sust / ev) if ev else 0
        if base is None:
            base = sust
        rel = f"{sust / base:.2f}x" if base else "-"
        print(f"    {p:>4} {sust / 1e6:10.2f}M {ev / 1000:9.0f}k {cores:11.1f} "
              f"{d.get('anon_mb', 0):8.0f} {rel:>8}")
    print()
print("  Flink's par-4 reference on this box (symmetric 1.0s window):")
print("    q0  4.67M sustained, 350k ev/CPU-s, 13.3 cores drawn, 1869 MB")
print("    q12 1.52M sustained, 108k ev/CPU-s, 14.1 cores drawn, 2308 MB")
PY
