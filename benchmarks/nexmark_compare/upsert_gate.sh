#!/usr/bin/env bash
# Cross-engine gate for the CHANGELOG queries, which the row-count gate cannot do.
#
# WHY A SECOND GATE. gate.sh compares output ROW COUNTS, which works for a query
# whose output is append-only. Three queries are not: q5 (top-1 per sliding window -
# a new leader retracts the old one), q18 and q19 (dedup and ranking per key -
# same). For those, the row count depends on how many times a row was revised on
# the way to the answer, which is an implementation detail. Two engines can be
# equally correct and emit wildly different counts.
#
# What IS comparable is the state the changelog converges to. Both engines write it
# by the same convention - clink's kafka_upsert_sink_string and Flink's
# upsert-kafka key each message by the primary key and emit an EMPTY PAYLOAD as a
# delete (the log-compaction tombstone) - so reducing each topic to
# last-value-per-key with tombstoned keys removed yields each engine's final
# answer, comparable directly.
#
# This is a STRONGER check than the append-only gate: it compares the row VALUES,
# not just how many rows there were.
#
# ONE DELIBERATE ASYMMETRY. The two engines encode the Kafka message KEY
# differently - clink concatenates the primary-key columns, Flink writes a JSON
# object - so the keys are not comparable textually. They do not need to be: the
# primary-key columns are also present in the VALUE, so comparing the set of live
# values is equivalent and independent of key encoding. The keys are still used
# per engine, to do last-value-per-key correctly within that engine's topic.
#
#   ./upsert_gate.sh                  # q5 q18 q19
#   QUERIES="q18" EVENTS=500000 ./upsert_gate.sh
#
# OPEN DEFECT, found here 2026-07-28: q5 DIVERGES AT PARALLELISM > 1.
#
#   par=4   153 of 247 windows have a different top-1. clink's count is never
#           HIGHER than Flink's - 104 lower, 0 higher - and in all 49 windows
#           where the counts tie, clink picked the higher auction, against the
#           query's `ORDER BY num DESC, auction ASC`. One-directional both ways,
#           so this is not two valid answers to an ambiguous query.
#   par=1   247 of 248 rows identical. The whole divergence disappears.
#
# The shape fits an unpartitioned stateful operator: q5's top-1 is PARTITION BY
# wstart while its input arrives partitioned by auction from the upstream GROUP
# BY, so each subtask would hold a local top-1 over its own auctions and write it
# under the shared wstart key - giving a local maximum, which can only ever be at
# or below the global one. Same class as 066af45. NOT yet confirmed at the plan
# level, and the windowed aggregate underneath has not been separately
# value-compared at par=4, so the top-N is the suspect and not the proven cause.
#
# The one remaining par=1 row is a different and much smaller question: Flink has
# one extra trailing window (wstart 1000000486000) that clink does not emit.
set -uo pipefail

cd "$(dirname "$0")"
CLINK_ROOT=../..
PY="$CLINK_ROOT/benchmarks/flink_compare/.venv/bin/python"
[ -x "$PY" ] || PY=python3

EVENTS="${EVENTS:-1000000}"
PAR="${PARALLELISM:-4}"
QUERIES="${QUERIES:-q5 q18 q19}"
OUT="${OUT:-results-upsert-gate}"
PROJECT=nxcompare
JM_HTTP=8095
FLINK_JM=nxcompare-flink-jobmanager-1
FLINK_REST=8081

cleanup() {
    docker compose -p "$PROJECT" --profile clink --profile flink down -v \
        --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

mkdir -p "$OUT"
rm -f "$OUT"/*.json

echo "upsert gate: $QUERIES, $EVENTS events, par=$PAR"
echo "  comparing the FINAL STATE each engine's changelog converges to"
echo

# One composed stack with the data loaded; each query then runs on both engines
# against its own output topic. Correctness does not drift with a warm cluster the
# way a timing measurement does, so one stack is fine here.
echo "=== bring up both engines + load $EVENTS events ==="
EVENTS="$EVENTS" PARALLELISM="$PAR" SINK=blackhole ENGINES="clink flink" \
    QUERIES=q0 KEEP_UP=1 ./throughput_sampled.sh >/dev/null 2>&1
docker inspect "$FLINK_JM" >/dev/null 2>&1 || { echo "  stack did not come up"; exit 1; }
echo "  up"
echo

recreate() {  # topic
    docker exec "${PROJECT}-kafka-1" kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic "$1" >/dev/null 2>&1 || true
    docker exec "${PROJECT}-kafka-1" kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$1" --partitions "$PAR" --replication-factor 1 >/dev/null 2>&1 || true
}

for q in $QUERIES; do
    echo "================ $q ================"
    ct="nx-up-$q-clink"
    ft="nx-up-$q-flink"
    recreate "$ct"
    recreate "$ft"

    # --- clink ---
    sed -e "s#__BROKERS__#kafka:29092#" -e "s#__OUT__#$ct#" \
        "queries/clink/${q}_up.tmpl.sql" > /tmp/nxq-up-clink.sql
    cjid=$("$CLINK_ROOT/build/clink_submit_sql" --file /tmp/nxq-up-clink.sql \
            --coordinator-host 127.0.0.1 --coordinator-port "$JM_HTTP" \
            --name "up_$q" --parallelism "$PAR" 2>/dev/null \
          | python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except: pass')
    clink_ok=1
    if [ -z "$cjid" ]; then echo "  clink submit FAILED"; clink_ok=0; else
        echo "  clink job $cjid running"
        # Wait for the source to drain, then a moment for the sink to flush.
        for _ in $(seq 1 60); do
            proc=$(curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$cjid/operators" 2>/dev/null \
                   | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(max([int(o.get("records_out",0) or 0) for o in d.get("operators",[]) if int(o.get("records_in",0) or 0)==0] or [0]))' 2>/dev/null || echo 0)
            [ "${proc:-0}" -gt 0 ] && break
            sleep 1
        done
        prev=-1
        for _ in $(seq 1 90); do
            cur=$(curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$cjid/operators" 2>/dev/null \
                  | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(max([int(o.get("records_out",0) or 0) for o in d.get("operators",[]) if int(o.get("records_in",0) or 0)==0] or [0]))' 2>/dev/null || echo 0)
            [ "$cur" = "$prev" ] && break
            prev=$cur; sleep 2
        done
        sleep 4
        curl -fsS -X POST "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$cjid/cancel" >/dev/null 2>&1 || true
    fi

    # --- Flink ---
    sed -e "s#__OUT__#$ft#" "flink-job/queries/${q}_up.tmpl.sql" > /tmp/nxq-up-flink.sql
    docker cp /tmp/nxq-up-flink.sql "$FLINK_JM:/tmp/up.sql" >/dev/null 2>&1
    docker exec "$FLINK_JM" flink run -d -p "$PAR" /tmp/nexmark-sql.jar /tmp/up.sql \
        > /tmp/nxq-flink-submit.err 2>&1
    fjid=$(grep -oE '[0-9a-f]{32}' /tmp/nxq-flink-submit.err | head -1)
    flink_ok=1
    if [ -z "$fjid" ]; then
        echo "  flink submit FAILED:"
        # DEFECT 3: the submit output was discarded, so a failure was undiagnosable.
        sed -n '1,12p' /tmp/nxq-flink-submit.err 2>/dev/null | sed 's/^/      /'
        flink_ok=0
    else
        echo "  flink job $fjid running"
        prev=-1
        for _ in $(seq 1 90); do
            cur=$(curl -fsS "http://127.0.0.1:$FLINK_REST/jobs/$fjid" 2>/dev/null \
                  | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(max([int(v.get("metrics",{}).get("write-records",0) or 0) for v in d.get("vertices",[])] or [0]))' 2>/dev/null || echo 0)
            [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && break
            prev=$cur; sleep 2
        done
        sleep 4
        docker exec "$FLINK_JM" flink cancel "$fjid" >/dev/null 2>&1 || true
    fi

    # --- reduce both topics to final state and compare ---
    "$PY" driver/read_upsert_topic.py --bootstrap localhost:9092 --topic "$ct" --json \
        > "$OUT/$q-clink.json" 2>/dev/null || echo '{}' > "$OUT/$q-clink.json"
    "$PY" driver/read_upsert_topic.py --bootstrap localhost:9092 --topic "$ft" --json \
        > "$OUT/$q-flink.json" 2>/dev/null || echo '{}' > "$OUT/$q-flink.json"
    python3 - "$OUT/$q-clink.json" "$OUT/$q-flink.json" "$q" "$clink_ok" "$flink_ok" <<'PY'
import json, sys
c = json.load(open(sys.argv[1])); f = json.load(open(sys.argv[2])); q = sys.argv[3]
clink_ok, flink_ok = sys.argv[4] == "1", sys.argv[5] == "1"
if not clink_ok or not flink_ok:
    who = " and ".join(w for w, ok in (("clink", clink_ok), ("flink", flink_ok)) if not ok)
    print(f"  {q}: NOT GATED - {who} failed to submit. Both topics are empty, so the")
    print("      comparison below would be 0 rows against 0 rows and would 'pass'.")
    raise SystemExit(0)
if "error" in c or "error" in f:
    print(f"  {q}: could not read a topic - clink={c.get('error','ok')} flink={f.get('error','ok')}")
    raise SystemExit(0)
cv = sorted((c.get("state") or {}).values())
fv = sorted((f.get("state") or {}).values())
if not cv or not fv:
    # An empty side is never a pass: it means the query produced nothing, which is
    # a failure to measure rather than agreement.
    print(f"  {q}: NOT GATED - clink {len(cv)} rows, flink {len(fv)} rows; an empty "
          f"side cannot agree with anything.")
    raise SystemExit(0)
print(f"  clink: {c.get('messages')} messages, {c.get('tombstones')} tombstones, "
      f"{len(cv)} live rows")
print(f"  flink: {f.get('messages')} messages, {f.get('tombstones')} tombstones, "
      f"{len(fv)} live rows")
if cv == fv:
    print(f"  {q}: FINAL STATE MATCHES ({len(cv)} rows)")
    raise SystemExit(0)
print(f"  {q}: FINAL STATE DIFFERS - clink {len(cv)} rows, flink {len(fv)} rows")
cs, fs = set(cv), set(fv)
for r in sorted(fs - cs)[:4]:
    print(f"      only in flink: {r}")
for r in sorted(cs - fs)[:4]:
    print(f"      only in clink: {r}")
PY
    echo
done
