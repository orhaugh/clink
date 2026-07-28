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
# q5 WAS a cross-engine failure at parallelism > 1 and is now FIXED. Kept here in
# short because the investigation is worth not repeating.
#
# Symptom: at par=4 over four workers, 153 of 247 windows carried a different
# top-1, clink's count never HIGHER than Flink's, and all 49 tied windows resolved
# against the query's own ORDER BY. At par=1, or at par=4 with every subtask on one
# worker (colocate_q5.sh), it matched exactly.
#
# Cause: a data batch larger than the send-credit window could never be admitted.
# Credit is conserved - the receiver grants back exactly what it pops - so
# remaining_credit_ never exceeds kInitialNetworkCredit = 2048 records, and
# acquire_credit_ for a bigger batch waited on a condition that could not hold. It
# released at teardown, returned false, and every caller discards that bool, so the
# batch vanished with no error. A windowed fire is thousands of rows where a Kafka
# source's batches are modest, which is why the shuffle in FRONT of the window was
# unaffected and the one BEHIND it was not, and why co-locating fixed it
# (LocalDataPlane has no credit at all).
#
# Fix: push_remote_ splits a batch above the window into kMaxRecordsPerFrame frames,
# slicing the Arrow sidecar zero-copy so the columnar carrier survives. Batches
# under the window are framed exactly as before. See
# docs/internals/network-stack.md and NetworkChannelCredit.ABatchLargerThanThe-
# CreditWindowArrivesWholeAndInOrder.
#
# Six hypotheses were eliminated before the right one, all by measurement: the
# plan's partitioning, an early cancel, the window aggregate, the sink fan-out, the
# window's columnar emission, and the upsert reduction. The one that finally
# localised it was colocate_q5.sh - same parallelism, one worker - because it varied
# the wire and nothing else.
set -uo pipefail

cd "$(dirname "$0")"
CLINK_ROOT=../..
PY="$CLINK_ROOT/benchmarks/flink_compare/.venv/bin/python"
[ -x "$PY" ] || PY=python3

EVENTS="${EVENTS:-1000000}"
PAR="${PARALLELISM:-4}"
QUERIES="${QUERIES:-q5 q18 q19}"
OUT="${OUT:-results-upsert-gate}"
# Quiet period after the pipeline stops moving, before the job is cancelled.
SETTLE_S="${SETTLE_S:-8}"
PROJECT=nxcompare
JM_HTTP=8095
FLINK_JM=nxcompare-flink-jobmanager-1
FLINK_REST=8081

cleanup() {
    # KEEP_UP=1 leaves the stack AND the output topics in place. The reduced state
    # this script prints is the answer; the raw topic is the only thing that can
    # explain it, and the reduction is what throws the sequence away. Pair with
    # driver/read_upsert_topic.py --raw --key <pk>.
    if [ -n "${KEEP_UP:-}" ]; then
        echo "  (KEEP_UP set: stack and topics left running)"
        return
    fi
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
    if [ -n "${SINK_PAR:-}${SRC_PAR:-}" ]; then
        # Submit a hand-patched spec so the SINK runs at a different parallelism
        # from the rest of the job. --parallelism is uniform and there is no
        # per-operator flag, but the coordinator accepts a raw JobGraphSpec, so
        # generate one, rewrite the sink's parallelism, and POST that.
        #
        # This exists to isolate one suspect in the q5 divergence: no in-suite test
        # can exercise a fanned-out sink, because run_query pins every connector
        # back to 1, so "sink at 4" is untested ground that only this harness
        # reaches.
        "$CLINK_ROOT/build/clink_submit_sql" --file /tmp/nxq-up-clink.sql \
            --parallelism "$PAR" 2>/dev/null > /tmp/nxq-spec.json
        python3 - "${SINK_PAR:-}" "${SRC_PAR:-}" > /tmp/nxq-spec-patched.json <<'PY'
import json, sys
raw = open("/tmp/nxq-spec.json").read()
d = json.loads(raw[raw.index("{"):])
sink_par, src_par = sys.argv[1], sys.argv[2]
for o in d.get("ops", []):
    t = o.get("type", "")
    if sink_par and "sink" in t:
        o["parallelism"] = int(sink_par)
    # The source and the ops fused to it before the first keyed shuffle: a Kafka
    # source at 1 feeding a decoder at 4 would still fan out, so the decode and
    # timestamp-assign stages move with it. Everything from the first keyed
    # operator onward stays at PAR.
    if src_par and ("source" in t or t in ("json_string_to_row_columnar",
                                           "json_string_to_row",
                                           "assign_timestamps_row")):
        o["parallelism"] = int(src_par)
json.dump(d, sys.stdout)
PY
        cjid=$(curl -fsS -X POST --data-binary @/tmp/nxq-spec-patched.json \
                 "http://127.0.0.1:$JM_HTTP/api/v1/jobs/spec?name=up_$q" 2>/dev/null \
               | python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("job_id",""))
except Exception: pass')
        echo "  (sink=${SINK_PAR:-$PAR} source=${SRC_PAR:-$PAR} rest=$PAR)"
    else
        cjid=$("$CLINK_ROOT/build/clink_submit_sql" --file /tmp/nxq-up-clink.sql \
                --coordinator-host 127.0.0.1 --coordinator-port "$JM_HTTP" \
                --name "up_$q" --parallelism "$PAR" 2>/dev/null \
              | python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except: pass')
    fi
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
        # Settle on the WHOLE pipeline, not just the source. A windowed query's
        # panes fire when the watermark passes, which is after the source has
        # finished, and the top-N and sink downstream of that are still moving
        # long after records_out on the source has stopped. Watching the source
        # alone cancelled the job mid-flight and left the sink holding a PREFIX of
        # the answer - for a top-N that is a prefix maximum, which is always at or
        # below the true one and reads exactly like an engine that undercounts.
        prev=-1
        for _ in $(seq 1 120); do
            cur=$(curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$cjid/operators" 2>/dev/null \
                  | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(sum(int(o.get("records_out",0) or 0) for o in d.get("operators",[])))' 2>/dev/null || echo 0)
            [ "$cur" = "$prev" ] && break
            prev=$cur; sleep 2
        done
        sleep "$SETTLE_S"
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
        sleep "$SETTLE_S"
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
