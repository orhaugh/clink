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
# OPEN DEFECT, found here 2026-07-28, and now LOCATED: q5's divergence at
# parallelism > 1 is RECORDS LOST ON A CROSS-WORKER KEYED ROW SHUFFLE.
#
# The controlled test is colocate_q5.sh: parallelism 4, but every clink subtask on
# ONE worker so the keyer -> top-N shuffle is local. It MATCHES Flink - 147 windows,
# zero differing - and the top-N receives 734,382 rows against a window that emitted
# 734,382. The same job over FOUR workers has the top-N receiving a fraction and 72
# of 147 windows wrong. Same parallelism, same query, same data; the only variable
# is whether the shuffle crosses a worker boundary.
#
# That also settles the counter question below: records_in and records_out ARE
# comparable across that boundary, because they agree exactly when the hop is local.
# The 461,334-of-1,533,886 reading was real loss, and the caveat against quoting it
# is withdrawn.
#
# ROOT CAUSE, read out of the code once the locus was known: A BATCH LARGER THAN THE
# CREDIT WINDOW IS SILENTLY DROPPED. Credit is conserved - the receiver grants back
# exactly the record count of each batch it pops - so remaining_credit_ can never
# exceed the kInitialNetworkCredit = 2048 records it starts with. push_remote_
# acquires credit for the WHOLE batch and does not chunk, so acquire_credit_(n) with
# n > 2048 waits on a condition that can never become true, unblocks only when
# closed_ is set at teardown, returns false - and NetworkBridgeSink discards that
# bool. No error, no failed task, batch gone.
#
# Which is why the shuffle IN FRONT of the window is fine and the one BEHIND it is
# not: a Kafka source delivers modest batches, a windowed fire here is ~4,996 rows.
# And why the loss is partial: the 4-way split brings a typical fire to ~1,249 rows
# per peer, under the window, so only skewed sub-batches exceed it.
#
# clink_net_bridge_credit_exhaustion_total already counts every credit block, which
# is the signal that would have caught this; nothing watched it. See
# docs/internals/network-stack.md.
#
# The per-operator eliminations below all stand, and they are why this is a wire
# defect rather than an operator one.
#
# ORIGINAL SYMPTOM:
#
#   par=4   153 of 247 windows have a different top-1. clink's count is never
#           HIGHER than Flink's - 104 lower, 0 higher - and in all 49 windows
#           where the counts tie, clink picked the higher auction, against the
#           query's `ORDER BY num DESC, auction ASC`. One-directional both ways,
#           so this is not two valid answers to an ambiguous query.
#   par=1   247 of 248 rows identical. The whole divergence disappears.
#
# WHAT HAS BEEN RULED OUT, each by measurement rather than by reading code. Five
# hypotheses, all mine, all wrong - recorded so the next attempt does not re-walk
# them:
#
#  1. NOT the plan's partitioning. The first guess was an unpartitioned stateful
#     operator (same class as 066af45), the top-1 holding a per-subtask local
#     maximum. Dumping the physical spec kills it: the planner emits a
#     row_compute_key over `wstart` feeding a top_n_per_key_row with
#     key_by="row_key", partition_columns=wstart, sort_columns=num,auction and
#     sort_descending=1,0. The routing and the sort keys are all correct.
#
#  2. NOT an early cancel. The gate used to settle on the SOURCE's records_out,
#     which for a windowed query stops long before the panes fire - a top-1 cut
#     off mid-flight is a PREFIX maximum, always at or below the true one, which
#     fits the signature exactly. It now settles on the whole pipeline and takes
#     SETTLE_S seconds of quiet. With SETTLE_S=30 the divergence is byte-identical
#     to the 4-second run, so it is deterministic, not truncation.
#
#  3. NOT the window aggregate as written to a topic. qhopv value-compares the HOP
#     counts that feed the top-1. At par=4 over 300k events: 734,382 keys on both
#     engines, ZERO differing counts, none missing on either side. Note the exact
#     claim - it proves the aggregate's output is right WHEN A SINK CONSUMES IT,
#     not when a top-N does. Those are different assertions and the gap between
#     them has not been closed.
#
#  4. NOT the sink fan-out. SINK_PAR=1 with the rest at 4 (this script patches the
#     spec, since --parallelism is uniform) gives a diff BYTE-IDENTICAL to the
#     par=4 sink. Worth having tried: run_query pins every connector to 1, so a
#     fanned-out sink is ground no in-suite test covers.
#
#  5. NOT the window's columnar emission. This one mattered because it showed (3)
#     had measured the wrong path: enable_columnar_output promotes a producer to
#     columnar only when EVERY consumer is in its allowlist, and q5's window feeds
#     row_compute_key (in the list, so COLUMNAR) while qhopv's feeds
#     row_to_json_string (not in it, so ROW). qhopv therefore cannot exercise the
#     path q5 takes. Settled by the lever the code provides for it:
#     CLINK_DISABLE_COLUMNAR_OUTPUT=1 forces the window back to row form, and the
#     diff is again BYTE-IDENTICAL.
#
# WHICH GIVES THE DEDUCTION THAT MATTERS. qhopv's window emits rows and its output
# is exact. q5 with CLINK_DISABLE_COLUMNAR_OUTPUT=1 also has its window emitting
# rows - same query, same data, same parallelism - so q5's window output is exact
# too. The top-N is therefore producing a wrong answer from CORRECT input, which is
# what the in-suite test says it does not do.
#
# The diff is byte-identical across settle time, sink parallelism and columnar
# emission. That stability is itself evidence: this is not a race and not routing.
# It is deterministic, and the remaining structural difference between the passing
# in-suite test and this failing one is the SINK TYPE - a file sink whose changelog
# the test replays in order, versus kafka_upsert_sink_string whose changelog is
# reduced to last-value-per-key with tombstoned keys removed. clink emits 1,030
# tombstones here and Flink emits 0, because clink's top-N retracts the displaced
# row (delete + insert, different row PKs) where Flink's upsert-kafka just
# overwrites the wstart key. Whether that delete/insert pair survives the reduction
# in the order it was emitted is the next thing to check, and it needs the RAW
# topic rather than the reduced state this script keeps.
#
# AND THE IN-SUITE REPRODUCTION PASSES. TopNOverWindow.HopTopOnePerWindowAt-
# ParallelismFour runs the same shape at par=4 over a contested dataset with a
# total ORDER BY, against an oracle, and agrees. So the top-N over a window is
# correct at parallelism 4 against a file sink whose changelog is replayed in
# order.
#
#  6. NOT the upsert reduction, and NOT the top-N losing its own answer. Dumped the
#     RAW topic (driver/read_upsert_topic.py --raw, and KEEP_UP=1 to keep it):
#     247 keys, every one confined to a SINGLE partition so last-write-wins is
#     well defined; the emission order is right (value, then the tombstone that
#     retracts the displaced row, then the next value); NO key ends on a tombstone;
#     and NO key's final value is lower than the best value ever inserted under it.
#     The changelog is internally consistent and the reduction is faithful. So the
#     top-N converges to the maximum of WHAT IT SAW - it simply never saw the
#     higher counts.
#
# WHICH LEAVES exactly two possibilities, and they are distinguishable:
#   (a) rows are lost between the window and the top-N, so the top-1 is a maximum
#       over a subset; or
#   (b) the window's per-(wstart, auction) counts are lower in q5's plan than in
#       qhopv's, despite the two queries being semantically identical.
#
# ANSWERED: IT IS (a). q5 and qhopv run on ONE stack at the SAME event count
# (300k, par=4), then for each disputed window look up clink's OWN qhopv count for
# the auction Flink declared the winner. Over all 72 disputed windows:
#
#   clink's own aggregate contains Flink's winning auction        72/72
#   ...with EXACTLY Flink's count                                 72/72
#   Flink's winner absent from clink's aggregate                      0
#   clink's own per-window MAX equals clink's top-1                   0
#
# e.g. wstart=1000000080000: clink's aggregate holds auction 66 at num 8 and
# auction 1557 at num 7, and clink's top-1 returned 1557 num 7. The winning row
# EXISTS in the window's output, with the right count, and the top-N did not
# select it.
#
# Put with the raw-topic result - the top-N converges to the maximum of what it
# saw - the three measurements only fit one way: the window emits the right rows,
# the top-N faithfully maximises what reaches it, and its answer is not the maximum
# of what was emitted. So rows do not all get from the window's output to the
# top-N's input at parallelism 4.
#
# WHICH MEANS THE COUNTER GAP IS BACK ON THE TABLE, and my dismissal of it above
# was made on a bad control. row_compute_key out=1,533,886 against
# top_n_per_key_row in=461,334 is consistent with this. The par=1 run I used to
# wave it away had read only 344k of 460k bids, so it was never a control. What is
# still unverified is whether those two counters count the same thing across that
# boundary; establish that with a COMPLETE par=1 run before quoting any ratio.
#
# THE CHANNEL MERGE AND THE SPLIT BOTH READ CORRECT. Dag::union_streams on the
# top-N's input already carries the closed-AND-drained rule from the earlier fix
# for exactly this symptom ("1-2% pane-count loss at par=4/16"), broadcasts
# watermarks, and only breaks when every input is closed and empty.
# Dag::add_split routes every record, broadcasts watermarks and barriers to all
# branches, and closes all branches. Neither loses rows on reading. And the row
# split and the columnar split produce the SAME wrong answer, so the split is not
# the differentiator either.
#
# WHICH LEAVES THE ONE STRUCTURAL DIFFERENCE THAT SURVIVES EVERY ELIMINATION: how
# a batch actually travels that edge. The in-suite test runs an InProcessCluster -
# one worker, so LocalDataPlane serves every edge as a direct in-process push. The
# harness runs FOUR worker containers, so a 4-way hash shuffle keeps roughly a
# quarter of its rows local and sends the rest over a socket as Arrow IPC. The
# top-N received 461,334 rows where the window emitted somewhere between 1.22M
# (scaling qhopv's 734,382 at 300k) and 1,533,886 (the counter) - 30% to 38%,
# which is the neighbourhood of "only the co-located slice arrived".
#
# Treat the arithmetic as suggestive and not as proof: the numerator's
# comparability across that boundary is still unverified (see above), and if
# exactly three quarters of rows vanished more than 49% of windows would be wrong.
# Some loss, not all of it.
#
# This pair has diverged silently before. local_data_plane.hpp documents the
# advertised-host bug that made the fast path disappear in this very benchmark
# while working everywhere else, because both sides defaulted to 127.0.0.1. A
# producer/consumer pair where the two ends can disagree needs an
# ASYMMETRIC-construction test; a matched-ends end-to-end run cannot see it.
#
# TWO TESTS THAT WOULD SETTLE IT:
#   - Place every clink subtask on ONE worker at parallelism 4, so the shuffle is
#     local but the parallelism is unchanged. If q5 then matches Flink, the socket
#     path is the culprit and the operator is exonerated.
#   - Scrape clink_dataplane_local_hits_total and
#     clink_dataplane_socket_fallbacks_total off /metrics for the run, which says
#     how many batches took each path rather than leaving it inferred.
#
# THE OBVIOUS-LOOKING EVIDENCE FOR (a) DOES NOT HOLD UP. The per-operator counters
# at par=4 read row_compute_key out=1,533,886 and top_n_per_key_row in=461,334,
# which looks like 70% of the rows vanishing in the shuffle. It is not safe to read
# them that way: the SAME gap appears at par=1 (1,148,337 -> 356,823) in a
# configuration that MATCHES Flink, so the two counters are not comparable across
# that boundary - one of them is not counting rows the way the other is, most likely
# on the columnar ingest path. The par=1 control was also incomplete (344k of
# 460k bids read before the settle loop broke), so it is not usable as one either.
# Do not quote the 70%.
#
# THE EXPERIMENT THAT DISTINGUISHES (a) FROM (b), and it needs no code change: run
# qhopv and q5 at the SAME event count on one stack, then for each disputed window
# look up clink's OWN qhopv count for the auction Flink declared the winner. If
# clink's aggregate says that auction had the higher count, the top-N never received
# its row and it is (a). If clink's aggregate agrees with clink's top-1, the
# aggregate undercounts in q5's plan shape and it is (b) - which would also mean
# qhopv cannot stand in for q5's aggregate, exactly as it could not for its emission
# mode.
#
# ONE MORE DATA POINT, NOT A CLEAN ONE. SRC_PAR=1 with the rest at 4 flips the
# error DIRECTION: clink then OVER-counts the early windows (auction 1 at num 78
# for wstart 999999994000) where before it undercounted. Over-emission is the other
# half of the failure the README's multi-partition watermark section describes. But
# that configuration introduces a parallelism CHANGE mid-pipeline that the real
# plan does not have, so it is a lead about watermark propagation across a
# rebalance, not an isolation of the source axis. Do not quote it as one.
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
