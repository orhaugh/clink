#!/usr/bin/env bash
# q5 at parallelism 4 with EVERY clink subtask on ONE worker, so the keyer -> top-N
# hash shuffle is served by LocalDataPlane instead of crossing sockets. Parallelism
# is unchanged, which is what makes it a controlled comparison against the 4-worker
# run rather than against the par=1 run.
#
# RESULT (2026-07-28, 300k events): MATCHES. 147 windows, zero differing, and the
# top-N received 734,382 rows against a window that emitted 734,382 - nothing lost.
# The same job over FOUR workers had the top-N receiving a fraction of its input and
# 72 of 147 windows wrong. Same parallelism, same query, same data; the only
# variable is whether the shuffle crosses a worker boundary.
#
# So the q5 divergence is RECORDS LOST ON A CROSS-WORKER KEYED ROW SHUFFLE, not a
# defect in the window, the keyer, the top-N, or the sink - each of which this
# investigation eliminated separately (see upsert_gate.sh for the trail).
#
# It also settles the counter question that was open: records_in and records_out ARE
# comparable across that boundary, since they agree exactly when the hop is local.
# The 461,334-of-1,533,886 reading from the 4-worker run was real loss.
#
# ONE THING THIS DOES NOT EXPLAIN, and it constrains the next step: the SAME job's
# source -> keyer -> window shuffle also crosses workers and loses nothing (q12 and
# qhopv are both exact at par=4 over four workers). What differs downstream is batch
# SIZE - the window fires roughly 5,000 rows per pane group where the Kafka source
# delivers modest batches - so a size-dependent failure on the wire fits and a
# blanket "cross-worker shuffle is broken" does not.
#
# One worker needs enough slots for the whole job: q5 at par=4 is 10 ops x 4 = 40
# tasks, against the compose default of 16 per worker.
#
# CAVEAT ON THE DATA-PLANE COUNTERS this script prints: it scrapes the COORDINATOR's
# /metrics, and clink_dataplane_local_hits_total / _socket_fallbacks_total live on
# the WORKERS, so both read 0 here and measure nothing. Scrape the worker HTTP ports
# if you want that split; the record counts above are what carry the result.
set -uo pipefail
cd /Users/rosshaugh/personal/clink/benchmarks/nexmark_compare

PROJECT=nxcompare
EVENTS="${EVENTS:-300000}"
PAR=4
JM_HTTP=8095
FLINK_JM=nxcompare-flink-jobmanager-1
CLINK_ROOT=../..
PY=../flink_compare/.venv/bin/python
[ -x "$PY" ] || PY=python3
DATA_DIR=/tmp/nx-oneworker
OVERRIDE=/tmp/nx-oneworker-override.yml

cat > "$OVERRIDE" <<'YML'
services:
  clink-worker1:
    command:
      - --role=worker
      - --id=worker-1
      - --coordinator-host=clink-coordinator
      - --coordinator-port=6123
      - --data-host=clink-worker1
      - --slots=64
      - --http-port=8082
      - --http-bind=0.0.0.0
YML

echo "=== teardown any previous stack ==="
docker compose -p "$PROJECT" --profile clink --profile flink down -v --remove-orphans >/dev/null 2>&1

echo "=== up: kafka + coordinator + ONE worker (64 slots) + flink ==="
docker compose -p "$PROJECT" -f docker-compose.yml -f "$OVERRIDE" \
    --profile clink --profile flink up -d \
    zookeeper kafka clink-coordinator clink-worker1 flink-jobmanager flink-taskmanager \
    >/dev/null 2>&1
for i in $(seq 1 45); do docker exec ${PROJECT}-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && break; sleep 2; done
for i in $(seq 1 60); do curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/health" >/dev/null 2>&1 && break; sleep 2; done
for i in $(seq 1 30); do docker exec "$FLINK_JM" flink list >/dev/null 2>&1 && break; sleep 2; done
docker cp flink-job/target/nexmark-sql.jar "$FLINK_JM:/tmp/nexmark-sql.jar" >/dev/null 2>&1

# Confirm only one clink worker registered, or the premise is void.
workers=$(curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/workers" 2>/dev/null \
          | python3 -c 'import json,sys
d=json.load(sys.stdin)
w=d.get("workers",d if isinstance(d,list) else [])
print(len(w), sum(int(x.get("slots",0) or 0) for x in w))' 2>/dev/null)
echo "  registered workers / total slots: $workers"

echo "=== generate + load $EVENTS events ==="
rm -rf "$DATA_DIR"; mkdir -p "$DATA_DIR"
"$CLINK_ROOT/build/benchmarks/nexmark_dump" --events "$EVENTS" --tps 1000 --out-dir "$DATA_DIR" | tail -1
for t in nx-person nx-auction nx-bid; do
    docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1
    docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic "$t" --partitions "$PAR" --replication-factor 1 >/dev/null 2>&1
done
"$PY" driver/load_ndjson.py --dir "$DATA_DIR" --bootstrap localhost:9092 --prefix nx- 2>/dev/null | tail -1

CT=nx-up-q5-one-clink
FT=nx-up-q5-one-flink
for t in "$CT" "$FT"; do
    docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092 --delete --topic "$t" >/dev/null 2>&1
    docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092 --create --topic "$t" --partitions "$PAR" --replication-factor 1 >/dev/null 2>&1
done

echo "=== clink q5, par=$PAR, all subtasks on one worker ==="
sed -e "s#__BROKERS__#kafka:29092#" -e "s#__OUT__#$CT#" queries/clink/q5_up.tmpl.sql > /tmp/q5-one.sql
JID=$("$CLINK_ROOT/build/clink_submit_sql" --file /tmp/q5-one.sql \
        --coordinator-host 127.0.0.1 --coordinator-port "$JM_HTTP" \
        --name q5one --parallelism "$PAR" 2>/dev/null \
      | python3 -c 'import json,sys
for l in sys.stdin:
    if l.strip().startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except Exception: pass')
[ -z "$JID" ] && { echo "  clink submit FAILED"; exit 1; }
echo "  job $JID"
prev=-1
for i in $(seq 1 150); do
    cur=$(curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$JID/operators" 2>/dev/null \
          | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(int(o.get("records_out",0) or 0) for o in d.get("operators",[])))' 2>/dev/null || echo 0)
    [ "$cur" = "$prev" ] && break
    prev=$cur; sleep 3
done
sleep 25
echo "  per-operator counters:"
curl -fsS "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$JID/operators" | python3 -c "
import json,sys
for o in json.load(sys.stdin).get('operators',[]):
    t=o.get('op_type',o.get('type','?'))
    if t in ('hopping_window_row','row_compute_key','top_n_per_key_row'):
        print(f\"     {t:22} in={o.get('records_in',0):>10} out={o.get('records_out',0):>10}\")
"
echo "  data-plane counters (local vs socket):"
curl -fsS "http://127.0.0.1:$JM_HTTP/metrics" 2>/dev/null | grep -E "dataplane_(local_hits|socket_fallbacks)_total" | sed 's/^/     /'
curl -fsS -X POST "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$JID/cancel" >/dev/null 2>&1

echo "=== flink q5, par=$PAR ==="
sed -e "s#__OUT__#$FT#" flink-job/queries/q5_up.tmpl.sql > /tmp/q5-one-flink.sql
docker cp /tmp/q5-one-flink.sql "$FLINK_JM:/tmp/up.sql" >/dev/null 2>&1
docker exec "$FLINK_JM" flink run -d -p "$PAR" /tmp/nexmark-sql.jar /tmp/up.sql > /tmp/fl.err 2>&1
FJID=$(grep -oE '[0-9a-f]{32}' /tmp/fl.err | head -1)
[ -z "$FJID" ] && { echo "  flink submit FAILED"; sed -n '1,12p' /tmp/fl.err; exit 1; }
echo "  job $FJID"
prev=-1
for i in $(seq 1 120); do
    cur=$(curl -fsS "http://127.0.0.1:8081/jobs/$FJID" 2>/dev/null \
          | python3 -c 'import json,sys; d=json.load(sys.stdin); print(max([int(v.get("metrics",{}).get("write-records",0) or 0) for v in d.get("vertices",[])] or [0]))' 2>/dev/null || echo 0)
    [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && break
    prev=$cur; sleep 2
done
sleep 25
docker exec "$FLINK_JM" flink cancel "$FJID" >/dev/null 2>&1

echo "=== compare ==="
mkdir -p results-oneworker
"$PY" driver/read_upsert_topic.py --bootstrap localhost:9092 --topic "$CT" --json > results-oneworker/q5-clink.json 2>/dev/null
"$PY" driver/read_upsert_topic.py --bootstrap localhost:9092 --topic "$FT" --json > results-oneworker/q5-flink.json 2>/dev/null
python3 - <<'PY'
import json
c=json.load(open('results-oneworker/q5-clink.json'))
f=json.load(open('results-oneworker/q5-flink.json'))
def rows(d):
    return {json.loads(v)['wstart'] if isinstance(v,str) else v['wstart']:
            (json.loads(v) if isinstance(v,str) else v) for v in (d.get('state') or {}).values()}
cw, fw = rows(c), rows(f)
print(f"  clink: {c.get('messages')} messages, {c.get('tombstones')} tombstones, {len(cw)} windows")
print(f"  flink: {f.get('messages')} messages, {f.get('tombstones')} tombstones, {len(fw)} windows")
if not cw or not fw:
    print("  NOT GATED - an empty side"); raise SystemExit(0)
common = set(cw) & set(fw)
diff = [w for w in common if cw[w] != fw[w]]
lower = sum(1 for w in diff if cw[w]['num'] < fw[w]['num'])
higher = sum(1 for w in diff if cw[w]['num'] > fw[w]['num'])
ties = len(diff) - lower - higher
print(f"  windows only in clink: {len(set(cw)-set(fw))}  only in flink: {len(set(fw)-set(cw))}")
print(f"  common {len(common)}, DIFFERING {len(diff)}  (clink num lower {lower}, higher {higher}, tie-diff {ties})")
print("  VERDICT:", "MATCHES" if not diff and set(cw)==set(fw) else "STILL DIFFERS")
for w in sorted(diff)[:4]:
    print(f"     wstart={w} clink={cw[w]} flink={fw[w]}")
PY
echo "=== teardown ==="
docker compose -p "$PROJECT" --profile clink --profile flink down -v --remove-orphans >/dev/null 2>&1
