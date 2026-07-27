#!/usr/bin/env bash
# Why is clink slower than Flink on the STATELESS queries, while being more
# efficient per record on those same queries?
#
# THE OBSERVATION. On the symmetric-window sweep, drain rate and cores drawn
# (throughput / events-per-CPU-second) line up exactly:
#
#   query   clink drain   cores   flink drain   cores   verdict
#   q0            1.28M     2.1         2.44M     7.0   clink loses
#   q2            1.27M     2.3         3.31M     7.4   clink loses
#   q12           1.75M     8.6         0.87M     8.1   clink WINS
#   q17           1.74M     9.5         0.82M     7.8   clink WINS
#
# clink wins precisely the queries where it uses the machine and loses precisely
# the ones where it does not - while being 1.4x to 2x MORE efficient per record on
# the ones it loses. It is not slow per record; it will not draw more than about
# two cores on a stateless chain.
#
# THE HYPOTHESIS. Every query clink wins has a keyed shuffle, which splits the
# pipeline into two thread groups so broker wait on one side overlaps processing
# on the other. Every query it loses is a pure forward-edge chain, which the
# planner FUSES into a single operator - one thread per pipeline instance, doing
# the Kafka fetch and all the processing in series. That thread then cannot
# overlap network wait with work, so it idles at about half busy however fast the
# broker is.
#
# WHAT THIS SCRIPT MEASURES, in the order that narrows it:
#
#   1. The source's standalone ceiling, on this box, against this broker
#      (clink_kafka_source_bench: produce() only, no operators, no channels).
#      If one subtask can do far more than the deployed pipeline achieves per
#      instance, the broker is not the limit and the deployment is.
#   2. Which operator's input queue backs up during the drain
#      (driver/bottleneck.py). A stage whose queue fills IS the bottleneck; if
#      none fill, nothing downstream was the constraint.
#
# Leaves nothing running.
set -uo pipefail

cd "$(dirname "$0")"
CLINK_ROOT=../..
EVENTS="${EVENTS:-8000000}"
PAR="${PAR:-4}"
JM_HTTP=8095

cleanup() {
    docker compose -p nxcompare --profile clink --profile flink down -v \
        --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "=== 1. Bring up clink + load $EVENTS events (par=$PAR) ==="
EVENTS="$EVENTS" PARALLELISM="$PAR" SINK=blackhole ENGINES=clink QUERIES=q0 \
    KEEP_UP=1 ./throughput_sampled.sh 2>&1 | grep -E "bid stream|broker:|clink drain|clink mem|DRAIN|q0 " | sed 's/^/  /'

echo
echo "=== 2. Source ceiling: ONE KafkaSource subtask, produce() only ==="
echo "    (no decode, no operators, no channels - the broker-to-Batch rate)"
CLINK_KAFKA_BROKERS=localhost:9092 CLINK_KAFKA_TOPIC=nx-bid \
    CLINK_KAFKA_RECORDS=2000000 CLINK_KAFKA_TRIALS=3 \
    "$CLINK_ROOT/build/benchmarks/clink_kafka_source_bench" 2>&1 | sed 's/^/  /'

echo
echo "=== 3. Which stage backs up during a drain? ==="
# A fresh consumer group, or the second run resumes at the committed offset and
# reads nothing.
sed -e "s#__BROKERS__#kafka:29092#" \
    -e "s#group_id='clink-q0bh-bid'#group_id='clink-diag-$$'#" \
    queries/clink/q0_bh.tmpl.sql > /tmp/nxq-diag-q0.sql
if ! grep -q "clink-diag-$$" /tmp/nxq-diag-q0.sql; then
    # q0_bh is hand-written and may use a different group id; rewrite whatever is there.
    sed -i.bak -E "s#group_id='[^']*'#group_id='clink-diag-$$'#" /tmp/nxq-diag-q0.sql
fi
JID=$("$CLINK_ROOT/build/clink_submit_sql" --file /tmp/nxq-diag-q0.sql \
        --coordinator-host 127.0.0.1 --coordinator-port "$JM_HTTP" \
        --name diag_q0 --parallelism "$PAR" 2>/dev/null \
      | python3 -c 'import json,sys
for l in sys.stdin:
    l=l.strip()
    if l.startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except: pass')
if [ -z "$JID" ]; then
    echo "  submit failed - cannot sample"
    exit 1
fi
echo "  job $JID submitted; sampling operator queue depths through the drain"
python3 driver/bottleneck.py --base "http://127.0.0.1:$JM_HTTP" --job "$JID" \
    --interval 0.05 --seconds 90 2>&1 | sed 's/^/  /'
curl -fsS -X POST "http://127.0.0.1:$JM_HTTP/api/v1/jobs/$JID/cancel" >/dev/null 2>&1 || true
