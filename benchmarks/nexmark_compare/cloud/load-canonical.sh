#!/usr/bin/env bash
# Fill the rig's Kafka topic with the CANONICAL nexmark bid dataset, generated on the
# rig by clink itself.
#
# WHY THIS EXISTS. split-run.sh measures; it does not load. The documented loader was
# `kafka-producer-perf-test --payload-file`, which cycles a small file to reach the
# record count - fine for a ratio, useless for an absolute rate, because repeated
# payloads decode and cache differently from distinct ones. The 2026-07-28 run had to
# be published as ratios-only for exactly that reason.
#
# The alternative was `nexmark_dump`, which needs a Linux clink build the engine node
# does not have, or a ~1.3 GB NDJSON upload. Neither is necessary: clink_node in the
# runtime image already registers `nexmark_source` (tools/clink_node.cpp), and that
# connector and nexmark_dump both drive the SAME deterministic generator
# (include/clink/nexmark/generator.hpp). So the canonical dataset can be generated
# in-place by a clink SQL job whose sink is the Kafka topic, at the same seed.
#
# EVENTS is the generator's TOTAL event count across all three nexmark streams. Bids
# are about 92% of them, so the default 10,000,000 yields ~9.2M bid records - the
# depth the published baselines used. The script prints the actual depth; pass that to
# split-run.sh as EVENTS so the sampler's target matches what is really there.
#
#   ENGINE_IP=<pub> BROKER_PRIVATE_IP=10.10.1.2 ./load-canonical.sh
set -uo pipefail
cd "$(dirname "$0")"

ENGINE_IP="${ENGINE_IP:?set ENGINE_IP to the public address of the engine node}"
BROKER_PRIVATE_IP="${BROKER_PRIVATE_IP:?set BROKER_PRIVATE_IP}"
BROKER="${BROKER_PRIVATE_IP}:9092"
TOPIC="${TOPIC:-nx-bid}"
EVENTS="${EVENTS:-10000000}"
PAR="${PAR:-4}"           # topic partitions, and what split-run.sh will measure at
# GENERATION IS PARALLELISM 1, DELIBERATELY. nexmark_source builds one
# NexmarkGenerator per SUBTASK from the same seed and has no subtask-index awareness
# (include/clink/nexmark/register.hpp), so generating at parallelism N would write N
# identical copies of the stream - 4x the records, every one duplicated, and a q12
# group count that looks right while every count is 4x. Verified by reading the
# factory rather than by discovering it in the data.
GEN_PAR=1
TPS="${TPS:-1000}"
SEED="${SEED:-1}"
KEY="${KEY:-$HOME/.ssh/clink-bench-ed25519}"
REMOTE=/root/clink/benchmarks/nexmark_compare
PROJECT=nxsplit
JM_HTTP=8095
ROOT=..

sshx() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o LogLevel=ERROR root@"$ENGINE_IP" "$@"; }

echo "=== 1. topic $TOPIC ($PAR partitions) on $BROKER ==="
sshx "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
        kafka-topics --bootstrap-server $BROKER --delete --topic $TOPIC" >/dev/null 2>&1
sshx "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
        kafka-topics --bootstrap-server $BROKER --create --topic $TOPIC \
        --partitions $PAR --replication-factor 1" >/dev/null 2>&1
echo "  recreated"

echo "=== 2. bring the clink cluster up (generation runs on it) ==="
sshx "cd $REMOTE/cloud && BROKER=$BROKER docker compose -p $PROJECT -f split-engine.yml \
        --profile clink up -d" >/dev/null 2>&1
for _ in $(seq 1 60); do
    sshx "curl -fsS http://127.0.0.1:$JM_HTTP/api/v1/health" >/dev/null 2>&1 && break
    sleep 2
done
sshx "curl -fsS http://127.0.0.1:$JM_HTTP/api/v1/health" >/dev/null 2>&1 || {
    echo "  clink coordinator did not come up"; exit 1; }
echo "  up"

echo "=== 3. generate $EVENTS events (bid stream, parallelism $GEN_PAR) into $TOPIC ==="
# nexmark_type=bid emits only the bid records of the run; the generator is
# deterministic on (seed, tps, base_time_ms), so this is reproducible and is the same
# stream nexmark_dump writes.
cat > /tmp/nx-gen.sql <<SQL
CREATE TABLE gen (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR,
                  url VARCHAR, datetime BIGINT)
  WITH (connector='nexmark', nexmark_type='bid', events_num='${EVENTS}',
        tps='${TPS}', seed='${SEED}');
CREATE TABLE sink_gen (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR,
                       url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='${BROKER}', topic='${TOPIC}');
INSERT INTO sink_gen SELECT auction, bidder, price, channel, url, datetime FROM gen;
SQL
jid=$("$ROOT/../../build/clink_submit_sql" --file /tmp/nx-gen.sql \
        --coordinator-host "$ENGINE_IP" --coordinator-port "$JM_HTTP" \
        --name nx_gen --parallelism "$GEN_PAR" 2>/dev/null | python3 -c 'import json,sys
for l in sys.stdin:
    if l.strip().startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except Exception: pass')
[ -z "$jid" ] && { echo "  generation submit FAILED"; exit 1; }
echo "  job $jid generating"

prev=-1
for _ in $(seq 1 300); do
    cur=$(sshx "curl -fsS http://127.0.0.1:$JM_HTTP/api/v1/jobs/$jid/operators" 2>/dev/null \
          | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(sum(int(o.get("records_out",0) or 0) for o in d.get("operators",[])))' 2>/dev/null || echo 0)
    [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && break
    prev=$cur; sleep 3
done
sleep 8
sshx "curl -fsS -X POST http://127.0.0.1:$JM_HTTP/api/v1/jobs/$jid/cancel" >/dev/null 2>&1

echo "=== 4. tear the clink cluster down (split-run.sh composes its own) ==="
sshx "cd $REMOTE/cloud && BROKER=$BROKER docker compose -p $PROJECT -f split-engine.yml \
        --profile clink --profile flink down -v" >/dev/null 2>&1

echo "=== 5. resulting topic depth ==="
depth=$(sshx "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
        kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $BROKER --topic $TOPIC 2>/dev/null \
        | awk -F: '{s += \$3} END {print s+0}'" 2>/dev/null | tr -d '\r')
echo "  $TOPIC holds ${depth:-0} records"
if [ -z "$depth" ] || [ "$depth" = "0" ]; then
    echo "  LOAD FAILED - topic is empty" >&2
    exit 1
fi
echo
echo "Now run split-run.sh with EVENTS=$depth so the sampler target matches the topic:"
echo "  ENGINE_IP=$ENGINE_IP BROKER_PRIVATE_IP=$BROKER_PRIVATE_IP \\"
echo "    ENGINES=\"clink flink\" QUERIES=\"q0 q12\" PAR=$PAR EVENTS=$depth REPEATS=2 ./split-run.sh"
