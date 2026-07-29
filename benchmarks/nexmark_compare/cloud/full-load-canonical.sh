#!/usr/bin/env bash
# Fill the FULL rig's Kafka topic with the canonical nexmark bid dataset, generated
# on the rig by clink itself - the multi-node counterpart of load-canonical.sh, and
# the same reasoning: nexmark_source and nexmark_dump drive one deterministic
# generator, so a clink SQL job whose sink is the topic produces byte-identical
# canonical data with no Linux build and no 1.3 GB upload.
#
# Generation runs at PARALLELISM 1 (one generator per subtask from one seed - par N
# would write N identical copies; see load-canonical.sh) on the control node plus
# ONE worker, then tears its stack down: full-run.sh composes fresh stacks per
# trial and must find nothing running.
#
#   CONTROL_IP=<pub> CONTROL_PRIV=<priv> BROKER_PRIV=<priv> WORKER1="pub:priv:id" \
#     ./full-load-canonical.sh
set -uo pipefail
cd "$(dirname "$0")"

CONTROL_IP="${CONTROL_IP:?public address of the control node}"
CONTROL_PRIV="${CONTROL_PRIV:?private address of the control node}"
BROKER_PRIV="${BROKER_PRIV:?private address of the broker node}"
WORKER1="${WORKER1:?one worker as public:private:id}"
BROKER="${BROKER_PRIV}:9092"
TOPIC="${TOPIC:-nx-bid}"
# Partition count must cover the WIDEST parallelism the sweep will run, or the
# extra subtasks read nothing and the run measures a narrower source than claimed.
PARTITIONS="${PARTITIONS:-12}"
EVENTS="${EVENTS:-10000000}"   # generator total; bids are ~92% -> ~9.2M records
TPS="${TPS:-1000}"
SEED="${SEED:-1}"
KEY="${KEY:-$HOME/.ssh/clink-bench-ed25519}"
REMOTE=/root/clink/benchmarks/nexmark_compare
PROJECT=nxfull
# Generation is pinned to :main deliberately: the generator and the Kafka sink
# predate any commit under test, and the MEASURED runs pass their own image.
GEN_IMAGE="${GEN_IMAGE:-ghcr.io/orhaugh/clink-runtime:main}"

sshx() { ssh -i "$KEY" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
             -o LogLevel=ERROR -o ConnectTimeout=10 root@"$1" "${@:2}"; }

w_pub=${WORKER1%%:*}; rest=${WORKER1#*:}; w_priv=${rest%%:*}; w_id=${rest#*:}

echo "=== 1. topic $TOPIC ($PARTITIONS partitions) on $BROKER ==="
sshx "$CONTROL_IP" "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
    kafka-topics --bootstrap-server $BROKER --delete --topic $TOPIC" >/dev/null 2>&1
sshx "$CONTROL_IP" "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
    kafka-topics --bootstrap-server $BROKER --create --topic $TOPIC \
    --partitions $PARTITIONS --replication-factor 1" >/dev/null 2>&1
echo "  recreated"

echo "=== 2. clink up (control + one worker) for generation ==="
sshx "$CONTROL_IP" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV CLINK_IMAGE=$GEN_IMAGE \
    docker compose -p $PROJECT -f full-control.yml --profile clink up -d" >/dev/null 2>&1
sshx "$w_pub" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV WORKER_IP=$w_priv WORKER_ID=$w_id \
    CLINK_IMAGE=$GEN_IMAGE docker compose -p $PROJECT -f full-worker.yml --profile clink up -d" \
    >/dev/null 2>&1
for _ in $(seq 1 60); do
    sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8095/api/v1/health" >/dev/null 2>&1 && break
    sleep 2
done
sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8095/api/v1/health" >/dev/null 2>&1 || {
    echo "  coordinator did not come up"; exit 1; }
echo "  up"

echo "=== 3. generate $EVENTS events (bid stream, parallelism 1) into $TOPIC ==="
cat > /tmp/nx-full-gen.sql <<SQL
CREATE TABLE gen (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR,
                  url VARCHAR, datetime BIGINT)
  WITH (connector='nexmark', nexmark_type='bid', events_num='${EVENTS}',
        tps='${TPS}', seed='${SEED}');
CREATE TABLE sink_gen (auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR,
                       url VARCHAR, datetime BIGINT)
  WITH (connector='kafka', format='json', brokers='${BROKER}', topic='${TOPIC}');
INSERT INTO sink_gen SELECT auction, bidder, price, channel, url, datetime FROM gen;
SQL
jid=$(../../../build/clink_submit_sql --file /tmp/nx-full-gen.sql \
        --coordinator-host "$CONTROL_IP" --coordinator-port 8095 \
        --name nx_gen --parallelism 1 2>/dev/null | python3 -c 'import json,sys
for l in sys.stdin:
    if l.strip().startswith("{"):
        try: print(json.loads(l).get("job_id","")); break
        except Exception: pass')
[ -z "$jid" ] && { echo "  generation submit FAILED"; exit 1; }
echo "  job $jid generating"
prev=-1
for _ in $(seq 1 300); do
    cur=$(sshx "$CONTROL_IP" "curl -fsS http://127.0.0.1:8095/api/v1/jobs/$jid/operators" 2>/dev/null \
          | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(sum(int(o.get("records_out",0) or 0) for o in d.get("operators",[])))' 2>/dev/null || echo 0)
    [ "$cur" = "$prev" ] && [ "$cur" != "0" ] && break
    prev=$cur; sleep 3
done
sleep 8
sshx "$CONTROL_IP" "curl -fsS -X POST http://127.0.0.1:8095/api/v1/jobs/$jid/cancel" >/dev/null 2>&1

echo "=== 4. generation stack down ==="
sshx "$CONTROL_IP" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV docker compose -p $PROJECT \
    -f full-control.yml --profile clink --profile flink down -v" >/dev/null 2>&1
sshx "$w_pub" "cd $REMOTE/cloud && CONTROL_IP=$CONTROL_PRIV WORKER_IP=$w_priv WORKER_ID=$w_id \
    docker compose -p $PROJECT -f full-worker.yml --profile clink --profile flink down -v" \
    >/dev/null 2>&1

echo "=== 5. resulting topic depth ==="
depth=$(sshx "$CONTROL_IP" "docker run --rm --network host confluentinc/cp-kafka:7.6.0 \
    kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server $BROKER --topic $TOPIC 2>/dev/null \
    | awk -F: '{s += \$3} END {print s+0}'" 2>/dev/null | tr -d '\r')
echo "  $TOPIC holds ${depth:-0} records across $PARTITIONS partitions"
if [ -z "$depth" ] || [ "$depth" = "0" ]; then
    echo "  LOAD FAILED - topic is empty" >&2
    exit 1
fi
echo
echo "Pass EVENTS=$depth to full-run.sh so the sampler target matches the topic."
