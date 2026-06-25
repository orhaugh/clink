#!/usr/bin/env bash
# Distributed FUNCTIONAL verification: run clink in CONTAINERS (1 JM + 4 TM, each
# its own container/hostname on the project network) and confirm the Nexmark
# queries are gate-correct when subtasks shuffle over real container-to-container
# TCP (separate network namespaces) at parallelism>1 - a faithful single-host
# stand-in for a multi-machine run. This is the cross-container counterpart to
# run.sh's host-process clink; it checks CORRECTNESS (output-row counts), not
# throughput, so there is no Flink side / CPU sampling here.
#
#   ./verify_distributed.sh                  # q0 q12 q8 at par 4, 500k events
#   PARALLELISM=2 QUERIES="q8" ./verify_distributed.sh
#
# Each clink source subtask reads one ordered Kafka partition; the keyed shuffle
# (incl. the two-input equi-join) is exercised across containers. Builds the
# clink-runtime image from docker/Dockerfile.runtime if absent (needs the
# clink-build:latest toolchain image first - see docker-compose.yml at repo root).
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLINK_ROOT="$(cd "$ROOT/../.." && pwd)"
PROJECT=nxcompare
PY="$ROOT/../flink_compare/.venv/bin/python"
KEX="docker exec ${PROJECT}-kafka-1 kafka-topics --bootstrap-server localhost:9092"
JM_HTTP=8095  # host port mapped to clink-jm:8081

EVENTS="${EVENTS:-500000}"
TPS="${TPS:-1000}"
PAR="${PARALLELISM:-4}"
QUERIES="${QUERIES:-q0 q12 q8}"
DATA_DIR="${DATA_DIR:-/tmp/nxcompare-data}"

# Data-true expected output-row count per query (same as run.sh).
expected_for() {
    case "$1" in
        q0) echo 460000 ;;
        q12) echo 184767 ;;
        q8) echo 1056 ;;
        *) echo 0 ;;
    esac
}
step() { printf '\n=== %s ===\n' "$*"; }
recreate_topic() {  # name partitions [append]
    $KEX --delete --topic "$1" >/dev/null 2>&1
    while $KEX --list 2>/dev/null | grep -qx "$1"; do sleep 1; done
    if [[ "${3:-}" == "append" ]]; then
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 \
            --config message.timestamp.type=LogAppendTime >/dev/null 2>&1
    else
        $KEX --create --topic "$1" --partitions "$2" --replication-factor 1 >/dev/null 2>&1
    fi
}

mkdir -p "$DATA_DIR"

step "1. Build clink (host: nexmark_dump for data gen) + clink-runtime image (containers)"
cmake --build "$CLINK_ROOT/build" --target nexmark_dump --parallel 10 >/dev/null 2>&1 || {
    echo "host nexmark_dump build failed"; exit 1; }
if ! docker image inspect clink-runtime:latest >/dev/null 2>&1; then
    if ! docker image inspect clink-build:latest >/dev/null 2>&1; then
        echo "clink-build:latest toolchain image missing - build it once with:"
        echo "  docker build -t clink-build:latest -f docker/Dockerfile ."
        exit 1
    fi
    echo "building clink-runtime:latest (clink_node for Linux; ~5-8 min cold)..."
    ( cd "$CLINK_ROOT" && docker build -t clink-runtime:latest -f docker/Dockerfile.runtime . ) || {
        echo "clink-runtime build failed"; exit 1; }
fi

step "2. Bring up Kafka + clink cluster (1 JM + 4 TM containers, par up to 4)"
docker compose -p "$PROJECT" up -d zookeeper kafka >/dev/null 2>&1
for i in $(seq 1 45); do
    docker exec ${PROJECT}-kafka-1 kafka-broker-api-versions --bootstrap-server localhost:9092 \
        >/dev/null 2>&1 && break
    sleep 2
done
docker compose -p "$PROJECT" --profile clink up -d clink-jm clink-tm1 clink-tm2 clink-tm3 clink-tm4 >/dev/null 2>&1
for i in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:${JM_HTTP}/api/v1/health" >/dev/null 2>&1 && break
    sleep 2
done
curl -fsS "http://127.0.0.1:${JM_HTTP}/api/v1/health" >/dev/null 2>&1 || { echo "clink JM not healthy"; exit 1; }

step "3. Generate dataset ($EVENTS events tps=$TPS) + load nx-{person,auction,bid} ($PAR partitions, keyed)"
"$CLINK_ROOT/build/benchmarks/nexmark_dump" --events "$EVENTS" --tps "$TPS" --out-dir "$DATA_DIR" | tail -1
for t in nx-person nx-auction nx-bid; do recreate_topic "$t" "$PAR"; done
"$PY" "$ROOT/driver/load_ndjson.py" --dir "$DATA_DIR" --bootstrap localhost:9092 --prefix nx- \
    2>/dev/null | tail -1

PASS=0; FAIL=0
for q in $QUERIES; do
    step "4. $q at par=$PAR on the containerized clink cluster"
    out="nx-out-$q-cverify"
    recreate_topic "$out" "$PAR" append
    # Containerized clink reaches Kafka via the in-network listener kafka:29092.
    sed -e "s#__OUT__#$out#" -e "s#__BROKERS__#kafka:29092#" \
        "$ROOT/queries/clink/$q.tmpl.sql" > "$DATA_DIR/$q-cverify.sql"
    "$CLINK_ROOT/build/clink_submit_sql" --file "$DATA_DIR/$q-cverify.sql" \
        --jm-host 127.0.0.1 --jm-port "$JM_HTTP" --name "cv_$q" --parallelism "$PAR" 2>&1 | tail -1
    exp=$(expected_for "$q")
    cnt=$("$PY" "$ROOT/driver/measure_steady.py" --brokers localhost:9092 --topic "$out" \
        --expected "$exp" --quiet-timeout 20 2>/dev/null | \
        "$PY" -c "import sys,json; print(json.load(sys.stdin)['count'])" 2>/dev/null)
    if [[ "$cnt" == "$exp" ]]; then
        echo "  PASS  $q: $cnt == $exp (gate-exact across containers at par=$PAR)"
        PASS=$((PASS+1))
    else
        echo "  FAIL  $q: $cnt != $exp"
        FAIL=$((FAIL+1))
    fi
done

step "5. Result"
echo "  $PASS passed, $FAIL failed (clink in containers, par=$PAR, cross-container shuffle)"

if [[ "${KEEP_UP:-}" != "1" ]]; then
    step "6. Teardown"
    docker compose -p "$PROJECT" --profile clink down -v >/dev/null 2>&1
fi
[[ "$FAIL" -eq 0 ]]
