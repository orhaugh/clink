#!/usr/bin/env bash
# The live gate for the sources that acknowledge their broker at checkpoint
# completion: Redis Streams, Pub/Sub, RabbitMQ, NATS JetStream and Pulsar. Each
# connector's live suites run against the service of docker/integration-services.yml,
# one service at a time, then a check that nothing was skipped and that every
# required suite ran in full.
#
#   scripts/ack-sources-live.sh [build-dir]
#
# Each service runs as a one-off container of its compose service, published on
# free loopback ports, so a broker already on the machine is left alone; the
# container goes, with its volumes, when its suites end or the script exits.
# For each source it runs the connector's round trip (RedisLive, PubSubLive,
# RabbitMqLive, NatsLive, PulsarLive) and its acknowledgement contract
# (<Connector>SourceAckLive): an aborted checkpoint, crashes before and after a
# completion, notifications out of order, close after a cancel, the bound on
# what is held, and per connector the leases, a failed acknowledgement and a
# scale-down. The Redis TLS case needs a TLS server this gate does not start
# and is left out.
#
# A live case skips when its service is not named and googletest counts that
# as a pass, so scripts/check-gtest-reports.py fails the run on any skip and on
# a suite that ran fewer cases than it should. A missing binary, a service that
# never answers and a failed case fail it too.
#
# Knobs:
#   BUILD_DIR   the build to take the binaries from (argument or default build/)
#   REPORT_DIR  where the reports and, on failure, the service logs go (default
#               <build-dir>/ack-sources-live-reports)
#   PIN_RUNNER  a prefix for every test command (CI runs the binaries inside the
#               toolchain image with `docker run --network host ... -e <each
#               variable below>`)
#
# What the tests read, all set here: CLINK_REDIS_TEST_URL,
# CLINK_PUBSUB_EMULATOR_HOST, CLINK_RABBITMQ_TEST_ENDPOINT,
# CLINK_RABBITMQ_TEST_PORT, CLINK_RABBITMQ_TEST_USER,
# CLINK_RABBITMQ_TEST_PASSWORD, CLINK_NATS_TEST_ENDPOINT,
# CLINK_PULSAR_TEST_ENDPOINT.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/ack-sources-live-reports}"
PROJECT=clink-ack-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
REDIS_BIN="$(find_bin clink_redis_tests)"
HTTP_BIN="$(find_bin clink_http_connector_tests)"
RABBITMQ_BIN="$(find_bin clink_rabbitmq_tests)"
NATS_BIN="$(find_bin clink_nats_tests)"
PULSAR_BIN="$(find_bin clink_pulsar_tests)"
for b in REDIS_BIN HTTP_BIN RABBITMQ_BIN NATS_BIN PULSAR_BIN; do
    if [ -z "${!b}" ]; then
        echo "ack-sources-live: clink_redis_tests, clink_http_connector_tests," \
            "clink_rabbitmq_tests, clink_nats_tests and clink_pulsar_tests must be built" \
            "under ${BUILD_DIR}" >&2
        exit 1
    fi
done

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.json "${REPORT_DIR}"/*.log
# The toolchain image's user is not the runner's; let either write here.
chmod a+rwx "${REPORT_DIR}"

status=0
container=""
stop() {
    if [ -n "${container}" ]; then
        if [ "${status}" -ne 0 ]; then
            docker logs "${container}" >"${REPORT_DIR}/${container}.log" 2>&1 || true
        fi
        docker rm -fv "${container}" >/dev/null 2>&1 || true
        container=""
    fi
}
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    stop
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

start() {  # <compose service> <container port>...
    local service="$1"
    shift
    container="${PROJECT}-${service}"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    local publish=()
    for p in "$@"; do
        publish+=(-p "127.0.0.1::${p}")
    done
    "${COMPOSE[@]}" run -d --name "${container}" "${publish[@]}" "${service}" >/dev/null
}

port_of() {  # <container port>
    docker port "${container}" "$1/tcp" | head -1 | sed 's/.*://'
}

await() {  # <what> <seconds> <command...>
    local what="$1" seconds="$2"
    shift 2
    for _ in $(seq 1 "${seconds}"); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "ack-sources-live: ${what} never became ready" >&2
    status=1
    exit 1
}

# shellcheck disable=SC2329 # called through await
http_ok() {  # <url>
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$1" || true)" = "200" ]
}

# shellcheck disable=SC2329 # called through await
http_any() {  # <url>
    local code
    code="$(curl -s -o /dev/null -w '%{http_code}' "$1" || true)"
    [ -n "${code}" ] && [ "${code}" != "000" ]
}

run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_brief=1 --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

start redis 6379
await redis 60 sh -c "docker exec '${container}' redis-cli ping | grep -q PONG"
port="$(port_of 6379)"
export CLINK_REDIS_TEST_URL="redis://127.0.0.1:${port}"
run redis "${REDIS_BIN}" \
    --gtest_filter='RedisLive.*:RedisSourceAckLive.*:-RedisLive.TlsConnectionRoundTrip'
stop

start pubsub 8085
await "the Pub/Sub emulator" 180 http_any "http://127.0.0.1:$(port_of 8085)/"
port="$(port_of 8085)"
export CLINK_PUBSUB_EMULATOR_HOST="127.0.0.1:${port}"
run pubsub "${HTTP_BIN}" --gtest_filter='PubSubLive.*:PubSubSourceAckLive.*'
stop

start rabbitmq 5672
# Asked only once the server has written its Erlang cookie: a CLI that finds
# none writes one as root, which the server cannot read, and it stops. The
# listener opens after the node answers a ping, so its port is asked too.
await rabbitmq 120 docker exec "${container}" sh -c \
    '[ -f /var/lib/rabbitmq/.erlang.cookie ] && rabbitmq-diagnostics -q check_port_connectivity'
port="$(port_of 5672)"
export CLINK_RABBITMQ_TEST_ENDPOINT=127.0.0.1 CLINK_RABBITMQ_TEST_PORT="${port}"
export CLINK_RABBITMQ_TEST_USER=clink CLINK_RABBITMQ_TEST_PASSWORD=clink
run rabbitmq "${RABBITMQ_BIN}" --gtest_filter='RabbitMqLive.*:RabbitMqSourceAckLive.*'
stop

start nats 4222 8222
await nats 60 http_ok "http://127.0.0.1:$(port_of 8222)/healthz?js-enabled-only=true"
port="$(port_of 4222)"
export CLINK_NATS_TEST_ENDPOINT="nats://127.0.0.1:${port}"
run nats "${NATS_BIN}" --gtest_filter='NatsLive.*:NatsSourceAckLive.*'
stop

start pulsar 6650 8080
await pulsar 240 http_ok "http://127.0.0.1:$(port_of 8080)/admin/v2/brokers/health"
port="$(port_of 6650)"
export CLINK_PULSAR_TEST_ENDPOINT="pulsar://127.0.0.1:${port}"
run pulsar "${PULSAR_BIN}" --gtest_filter='PulsarLive.*:PulsarSourceAckLive.*'
stop

python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'RedisLive.*=5' \
    --expect 'RedisSourceAckLive.*=9' \
    --expect 'PubSubLive.*=1' \
    --expect 'PubSubSourceAckLive.*=3' \
    --expect 'RabbitMqLive.*=3' \
    --expect 'RabbitMqSourceAckLive.*=7' \
    --expect 'NatsLive.*=2' \
    --expect 'NatsSourceAckLive.*=6' \
    --expect 'PulsarLive.*=2' \
    --expect 'PulsarSourceAckLive.*=4' \
    "${REPORT_DIR}"/{redis,pubsub,rabbitmq,nats,pulsar}.json || status=1
exit "${status}"
