#!/usr/bin/env bash
# The Redis Streams and Pub/Sub sources' live gate: their live suites against
# the Redis server and the Pub/Sub emulator of docker/integration-services.yml,
# one service at a time, then a check that nothing was skipped and that every
# required suite ran in full.
#
#   scripts/redis-pubsub-live.sh [build-dir]
#
# Each service runs as a one-off container of its compose service, published on
# a free loopback port, so a Redis or an emulator already on the machine is left
# alone; the container goes, with its volumes, when its suites end or the script
# exits. It runs:
#   - clink_redis_tests: RedisLive.* (but the TLS case, which needs a TLS server
#     this gate does not start) and RedisSourceAckLive.*, the source's
#     acknowledgement contract against the server;
#   - clink_http_connector_tests: PubSubLive.* and PubSubSourceAckLive.*, the
#     same against the emulator.
# A live case skips when its service is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, a service that
# never answers and a failed case fail it too.
#
# Knobs:
#   BUILD_DIR   the build to take the binaries from (argument or default build/)
#   REPORT_DIR  where the reports and, on failure, the service logs go (default
#               <build-dir>/redis-pubsub-live-reports)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/redis-pubsub-live-reports}"
PROJECT=clink-ack-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
REDIS_BIN="$(find_bin clink_redis_tests)"
HTTP_BIN="$(find_bin clink_http_connector_tests)"
if [ -z "${REDIS_BIN}" ] || [ -z "${HTTP_BIN}" ]; then
    echo "redis-pubsub-live: clink_redis_tests and clink_http_connector_tests must be built" \
        "under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.json "${REPORT_DIR}"/*.log

status=0
container=""
host_port=""
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

start() {  # <compose service> <container port>
    container="${PROJECT}-$1"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" run -d --name "${container}" -p "127.0.0.1::$2" "$1" >/dev/null
    host_port="$(docker port "${container}" "$2/tcp" | head -1 | sed 's/.*://')"
    if [ -z "${host_port}" ]; then
        echo "redis-pubsub-live: $1 published no port" >&2
        status=1
        exit 1
    fi
}

run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    "${bin}" --gtest_brief=1 --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

start redis 6379
ready=0
for _ in $(seq 1 60); do
    if docker exec "${container}" redis-cli ping 2>/dev/null | grep -q PONG; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" -ne 1 ]; then
    echo "redis-pubsub-live: redis never answered PING" >&2
    status=1
    exit 1
fi
CLINK_REDIS_TEST_URL="redis://127.0.0.1:${host_port}" run redis "${REDIS_BIN}" \
    --gtest_filter='RedisLive.*:RedisSourceAckLive.*:-RedisLive.TlsConnectionRoundTrip'
stop

start pubsub 8085
ready=0
for _ in $(seq 1 120); do
    code="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${host_port}/" || true)"
    if [ -n "${code}" ] && [ "${code}" != "000" ]; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" -ne 1 ]; then
    echo "redis-pubsub-live: the Pub/Sub emulator never served HTTP" >&2
    status=1
    exit 1
fi
CLINK_PUBSUB_EMULATOR_HOST="127.0.0.1:${host_port}" run pubsub "${HTTP_BIN}" \
    --gtest_filter='PubSubLive.*:PubSubSourceAckLive.*'
stop

python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'RedisLive.*=5' \
    --expect 'RedisSourceAckLive.*=9' \
    --expect 'PubSubLive.*=1' \
    --expect 'PubSubSourceAckLive.*=3' \
    "${REPORT_DIR}/redis.json" "${REPORT_DIR}/pubsub.json" || status=1
exit "${status}"
