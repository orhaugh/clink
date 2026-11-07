#!/usr/bin/env bash
# The InfluxDB sink's live gate: every InfluxDB test against a pinned InfluxDB v2,
# then a check that nothing was skipped and that every required suite ran in full.
#
#   scripts/influxdb-live.sh [build-dir]
#
# It starts one container of the pinned image on a free loopback port, set up
# with an organisation, a bucket and an operator token for the tests (all three
# named clink, the token a throwaway the container is created with), waits for
# /health to pass, and runs:
#   - clink_http_connector_tests' InfluxDB suites: the renderer (point identity,
#     escaping, refusals, timestamps, numbers), the reading of write responses,
#     the sink against a stub server (partial writes, refusals, lost replies,
#     cancellation, the factory's options), and the live suite against the
#     server (exact timestamps, every escape read back, refused records beside
#     good ones, a type conflict's partial write, merged identities, a reply
#     lost after the server applied a batch, a refused token and a missing
#     bucket), with a check that the server is the version started;
#   - clink_integration_tests' InfluxDbRecovery cells when that binary is built
#     (INFLUXDB_LIVE_RECOVERY=0 leaves them out): a coordinator and two workers
#     polling a feed into the sink, a worker killed after writes the server
#     applied and no checkpoint covered, timestamped, in server-time mode, and
#     as a SQL job, each verified by reading the measurement back with Flux.
#
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, a server that
# never passes its health check and a failed case fail it too.
#
# What it keeps in REPORT_DIR (default <build-dir>/influxdb-live-reports): each
# run's googletest report and output (the "[influxdb-live]" and
# "[influxdb-recovery]" lines and every assertion), the recovery cells'
# coordinator and worker logs (recovery-logs/<measurement>/), provenance.txt
# (the commit, whether the tree was clean, the build's configuration, the
# server's version and image) and, on failure, the server's log. The token is
# passed to the tests in their environment and written nowhere here.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#   INFLUXDB_LIVE_RECOVERY  1 (default) or 0
#
# What the tests read, all set here: CLINK_INFLUXDB_TEST_ENDPOINT,
# CLINK_INFLUXDB_TEST_ORG, CLINK_INFLUXDB_TEST_BUCKET, CLINK_INFLUXDB_TEST_TOKEN,
# CLINK_INFLUXDB_TEST_VERSION, CLINK_INFLUXDB_RECOVERY_LOG_DIR.
set -euo pipefail

INFLUXDB_VERSION=2.7.12
INFLUXDB_IMAGE="influxdb:${INFLUXDB_VERSION}@sha256:b8d940ca9376f85118260f5b6bd236b8a00b1749c3350c5578d4cde8e27f31f2"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/influxdb-live-reports}"
INFLUXDB_LIVE_RECOVERY="${INFLUXDB_LIVE_RECOVERY:-1}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
if [ -z "${UNIT_BIN}" ]; then
    echo "influxdb-live: clink_http_connector_tests is not built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${INFLUXDB_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "influxdb-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or INFLUXDB_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container="clink-influxdb-live"
# A throwaway operator token the container is created with; the tests read it
# from their environment.
token="clink-live-$(od -An -N8 -tx1 /dev/urandom | tr -d ' \n')"
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/influxdb-server.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
docker rm -fv "${container}" >/dev/null 2>&1 || true
docker run -d --name "${container}" -p "127.0.0.1:${port}:8086" \
    -e DOCKER_INFLUXDB_INIT_MODE=setup -e DOCKER_INFLUXDB_INIT_USERNAME=clink \
    -e DOCKER_INFLUXDB_INIT_PASSWORD="clink-password-${token}" -e DOCKER_INFLUXDB_INIT_ORG=clink \
    -e DOCKER_INFLUXDB_INIT_BUCKET=clink -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN="${token}" \
    "${INFLUXDB_IMAGE}" >/dev/null

endpoint="http://127.0.0.1:${port}"
ready=0
for _ in $(seq 1 90); do
    # Setup creates the bucket after /health first passes; wait for both.
    if curl -sf "${endpoint}/health" | grep -q '"status":"pass"' &&
        curl -sf -H "Authorization: Token ${token}" "${endpoint}/api/v2/buckets?name=clink" |
        grep -q '"name": *"clink"'; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "influxdb-live: the server never became ready" >&2
    status=1
    exit 1
fi

export CLINK_INFLUXDB_TEST_ENDPOINT="${endpoint}"
export CLINK_INFLUXDB_TEST_ORG=clink
export CLINK_INFLUXDB_TEST_BUCKET=clink
export CLINK_INFLUXDB_TEST_TOKEN="${token}"
export CLINK_INFLUXDB_TEST_VERSION="${INFLUXDB_VERSION}"
export CLINK_INFLUXDB_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "server: $(curl -sf "${endpoint}/health" | python3 -c 'import json, sys; print(json.load(sys.stdin)["version"])')"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "recovery cells: $( [ "${INFLUXDB_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

run influxdb "${UNIT_BIN}" --gtest_filter='Influx*'
reports=("${REPORT_DIR}/influxdb.json")
expect=(
    --expect 'InfluxRender.*=10'
    --expect 'InfluxWriteResult.*=4'
    --expect 'InfluxSink.*=11'
    --expect 'InfluxDbLive.*=9'
)
if [ "${INFLUXDB_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter='InfluxDbRecovery.*'
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect 'InfluxDbRecovery.*=3')
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
