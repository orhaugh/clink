#!/usr/bin/env bash
# The Prometheus Pushgateway sink's live gate: every Pushgateway sink test
# against a pinned gateway, then a check that nothing was skipped and that every
# required suite ran in full.
#
#   scripts/prometheus-live.sh [build-dir]
#
# It starts one container of the pinned image on a free loopback port, waits
# for /-/ready to pass, and runs:
#   - clink_http_connector_tests' Pushgateway suites: the sample rules (series
#     identity, names, label values, the value, grouping conflicts, bounds, the
#     path), the reading of push responses, the sink against a stub gateway
#     (both push_series modes and every push trigger, refusals under each
#     policy, a 400, throttling, refused credentials, lost replies,
#     cancellation, a restored run adopting its group, the factory's options),
#     and the live suite against the gateway (every label and group value read
#     back by a scrape, values, refused records, each mode's replacement, a
#     restored run, two writers, a type conflict, a reply withheld after the
#     gateway applied a push, a group deleted under the sink), with a check that
#     the gateway is the version started;
#   - clink_integration_tests' PushgatewayRecovery cells when that binary is
#     built (PROMETHEUS_LIVE_RECOVERY=0 leaves them out): a coordinator and two
#     workers polling a feed into the sink, a worker killed after a push the
#     gateway applied and no checkpoint covered, in each push_series mode and as
#     a SQL job, each verified by scraping the gateway.
#
# A live case skips when its gateway is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, a gateway that
# never becomes ready and a failed case fail it too.
#
# What it keeps in REPORT_DIR (default <build-dir>/prometheus-live-reports):
# each run's googletest report and output (the "[prometheus-live]" and
# "[prometheus-recovery]" lines and every assertion), the recovery cells'
# coordinator and worker logs and the final scrape each verdict rests on
# (recovery-logs/<cell>/), provenance.txt (the commit, whether the tree was
# clean, the build's configuration, the gateway's version and image) and, on
# failure, the gateway's log. The gateway takes no credentials here.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#   PROMETHEUS_LIVE_RECOVERY  1 (default) or 0
#
# What the tests read, all set here: CLINK_PUSHGATEWAY_TEST_ENDPOINT,
# CLINK_PUSHGATEWAY_TEST_VERSION, CLINK_PUSHGATEWAY_RECOVERY_LOG_DIR.
set -euo pipefail

PUSHGATEWAY_VERSION=1.10.0
PUSHGATEWAY_IMAGE="prom/pushgateway:v${PUSHGATEWAY_VERSION}@sha256:7a4d0696a24ef4e8bad62bee5656855a0aff2f26416d8cb32009dc28d6263604"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/prometheus-live-reports}"
PROMETHEUS_LIVE_RECOVERY="${PROMETHEUS_LIVE_RECOVERY:-1}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
if [ -z "${UNIT_BIN}" ]; then
    echo "prometheus-live: clink_http_connector_tests is not built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${PROMETHEUS_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "prometheus-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or PROMETHEUS_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container="clink-pushgateway-live"
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/pushgateway-server.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
docker rm -fv "${container}" >/dev/null 2>&1 || true
docker run -d --name "${container}" -p "127.0.0.1:${port}:9091" "${PUSHGATEWAY_IMAGE}" >/dev/null

endpoint="http://127.0.0.1:${port}"
ready=0
for _ in $(seq 1 60); do
    if curl -sf "${endpoint}/-/ready" >/dev/null; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "prometheus-live: the gateway never became ready" >&2
    status=1
    exit 1
fi

export CLINK_PUSHGATEWAY_TEST_ENDPOINT="${endpoint}"
export CLINK_PUSHGATEWAY_TEST_VERSION="${PUSHGATEWAY_VERSION}"
export CLINK_PUSHGATEWAY_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "gateway: $(curl -sf "${endpoint}/api/v1/status" | python3 -c 'import json, sys; print(json.load(sys.stdin)["data"]["build_information"]["version"])')"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "recovery cells: $( [ "${PROMETHEUS_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
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

run pushgateway "${UNIT_BIN}" --gtest_filter='Prom*:PushgatewayLive.*'
reports=("${REPORT_DIR}/pushgateway.json")
expect=(
    --expect 'PromRender.*=12'
    --expect 'PromPushResult.*=3'
    --expect 'PromSink.*=18'
    --expect 'PushgatewayLive.*=14'
)
if [ "${PROMETHEUS_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter='PushgatewayRecovery.*'
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect 'PushgatewayRecovery.*=3')
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
