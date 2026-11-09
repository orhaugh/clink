#!/usr/bin/env bash
# The generic HTTP sink's gate: every case of its contract against local HTTP
# endpoints the tests run themselves, then a check that nothing was skipped and
# that every required suite ran in full. It needs no service: an HTTP endpoint
# the test serves is the sink's counterpart.
#
#   scripts/http-sink-live.sh [build-dir]
#
# It runs:
#   - clink_http_connector_tests, the sink's suites: HttpSinkContract, the sink
#     as its factory builds it against an endpoint that refuses a body it cannot
#     read (each record as written in both framings, a record the framing
#     cannot carry refused alone before it is sent, a 404 that dead-letters
#     nothing, a 400 dead-lettered under dlq=drop, a passing refusal retried, a
#     cancel ending a post in flight); HttpSinkFactory, the factory's refusals
#     and the record; and the shared bulk sink's own suites (HttpBulkSink,
#     HttpDlq, HttpFailureClassify), bar HttpBulkSink's refusal of https in a
#     build without TLS, which a build with TLS skips and so cannot pass;
#   - clink_integration_tests' HttpSinkRecovery cells: a coordinator and two
#     workers polling a feed the test serves (3,000 records released at 300 a
#     second) into http_sink, posting to an endpoint the test serves, worker 0
#     parked and killed at http_sink.before_write (the request built, nothing
#     sent) and at http_sink.after_write (the endpoint answered, the barrier
#     not), the latter also as a SQL job at parallelism 2; each checks that
#     every record arrived, at most twice, and twice only after the restored
#     checkpoint.
#
# A case that cannot run skips and googletest counts that as a pass, so
# scripts/check-gtest-reports.py fails the run on any skip and on a suite that
# ran fewer cases than it should. REPORT_DIR keeps the reports, the cells'
# cluster logs and provenance.txt (the commit, whether the tree was clean and
# the build's configuration).
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/http-sink-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e
#                CLINK_HTTP_SINK_RECOVERY_LOG_DIR`)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/http-sink-live-reports}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
HTTP_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
SQL_BIN="$(find_bin clink_submit_sql)"
if [ -z "${HTTP_BIN}" ] || [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ] || [ -z "${SQL_BIN}" ]; then
    echo "http-sink-live: clink_http_connector_tests, clink_integration_tests, clink_node and" \
        "clink_submit_sql must be built under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

export CLINK_HTTP_SINK_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

status=0
{
    # CI runs this as another user than the checkout's owner; safe.directory
    # lets git read it there.
    echo "commit: $(git -c safe.directory='*' -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -c safe.directory='*' -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "httplib: $(grep -m1 -E '#define CPPHTTPLIB_VERSION' "${BUILD_DIR}/_deps/httplib-src/httplib.h" 2>/dev/null | awk '{print $3}' || echo '?')"
    echo "evidence: every case ran against endpoints the tests serve; nothing here" \
        "qualifies a third-party API, whose own replies decide what a 2xx means"
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

run sink "${HTTP_BIN}" \
    --gtest_filter='HttpSinkContract.*:HttpSinkFactory.*:HttpBulkSink.*:HttpDlq.*:HttpFailureClassify.*:-HttpBulkSink.RejectsHttpsWhenTlsUnsupported'
run recovery "${IT_BIN}" --gtest_filter='HttpSinkRecovery.*'
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'HttpSinkContract.*=8' \
    --expect 'HttpSinkFactory.*=2' \
    --expect 'HttpBulkSink.*=8' \
    --expect 'HttpDlq.*=6' \
    --expect 'HttpFailureClassify.*=1' \
    --expect 'HttpSinkRecovery.*=3' \
    "${REPORT_DIR}/sink.json" "${REPORT_DIR}/recovery.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
