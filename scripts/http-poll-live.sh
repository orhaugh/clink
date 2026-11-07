#!/usr/bin/env bash
# The HTTP polling source's gate: every case of its contract against local HTTP
# servers the tests run themselves, then a check that nothing was skipped and
# that every required suite ran in full. It needs no service.
#
#   scripts/http-poll-live.sh [build-dir]
#
# It runs:
#   - clink_http_connector_tests, every HttpPoll suite: the page reader on its
#     own (response shapes, numbers kept as served, the cursor contract), the
#     options and the factory's parsing, and the source against a scripted
#     endpoint (paging, bounded completion, 204 and 304, conditional requests
#     across cursor moves, restores and reopens, exact and encoded cursors, an
#     endpoint that ignores the cursor, the response bounds, retries, throttling,
#     cancellation, and errors that name no credential), with the capability
#     record and the delivery analyser's verdicts;
#   - clink_integration_tests' HttpPollRecovery cells: a coordinator and two
#     workers polling a retained feed the test serves (2,400 records released
#     at 600 a second, at most 100 a page) into a two-phase-commit file sink,
#     worker 0 parked just after the source emitted a page
#     (http_poll_source.after_emit) and killed with that page in flight, at
#     parallelism 1 and 2 and as a SQL job submitted by clink_submit_sql; each
#     checks the committed files against the feed's dataset (every record once,
#     byte for byte or value for value) and that the restored source asked
#     again from before the page it had emitted;
#   - its HttpPollSql cases: `clink run` reading the feed bounded to its end,
#     and the coordinator refusing a stronger delivery guarantee over a table
#     in resource mode.
#
# A case that cannot run skips and googletest counts that as a pass, so
# scripts/check-gtest-reports.py fails the run on any skip and on a suite that
# ran fewer cases than it should. A missing binary and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each run's googletest report and output (the
# cells' "[http-poll-recovery]" and "[http-poll-sql]" verdict lines and every
# assertion), the cells' coordinator and worker logs (recovery-logs/<cell>/) and
# provenance.txt (the commit, whether the tree was clean, the build's
# configuration and the feed the cells served). The tests use no credential;
# the one case about credentials sends made-up ones and checks no error repeats
# them.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/http-poll-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e
#                CLINK_HTTP_POLL_RECOVERY_LOG_DIR`)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/http-poll-live-reports}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
HTTP_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
SQL_BIN="$(find_bin clink_submit_sql)"
CLI_BIN="$(find_bin clink)"
if [ -z "${HTTP_BIN}" ] || [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ] || [ -z "${SQL_BIN}" ] ||
    [ -z "${CLI_BIN}" ]; then
    echo "http-poll-live: clink_http_connector_tests, clink_integration_tests, clink_node," \
        "clink_submit_sql and clink must be built under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

export CLINK_HTTP_POLL_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

status=0
{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION HTTPLIB_USE_ZLIB_IF_AVAILABLE; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "httplib: $(grep -m1 -E '#define CPPHTTPLIB_VERSION' "${BUILD_DIR}/_deps/httplib-src/httplib.h" 2>/dev/null | awk '{print $3}' || echo '?')"
    echo "feed: 2,400 records, released at 600 a second, at most 100 a page after the cursor;" \
        "record n is {\"seq\":n,\"id\":\"k<n>\",\"big\":<an integer past int64>,\"v\":<a string" \
        "with a \\u escape>}; cells park worker 0 at http_poll_source.after_emit (block at" \
        "the 60th, 80th and 70th emitted page) and kill it"
    echo "evidence: every case ran against local servers the tests start; nothing here" \
        "qualifies a third-party API, whose own contract decides whether cursor mode is" \
        "safe against it"
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

run source "${HTTP_BIN}" --gtest_filter='HttpPoll*'
run recovery "${IT_BIN}" --gtest_filter='HttpPollRecovery.*:HttpPollSql.*'
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'HttpPollPage.*=10' \
    --expect 'HttpPollConfig.*=4' \
    --expect 'HttpPollSourceCursor.*=10' \
    --expect 'HttpPollSourceResource.*=3' \
    --expect 'HttpPollSourceResponses.*=1' \
    --expect 'HttpPollSourceBounds.*=3' \
    --expect 'HttpPollSourceRetries.*=3' \
    --expect 'HttpPollSourceCancel.*=2' \
    --expect 'HttpPollSourceErrors.*=1' \
    --expect 'HttpPollCapability.*=2' \
    --expect 'HttpPollRecovery.*=3' \
    --expect 'HttpPollSql.*=2' \
    "${REPORT_DIR}/source.json" "${REPORT_DIR}/recovery.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
