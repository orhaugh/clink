#!/usr/bin/env bash
# The Splunk HEC sink's simulated gate: every HEC sink test against the
# protocol stub (impls/http_connector/tests/hec_stub.hpp), then a check that
# nothing was skipped and that every required suite ran in full. No Splunk
# server takes part, and nothing here is evidence about one; the live gate,
# against a server someone supplies, is scripts/splunk-live.sh.
#
#   scripts/splunk-hec-gate.sh [build-dir]
#
# It runs:
#   - clink_http_connector_tests' HEC suites: the event rules (one JSON object
#     per record, written exactly, no added envelope members or events, the
#     bounds, the options), the reading of event and acknowledgement replies
#     (every documented code, contradictory and foreign replies), the sink
#     against the stub (batches and metadata, refused records under each
#     policy, a refused event under each policy, a busy server's partial
#     acceptance, a lost reply, spent retries, refused tokens and indexes,
#     replies that are not HEC's, cancellation, indexer acknowledgement
#     delayed, never arriving, unreadable and bounded), and the factory;
#   - clink_integration_tests' SplunkHecRecovery cells: a coordinator and two
#     workers polling a feed into the sink, a worker killed after a request the
#     stub accepted and no checkpoint covered, with ack='none', with
#     ack='indexer' and as a SQL job, each verified from the stub's own
#     acceptance log.
#
# googletest counts a skipped case as a pass, so scripts/check-gtest-reports.py
# fails the run on any skip and on a suite that ran fewer cases than it should.
# A missing binary and a failed case fail it too.
#
# What it keeps in REPORT_DIR (default <build-dir>/splunk-hec-reports/simulated):
# each run's googletest report and output (the "[splunk-recovery]" lines and
# every assertion), the cells' coordinator and worker logs and the stub's
# acceptance log each verdict rests on (recovery-logs/<cell>/), and
# provenance.txt (the commit, whether the tree was clean, the build's
# configuration, the stub it ran against, and that no live server took part).
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#
# What the tests read, all set here: CLINK_SPLUNK_RECOVERY_LOG_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/splunk-hec-reports/simulated}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
for pair in "clink_http_connector_tests:${UNIT_BIN}" "clink_integration_tests:${IT_BIN}"; do
    if [ -z "${pair#*:}" ]; then
        echo "splunk-hec-gate: ${pair%%:*} is not built under ${BUILD_DIR}" >&2
        exit 1
    fi
done

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

export CLINK_SPLUNK_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "server: the HEC protocol stub, impls/http_connector/tests/hec_stub.hpp at $(git -C "${ROOT}" log -1 --format=%h -- impls/http_connector/tests/hec_stub.hpp)"
    echo "live server: none (simulated evidence only; scripts/splunk-live.sh is the live gate)"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

status=0
run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

run hec "${UNIT_BIN}" --gtest_filter='Hec*'
run recovery "${IT_BIN}" --gtest_filter='SplunkHecRecovery.*'

python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'HecRender.*=11' \
    --expect 'HecReply.*=6' \
    --expect 'HecSink.*=22' \
    --expect 'HecFactory.*=1' \
    --expect 'SplunkHecRecovery.*=3' \
    "${REPORT_DIR}/hec.json" "${REPORT_DIR}/recovery.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
