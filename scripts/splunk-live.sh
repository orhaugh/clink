#!/usr/bin/env bash
# The Splunk HEC sink's live gate, against a Splunk server someone supplies:
# the SplunkHecLive suite writes through the sink and finds every event again
# with a search of its own through Splunk's REST API. It starts no server and
# acquires no licence; without every variable below it refuses to run and says
# that live verification did not happen, so an absent server is never a pass.
#
#   scripts/splunk-live.sh [build-dir]
#
# Required:
#   CLINK_SPLUNK_HEC_TEST_ENDPOINT      HEC base URL, e.g. https://splunk:8088
#   CLINK_SPLUNK_HEC_TEST_TOKEN         a token without indexer acknowledgement
#   CLINK_SPLUNK_HEC_TEST_ACK_TOKEN     a token with indexer acknowledgement
#   CLINK_SPLUNK_TEST_INDEX             an index both tokens may write to
#   CLINK_SPLUNK_TEST_SEARCH_ENDPOINT   management API base, e.g. https://splunk:8089
#   CLINK_SPLUNK_TEST_SEARCH_USER       a user that may search that index
#   CLINK_SPLUNK_TEST_SEARCH_PASSWORD
# Optional:
#   CLINK_SPLUNK_TEST_VERIFY_TLS        "false" for a self-signed certificate
#
# It fails on a missing binary, a failed or skipped case, or a short suite
# (scripts/check-gtest-reports.py). What it keeps in REPORT_DIR (default
# <build-dir>/splunk-hec-reports/live): the run's googletest report and output
# (the "[splunk-live]" lines and every assertion) and provenance.txt (the
# commit, whether the tree was clean, the endpoints' hosts, the server's
# version as it reports it). It never writes a token or the password.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/splunk-hec-reports/live}"

missing=()
for v in CLINK_SPLUNK_HEC_TEST_ENDPOINT CLINK_SPLUNK_HEC_TEST_TOKEN \
    CLINK_SPLUNK_HEC_TEST_ACK_TOKEN CLINK_SPLUNK_TEST_INDEX \
    CLINK_SPLUNK_TEST_SEARCH_ENDPOINT CLINK_SPLUNK_TEST_SEARCH_USER \
    CLINK_SPLUNK_TEST_SEARCH_PASSWORD; do
    if [ -z "${!v:-}" ]; then
        missing+=("${v}")
    fi
done
if [ "${#missing[@]}" -gt 0 ]; then
    echo "splunk-live: live verification NOT run: set ${missing[*]}" >&2
    exit 2
fi

UNIT_BIN="$(find "${BUILD_DIR}" -name clink_http_connector_tests -type f -perm -u+x 2>/dev/null | head -1)"
if [ -z "${UNIT_BIN}" ]; then
    echo "splunk-live: clink_http_connector_tests is not built under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/provenance.txt"

host_of() {  # scheme://[user@]host[:port]/... -> host[:port]
    local rest="${1#*://}"
    rest="${rest%%/*}"
    echo "${rest##*@}"
}
config_quote() {  # a value for curl's config file: backslash and quote escaped
    local v="${1//\\/\\\\}"
    echo "${v//\"/\\\"}"
}
insecure=()
if [ "${CLINK_SPLUNK_TEST_VERIFY_TLS:-true}" = "false" ]; then
    insecure=(-k)
fi
# The credentials go to curl on stdin, not its command line, which other users
# of the machine can read.
version="$(printf 'user = "%s"\n' \
    "$(config_quote "${CLINK_SPLUNK_TEST_SEARCH_USER}:${CLINK_SPLUNK_TEST_SEARCH_PASSWORD}")" |
    curl -sf ${insecure[@]+"${insecure[@]}"} --config - \
        "${CLINK_SPLUNK_TEST_SEARCH_ENDPOINT}/services/server/info?output_mode=json" |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["entry"][0]["content"]["version"])' \
        2>/dev/null || echo "(not reported)")"

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    echo "hec endpoint: $(host_of "${CLINK_SPLUNK_HEC_TEST_ENDPOINT}")"
    echo "search endpoint: $(host_of "${CLINK_SPLUNK_TEST_SEARCH_ENDPOINT}")"
    echo "index: ${CLINK_SPLUNK_TEST_INDEX}"
    echo "splunk version: ${version}"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

status=0
echo "==> live"
"${UNIT_BIN}" --gtest_filter='SplunkHecLive.*' --gtest_output="json:${REPORT_DIR}/live.json" \
    2>&1 | tee "${REPORT_DIR}/live.log" || status=1
python3 "${ROOT}/scripts/check-gtest-reports.py" --expect 'SplunkHecLive.*=4' \
    "${REPORT_DIR}/live.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
