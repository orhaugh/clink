#!/usr/bin/env bash
# The Elasticsearch / OpenSearch bulk sink's live gate, for one product at a
# time: its pinned server from docker/integration-services.yml, every sink test
# against it, then a check that nothing was skipped and that every required
# suite ran in full.
#
#   scripts/search-live.sh elasticsearch [build-dir]
#   scripts/search-live.sh opensearch [build-dir]
#
# Run the two one after the other: each starts its own server on a free loopback
# port and removes it, with its volumes, when it ends. It runs:
#   - clink_http_connector_tests: the sink's offline suites (ElasticsearchSink,
#     EsBulkResult, EsDocumentId, against a model of the _bulk endpoint) and the
#     product's live suite (<Product>/SearchSinkLive): contents read back, ids
#     repeated within and across batches, item failures, unconfirmed replies,
#     a reply lost after the server applied the request, index-level refusals,
#     throttling, cancellation;
#   - clink_integration_tests' <Product>/SearchRecovery cells when that binary
#     is built (SEARCH_LIVE_RECOVERY=0 leaves them out): a coordinator and two
#     workers polling a feed into the sink, with a worker killed at the sink's
#     fault points, replies that confirm nothing, and a reply lost after the
#     server applied a flush, each cell verified by reading the index.
# Which faults come from a proxy and which from the server itself is written in
# each case and in docs/connectors/http.md.
#
# A live case skips when its server is not named and googletest counts that as a
# pass, so scripts/check-gtest-reports.py fails the run on any skip and on a suite
# that ran fewer cases than it should. A missing binary, a server that never
# answers, a server that is not the product named, and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each run's googletest report and output (every
# case's "[search-live]" or "[search-recovery]" verdict line and every
# assertion), the recovery cells' coordinator and worker logs
# (recovery-logs/<product>-<cell>/), provenance.txt (the commit, whether the tree
# was clean, the build's configuration, the server's version, image and digest)
# and, on failure, the server's log. The server runs without authentication, so
# no credential is set or recorded, and nothing here records the environment.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/search-live-reports/<product>)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#   SEARCH_LIVE_RECOVERY  1 (default) or 0
#
# What the tests read, all set here: CLINK_ELASTICSEARCH_TEST_ENDPOINT or
# CLINK_OPENSEARCH_TEST_ENDPOINT, and CLINK_SEARCH_RECOVERY_LOG_DIR.
set -euo pipefail

PRODUCT="${1:-}"
case "${PRODUCT}" in
    elasticsearch)
        SUITE=Elasticsearch
        ENDPOINT_VAR=CLINK_ELASTICSEARCH_TEST_ENDPOINT
        ;;
    opensearch)
        SUITE=OpenSearch
        ENDPOINT_VAR=CLINK_OPENSEARCH_TEST_ENDPOINT
        ;;
    *)
        echo "usage: scripts/search-live.sh elasticsearch|opensearch [build-dir]" >&2
        exit 2
        ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${2:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/search-live-reports/${PRODUCT}}"
PROJECT="clink-search-live-${PRODUCT}"
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")
SEARCH_LIVE_RECOVERY="${SEARCH_LIVE_RECOVERY:-1}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
if [ -z "${UNIT_BIN}" ]; then
    echo "search-live: clink_http_connector_tests is not built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${SEARCH_LIVE_RECOVERY}" = "1" ] && { [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ]; }; then
    echo "search-live: clink_integration_tests and clink_node must be built under ${BUILD_DIR}" \
        "(or SEARCH_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container="${PROJECT}-server"
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/server.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
docker rm -fv "${container}" >/dev/null 2>&1 || true
"${COMPOSE[@]}" run -d --name "${container}" -p "127.0.0.1:${port}:9200" "${PRODUCT}" >/dev/null
endpoint="http://127.0.0.1:${port}"

ready=0
for _ in $(seq 1 180); do
    if curl -fsS "${endpoint}/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "search-live: ${PRODUCT} never answered at ${endpoint}" >&2
    status=1
    exit 1
fi

export "${ENDPOINT_VAR}=${endpoint}"
export CLINK_SEARCH_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

{
    echo "product: ${PRODUCT}"
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "server: $(curl -fsS "${endpoint}/" | python3 -c 'import json, sys; v = json.load(sys.stdin)["version"]; print(v.get("distribution", "elasticsearch"), v["number"], v.get("build_hash", ""))')"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "security: off (no credentials set or recorded)"
    echo "recovery cells: $( [ "${SEARCH_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
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

run sink "${UNIT_BIN}" \
    --gtest_filter="ElasticsearchSink.*:EsBulkResult.*:EsDocumentId.*:${SUITE}/SearchSinkLive.*"
reports=("${REPORT_DIR}/sink.json")
expect=(
    --expect 'ElasticsearchSink.*=18'
    --expect 'EsBulkResult.*=5'
    --expect 'EsDocumentId.*=5'
    --expect "${SUITE}/SearchSinkLive.*=11"
)
if [ "${SEARCH_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter="${SUITE}/SearchRecovery.*"
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect "${SUITE}/SearchRecovery.*=4")
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
