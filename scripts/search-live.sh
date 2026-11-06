#!/usr/bin/env bash
# The Elasticsearch / OpenSearch bulk sink's live gate, for one server at a
# time: its pinned image from docker/integration-services.yml, every sink test
# against it, then a check that nothing was skipped and that every required
# suite ran in full.
#
#   scripts/search-live.sh elasticsearch|elasticsearch9 [build-dir]
#   scripts/search-live.sh opensearch|opensearch3 [build-dir]
#
# Run them one after the other. Each runs in phases, one server at a time, each
# started on a free loopback port and removed, with its volumes, when the phase
# ends:
#   main       the server as configured: clink_http_connector_tests' offline
#              suites (ElasticsearchSink, EsBulkResult, EsDocumentId, against a
#              model of the _bulk endpoint) and the product's live suite
#              (<Product>/SearchSinkLive); then clink_integration_tests'
#              <Product>/SearchRecovery cells when that binary is built
#              (SEARCH_LIVE_RECOVERY=0 leaves them out): a coordinator and two
#              workers polling a feed into the sink, faulted at the sink's
#              points and through the fault proxies, one of them a SQL job
#              submitted by clink_submit_sql, each verified by reading the
#              index;
#   throttled  the same image with a write pool of one thread and a write queue
#              of one, so the server itself rejects writes with 429:
#              <Product>/SearchSinkThrottledLive (SEARCH_LIVE_THROTTLED=0
#              leaves it out);
#   secured    the same image with its own security on (<product>-secured):
#              HTTPS with the server's own CA, every request authenticated, an
#              administrator's password generated for the run and passed only
#              through the environment: <Product>/SearchSinkSecuredLive
#              (SEARCH_LIVE_SECURED=0 leaves it out). The gate then fails if
#              that password appears anywhere in what it keeps;
#   cluster    three nodes of the same image from docker/search-cluster.yml, the
#              sink writing to node 1 while a case kills a node with `docker
#              kill`: <Product>/SearchSinkClusterLive (SEARCH_LIVE_CLUSTER=0
#              leaves it out). The test binary drives Docker for this, so under
#              PIN_RUNNER it needs the Docker socket and a CLI.
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
# was clean, the build's configuration, each phase's server version, image and
# digest), the secured server's CA certificate (public) and, on failure, the
# servers' logs, the secured one's with its password redacted. No credential is
# recorded, and nothing here records the environment.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/search-live-reports/<product>)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#   SEARCH_LIVE_RECOVERY   1 (default) or 0
#   SEARCH_LIVE_THROTTLED  1 (default) or 0
#   SEARCH_LIVE_SECURED    1 (default) or 0
#   SEARCH_LIVE_CLUSTER    1 (default) or 0
#
# What the tests read, all set here: CLINK_ELASTICSEARCH_TEST_ENDPOINT or
# CLINK_OPENSEARCH_TEST_ENDPOINT, the matching *_THROTTLED_ENDPOINT and
# *_SECURED_ENDPOINT, _CA, _USER and _PASSWORD, *_CLUSTER_ENDPOINT and _NODES, and
# CLINK_SEARCH_RECOVERY_LOG_DIR.
set -euo pipefail

# The server to test, as docker/integration-services.yml names its service:
# elasticsearch (8.13.4), elasticsearch9 (9.2.0), opensearch (2.17.1) or
# opensearch3 (3.3.0). The suites are the product's, whatever its version.
PRODUCT="${1:-}"
case "${PRODUCT}" in
    elasticsearch | elasticsearch9)
        SUITE=Elasticsearch
        VAR_PREFIX=CLINK_ELASTICSEARCH
        ;;
    opensearch | opensearch3)
        SUITE=OpenSearch
        VAR_PREFIX=CLINK_OPENSEARCH
        ;;
    *)
        echo "usage: scripts/search-live.sh elasticsearch|elasticsearch9|opensearch|opensearch3 [build-dir]" >&2
        exit 2
        ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${2:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/search-live-reports/${PRODUCT}}"
PROJECT="clink-search-live-${PRODUCT}"
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")
CLUSTER_COMPOSE=(docker compose -p "${PROJECT}-cluster" -f "${ROOT}/docker/search-cluster.yml")
SEARCH_LIVE_RECOVERY="${SEARCH_LIVE_RECOVERY:-1}"
SEARCH_LIVE_THROTTLED="${SEARCH_LIVE_THROTTLED:-1}"
SEARCH_LIVE_SECURED="${SEARCH_LIVE_SECURED:-1}"
SEARCH_LIVE_CLUSTER="${SEARCH_LIVE_CLUSTER:-1}"
SECURED_PASSWORD=""

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_http_connector_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
SQL_BIN="$(find_bin clink_submit_sql)"
if [ -z "${UNIT_BIN}" ]; then
    echo "search-live: clink_http_connector_tests is not built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${SEARCH_LIVE_RECOVERY}" = "1" ] && { [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ] || [ -z "${SQL_BIN}" ]; }; then
    echo "search-live: clink_integration_tests, clink_node and clink_submit_sql must be built under ${BUILD_DIR}" \
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
containers=()
# A container's log, with the secured phase's password redacted. A container
# already removed keeps the log saved when it was stopped.
save_logs() {
    docker inspect "$1" >/dev/null 2>&1 || return 0
    docker logs "$1" 2>&1 | SECRET="${SECURED_PASSWORD}" python3 -c \
        'import os, sys; s = sys.stdin.read(); p = os.environ["SECRET"]; sys.stdout.write(s.replace(p, "[redacted]") if p else s)' \
        >"${REPORT_DIR}/$1.log" || true
}

# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    for c in "${containers[@]}"; do
        if [ "${status}" -ne 0 ]; then
            save_logs "${c}"
        fi
        docker rm -fv "${c}" >/dev/null 2>&1 || true
    done
    if [ "${status}" -ne 0 ]; then
        "${CLUSTER_COMPOSE[@]}" logs --no-color >"${REPORT_DIR}/cluster.log" 2>&1 || true
    fi
    "${CLUSTER_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# start_server <container> [docker compose run options...]: one container of the
# product's service on a free loopback port. Prints its endpoint once it answers;
# fails the gate if it never does. Run in a command substitution, so the caller
# records the container for the cleanup.
start_server() {
    local name="$1" port endpoint
    shift
    port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    docker rm -fv "${name}" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" run -d --name "${name}" -p "127.0.0.1:${port}:9200" "$@" "${PRODUCT}" >/dev/null
    endpoint="http://127.0.0.1:${port}"
    for _ in $(seq 1 180); do
        if curl -fsS "${endpoint}/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null 2>&1; then
            echo "${endpoint}"
            return 0
        fi
        sleep 1
    done
    echo "search-live: ${PRODUCT} (${name}) never answered at ${endpoint}" >&2
    return 1
}

stop_server() {
    if [ "${status}" -ne 0 ]; then
        save_logs "$1"
    fi
    docker rm -fv "$1" >/dev/null 2>&1 || true
}

describe_server() {  # <phase> <container> <endpoint>
    echo "${1} server: $(curl -fsS "${3}/" | python3 -c 'import json, sys; v = json.load(sys.stdin)["version"]; print(v.get("distribution", "elasticsearch"), v["number"], v.get("build_hash", ""))')"
    echo "${1} image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${2}")"
}

run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

{
    echo "product: ${PRODUCT}"
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_HTTP CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "security: off in the main and throttled phases; the secured phase's password is generated for the run, passed only through the environment and not recorded"
    echo "recovery cells: $( [ "${SEARCH_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
    echo "throttled phase: $( [ "${SEARCH_LIVE_THROTTLED}" = 1 ] && echo run || echo left out)"
    echo "secured phase: $( [ "${SEARCH_LIVE_SECURED}" = 1 ] && echo run || echo left out)"
    echo "cluster phase: $( [ "${SEARCH_LIVE_CLUSTER}" = 1 ] && echo run || echo left out)"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

reports=()
expect=()

# ---- main ----------------------------------------------------------------
main="${PROJECT}-server"
containers+=("${main}")
endpoint="$(start_server "${main}")" || { status=1; exit 1; }
describe_server main "${main}" "${endpoint}" >>"${REPORT_DIR}/provenance.txt"
export "${VAR_PREFIX}_TEST_ENDPOINT=${endpoint}"
export CLINK_SEARCH_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

run sink "${UNIT_BIN}" \
    --gtest_filter="ElasticsearchSink.*:EsBulkResult.*:EsDocumentId.*:${SUITE}/SearchSinkLive.*"
reports+=("${REPORT_DIR}/sink.json")
expect+=(
    --expect 'ElasticsearchSink.*=19'
    --expect 'EsBulkResult.*=5'
    --expect 'EsDocumentId.*=5'
    --expect "${SUITE}/SearchSinkLive.*=12"
)
if [ "${SEARCH_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter="${SUITE}/SearchRecovery.*"
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect "${SUITE}/SearchRecovery.*=5")
fi
stop_server "${main}"

# ---- throttled -----------------------------------------------------------
if [ "${SEARCH_LIVE_THROTTLED}" = "1" ]; then
    throttled="${PROJECT}-throttled"
    containers+=("${throttled}")
    endpoint="$(start_server "${throttled}" \
        -e thread_pool.write.size=1 -e thread_pool.write.queue_size=1)" || { status=1; exit 1; }
    describe_server throttled "${throttled}" "${endpoint}" >>"${REPORT_DIR}/provenance.txt"
    echo "throttled settings: thread_pool.write.size=1 thread_pool.write.queue_size=1" \
        >>"${REPORT_DIR}/provenance.txt"
    export "${VAR_PREFIX}_THROTTLED_ENDPOINT=${endpoint}"
    run throttled "${UNIT_BIN}" --gtest_filter="${SUITE}/SearchSinkThrottledLive.*"
    reports+=("${REPORT_DIR}/throttled.json")
    expect+=(--expect "${SUITE}/SearchSinkThrottledLive.*=2")
    stop_server "${throttled}"
fi

# ---- secured -------------------------------------------------------------
if [ "${SEARCH_LIVE_SECURED}" = "1" ]; then
    secured="${PROJECT}-secured"
    containers+=("${secured}")
    SECURED_PASSWORD="$(python3 -c 'import secrets, string; a = string.ascii_letters + string.digits; print("Aa7!" + "".join(secrets.choice(a) for _ in range(24)))')"
    case "${SUITE}" in
        Elasticsearch)
            password_var=ELASTIC_PASSWORD
            admin=elastic
            ca_in_container=/usr/share/elasticsearch/config/certs/http_ca.crt
            ;;
        OpenSearch)
            password_var=OPENSEARCH_INITIAL_ADMIN_PASSWORD
            admin=admin
            ca_in_container=/usr/share/opensearch/config/root-ca.pem
            ;;
    esac
    export "${password_var}=${SECURED_PASSWORD}"
    port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    docker rm -fv "${secured}" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" run -d --name "${secured}" -p "127.0.0.1:${port}:9200" -e "${password_var}" \
        "${PRODUCT}-secured" >/dev/null
    unset "${password_var}"
    endpoint="https://127.0.0.1:${port}"
    ca="${REPORT_DIR}/secured-ca.pem"
    ready=0
    for _ in $(seq 1 240); do
        if docker cp "${secured}:${ca_in_container}" "${ca}" >/dev/null 2>&1 &&
            curl -fsS --cacert "${ca}" -u "${admin}:${SECURED_PASSWORD}" \
                "${endpoint}/_cluster/health?wait_for_status=yellow&timeout=1s" >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    if [ "${ready}" != 1 ]; then
        echo "search-live: the secured ${PRODUCT} never answered at ${endpoint}" >&2
        status=1
        exit 1
    fi
    chmod a+r "${ca}"
    {
        echo "secured server: $(curl -fsS --cacert "${ca}" -u "${admin}:${SECURED_PASSWORD}" "${endpoint}/" | python3 -c 'import json, sys; v = json.load(sys.stdin)["version"]; print(v.get("distribution", "elasticsearch"), v["number"], v.get("build_hash", ""))')"
        echo "secured image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${secured}")"
        echo "secured CA: secured-ca.pem (the server's own)"
    } >>"${REPORT_DIR}/provenance.txt"
    export "${VAR_PREFIX}_SECURED_ENDPOINT=${endpoint}"
    export "${VAR_PREFIX}_SECURED_CA=${ca}"
    export "${VAR_PREFIX}_SECURED_USER=${admin}"
    export "${VAR_PREFIX}_SECURED_PASSWORD=${SECURED_PASSWORD}"
    run secured "${UNIT_BIN}" --gtest_filter="${SUITE}/SearchSinkSecuredLive.*"
    reports+=("${REPORT_DIR}/secured.json")
    expect+=(--expect "${SUITE}/SearchSinkSecuredLive.*=5")
    unset "${VAR_PREFIX}_SECURED_PASSWORD"
    stop_server "${secured}"
    if grep -rqF -- "${SECURED_PASSWORD}" "${REPORT_DIR}"; then
        echo "search-live: the secured phase's password reached the reports" >&2
        status=1
    fi
fi

# ---- cluster -------------------------------------------------------------
if [ "${SEARCH_LIVE_CLUSTER}" = "1" ]; then
    # The version under test, as the services file pins it.
    image="$("${COMPOSE[@]}" config --format json |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["services"][sys.argv[1]]["image"])' "${PRODUCT}")"
    case "${SUITE}" in
        Elasticsearch)
            export SEARCH_ES_IMAGE="${image}"
            nodes=(es01 es02 es03)
            ;;
        OpenSearch)
            export SEARCH_OS_IMAGE="${image}"
            nodes=(os01 os02 os03)
            ;;
    esac
    SEARCH_CLUSTER_PORT="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    export SEARCH_CLUSTER_PORT
    "${CLUSTER_COMPOSE[@]}" up -d "${nodes[@]}" >/dev/null
    endpoint="http://127.0.0.1:${SEARCH_CLUSTER_PORT}"
    ready=0
    for _ in $(seq 1 300); do
        if curl -fsS "${endpoint}/_cluster/health?wait_for_status=green&wait_for_nodes=3&timeout=1s" \
            >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    if [ "${ready}" != 1 ]; then
        echo "search-live: the ${PRODUCT} cluster never had three nodes at ${endpoint}" >&2
        status=1
        exit 1
    fi
    names=()
    for n in "${nodes[@]}"; do
        names+=("$(docker inspect --format '{{.Name}}' "$("${CLUSTER_COMPOSE[@]}" ps -q "${n}")" | sed 's#^/##')")
    done
    {
        echo "cluster server: $(curl -fsS "${endpoint}/" | python3 -c 'import json, sys; v = json.load(sys.stdin)["version"]; print(v.get("distribution", "elasticsearch"), v["number"], v.get("build_hash", ""))')"
        echo "cluster image: ${image} $(docker inspect --format '{{.Image}}' "${names[0]}")"
        echo "cluster nodes: ${names[*]}"
    } >>"${REPORT_DIR}/provenance.txt"
    export "${VAR_PREFIX}_CLUSTER_ENDPOINT=${endpoint}"
    export "${VAR_PREFIX}_CLUSTER_NODES=$(IFS=,; echo "${names[*]}")"
    run cluster "${UNIT_BIN}" --gtest_filter="${SUITE}/SearchSinkClusterLive.*"
    reports+=("${REPORT_DIR}/cluster.json")
    expect+=(--expect "${SUITE}/SearchSinkClusterLive.*=2")
    if [ "${status}" -ne 0 ]; then
        "${CLUSTER_COMPOSE[@]}" logs --no-color >"${REPORT_DIR}/cluster.log" 2>&1 || true
    fi
    "${CLUSTER_COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
