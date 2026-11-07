#!/usr/bin/env bash
# The Cassandra connector's live gate: every Cassandra test against one pinned
# server, Apache Cassandra or ScyllaDB, then a check that nothing was skipped and
# that every required suite ran in full.
#
#   scripts/cassandra-live.sh cassandra|scylladb [build-dir]
#
# It starts one container of the named product, pinned below by version and
# digest, on a free loopback port, waits until it answers CQL, and runs:
#   - clink_cassandra_tests: the offline suites (the record and key rules, the
#     options, the table check, the bounds, write order and the failure handling
#     through a model of the server), the live suites (CassandraLive: the window,
#     repeated writes, deletes across early write-outs, composite keys,
#     Unavailable, a timed-out write the server applied, a lost connection,
#     cancel and close, decimals; the upsert sink's and the upsert contract's
#     cases), and a check that the server is the product and version started;
#   - clink_cassandra_sql_tests: embedded SQL pipelines into connector='cassandra'
#     (CassandraSqlLive);
#   - clink_integration_tests' CassandraRecovery cells when that binary is built
#     (CASSANDRA_LIVE_RECOVERY=0 leaves them out): a coordinator and two workers
#     polling a feed into the sinks, a worker killed at the sinks' fault points,
#     one of them a SQL job submitted by clink_submit_sql, each verified by
#     reading the table directly.
# Run the two products one after the other, never together. The container goes,
# with its volumes, when the gate ends.
#
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, a server that
# never answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR (default <build-dir>/cassandra-live-reports/<product>):
# each run's googletest report and output (the "[cassandra-live]" and
# "[cassandra-recovery]" lines and every assertion), the recovery cells'
# coordinator and worker logs (recovery-logs/<product>-<cell>/), provenance.txt
# (the commit, whether the tree was clean, the build's configuration, the
# product, its version and image) and, on failure, the server's log. The server
# runs without authentication, so nothing here holds a credential.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (second argument or default build/)
#   REPORT_DIR   where the reports go
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#   CASSANDRA_LIVE_RECOVERY  1 (default) or 0
#
# What the tests read, all set here: CLINK_CASSANDRA_TEST_CONTACT_POINTS,
# CLINK_CASSANDRA_TEST_PORT, CLINK_CASSANDRA_TEST_PRODUCT,
# CLINK_CASSANDRA_TEST_VERSION, CLINK_CASSANDRA_RECOVERY_LOG_DIR.
set -euo pipefail

CASSANDRA_VERSION=4.1.12
CASSANDRA_IMAGE="cassandra:${CASSANDRA_VERSION}@sha256:1db627db250f2561e0ceaa2bcb25342010a06d0ae26756ac9e18250d957f5f80"
SCYLLADB_VERSION=6.2.3
SCYLLADB_IMAGE="scylladb/scylla:${SCYLLADB_VERSION}@sha256:a9d904089abe9a4f8b5b893ebb5b5bf8b5a1bd0dc6658921cf05f89d3712289c"

PRODUCT="${1:-}"
case "${PRODUCT}" in
    cassandra)
        VERSION="${CASSANDRA_VERSION}"
        IMAGE="${CASSANDRA_IMAGE}"
        # A small heap: the gate's tables are small, and it shares the machine.
        RUN_ARGS=(-e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256M)
        SERVER_ARGS=()
        ;;
    scylladb)
        VERSION="${SCYLLADB_VERSION}"
        IMAGE="${SCYLLADB_IMAGE}"
        RUN_ARGS=()
        # One shard and 1 GB, without the production checks a container fails.
        SERVER_ARGS=(--smp 1 --memory 1G --overprovisioned 1 --developer-mode 1)
        ;;
    *)
        echo "usage: scripts/cassandra-live.sh cassandra|scylladb [build-dir]" >&2
        exit 2
        ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${2:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/cassandra-live-reports/${PRODUCT}}"
CASSANDRA_LIVE_RECOVERY="${CASSANDRA_LIVE_RECOVERY:-1}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_cassandra_tests)"
SQL_BIN="$(find_bin clink_cassandra_sql_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
if [ -z "${UNIT_BIN}" ] || [ -z "${SQL_BIN}" ]; then
    echo "cassandra-live: clink_cassandra_tests and clink_cassandra_sql_tests must be built" \
        "under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${CASSANDRA_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "cassandra-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or CASSANDRA_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container="clink-cassandra-live-${PRODUCT}"
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/${PRODUCT}-server.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
docker rm -fv "${container}" >/dev/null 2>&1 || true
# The ${a[@]+...} form: macOS's bash 3.2 calls an empty array unbound under set -u.
docker run -d --name "${container}" -p "127.0.0.1:${port}:9042" \
    ${RUN_ARGS[@]+"${RUN_ARGS[@]}"} "${IMAGE}" ${SERVER_ARGS[@]+"${SERVER_ARGS[@]}"} >/dev/null

cql() {
    docker exec "${container}" cqlsh -e "$1"
}
ready=0
for _ in $(seq 1 120); do
    if cql "SELECT release_version FROM system.local" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 2
done
if [ "${ready}" != 1 ]; then
    echo "cassandra-live: ${PRODUCT} never answered CQL" >&2
    status=1
    exit 1
fi

export CLINK_CASSANDRA_TEST_CONTACT_POINTS=127.0.0.1
export CLINK_CASSANDRA_TEST_PORT="${port}"
export CLINK_CASSANDRA_TEST_PRODUCT="${PRODUCT}"
export CLINK_CASSANDRA_TEST_VERSION="${VERSION}"
export CLINK_CASSANDRA_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_CASSANDRA CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "product: ${PRODUCT}"
    if [ "${PRODUCT}" = scylladb ]; then
        echo "server: $(cql "SELECT version FROM system.versions WHERE key = 'local'" | sed -n '4p' | tr -d ' ')"
    else
        echo "server: $(cql "SELECT release_version FROM system.local" | sed -n '4p' | tr -d ' ')"
    fi
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "recovery cells: $( [ "${CASSANDRA_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
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

run cassandra "${UNIT_BIN}"
run sql "${SQL_BIN}"
reports=("${REPORT_DIR}/cassandra.json" "${REPORT_DIR}/sql.json")
expect=(
    --expect 'CassandraFactoryRegistration.*=2'
    --expect 'CassandraSink.*=2'
    --expect 'CassandraRules.*=15'
    --expect 'CassandraSinkOptions.*=3'
    --expect 'CassandraSinkOpen.*=5'
    --expect 'CassandraSinkBounds.*=5'
    --expect 'CassandraSinkOrder.*=6'
    --expect 'CassandraSinkFailure.*=6'
    --expect 'CassandraLive.*=13'
    --expect 'CassandraUpsertSink.*=1'
    --expect 'CassandraUpsertSinkLive.*=4'
    --expect 'CassandraUpsert/UpsertContractSuite/0.*=5'
    --expect 'CassandraSqlLive.*=3'
)
if [ "${CASSANDRA_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter='CassandraRecovery.*'
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect 'CassandraRecovery.*=4')
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
