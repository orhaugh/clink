#!/usr/bin/env bash
# The Neo4j sink's live gate: every Neo4j test against the pinned server of
# docker/integration-services.yml, then a check that nothing was skipped and
# that every required suite ran.
#
#   scripts/neo4j-live.sh [build-dir]
#
# It brings up, and always tears down with their volumes, two services of the
# same pinned build: neo4j (the Query API over HTTP on host port 17474) and
# neo4j-tls (over HTTPS only, on 17443, with a CA and a certificate for
# localhost that scripts/make-neo4j-test-tls.sh generates per run). Then it runs:
#   - clink_neo4j_tests: the unit suites, the live suite (Neo4jLive.*), whose
#     outage case stops and starts the neo4j container, and the TLS suite
#     (Neo4jLiveTls.*);
#   - clink_neo4j_sql_tests: the SQL-level suites, including an embedded SQL
#     pipeline into the server (Neo4jSqlLive.*);
#   - clink_integration_tests' Neo4jRecovery.* cells when that binary is built
#     (NEO4J_LIVE_RECOVERY=0 leaves them out): a coordinator and two workers
#     running a SQL job from Kafka into the server, a worker killed at the
#     sink's fault points.
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports and, on failure, the server logs go (default
#                <build-dir>/neo4j-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run ... -e <each variable below>`,
#                the Docker socket and a Docker CLI mounted, so the outage case
#                can stop the server)
#   NEO4J_LIVE_TMPDIR  where the TLS material is generated (default TMPDIR); under
#                PIN_RUNNER it must be a directory the container mounts at the
#                same path
#
# What the tests read, all set here: CLINK_NEO4J_TEST_ENDPOINT,
# CLINK_NEO4J_TEST_PASSWORD, CLINK_NEO4J_TEST_CONTAINER,
# CLINK_NEO4J_TEST_TLS_ENDPOINT, CLINK_NEO4J_TEST_TLS_CA,
# CLINK_NEO4J_TEST_TLS_WRONG_CA.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/neo4j-live-reports}"
PROJECT=clink-neo4j-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")
NEO4J_LIVE_RECOVERY="${NEO4J_LIVE_RECOVERY:-1}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_neo4j_tests)"
SQL_BIN="$(find_bin clink_neo4j_sql_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
for b in "${UNIT_BIN}" "${SQL_BIN}"; do
    if [ -z "${b}" ]; then
        echo "neo4j-live: clink_neo4j_tests and clink_neo4j_sql_tests must be built under ${BUILD_DIR}" >&2
        exit 1
    fi
done
if [ "${NEO4J_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "neo4j-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or NEO4J_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.json "${REPORT_DIR}"/*.log
# The toolchain image's user is not the runner's; let either write here.
chmod a+rwx "${REPORT_DIR}"
tls_dir="$(mktemp -d "${NEO4J_LIVE_TMPDIR:-${TMPDIR:-/tmp}}/clink-neo4j-tls.XXXXXX")"
"${ROOT}/scripts/make-neo4j-test-tls.sh" "${tls_dir}"
export CLINK_NEO4J_TLS_DIR="${tls_dir}"

status=0
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        "${COMPOSE[@]}" logs --no-color neo4j >"${REPORT_DIR}/neo4j-server.log" 2>&1 || true
        "${COMPOSE[@]}" logs --no-color neo4j-tls >"${REPORT_DIR}/neo4j-tls-server.log" 2>&1 || true
    fi
    "${COMPOSE[@]}" rm -sfv neo4j neo4j-tls >/dev/null 2>&1 || true
    rm -rf "${tls_dir}"
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d --wait neo4j neo4j-tls
container="$("${COMPOSE[@]}" ps -q neo4j)"

export CLINK_NEO4J_TEST_ENDPOINT=http://localhost:17474
export CLINK_NEO4J_TEST_PASSWORD=clink-live-test-pw
export CLINK_NEO4J_TEST_CONTAINER="${container}"
export CLINK_NEO4J_TEST_TLS_ENDPOINT=https://localhost:17443
export CLINK_NEO4J_TEST_TLS_CA="${tls_dir}/ca.crt"
export CLINK_NEO4J_TEST_TLS_WRONG_CA="${tls_dir}/wrong-ca.crt"

# The Query API answers once the server is up; the healthcheck only saw the
# discovery page.
for _ in $(seq 1 60); do
    if curl -fsS -u "neo4j:${CLINK_NEO4J_TEST_PASSWORD}" -H 'Content-Type: application/json' \
        -d '{"statement":"RETURN 1"}' "${CLINK_NEO4J_TEST_ENDPOINT}/db/neo4j/query/v2" >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_brief=1 --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

run neo4j "${UNIT_BIN}"
run neo4j-sql "${SQL_BIN}"
reports=("${REPORT_DIR}/neo4j.json" "${REPORT_DIR}/neo4j-sql.json")
expect=(
    --expect 'Neo4jFactory.*=4'
    --expect 'Neo4jOptions.*=12'
    --expect 'Neo4jTypes.*=8'
    --expect 'Neo4jReply.*=7'
    --expect 'Neo4jSink.*=16'
    --expect 'Neo4jLive.*=15'
    --expect 'Neo4jLiveTls.*=3'
    --expect 'Neo4jSql.*=4'
    --expect 'Neo4jSqlLive.*=1'
)
if [ "${NEO4J_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter='Neo4jRecovery.*'
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect 'Neo4jRecovery.*=3')
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
exit "${status}"
