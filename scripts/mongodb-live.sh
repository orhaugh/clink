#!/usr/bin/env bash
# The MongoDB connector's live gate: every MongoDB test against a single-member
# replica set of docker/integration-services.yml's mongodb service, then a check
# that nothing was skipped and that every required suite ran in full.
#
#   scripts/mongodb-live.sh [build-dir]
#
# It starts one container of the service on a free loopback port, published at
# a fixed host port so a restart of the container keeps it, initiates the
# replica set (change streams need one) and waits for it to elect itself
# primary. The service runs with enableTestCommands, which the sink's failure
# cases need for the server's failCommand fail point. Then it runs:
#   - clink_mongodb_tests: the offline suites, the connector's round trips
#     (MongoLive), the source's recovery contract (MongoSourceRecoveryLive) and
#     the sink's failure cases (MongoSinkLive);
#   - clink_mongodb_sql_tests: the SQL plans and refusals (MongoSqlPlan) and
#     embedded SQL pipelines through the replica set (MongoSqlLive);
#   - clink_integration_tests' MongoRecovery cells when that binary is built
#     (MONGODB_LIVE_RECOVERY=0 leaves them out): a coordinator and two workers
#     running a change-stream job into MongoDB, one of them a SQL job submitted
#     by clink_submit_sql, with a worker killed at the connector's fault points
#     and the server stopped under a running job, each cell verified by reading
#     MongoDB directly.
# The container goes, with its volumes, when the gate ends.
#
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, a server that
# never becomes primary and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each run's googletest report and output (the
# recovery cells' verdict lines and every assertion), the recovery cells'
# coordinator and worker logs (recovery-logs/<cell>/), provenance.txt (the
# commit, whether the tree was clean, the build's configuration, the server's
# version and image) and, on failure, the server's log. The test URI carries no
# credentials, and nothing here records the environment.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/mongodb-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`, the Docker socket and a Docker CLI mounted, so
#                the outage cell can stop the server)
#   MONGODB_LIVE_RECOVERY  1 (default) or 0
#
# What the tests read, all set here: CLINK_MONGODB_TEST_URI,
# CLINK_MONGODB_TEST_CONTAINER, CLINK_MONGODB_RECOVERY_LOG_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/mongodb-live-reports}"
PROJECT=clink-mongodb-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")
MONGODB_LIVE_RECOVERY="${MONGODB_LIVE_RECOVERY:-1}"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_mongodb_tests)"
SQL_BIN="$(find_bin clink_mongodb_sql_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
if [ -z "${UNIT_BIN}" ] || [ -z "${SQL_BIN}" ]; then
    echo "mongodb-live: clink_mongodb_tests and clink_mongodb_sql_tests must be built under" \
        "${BUILD_DIR}" >&2
    exit 1
fi
if [ "${MONGODB_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "mongodb-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or MONGODB_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container="${PROJECT}-mongodb"
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/mongodb-server.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# A fixed host port, chosen free now: a stopped and started container keeps it,
# which the outage cell relies on.
port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
docker rm -fv "${container}" >/dev/null 2>&1 || true
"${COMPOSE[@]}" run -d --name "${container}" -p "127.0.0.1:${port}:27017" mongodb >/dev/null

mongosh_eval() {
    docker exec "${container}" mongosh --quiet --eval "$1"
}
ready=0
for _ in $(seq 1 90); do
    if mongosh_eval "db.adminCommand('ping').ok" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "mongodb-live: the server never answered" >&2
    status=1
    exit 1
fi
mongosh_eval "try { rs.status().ok } catch (e) { rs.initiate({_id: 'rs0', members: [{_id: 0, host: 'localhost:27017'}]}).ok }" >/dev/null
primary=0
for _ in $(seq 1 90); do
    if [ "$(mongosh_eval 'db.hello().isWritablePrimary' 2>/dev/null)" = "true" ]; then
        primary=1
        break
    fi
    sleep 1
done
if [ "${primary}" != 1 ]; then
    echo "mongodb-live: the replica set never elected a primary" >&2
    status=1
    exit 1
fi

export CLINK_MONGODB_TEST_URI="mongodb://127.0.0.1:${port}/?directConnection=true"
export CLINK_MONGODB_TEST_CONTAINER="${container}"
export CLINK_MONGODB_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_MONGODB CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION mongocxx_DIR; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "server: $(docker exec "${container}" mongod --version | head -1)"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "replica set: rs0, one member, enableTestCommands"
    echo "recovery cells: $( [ "${MONGODB_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
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

run mongodb "${UNIT_BIN}"
run sql "${SQL_BIN}"
reports=("${REPORT_DIR}/mongodb.json" "${REPORT_DIR}/sql.json")
expect=(
    --expect 'MongoFactoryRegistration.*=3'
    --expect 'MongoCapabilityRecords.*=1'
    --expect 'MongoEvent.*=10'
    --expect 'MongoChangePass.*=4'
    --expect 'MongoSourceRestore.*=1'
    --expect 'MongoSinkRules.*=6'
    --expect 'MongoSqlRow.*=14'
    --expect 'MongoSqlSink.*=6'
    --expect 'MongoLive.*=6'
    --expect 'MongoSourceRecoveryLive.*=10'
    --expect 'MongoSinkLive.*=9'
    --expect 'MongoSqlPlan.*=6'
    --expect 'MongoSqlLive.*=3'
)
if [ "${MONGODB_LIVE_RECOVERY}" = "1" ]; then
    run recovery "${IT_BIN}" --gtest_filter='MongoRecovery.*'
    reports+=("${REPORT_DIR}/recovery.json")
    expect+=(--expect 'MongoRecovery.*=7')
fi

python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
