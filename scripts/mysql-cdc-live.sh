#!/usr/bin/env bash
# The MySQL CDC source's live gate: every MySQL test and the change stream's
# recovery cells against each pinned MySQL server in turn, then a check that
# nothing was skipped and that every required suite ran in full.
#
#   scripts/mysql-cdc-live.sh [build-dir]
#
# For each server below, one after another, it starts a container on a free
# loopback port (binlog_format=ROW, binlog_row_image=FULL, a server id, and a
# root password made for the run), waits until it answers, and runs:
#   - clink_mysql_tests: the offline suites (the change stream's rules and
#     event processing, the row decoder, the snapshot, the sinks) and every live
#     suite: the change stream's round trips (MysqlCdcLive) and its contract
#     (MysqlCdcContractLive: decode failures and their policies, schema changes
#     under both metadata modes, restores from rotated, purged, corrupt and
#     foreign coordinates, large transactions, the snapshot's locks and
#     concurrent writes), and the sinks' and polling source's live cases;
#   - clink_integration_tests' MysqlCdcRecovery cells when that binary is built
#     (MYSQL_CDC_LIVE_RECOVERY=0 leaves them out): a coordinator and two workers
#     reading the binlog into the file two-phase-commit sink, one of them a SQL
#     job submitted by clink_submit_sql, with a worker killed inside the initial
#     snapshot and inside a large transaction, each verified through the mysql
#     client in the server's container.
# The container goes, with its volumes, before the next server starts.
#
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, a server that
# never answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR/<server>/: each run's googletest report and
# output (the recovery cells' "[mysql-cdc-recovery]" lines and every
# assertion), the recovery cells' coordinator and worker logs
# (recovery-logs/<cell>/), and, on failure, the server's log. REPORT_DIR keeps
# provenance.txt: the commit, whether the tree was clean, the build's
# configuration, the client library, and for each server its version, image,
# digest and binlog settings. The password is made for the run and appears in
# none of them; nothing here records the environment.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/mysql-cdc-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`, the Docker socket and a Docker CLI mounted, so
#                the recovery cells can reach the server's mysql client)
#   MYSQL_CDC_LIVE_RECOVERY  1 (default) or 0
#   MYSQL_CDC_LIVE_SERVERS   the servers to run, of "8.0 8.4" (default both)
#
# What the tests read, all set here: CLINK_MYSQL_TEST_DSN,
# CLINK_MYSQL_TEST_CONTAINER, CLINK_MYSQL_RECOVERY_LOG_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/mysql-cdc-live-reports}"
MYSQL_CDC_LIVE_RECOVERY="${MYSQL_CDC_LIVE_RECOVERY:-1}"
MYSQL_CDC_LIVE_SERVERS="${MYSQL_CDC_LIVE_SERVERS:-8.0 8.4}"

# The servers the change stream supports, pinned by digest (the multi-arch index).
image_of() {
    case "$1" in
        8.0) echo "mysql:8.0.46@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b" ;;
        8.4) echo "mysql:8.4.13@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902" ;;
        *) return 1 ;;
    esac
}

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_mysql_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
if [ -z "${UNIT_BIN}" ]; then
    echo "mysql-cdc-live: clink_mysql_tests must be built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${MYSQL_CDC_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "mysql-cdc-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or MYSQL_CDC_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi
for server in ${MYSQL_CDC_LIVE_SERVERS}; do
    if ! image_of "${server}" >/dev/null; then
        echo "mysql-cdc-live: no pinned server '${server}' (choose from 8.0 8.4)" >&2
        exit 1
    fi
done

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/8.* "${REPORT_DIR:?}/provenance.txt"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
failed=0  # the current server's
container=""
# shellcheck disable=SC2329 # run by the EXIT trap and between servers
remove_server() {
    if [ -n "${container}" ]; then
        docker rm -fv "${container}" >/dev/null 2>&1 || true
        container=""
    fi
}
trap remove_server EXIT

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_MYSQL CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    echo "client library: mariadb-connector-c $(${PIN_RUNNER:-} sh -c 'mariadb_config --cc_version 2>/dev/null || /opt/homebrew/opt/mariadb-connector-c/bin/mariadb_config --cc_version 2>/dev/null || pkg-config --modversion libmariadb 2>/dev/null || echo unknown')"
    echo "recovery cells: $( [ "${MYSQL_CDC_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

run() {  # <dir> <report name> <binary> [gtest args...]
    local dir="$1" name="$2" bin="$3"
    shift 3
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_output="json:${dir}/${name}.json" "$@" \
        2>&1 | tee "${dir}/${name}.log" || failed=1
}

for server in ${MYSQL_CDC_LIVE_SERVERS}; do
    image="$(image_of "${server}")"
    dir="${REPORT_DIR}/${server}"
    mkdir -p "${dir}/recovery-logs"
    chmod -R a+rwx "${dir}"
    echo "==> MySQL ${server} (${image})"
    failed=0

    password="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
    port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    container="clink-mysql-cdc-live-${server/./}-$$"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    docker run -d --name "${container}" -p "127.0.0.1:${port}:3306" \
        -e MYSQL_ROOT_PASSWORD="${password}" -e MYSQL_DATABASE=test \
        "${image}" --server-id=1 --log-bin=mysql-bin --binlog-format=ROW \
        --binlog-row-image=FULL >/dev/null

    ready=0
    for _ in $(seq 1 120); do
        # TCP, so the entrypoint's temporary server (no networking) does not count.
        if docker exec -e MYSQL_PWD="${password}" "${container}" \
            mysqladmin -uroot -h127.0.0.1 ping >/dev/null 2>&1; then
            ready=1
            break
        fi
        sleep 1
    done
    if [ "${ready}" != 1 ]; then
        echo "mysql-cdc-live: MySQL ${server} never answered" >&2
        docker logs "${container}" >"${dir}/mysql-server.log" 2>&1 || true
        echo "  server ${server}: never answered" >>"${REPORT_DIR}/provenance.txt"
        status=1
        remove_server
        continue
    fi

    export CLINK_MYSQL_TEST_DSN="host=127.0.0.1 port=${port} user=root password=${password} database=test"
    export CLINK_MYSQL_TEST_CONTAINER="${container}"
    export CLINK_MYSQL_RECOVERY_LOG_DIR="${dir}/recovery-logs"

    {
        echo "server ${server}:"
        echo "  image: ${image}"
        echo "  image id: $(docker inspect --format '{{.Image}}' "${container}")"
        echo "  version and binlog: $(docker exec -e MYSQL_PWD="${password}" "${container}" \
            mysql -uroot -N -B -e 'SELECT VERSION(), @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@binlog_checksum, @@gtid_mode' | tr '\t' ' ')"
    } >>"${REPORT_DIR}/provenance.txt"

    run "${dir}" mysql "${UNIT_BIN}"
    reports=("${dir}/mysql.json")
    expect=(
        --expect 'MysqlFactoryRegistration.*=6'
        --expect 'MysqlCdcCheckpointRow.*=6'
        --expect 'MysqlCdcDefinitions.*=2'
        --expect 'MysqlCdcEvents.*=1'
        --expect 'MysqlCdcStatements.*=8'
        --expect 'MysqlCdcBoundaries.*=1'
        --expect 'MysqlCdcMetadata.*=2'
        --expect 'MysqlCdcProcessor.*=18'
        --expect 'MysqlRowDecode.*=6'
        --expect 'MysqlCdcLive.*=6'
        --expect 'MysqlCdcContractLive.*=26'
    )
    if [ "${MYSQL_CDC_LIVE_RECOVERY}" = "1" ]; then
        run "${dir}" recovery "${IT_BIN}" --gtest_filter='MysqlCdcRecovery.*'
        reports+=("${dir}/recovery.json")
        expect+=(--expect 'MysqlCdcRecovery.*=3')
    fi
    python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || failed=1
    if [ "${failed}" != 0 ]; then
        docker logs "${container}" >"${dir}/mysql-server.log" 2>&1 || true
        status=1
    fi
    echo "  result: $( [ "${failed}" = 0 ] && echo pass || echo fail)" >>"${REPORT_DIR}/provenance.txt"
    remove_server
done

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
