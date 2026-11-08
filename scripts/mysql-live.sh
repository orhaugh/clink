#!/usr/bin/env bash
# The MySQL connector's live gate: two phases, the change stream and the sinks,
# against each pinned MySQL server in turn, then a check that nothing was
# skipped and that every required suite ran in full.
#
#   scripts/mysql-live.sh [build-dir]
#
# For each server below, one after another, it starts a container on a free
# loopback port (binlog_format=ROW, binlog_row_image=FULL, a server id, and a
# root password made for the run), waits until it answers, and runs the phases
# MYSQL_LIVE_PHASES names, one after the other:
#
#   cdc: clink_mysql_tests' registration, polling-source, row-decoding,
#     snapshot and change-stream suites, offline and live: the change stream's
#     round trips (MysqlCdcLive) and its contract (MysqlCdcContractLive: decode
#     failures and their policies, schema changes under both metadata modes,
#     restores from rotated, purged, corrupt and foreign coordinates, large
#     transactions, the snapshot's locks and concurrent writes); then
#     clink_integration_tests' MysqlCdcRecovery cells: a coordinator and two
#     workers reading the binlog into the file two-phase-commit sink, one of
#     them a SQL job submitted by clink_submit_sql, with a worker killed inside
#     the initial snapshot and inside a large transaction.
#   sink: clink_mysql_tests' sink suites, offline and live: values, statements
#     and the table checks (the record rules the SQL sinks share are tested in
#     clink_core_tests); the failure state against a scripted server (MysqlSinkFailures);
#     the contract against the server (MysqlSinkContractLive: keys the table
#     treats as one, the tables the sinks refuse, exact values, refused values,
#     the session's mode, transactions, packet limits, lock waits, a cancel, a
#     lost COMMIT reply, the factories); and the round trips between the sinks
#     and the polling source (MysqlLive); then the MysqlSinkRecovery cells: a
#     coordinator and two workers polling a feed the test serves into each
#     sink, one of them a SQL job at parallelism 2, with a worker killed before
#     a flush's COMMIT and after it, before its checkpoint completes.
#
# Every recovery cell is verified through the mysql client in the server's
# container. The container goes, with its volumes, before the next server
# starts.
#
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. When both phases run, the cases
# they ran between them must be every case clink_mysql_tests lists, so a suite
# outside both phases fails the run. A missing binary, a server that never
# answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR/<server>/<phase>/: each run's googletest report
# and output (the recovery cells' "[mysql-cdc-recovery]" and
# "[mysql-sink-recovery]" lines and every assertion) and the recovery cells'
# coordinator and worker logs (recovery-logs/<cell>/); on failure, the server's
# log in REPORT_DIR/<server>/. REPORT_DIR keeps provenance.txt: the commit,
# whether the tree was clean, the build's configuration, the client library,
# the phases, and for each server its version, image, digest and binlog
# settings and each phase's result. The password is made for the run and
# appears in none of them; nothing here records the environment.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/mysql-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`, the Docker socket and a Docker CLI mounted, so
#                the recovery cells can reach the server's mysql client)
#   MYSQL_LIVE_PHASES    the phases to run, of "cdc sink" (default both)
#   MYSQL_LIVE_RECOVERY  1 (default) or 0, which leaves the recovery cells out
#   MYSQL_LIVE_SERVERS   the servers to run, of "8.0 8.4" (default both)
#
# What the tests read, all set here: CLINK_MYSQL_TEST_DSN,
# CLINK_MYSQL_TEST_CONTAINER, CLINK_MYSQL_RECOVERY_LOG_DIR.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/mysql-live-reports}"
MYSQL_LIVE_PHASES="${MYSQL_LIVE_PHASES:-cdc sink}"
MYSQL_LIVE_RECOVERY="${MYSQL_LIVE_RECOVERY:-1}"
MYSQL_LIVE_SERVERS="${MYSQL_LIVE_SERVERS:-8.0 8.4}"

# The servers the connector supports, pinned by digest (the multi-arch index).
image_of() {
    case "$1" in
        8.0) echo "mysql:8.0.46@sha256:7dcddc01f13bab2f15cde676d44d01f61fc9f99fe7785e86196dfc07d358ae2b" ;;
        8.4) echo "mysql:8.4.13@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902" ;;
        *) return 1 ;;
    esac
}

# Each phase's cases in clink_mysql_tests, as a googletest filter; its recovery
# cells; and the passing cases it must count, suite by suite.
filter_of() {
    case "$1" in
        cdc) echo 'MysqlFactoryRegistration.*:MysqlSourceLogic.*:MysqlRowDecode.*:MysqlJsonb.*:MysqlCdc*:MysqlSnapshot.*' ;;
        sink) echo 'MysqlSink*:MysqlUpsert*:MysqlLive.*' ;;
        *) return 1 ;;
    esac
}
cells_of() {
    case "$1" in
        cdc) echo 'MysqlCdcRecovery' ;;
        sink) echo 'MysqlSinkRecovery' ;;
    esac
}
expectations_of() {
    case "$1" in
        cdc)
            echo 'MysqlFactoryRegistration.*=8'
            echo 'MysqlSourceLogic.*=9'
            echo 'MysqlRowDecode.*=6'
            echo 'MysqlJsonb.*=1'
            echo 'MysqlCdcCheckpointRow.*=6'
            echo 'MysqlCdcDefinitions.*=2'
            echo 'MysqlCdcEvents.*=1'
            echo 'MysqlCdcStatements.*=8'
            echo 'MysqlCdcBoundaries.*=1'
            echo 'MysqlCdcMetadata.*=2'
            echo 'MysqlCdcProcessor.*=18'
            echo 'MysqlSnapshot.*=12'
            echo 'MysqlCdcLive.*=6'
            echo 'MysqlCdcContractLive.*=26'
            ;;
        sink)
            echo 'MysqlSinkLogic.*=13'
            echo 'MysqlSinkRender.*=5'
            echo 'MysqlSinkStatements.*=2'
            echo 'MysqlSinkTarget.*=4'
            echo 'MysqlSinkFailures.*=24'
            echo 'MysqlUpsertSink.*=1'
            echo 'MysqlUpsertSinkLive.*=4'
            echo 'MysqlUpsert/UpsertContractSuite/0.*=5'
            echo 'MysqlLive.*=6'
            echo 'MysqlSinkContractLive.*=15'
            ;;
    esac
}
cell_count_of() {
    case "$1" in
        cdc) echo 3 ;;
        sink) echo 5 ;;
    esac
}

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
UNIT_BIN="$(find_bin clink_mysql_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
if [ -z "${UNIT_BIN}" ]; then
    echo "mysql-live: clink_mysql_tests must be built under ${BUILD_DIR}" >&2
    exit 1
fi
if [ "${MYSQL_LIVE_RECOVERY}" = "1" ] && [ -z "${IT_BIN}" ]; then
    echo "mysql-live: clink_integration_tests is not built under ${BUILD_DIR}" \
        "(build it, or MYSQL_LIVE_RECOVERY=0 to leave the recovery cells out)" >&2
    exit 1
fi
for server in ${MYSQL_LIVE_SERVERS}; do
    if ! image_of "${server}" >/dev/null; then
        echo "mysql-live: no pinned server '${server}' (choose from 8.0 8.4)" >&2
        exit 1
    fi
done
for phase in ${MYSQL_LIVE_PHASES}; do
    if ! filter_of "${phase}" >/dev/null; then
        echo "mysql-live: no phase '${phase}' (choose from cdc sink)" >&2
        exit 1
    fi
done

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/8.* "${REPORT_DIR:?}/provenance.txt"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
failed=0  # the current phase's
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
    echo "phases: ${MYSQL_LIVE_PHASES}"
    echo "recovery cells: $( [ "${MYSQL_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
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

# The cases a phase's report holds.
cases_in() {
    python3 -c 'import json, sys
print(sum(len(s.get("testsuite", [])) for s in json.load(open(sys.argv[1])).get("testsuites", [])))' "$1"
}

for server in ${MYSQL_LIVE_SERVERS}; do
    image="$(image_of "${server}")"
    echo "==> MySQL ${server} (${image})"
    server_failed=0

    password="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
    port="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    container="clink-mysql-live-${server/./}-$$"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    docker run -d --name "${container}" -p "127.0.0.1:${port}:3306" \
        -e MYSQL_ROOT_PASSWORD="${password}" -e MYSQL_DATABASE=test \
        "${image}" --server-id=1 --log-bin=mysql-bin --binlog-format=ROW \
        --binlog-row-image=FULL >/dev/null

    mkdir -p "${REPORT_DIR}/${server}"
    chmod -R a+rwx "${REPORT_DIR}/${server}"
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
        echo "mysql-live: MySQL ${server} never answered" >&2
        docker logs "${container}" >"${REPORT_DIR}/${server}/mysql-server.log" 2>&1 || true
        echo "server ${server}: never answered" >>"${REPORT_DIR}/provenance.txt"
        status=1
        remove_server
        continue
    fi

    export CLINK_MYSQL_TEST_DSN="host=127.0.0.1 port=${port} user=root password=${password} database=test"
    export CLINK_MYSQL_TEST_CONTAINER="${container}"

    {
        echo "server ${server}:"
        echo "  image: ${image}"
        echo "  image id: $(docker inspect --format '{{.Image}}' "${container}")"
        echo "  version and binlog: $(docker exec -e MYSQL_PWD="${password}" "${container}" \
            mysql -uroot -N -B -e 'SELECT VERSION(), @@binlog_format, @@binlog_row_image, @@binlog_row_metadata, @@binlog_checksum, @@gtid_mode' | tr '\t' ' ')"
    } >>"${REPORT_DIR}/provenance.txt"

    for phase in ${MYSQL_LIVE_PHASES}; do
        dir="${REPORT_DIR}/${server}/${phase}"
        mkdir -p "${dir}/recovery-logs"
        chmod -R a+rwx "${dir}"
        export CLINK_MYSQL_RECOVERY_LOG_DIR="${dir}/recovery-logs"
        echo "==> MySQL ${server}, phase ${phase}"
        failed=0
        run "${dir}" mysql "${UNIT_BIN}" --gtest_filter="$(filter_of "${phase}")"
        reports=("${dir}/mysql.json")
        expect=()
        while read -r e; do
            expect+=(--expect "${e}")
        done < <(expectations_of "${phase}")
        if [ "${MYSQL_LIVE_RECOVERY}" = "1" ]; then
            cells="$(cells_of "${phase}")"
            run "${dir}" recovery "${IT_BIN}" --gtest_filter="${cells}.*"
            reports+=("${dir}/recovery.json")
            expect+=(--expect "${cells}.*=$(cell_count_of "${phase}")")
        fi
        python3 "${ROOT}/scripts/check-gtest-reports.py" "${expect[@]}" "${reports[@]}" || failed=1
        echo "  ${phase}: $( [ "${failed}" = 0 ] && echo pass || echo fail)" >>"${REPORT_DIR}/provenance.txt"
        if [ "${failed}" != 0 ]; then
            server_failed=1
        fi
    done

    # Between them the phases ran every case the binary has.
    if [[ " ${MYSQL_LIVE_PHASES} " == *" cdc "* && " ${MYSQL_LIVE_PHASES} " == *" sink "* ]]; then
        # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
        listed="$(${PIN_RUNNER:-} "${UNIT_BIN}" --gtest_list_tests | grep -c '^  ' || true)"
        ran=$(( $(cases_in "${REPORT_DIR}/${server}/cdc/mysql.json" || echo 0) + \
                $(cases_in "${REPORT_DIR}/${server}/sink/mysql.json" || echo 0) ))
        echo "  clink_mysql_tests cases: ${listed} listed, ${ran} run by the phases" \
            >>"${REPORT_DIR}/provenance.txt"
        if [ "${listed}" != "${ran}" ]; then
            echo "mysql-live: clink_mysql_tests lists ${listed} cases and the phases ran ${ran}:" \
                "a suite is outside both phases' filters" >&2
            server_failed=1
        fi
    fi

    if [ "${server_failed}" != 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/${server}/mysql-server.log" 2>&1 || true
        status=1
    fi
    echo "  result: $( [ "${server_failed}" = 0 ] && echo pass || echo fail)" >>"${REPORT_DIR}/provenance.txt"
    remove_server
done

echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
