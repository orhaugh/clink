#!/usr/bin/env bash
# The PostgreSQL connector's live gate: three phases, the change stream, the
# sinks and the SELECT source, against the server CLINK_POSTGRES_CDC_TEST_DSN
# names, then a check that nothing was skipped and that every required suite
# ran in full.
#
#   CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres" \
#     scripts/postgres-live.sh [build-dir]
#
# The phases POSTGRES_LIVE_PHASES names run one after the other:
#
#   cdc: clink_postgres_tests' change-stream and decoder suites and the
#     factories, the SQL CDC round trip (clink_postgres_sql_tests), and
#     clink_integration_tests' change-stream cells: a coordinator and workers
#     into ClickHouse with faults at the source's phase boundaries, and a column
#     added and backfilled into ClickHouse.
#   sink: clink_postgres_tests' sink suites, offline and live: the rules, the
#     failure state against a scripted server (PgSinkFailures), the contract
#     against the server (PgSinkContractLive), the older round trips and the
#     two-phase-commit sink's cases; then the PostgresSinkRecovery cells: a
#     coordinator and two workers polling a feed the test serves into each
#     sink, one of them a SQL job at parallelism 2, with a worker killed before
#     a flush's COMMIT and after it, before its checkpoint completes.
#   select: the SELECT source's suites: its position round-tripping through a
#     checkpoint, its row codec, and PostgresSourceLive against the server
#     (values and NULLs as read, and a restore over the same rows, after an
#     earlier row was deleted or changed, after rows were added at the end, and
#     from an earlier release's position). It has no cells and needs no
#     Docker.
#
# Needs the server the DSN names, started with wal_level=logical and
# max_prepared_transactions above zero (the postgres-cdc service of
# docker/integration-services.yml), a Docker daemon for the change stream's
# cells (they start their own PostgreSQL and ClickHouse), and a build with
# CLINK_BUILD_SQL, CLINK_INTEGRATION_TESTS and the ClickHouse client. The live
# tests run in one process at a time: the two-phase-commit sink cases name their
# prepared transactions, which collide across processes on one server.
#
# A live case skips when its server is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. When every phase runs, the cases
# clink_postgres_tests ran between them must be every case it lists, so a suite
# outside every phase fails the gate. Reports land in REPORT_DIR/<phase>/
# (default <build-dir>/postgres-live-reports), with provenance.txt (the commit,
# whether the tree was clean, the build's configuration, the client library,
# the server's version and each phase's result), without credentials. This is
# what the postgres-cdc CI workflow runs.
#
# Knobs:
#   POSTGRES_LIVE_PHASES     the phases to run, of "cdc sink select" (default all)
#   POSTGRES_LIVE_RECOVERY   1 (default) or 0, which leaves the recovery cells out
set -euo pipefail
cd "$(dirname "$0")/.."

BUILD_DIR="${1:-build}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/postgres-live-reports}"
POSTGRES_LIVE_PHASES="${POSTGRES_LIVE_PHASES:-cdc sink select}"
POSTGRES_LIVE_RECOVERY="${POSTGRES_LIVE_RECOVERY:-1}"
if [ -z "${CLINK_POSTGRES_CDC_TEST_DSN:-}" ]; then
    echo "postgres-live: set CLINK_POSTGRES_CDC_TEST_DSN; without it every live case skips" >&2
    exit 1
fi
UNIT_BIN="${BUILD_DIR}/impls/postgres/tests/clink_postgres_tests"
SQL_BIN="${BUILD_DIR}/tests/clink_postgres_sql_tests"
IT_BIN="${BUILD_DIR}/tests/clink_integration_tests"

# Each phase's cases in clink_postgres_tests, as a googletest filter, and the
# passing cases it must count, suite by suite.
filter_of() {
    case "$1" in
        cdc) echo 'CdcRowEncoder.*:CdcCursor.*:CdcJson.*:CdcEventCodec.*:PostgresCdc*:PostgresDecoder.*:PostgresTypedCdcSource.*:PostgresFactoryRegistration.*' ;;
        sink) echo 'PgSink*:PostgresJsonSink*:PostgresUpsert*:PostgresSink*' ;;
        select) echo 'PostgresSourceReplay.*:PostgresSourceLive.*:PostgresRowCodec.*' ;;
        *) return 1 ;;
    esac
}
expectations_of() {
    case "$1" in
        cdc)
            echo 'CdcRowEncoder.*=13'
            echo 'CdcCursor.*=9'
            echo 'PostgresCdcFactoryOptions.*=9'
            echo 'PostgresCdcDecodeStrict.*=14'
            echo 'PostgresCdcLive.*=3'
            echo 'PostgresCdcContractLive.*=28'
            echo 'PostgresFactoryRegistration.*=11'
            echo 'PostgresCdcSql.*=4'
            ;;
        sink)
            echo 'PgSinkRender.*=10'
            echo 'PgSinkStatements.*=2'
            echo 'PgSinkTarget.*=4'
            echo 'PgSinkFailures.*=24'
            echo 'PgSinkContractLive.*=17'
            echo 'PostgresJsonSinkLogic.*=16'
            echo 'PostgresJsonSinkLive.*=6'
            echo 'PostgresUpsertSink.*=2'
            echo 'PostgresUpsertSinkLive.*=4'
            echo 'PostgresUpsert/UpsertContractSuite/0.*=5'
            echo 'PostgresSinkUnit.*=3'
            echo 'PostgresSinkLive.*=1'
            echo 'PostgresSink2PC.*=3'
            echo 'PostgresSink2PCLive.*=8'
            echo 'PostgresSink2PC/SinkContractSuite/0.*=8'
            ;;
        select)
            echo 'PostgresSourceReplay.*=2'
            echo 'PostgresSourceLive.*=6'
            echo 'PostgresRowCodec.*=4'
            ;;
    esac
}
recovery_filter_of() {
    case "$1" in
        cdc) echo 'Cells/PostgresCdcClickHouseRecovery.*:PostgresCdcSlot.*:PostgresCdcIntegration.*:PostgresCdcAddColumn.*:PostgresCdcClickHouseBackfill.*' ;;
        sink) echo 'PostgresSinkRecovery.*' ;;
    esac
}
recovery_expectations_of() {
    case "$1" in
        cdc)
            echo 'Cells/PostgresCdcClickHouseRecovery.*=5'
            echo 'PostgresCdcSlot.*=1'
            echo 'PostgresCdcIntegration.*=2'
            echo 'PostgresCdcAddColumn.*=1'
            echo 'PostgresCdcClickHouseBackfill.*=2'
            ;;
        sink) echo 'PostgresSinkRecovery.*=5' ;;
    esac
}

for phase in ${POSTGRES_LIVE_PHASES}; do
    if ! filter_of "${phase}" >/dev/null; then
        echo "postgres-live: no phase '${phase}' (choose from cdc sink select)" >&2
        exit 1
    fi
done
mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}/cdc" "${REPORT_DIR:?}/sink" "${REPORT_DIR:?}/select" \
    "${REPORT_DIR:?}/provenance.txt"

rc=0
{
    # The CI workflow runs this inside the toolchain image as another user than
    # the checkout's owner; safe.directory lets git read it there.
    echo "commit: $(git -c safe.directory='*' rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -c safe.directory='*' status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_POSTGRES CLINK_BUILD_SQL CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "client library: libpq $(pg_config --version 2>/dev/null || echo unknown)"
    echo "server: $(python3 - <<'PYEOF' 2>/dev/null || echo unknown
import os, subprocess
print(subprocess.run(["psql", os.environ["CLINK_POSTGRES_CDC_TEST_DSN"], "-tAc", "SELECT version()"],
                     capture_output=True, text=True).stdout.strip() or "unknown")
PYEOF
)"
    echo "phases: ${POSTGRES_LIVE_PHASES}"
    echo "recovery cells: $( [ "${POSTGRES_LIVE_RECOVERY}" = 1 ] && echo run || echo left out)"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

run() {  # <dir> <report name> <binary> [gtest args...]
    local dir="$1" name="$2" bin="$3"
    shift 3
    if [ ! -x "${bin}" ]; then
        echo "postgres-live: ${bin} is not built" >&2
        failed=1
        return
    fi
    echo "==> ${name}"
    "${bin}" --gtest_brief=1 --gtest_output="json:${dir}/${name}.json" "$@" || failed=1
}

# The cases a report holds.
cases_in() {
    python3 -c 'import json, sys
print(sum(len(s.get("testsuite", [])) for s in json.load(open(sys.argv[1])).get("testsuites", [])))' "$1"
}

for phase in ${POSTGRES_LIVE_PHASES}; do
    dir="${REPORT_DIR}/${phase}"
    mkdir -p "${dir}"
    export CLINK_POSTGRES_RECOVERY_LOG_DIR="${dir}/recovery-logs"
    mkdir -p "${CLINK_POSTGRES_RECOVERY_LOG_DIR}"
    echo "==> phase ${phase}"
    failed=0
    run "${dir}" postgres "${UNIT_BIN}" --gtest_filter="$(filter_of "${phase}")"
    reports=("${dir}/postgres.json")
    expect=()
    while read -r e; do
        expect+=(--expect "${e}")
    done < <(expectations_of "${phase}")
    if [ "${phase}" = "cdc" ]; then
        run "${dir}" postgres-sql "${SQL_BIN}"
        reports+=("${dir}/postgres-sql.json")
    fi
    # A phase with no cells runs none: an empty filter would run every case.
    if [ "${POSTGRES_LIVE_RECOVERY}" = "1" ] && [ -n "$(recovery_filter_of "${phase}")" ]; then
        run "${dir}" recovery "${IT_BIN}" --gtest_filter="$(recovery_filter_of "${phase}")"
        reports+=("${dir}/recovery.json")
        while read -r e; do
            expect+=(--expect "${e}")
        done < <(recovery_expectations_of "${phase}")
    fi
    python3 scripts/check-gtest-reports.py "${expect[@]}" "${reports[@]}" || failed=1
    echo "  ${phase}: $( [ "${failed}" = 0 ] && echo pass || echo fail)" >>"${REPORT_DIR}/provenance.txt"
    if [ "${failed}" != 0 ]; then
        rc=1
    fi
done

# Between them the phases ran every case the binary has.
if [[ " ${POSTGRES_LIVE_PHASES} " == *" cdc "* && " ${POSTGRES_LIVE_PHASES} " == *" sink "* &&
    " ${POSTGRES_LIVE_PHASES} " == *" select "* ]]; then
    listed="$("${UNIT_BIN}" --gtest_list_tests | grep -c '^  ' || true)"
    ran=$(( $(cases_in "${REPORT_DIR}/cdc/postgres.json" || echo 0) + \
            $(cases_in "${REPORT_DIR}/sink/postgres.json" || echo 0) + \
            $(cases_in "${REPORT_DIR}/select/postgres.json" || echo 0) ))
    echo "  clink_postgres_tests cases: ${listed} listed, ${ran} run by the phases" \
        >>"${REPORT_DIR}/provenance.txt"
    if [ "${listed}" != "${ran}" ]; then
        echo "postgres-live: clink_postgres_tests lists ${listed} cases and the phases ran ${ran}:" \
            "a suite is outside every phase's filter" >&2
        rc=1
    fi
fi
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${rc}" >>"${REPORT_DIR}/provenance.txt"
exit "${rc}"
