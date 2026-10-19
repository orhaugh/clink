#!/usr/bin/env bash
# The PostgreSQL CDC live gate: every PostgreSQL connector test against a real
# server, the SQL CDC round trip, and the CDC recovery cells (a coordinator and
# workers into ClickHouse, with faults at the source's phase boundaries), then
# a check that nothing was skipped and that every required suite ran.
#
#   CLINK_POSTGRES_CDC_TEST_DSN="host=localhost port=5432 user=postgres password=postgres dbname=postgres" \
#     scripts/postgres-cdc-live.sh [build-dir]
#
# Needs the server the DSN names, started with wal_level=logical (the
# postgres-cdc service of docker/integration-services.yml), a Docker daemon
# for the recovery cells (they start their own PostgreSQL and ClickHouse), and
# a build with CLINK_BUILD_SQL, CLINK_INTEGRATION_TESTS and the ClickHouse
# client. The PostgreSQL live tests run in one process at a time: the
# two-phase-commit sink cases name their prepared transactions, which collide
# across processes on one server. Reports land in REPORT_DIR (default
# <build-dir>/postgres-cdc-reports). This is what the postgres-cdc CI workflow
# runs.
set -euo pipefail
cd "$(dirname "$0")/.."

BUILD_DIR="${1:-build}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/postgres-cdc-reports}"
if [ -z "${CLINK_POSTGRES_CDC_TEST_DSN:-}" ]; then
    echo "postgres-cdc-live: set CLINK_POSTGRES_CDC_TEST_DSN; without it every live case skips" >&2
    exit 1
fi
mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.json

rc=0
run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    if [ ! -x "${bin}" ]; then
        echo "postgres-cdc-live: ${bin} is not built" >&2
        rc=1
        return
    fi
    echo "==> ${name}"
    "${bin}" --gtest_brief=1 --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" || rc=1
}

run postgres "${BUILD_DIR}/impls/postgres/tests/clink_postgres_tests"
run postgres-sql "${BUILD_DIR}/tests/clink_postgres_sql_tests"
run recovery "${BUILD_DIR}/tests/clink_integration_tests" \
    --gtest_filter='Cells/PostgresCdcClickHouseRecovery.*:PostgresCdcSlot.*:PostgresCdcIntegration.*:PostgresCdcAddColumn.*'

python3 scripts/check-gtest-reports.py \
    --expect 'CdcRowEncoder.*=13' \
    --expect 'CdcCursor.*=9' \
    --expect 'PostgresCdcFactoryOptions.*=8' \
    --expect 'PostgresCdcDecodeStrict.*=14' \
    --expect 'PostgresCdcLive.*=3' \
    --expect 'PostgresCdcContractLive.*=27' \
    --expect 'PostgresCdcSql.*=3' \
    --expect 'Cells/PostgresCdcClickHouseRecovery.*=5' \
    --expect 'PostgresCdcSlot.*=1' \
    --expect 'PostgresCdcIntegration.*=2' \
    --expect 'PostgresCdcAddColumn.*=1' \
    "${REPORT_DIR}"/postgres.json "${REPORT_DIR}"/postgres-sql.json "${REPORT_DIR}"/recovery.json || rc=1
exit "${rc}"
