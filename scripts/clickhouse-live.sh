#!/usr/bin/env bash
# Run the native ClickHouse sink's live suites against real servers on every
# supported line, and the SQL-level live cases of the SQL-linked binary.
#
#   scripts/clickhouse-live.sh                          # every line, build/ binaries
#   CLICKHOUSE_LINES="26.8" scripts/clickhouse-live.sh  # one line
#
# For each line it brings up, runs and tears down, one profile at a time, the
# services of docker/integration-services.yml:
#   - the line's plain and async-default servers (the same two the pins use):
#     ClickHouseNativeLive.*, then the SQL-linked ClickHouseNativeSqlLive.* and
#     ClickHouseLegacySqlLive.*, which include the typed job module's plugin
#     route;
#   - two replicas sharing one Keeper: pin P18 first, then
#     ClickHouseNativeLiveReplicated.*, whose failover case shuts the first
#     replica down;
#   - on 26.8 only, a server with the native port over TLS, for which this
#     script generates a CA, a server certificate for localhost and a second,
#     unrelated CA into a temporary directory (scripts/make-clickhouse-test-tls.sh):
#     ClickHouseNativeLiveTls.*.
# Then the 25.3 server, for the legacy sink's round trip
# (ClickHouseLegacySqlLive.* only). A failing case fails the run; every service
# started is stopped and removed, with its data volume, as soon as its profile
# is done and again on exit, whatever happened. The legacy sink's cases run on
# their own, each line writing a googletest report that
# scripts/check-gtest-reports.py then requires to show every one of them passed
# and none skipped, so a case whose server variable never arrived fails the run
# instead of reading as a pass.
#
# What the test binaries read, all set here per profile:
#   CLINK_CLICKHOUSE_TEST_HOST, CLINK_CLICKHOUSE_TEST_PORT   the server (plain port)
#   CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT  a server of the same line whose
#                                             <merge_tree> sets async_insert=1
#   CLINK_CLICKHOUSE_TEST_REPLICA_PORTS       "<r1 port>,<r2 port>"
#   CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT=1 the server's <replicated_merge_tree>
#                                             sets async_insert=1 (pin P18)
#   CLINK_CLICKHOUSE_TEST_TLS_PORT            the TLS native port
#   CLINK_CLICKHOUSE_TEST_TLS_CA              the CA that signed the server
#   CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA        a CA that did not
#   CLINK_CLICKHOUSE_TEST_LINE                the line the profile runs ("26.8")
#   CLINK_CLICKHOUSE_TYPED_JOB                the typed job module (TYPED_JOB),
#                                             for the SQL-level cases
# CLINK_CLICKHOUSE_TEST_USER and CLINK_CLICKHOUSE_TEST_PASSWORD pass through
# when set.
#
# Knobs:
#   BUILD_DIR      the build to take the binaries from (default build/)
#   BIN            clink_clickhouse_tests to run, overriding the one in BUILD_DIR
#   SQL_BIN        clink_clickhouse_sql_tests, likewise; CLICKHOUSE_LIVE_SQL=0
#                  skips the SQL-level cases where that binary is not built
#   TYPED_JOB      clickhouse_typed_job.so, the job module the SQL-level plugin
#                  case submits, found under BUILD_DIR by default. Passed by
#                  path, because the one compiled into SQL_BIN names the tree it
#                  was built in; that case skips when neither exists, and a
#                  TYPED_JOB set to a missing file fails the run
#   PIN_RUNNER     a prefix for every test command (CI runs the binaries inside
#                  the toolchain image with `docker run ... -e <each variable
#                  above>`, so the variables set on the command reach the
#                  container, and mounts CLICKHOUSE_LIVE_TMPDIR so the CA files
#                  are readable there)
#   CLICKHOUSE_LIVE_TMPDIR  where the TLS material is generated (default TMPDIR)
#   REPORT_DIR     where the legacy cases' reports go (default
#                  <BUILD_DIR>/clickhouse-live-reports); under PIN_RUNNER it must
#                  be inside a directory the container mounts at the same path
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -p clink-clickhouse-live -f "${ROOT}/docker/integration-services.yml")
BUILD_DIR="${BUILD_DIR:-${ROOT}/build}"
CLICKHOUSE_LINES="${CLICKHOUSE_LINES:-26.3 26.8}"
CLICKHOUSE_LIVE_SQL="${CLICKHOUSE_LIVE_SQL:-1}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/clickhouse-live-reports}"
# ClickHouseLegacySqlLive's cases: every line must pass all of them.
LEGACY_CASES=2

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
BIN="${BIN:-$(find_bin clink_clickhouse_tests)}"
if [ -z "${BIN}" ] || [ ! -x "${BIN}" ]; then
    echo "clickhouse-live: clink_clickhouse_tests is not built under ${BUILD_DIR} (or set BIN)" >&2
    exit 1
fi
SQL_BIN="${SQL_BIN:-$(find_bin clink_clickhouse_sql_tests)}"
if [ "${CLICKHOUSE_LIVE_SQL}" = "1" ] && { [ -z "${SQL_BIN}" ] || [ ! -x "${SQL_BIN}" ]; }; then
    echo "clickhouse-live: clink_clickhouse_sql_tests is not built under ${BUILD_DIR}" \
        "(set SQL_BIN, or CLICKHOUSE_LIVE_SQL=0 to skip the SQL-level cases)" >&2
    exit 1
fi

# A module needs only to be readable, so it is found without the executable bit
# an artifact download drops. One named by the caller must exist: the case it
# serves would otherwise skip, and the plugin route go untested unnoticed.
if [ -n "${TYPED_JOB:-}" ] && [ ! -f "${TYPED_JOB}" ]; then
    echo "clickhouse-live: TYPED_JOB=${TYPED_JOB} does not exist" >&2
    exit 1
fi
TYPED_JOB="${TYPED_JOB:-$(find "${BUILD_DIR}" -name clickhouse_typed_job.so -type f 2>/dev/null | head -1)}"
typed_job_env=()
if [ -n "${TYPED_JOB}" ]; then
    typed_job_env=(CLINK_CLICKHOUSE_TYPED_JOB="${TYPED_JOB}")
fi

started=()
tls_dir=""
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${#started[@]}" -gt 0 ]; then
        "${COMPOSE[@]}" rm -sfv "${started[@]}" >/dev/null 2>&1 || true
    fi
    if [ -n "${tls_dir}" ]; then
        rm -rf "${tls_dir}"
    fi
}
trap cleanup EXIT

up() {
    started+=("$@")
    "${COMPOSE[@]}" up -d --wait "$@"
}

down() {
    "${COMPOSE[@]}" rm -sfv "$@" >/dev/null
}

status=0
# Appended to the test command when set (run_counted's report).
gtest_args=()
# run <label> <binary> <gtest filter> [VAR=value ...]: one test command, with
# the variables given and nothing else from a previous profile.
run() {
    local label="$1" bin="$2" filter="$3"
    shift 3
    echo "clickhouse-live: ${label}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    if ! env -u CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT -u CLINK_CLICKHOUSE_TEST_REPLICA_PORTS \
        -u CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT -u CLINK_CLICKHOUSE_TEST_TLS_PORT \
        -u CLINK_CLICKHOUSE_TEST_TLS_CA -u CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA \
        CLINK_CLICKHOUSE_TEST_HOST=localhost "$@" \
        ${PIN_RUNNER:-} "${bin}" --gtest_filter="${filter}" ${gtest_args[@]+"${gtest_args[@]}"}; then
        echo "clickhouse-live: FAILED: ${label}" >&2
        status=1
    fi
}

# run_counted <label> <binary> <gtest filter> <PATTERN=N> [VAR=value ...]: as
# run, and the run's googletest report must show at least N passing cases
# matching PATTERN and no skipped one.
run_counted() {
    local label="$1" bin="$2" filter="$3" expect="$4"
    shift 4
    local report="${REPORT_DIR}/${label//[^A-Za-z0-9.]/_}.json"
    mkdir -p "${REPORT_DIR}"
    rm -f "${report}"
    gtest_args=(--gtest_output="json:${report}")
    run "${label}" "${bin}" "${filter}" "$@"
    gtest_args=()
    if ! python3 "${ROOT}/scripts/check-gtest-reports.py" --expect "${expect}" "${report}"; then
        echo "clickhouse-live: FAILED: ${label} (its report)" >&2
        status=1
    fi
}

# A CA, a server certificate it signs for localhost and 127.0.0.1, and a second
# CA that signs nothing here (scripts/make-clickhouse-test-tls.sh, which the wheels
# workflow uses too).
make_tls() {
    tls_dir="$(mktemp -d "${CLICKHOUSE_LIVE_TMPDIR:-${TMPDIR:-/tmp}}/clink-clickhouse-tls.XXXXXX")"
    "${ROOT}/scripts/make-clickhouse-test-tls.sh" "${tls_dir}"
}

for line in ${CLICKHOUSE_LINES}; do
    tag="${line//./-}"
    minor="${line#*.}"
    svc="clickhouse-${tag}"
    async_svc="clickhouse-${tag}-async-default"
    port="191$(printf '%02d' "${minor}")"
    async_port="191$((10 + minor))"
    r1_port="191$((30 + minor))"
    r2_port="191$((40 + minor))"

    up "${svc}" "${async_svc}"
    run "${line} native" "${BIN}" 'ClickHouseNativeLive.*' \
        CLINK_CLICKHOUSE_TEST_PORT="${port}" CLINK_CLICKHOUSE_TEST_LINE="${line}" \
        CLINK_CLICKHOUSE_TEST_ASYNC_DEFAULT_PORT="${async_port}"
    if [ "${CLICKHOUSE_LIVE_SQL}" = "1" ]; then
        run "${line} SQL-level" "${SQL_BIN}" 'ClickHouseNativeSqlLive.*' \
            CLINK_CLICKHOUSE_TEST_PORT="${port}" CLINK_CLICKHOUSE_TEST_LINE="${line}" \
            ${typed_job_env[@]+"${typed_job_env[@]}"}
        run_counted "${line} legacy SQL-level" "${SQL_BIN}" 'ClickHouseLegacySqlLive.*' \
            "ClickHouseLegacySqlLive.*=${LEGACY_CASES}" \
            CLINK_CLICKHOUSE_TEST_PORT="${port}" CLINK_CLICKHOUSE_TEST_LINE="${line}"
    fi
    down "${svc}" "${async_svc}"

    up "clickhouse-keeper-${tag}" "clickhouse-${tag}-r1" "clickhouse-${tag}-r2"
    run "${line} replicated pins" "${BIN}" 'ClickHousePins.P18*' \
        CLINK_CLICKHOUSE_TEST_PORT="${r1_port}" CLINK_CLICKHOUSE_TEST_LINE="${line}" \
        CLINK_CLICKHOUSE_TEST_RMT_ASYNC_DEFAULT=1
    run "${line} replicated" "${BIN}" 'ClickHouseNativeLiveReplicated.*' \
        CLINK_CLICKHOUSE_TEST_PORT="${r1_port}" CLINK_CLICKHOUSE_TEST_LINE="${line}" \
        CLINK_CLICKHOUSE_TEST_REPLICA_PORTS="${r1_port},${r2_port}"
    down "clickhouse-${tag}-r1" "clickhouse-${tag}-r2" "clickhouse-keeper-${tag}"

    if [ "${line}" = "26.8" ]; then
        make_tls
        export CLINK_CLICKHOUSE_TLS_DIR="${tls_dir}"
        up clickhouse-26-8-tls
        run "26.8 TLS" "${BIN}" 'ClickHouseNativeLiveTls.*' \
            CLINK_CLICKHOUSE_TEST_PORT=19158 CLINK_CLICKHOUSE_TEST_LINE=26.8 \
            CLINK_CLICKHOUSE_TEST_TLS_PORT=19458 \
            CLINK_CLICKHOUSE_TEST_TLS_CA="${tls_dir}/ca.crt" \
            CLINK_CLICKHOUSE_TEST_TLS_WRONG_CA="${tls_dir}/wrong-ca.crt"
        down clickhouse-26-8-tls
        rm -rf "${tls_dir}"
        tls_dir=""
    fi
done

if [ "${CLICKHOUSE_LIVE_SQL}" = "1" ]; then
    up clickhouse-25-3
    run_counted "25.3 legacy" "${SQL_BIN}" 'ClickHouseLegacySqlLive.*' \
        "ClickHouseLegacySqlLive.*=${LEGACY_CASES}" \
        CLINK_CLICKHOUSE_TEST_PORT=19203 CLINK_CLICKHOUSE_TEST_LINE=25.3
    down clickhouse-25-3
fi
exit "${status}"
