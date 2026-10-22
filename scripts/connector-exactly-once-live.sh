#!/usr/bin/env bash
# The connector exactly-once gate: the exactly-once suites of the Kafka,
# PostgreSQL and S3 sinks, the Kafka window-recovery suite and the Kafka
# two-phase-commit fault matrix. Each runs a job in a cluster of a coordinator
# and workers against a real server, kills processes or takes the server away
# at the points it names, and checks the output exactly. Then a check that
# nothing was skipped and that every case ran.
#
#   scripts/connector-exactly-once-live.sh [build-dir]
#
# The suites start their own servers in Docker, each on a free host port: a
# Kafka broker (tests/integration/docker_kafka.hpp), PostgreSQL
# (docker_postgres.hpp) and SILO, the S3 store (docker_minio.hpp). Needs a
# Docker daemon, curl, and a build with CLINK_INTEGRATION_TESTS and the Kafka,
# PostgreSQL and S3 connectors (clink_integration_tests, clink_node,
# clink_submit_job, clink_submit_sql and the three 2PC job plugins). Each suite
# runs in a process of its own and shares one server across its cases. With
# CLINK_TEST_S3_TRACE=1 the S3 store's trace of every call goes to a file in
# the temporary directory, its path printed on stderr. Reports land in
# REPORT_DIR (default <build-dir>/connector-exactly-once-reports). This is what
# the connector-exactly-once CI workflow runs.
set -euo pipefail
cd "$(dirname "$0")/.."

BUILD_DIR="${1:-build}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/connector-exactly-once-reports}"
mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.json

rc=0
run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    if [ ! -x "${bin}" ]; then
        echo "connector-exactly-once-live: ${bin} is not built" >&2
        rc=1
        return
    fi
    echo "==> ${name}"
    "${bin}" --gtest_brief=1 --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" || rc=1
}

IT="${BUILD_DIR}/tests/clink_integration_tests"
run postgres-exactly-once "${IT}" --gtest_filter='PostgresExactlyOnceTest.*'
run kafka-exactly-once "${IT}" --gtest_filter='KafkaExactlyOnceTest.*'
run s3-exactly-once "${IT}" --gtest_filter='S3ExactlyOnceTest.*'
run kafka-window-recovery "${IT}" --gtest_filter='KafkaWindowRecoveryTest.*'
run kafka-2pc-fault-matrix "${IT}" --gtest_filter='Qual01FaultPoints/KafkaTwopcFaultMatrixTest.*'

python3 scripts/check-gtest-reports.py \
    --expect 'PostgresExactlyOnceTest.*=5' \
    --expect 'KafkaExactlyOnceTest.*=7' \
    --expect 'S3ExactlyOnceTest.*=6' \
    --expect 'KafkaWindowRecoveryTest.*=12' \
    --expect 'Qual01FaultPoints/KafkaTwopcFaultMatrixTest.*=7' \
    "${REPORT_DIR}"/postgres-exactly-once.json "${REPORT_DIR}"/kafka-exactly-once.json \
    "${REPORT_DIR}"/s3-exactly-once.json "${REPORT_DIR}"/kafka-window-recovery.json \
    "${REPORT_DIR}"/kafka-2pc-fault-matrix.json || rc=1
exit "${rc}"
