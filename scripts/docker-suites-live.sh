#!/usr/bin/env bash
# The Docker suites gate: every test suite that starts its own servers in
# Docker, apart from those with a gate of their own (postgres-cdc, neo4j and
# the nightly ClickHouse recovery matrix), then a check that nothing was
# skipped and that every case ran.
#
#   scripts/docker-suites-live.sh [build-dir]
#
# The exactly-once suites of the Kafka, PostgreSQL and S3 sinks, the Kafka
# window-recovery suite and the Kafka two-phase-commit fault matrix each run a
# job in a cluster of a coordinator and workers, kill processes or take the
# server away at the points they name, and check the output exactly. The Kafka
# connector's live suites (transaction resume, with and without SASL, the
# end-of-input commit and the registry formats), the embedded resume, the
# Kafka source's ownership, the PostgreSQL pipeline and sink, and the
# PostgreSQL transport refusals run against servers of their own too.
#
# The servers are a Kafka broker (tests/integration/docker_kafka.hpp),
# PostgreSQL (docker_postgres.hpp) and SILO, the S3 store (docker_minio.hpp),
# each on a free host port. Needs a Docker daemon, curl, openssl, and a build
# with CLINK_INTEGRATION_TESTS and the Kafka, PostgreSQL, S3 and schema
# registry connectors (clink_integration_tests, clink_kafka_tests, clink_node,
# clink_submit_job, clink_submit_sql, the clink CLI and the three 2PC job
# plugins). Each suite runs in a process of its own and shares one server
# across its cases. With CLINK_TEST_S3_TRACE=1 the S3 store's trace of every
# call goes to a file in the temporary directory, its path printed on stderr.
# Reports land in REPORT_DIR (default <build-dir>/docker-suites-reports). This
# is what the docker-suites CI workflow runs.
set -euo pipefail
cd "$(dirname "$0")/.."

BUILD_DIR="${1:-build}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/docker-suites-reports}"
mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.json

rc=0
reports=()
run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    reports+=("${REPORT_DIR}/${name}.json")
    if [ ! -x "${bin}" ]; then
        echo "docker-suites-live: ${bin} is not built" >&2
        rc=1
        return
    fi
    echo "==> ${name}"
    "${bin}" --gtest_brief=1 --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" || rc=1
}

IT="${BUILD_DIR}/tests/clink_integration_tests"
KT="${BUILD_DIR}/impls/kafka/tests/clink_kafka_tests"
run postgres-exactly-once "${IT}" --gtest_filter='PostgresExactlyOnceTest.*'
run kafka-exactly-once "${IT}" --gtest_filter='KafkaExactlyOnceTest.*'
run s3-exactly-once "${IT}" --gtest_filter='S3ExactlyOnceTest.*'
run kafka-window-recovery "${IT}" --gtest_filter='KafkaWindowRecoveryTest.*'
run kafka-2pc-fault-matrix "${IT}" --gtest_filter='Qual01FaultPoints/KafkaTwopcFaultMatrixTest.*'
run kafka-source-ownership "${IT}" --gtest_filter='KafkaSourceOwnershipTest.*'
run embedded-resume-kafka "${IT}" --gtest_filter='EmbeddedResumeKafka.*'
run postgres-pipeline-and-sink "${IT}" --gtest_filter='PostgresIntegration.*:PostgresSinkIntegration.*'
run postgres-transport "${IT}" --gtest_filter='Qual12PgTransportLive.*'
run kafka-txn-resume "${KT}" --gtest_filter='TxnResumeLive.*:TxnResumeSaslLive.*'
run kafka-end-of-input "${KT}" --gtest_filter='KafkaTxnEndOfInputLive.*'
run kafka-registry-formats "${KT}" --gtest_filter='KafkaRegistryFormatsLive.*'

python3 scripts/check-gtest-reports.py \
    --expect 'PostgresExactlyOnceTest.*=5' \
    --expect 'KafkaExactlyOnceTest.*=7' \
    --expect 'S3ExactlyOnceTest.*=6' \
    --expect 'KafkaWindowRecoveryTest.*=12' \
    --expect 'Qual01FaultPoints/KafkaTwopcFaultMatrixTest.*=7' \
    --expect 'KafkaSourceOwnershipTest.*=1' \
    --expect 'EmbeddedResumeKafka.*=1' \
    --expect 'PostgresIntegration.*=1' \
    --expect 'PostgresSinkIntegration.*=2' \
    --expect 'Qual12PgTransportLive.*=4' \
    --expect 'TxnResumeLive.*=16' \
    --expect 'TxnResumeSaslLive.*=4' \
    --expect 'KafkaTxnEndOfInputLive.*=3' \
    --expect 'KafkaRegistryFormatsLive.*=3' \
    "${reports[@]}" || rc=1
exit "${rc}"
