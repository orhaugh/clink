#!/usr/bin/env bash
# The Kinesis gate, the source's phase and then the sink's: every Kinesis test,
# the deterministic cases against models of the service and the live ones
# against LocalStack (the docker/integration-services.yml localstack service),
# then a check per phase that nothing was skipped and that every required suite
# ran in full.
#
#   scripts/kinesis-live.sh [build-dir]
#
# It starts one LocalStack container on a free loopback port and waits until its
# Kinesis API answers. The source's phase runs:
#   - clink_aws_tests, every Kinesis suite: the source's deterministic cases
#     (ownership, discovery, lineage, restore, rescale through the runtime's own
#     file:// and rocksdb:// restores, failures, cancellation, bounded state)
#     against a model of the service, and the KinesisLive cases against the
#     emulator (a split and a merge while the source reads, a uniform reshard
#     under two subtasks with one-shard listing pages, latest-mode restores, a
#     missing stream, a recreated stream, cancel);
#   - clink_integration_tests' KinesisRecovery cells: a coordinator and two
#     workers reading the emulator's stream into the file 2PC sink, a worker
#     killed with records in flight while the stream is split, merged or
#     resharded under the job, each cell verified against the test's own record
#     of what it wrote.
# The sink's phase, its reports, provenance and cell logs kept apart under
# REPORT_DIR/sink, runs:
#   - clink_aws_tests, every Kinesis sink suite: the partition-key rules (exact
#     numbers, Unicode, the 256-character limit, refusals), the response judge
#     and the error classes, pure; the sink's write path against a model of
#     PutRecords (record and request limits, partial failures resent with their
#     keys, retry bounds, ambiguous responses, permanent errors never
#     dead-lettered, a lost reply, cancellation, the failed state, reopening and
#     replay); the SDK path against an in-process stand-in for PutRecords (what
#     the SDK sends, responses and errors on the wire, one request per attempt,
#     a lost reply, cancel while sending and while a reply is held); and the
#     KinesisSinkLive cases, which read the emulator's stream back with a client
#     of their own and compare every record's bytes and partition key;
#   - clink_integration_tests' KinesisSinkRecovery cells: a coordinator and two
#     workers reading one emulator stream into the sink, which writes a second,
#     a worker parked after a store no checkpoint covers or before a send and
#     killed, and the same as a SQL job at parallelism 2, each output stream read
#     back and checked for every record at least once, with its key, and nothing
#     else.
# The container goes, with its volumes, when the gate ends.
#
# A live case skips when its endpoint is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, an emulator that
# never answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each run's googletest report and output (the
# recovery cells' "[kinesis-recovery]" verdict lines and every assertion), the
# recovery cells' coordinator and worker logs (recovery-logs/<cell>/),
# provenance.txt (the commit, whether the tree was clean, the build's
# configuration, the emulator's version and image) and, on failure, the
# emulator's log; and under sink/ the same for the sink's phase (its live
# cases' "[kinesis-sink-live]" and its cells' "[kinesis-sink-recovery]"
# verdict lines, what the independent reader found). The emulator takes any
# credentials; the gate hands the tests fixed dummy ones and records none of
# them.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/kinesis-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#
# What the tests read, all set here: CLINK_KINESIS_TEST_ENDPOINT,
# CLINK_KINESIS_RECOVERY_LOG_DIR, CLINK_KINESIS_SINK_RECOVERY_LOG_DIR,
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_REGION,
# AWS_EC2_METADATA_DISABLED.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/kinesis-live-reports}"
PROJECT=clink-kinesis-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
AWS_BIN="$(find_bin clink_aws_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
SQL_BIN="$(find_bin clink_submit_sql)"
if [ -z "${AWS_BIN}" ] || [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ] || [ -z "${SQL_BIN}" ]; then
    echo "kinesis-live: clink_aws_tests, clink_integration_tests, clink_node and clink_submit_sql" \
        "must be built under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt" "${REPORT_DIR:?}/sink"
mkdir -p "${REPORT_DIR}/recovery-logs" "${REPORT_DIR}/sink/recovery-logs"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container="${PROJECT}-localstack"
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/localstack.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

# Docker's port allocator can hold a port the host shows free and then refuse
# to bind it, so a refused start is tried again on another port.
port=""
for attempt in 1 2 3; do
    candidate="$(free_port)"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    if "${COMPOSE[@]}" run -d --name "${container}" -p "127.0.0.1:${candidate}:4566" localstack \
        >/dev/null; then
        port="${candidate}"
        break
    fi
    echo "kinesis-live: LocalStack did not start on port ${candidate} (attempt ${attempt})" >&2
done
if [ -z "${port}" ]; then
    echo "kinesis-live: LocalStack did not start" >&2
    status=1
    exit 1
fi
endpoint="http://127.0.0.1:${port}"

# The Kinesis API answers a ListStreams call once the emulator's Kinesis backend
# is up (it starts on first use). LocalStack does not check signatures; the
# header only names the region.
ready=0
for _ in $(seq 1 120); do
    if curl -fsS -X POST "${endpoint}/" \
        -H 'Content-Type: application/x-amz-json-1.1' \
        -H 'X-Amz-Target: Kinesis_20131202.ListStreams' \
        -H 'Authorization: AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/kinesis/aws4_request, SignedHeaders=host, Signature=0' \
        -d '{}' >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "kinesis-live: the emulator's Kinesis API never answered at ${endpoint}" >&2
    status=1
    exit 1
fi

export CLINK_KINESIS_TEST_ENDPOINT="${endpoint}"
export CLINK_KINESIS_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"
export CLINK_KINESIS_SINK_RECOVERY_LOG_DIR="${REPORT_DIR}/sink/recovery-logs"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true

provenance() {  # <evidence line, in as many words as it takes>
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_AWS CLINK_WITH_ROCKSDB CLINK_INTEGRATION_TESTS \
        CLINK_ENABLE_FAULT_INJECTION aws-cpp-sdk-kinesis_DIR; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "emulator: LocalStack $(curl -fsS "${endpoint}/_localstack/health" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("version", "?"))')"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "evidence: $*"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
provenance "the source's phase: KinesisSource* and Kinesis* unit cases are deterministic (a" \
    "model of the service); KinesisLive and KinesisRecovery ran against the emulator; nothing" \
    "here ran against AWS" >"${REPORT_DIR}/provenance.txt"

run() {  # <report name> <binary> [gtest args...]
    local name="$1" bin="$2"
    shift 2
    echo "==> ${name}"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${bin}" --gtest_output="json:${REPORT_DIR}/${name}.json" "$@" \
        2>&1 | tee "${REPORT_DIR}/${name}.log" || status=1
}

run kinesis "${AWS_BIN}" --gtest_filter='Kinesis*:-KinesisSink*'
run recovery "${IT_BIN}" --gtest_filter='KinesisRecovery.*'
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'KinesisShardOwnership.*=1' \
    --expect 'KinesisErrorClassification.*=1' \
    --expect 'KinesisCheckpointRows.*=1' \
    --expect 'KinesisSourceConfig.*=1' \
    --expect 'KinesisSourceDiscovery.*=4' \
    --expect 'KinesisSourceLineage.*=5' \
    --expect 'KinesisSourceRestore.*=10' \
    --expect 'KinesisSourceRescale.*=2' \
    --expect 'KinesisSourceFailures.*=3' \
    --expect 'KinesisSourceCancel.*=1' \
    --expect 'KinesisSourceState.*=1' \
    --expect 'KinesisLive.*=8' \
    --expect 'KinesisRecovery.*=3' \
    "${REPORT_DIR}/kinesis.json" "${REPORT_DIR}/recovery.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"

# The sink's phase.
sink_status=0
provenance "the sink's phase: KinesisSink* unit cases are deterministic (the partition-key" \
    "rules, the response judge and the error classes, pure; the write path against a model of" \
    "PutRecords); KinesisSinkSdk ran the SDK's own client against an in-process stand-in for" \
    "PutRecords, not a service; KinesisSinkLive and KinesisSinkRecovery ran against the" \
    "emulator, read back by a client of their own; nothing here ran against AWS" \
    >"${REPORT_DIR}/sink/provenance.txt"
status_before_sink="${status}"
status=0
run sink/kinesis-sink "${AWS_BIN}" --gtest_filter='KinesisSink*'
run sink/recovery "${IT_BIN}" --gtest_filter='KinesisSinkRecovery.*'
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'KinesisSinkKeys.*=6' \
    --expect 'KinesisSinkResponse.*=2' \
    --expect 'KinesisSinkErrors.*=1' \
    --expect 'KinesisSinkConfig.*=3' \
    --expect 'KinesisSinkRecords.*=4' \
    --expect 'KinesisSinkLimits.*=3' \
    --expect 'KinesisSinkWrites.*=5' \
    --expect 'KinesisSinkResponses.*=1' \
    --expect 'KinesisSinkFailures.*=2' \
    --expect 'KinesisSinkCancel.*=2' \
    --expect 'KinesisSinkLifecycle.*=5' \
    --expect 'KinesisSinkSdk.*=7' \
    --expect 'KinesisSinkLive.*=4' \
    --expect 'KinesisSinkRecovery.*=3' \
    "${REPORT_DIR}/sink/kinesis-sink.json" "${REPORT_DIR}/sink/recovery.json" || status=1
sink_status="${status}"
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${sink_status}" \
    >>"${REPORT_DIR}/sink/provenance.txt"
status=$(( status_before_sink || sink_status ))
exit "${status}"
