#!/usr/bin/env bash
# The DynamoDB sink's live gate: every DynamoDB test, the deterministic cases
# against the service model and the live ones against LocalStack (the
# docker/integration-services.yml localstack service, whose DynamoDB is DynamoDB
# Local), then a check that nothing was skipped and that every required suite
# ran in full.
#
#   scripts/dynamodb-live.sh [build-dir]
#
# It starts one LocalStack container on a free loopback port with DynamoDB and
# Kinesis, and waits until its DynamoDB API answers (DynamoDB Local starts on
# first use). Then it runs:
#   - clink_aws_tests, every DynamoDB suite: numbers, item identity, the record
#     checks and size rules, and the sink's write path against a model of
#     DescribeTable and BatchWriteItem (partial writes, retry bounds, malformed
#     responses, permanent errors, a lost reply, cancellation, the failed state
#     and a restart); and the DynamoDbLive cases against the emulator, read back
#     with a separate client item by item;
#   - clink_integration_tests' DynamoDbRecovery cells: a coordinator and two
#     workers reading the emulator's Kinesis stream into the sink, a worker
#     parked at the sink's own fault points with writes no checkpoint covers and
#     killed, each cell's table checked against the test's record of the last
#     record written for every item.
# The container goes, with its volumes, when the gate ends.
#
# A live case skips when its endpoint is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, an emulator that
# never answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each run's googletest report and output (the
# recovery cells' "[dynamodb-recovery]" verdict lines and every assertion), the
# recovery cells' coordinator and worker logs (recovery-logs/<cell>/),
# provenance.txt (the commit, whether the tree was clean, the build's
# configuration, the emulator's version and image) and, on failure, the
# emulator's log. The emulator takes any credentials; the gate hands the tests
# fixed dummy ones and records none of them.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/dynamodb-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#
# What the tests read, all set here: CLINK_DYNAMODB_TEST_ENDPOINT,
# CLINK_DYNAMODB_RECOVERY_LOG_DIR, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
# AWS_DEFAULT_REGION, AWS_REGION, AWS_EC2_METADATA_DISABLED.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/dynamodb-live-reports}"
PROJECT=clink-dynamodb-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
AWS_BIN="$(find_bin clink_aws_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
if [ -z "${AWS_BIN}" ] || [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ]; then
    echo "dynamodb-live: clink_aws_tests, clink_integration_tests and clink_node must be built" \
        "under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -rf "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/recovery-logs" \
    "${REPORT_DIR:?}/provenance.txt"
mkdir -p "${REPORT_DIR}/recovery-logs"
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
    if "${COMPOSE[@]}" run -d --name "${container}" -e SERVICES=dynamodb,kinesis \
        -p "127.0.0.1:${candidate}:4566" localstack >/dev/null; then
        port="${candidate}"
        break
    fi
    echo "dynamodb-live: LocalStack did not start on port ${candidate} (attempt ${attempt})" >&2
done
if [ -z "${port}" ]; then
    echo "dynamodb-live: LocalStack did not start" >&2
    status=1
    exit 1
fi
endpoint="http://127.0.0.1:${port}"

# The DynamoDB API answers a ListTables call once DynamoDB Local has started,
# which it does on the first call and which takes a while. LocalStack does not
# check signatures; the header only names the region.
ready=0
for _ in $(seq 1 180); do
    if curl -fsS --max-time 60 -X POST "${endpoint}/" \
        -H 'Content-Type: application/x-amz-json-1.0' \
        -H 'X-Amz-Target: DynamoDB_20120810.ListTables' \
        -H 'Authorization: AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=0' \
        -d '{}' >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "dynamodb-live: the emulator's DynamoDB API never answered at ${endpoint}" >&2
    status=1
    exit 1
fi

export CLINK_DYNAMODB_TEST_ENDPOINT="${endpoint}"
export CLINK_DYNAMODB_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_AWS CLINK_INTEGRATION_TESTS CLINK_ENABLE_FAULT_INJECTION \
        aws-cpp-sdk-dynamodb_DIR; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "emulator: LocalStack $(curl -fsS "${endpoint}/_localstack/health" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("version", "?"))'), DynamoDB Local inside it"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "evidence: DynamoDbNumber, DynamoDbIdentity, DynamoDbRecord, DynamoDbItemSize and DynamoDbSink*" \
        "cases are deterministic (the sink's write path against a model of the service);" \
        "DynamoDbLive and DynamoDbRecovery ran against the emulator; nothing here ran against AWS"
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

run dynamodb "${AWS_BIN}" --gtest_filter='DynamoDb*'
run recovery "${IT_BIN}" --gtest_filter='DynamoDbRecovery.*'
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'DynamoDbNumber.*=6' \
    --expect 'DynamoDbIdentity.*=4' \
    --expect 'DynamoDbRecord.*=8' \
    --expect 'DynamoDbItemSize.*=2' \
    --expect 'DynamoDbSinkConfig.*=5' \
    --expect 'DynamoDbSinkIdentity.*=4' \
    --expect 'DynamoDbSinkValidation.*=5' \
    --expect 'DynamoDbSinkWrites.*=5' \
    --expect 'DynamoDbSinkResponses.*=2' \
    --expect 'DynamoDbSinkFailures.*=2' \
    --expect 'DynamoDbSinkCancel.*=2' \
    --expect 'DynamoDbSinkLifecycle.*=5' \
    --expect 'DynamoDbErrorClassification.*=1' \
    --expect 'DynamoDbLive.*=6' \
    --expect 'DynamoDbRecovery.*=3' \
    "${REPORT_DIR}/dynamodb.json" "${REPORT_DIR}/recovery.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
