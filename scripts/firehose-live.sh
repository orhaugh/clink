#!/usr/bin/env bash
# The Firehose sink's live gate: every Firehose test, the deterministic cases
# against the service model and the live ones against LocalStack (the
# docker/integration-services.yml localstack service), then a check that nothing
# was skipped and that every required suite ran in full.
#
#   scripts/firehose-live.sh [build-dir]
#
# It starts one LocalStack container on a free loopback port with Firehose, S3,
# STS (the delivery stream assumes its role to write to S3), IAM and Kinesis,
# and waits until its Firehose API answers. Then it runs:
#   - clink_aws_tests, every Firehose suite: the request accounting against the
#     SDK's own JSON, the response judge, and the sink's write path against a
#     model of PutRecordBatch (record and request limits, partial acceptance,
#     retry bounds, responses that do not account for their request, permanent
#     errors, a lost reply, cancellation, the failed state and reopening); and
#     the FirehoseLive cases, each reading the delivery stream's S3 destination
#     back with the SDK's S3 client, record by record;
#   - clink_integration_tests' FirehoseRecovery cells: a coordinator and two
#     workers reading the emulator's Kinesis stream into the sink, a worker
#     parked after an acceptance no checkpoint covers, or before a send, and
#     killed, each bucket checked against the test's record of what it wrote:
#     every record at least once, nothing else.
# The container goes, with its volumes, when the gate ends.
#
# A live case skips when its endpoint is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it should. A missing binary, an emulator that
# never answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each run's googletest report and output (the
# recovery cells' "[firehose-recovery]" verdict lines and every assertion), the
# recovery cells' coordinator and worker logs (recovery-logs/<cell>/),
# provenance.txt (the commit, whether the tree was clean, the build's
# configuration, the emulator's version and image) and, on failure, the
# emulator's log. The emulator takes any credentials; the gate hands the tests
# fixed dummy ones and records none of them.
#
# Knobs:
#   BUILD_DIR    the build to take the binaries from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/firehose-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binaries inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#
# What the tests read, all set here: CLINK_FIREHOSE_TEST_ENDPOINT,
# CLINK_FIREHOSE_RECOVERY_LOG_DIR, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
# AWS_DEFAULT_REGION, AWS_REGION, AWS_EC2_METADATA_DISABLED.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/firehose-live-reports}"
PROJECT=clink-firehose-live
COMPOSE=(docker compose -p "${PROJECT}" -f "${ROOT}/docker/integration-services.yml")

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}
AWS_BIN="$(find_bin clink_aws_tests)"
IT_BIN="$(find_bin clink_integration_tests)"
NODE_BIN="$(find_bin clink_node)"
if [ -z "${AWS_BIN}" ] || [ -z "${IT_BIN}" ] || [ -z "${NODE_BIN}" ]; then
    echo "firehose-live: clink_aws_tests, clink_integration_tests and clink_node must be built" \
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
    if "${COMPOSE[@]}" run -d --name "${container}" -e SERVICES=firehose,s3,sts,iam,kinesis \
        -p "127.0.0.1:${candidate}:4566" localstack >/dev/null; then
        port="${candidate}"
        break
    fi
    echo "firehose-live: LocalStack did not start on port ${candidate} (attempt ${attempt})" >&2
done
if [ -z "${port}" ]; then
    echo "firehose-live: LocalStack did not start" >&2
    status=1
    exit 1
fi
endpoint="http://127.0.0.1:${port}"

# The Firehose API answers a ListDeliveryStreams call once the emulator's
# Firehose is up. LocalStack does not check signatures; the header only names
# the region.
ready=0
for _ in $(seq 1 180); do
    if curl -fsS --max-time 60 -X POST "${endpoint}/" \
        -H 'Content-Type: application/x-amz-json-1.1' \
        -H 'X-Amz-Target: Firehose_20150804.ListDeliveryStreams' \
        -H 'Authorization: AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/firehose/aws4_request, SignedHeaders=host, Signature=0' \
        -d '{}' >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "firehose-live: the emulator's Firehose API never answered at ${endpoint}" >&2
    status=1
    exit 1
fi

export CLINK_FIREHOSE_TEST_ENDPOINT="${endpoint}"
export CLINK_FIREHOSE_RECOVERY_LOG_DIR="${REPORT_DIR}/recovery-logs"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_AWS CLINK_INTEGRATION_TESTS CLINK_ENABLE_FAULT_INJECTION \
        aws-cpp-sdk-firehose_DIR aws-cpp-sdk-s3_DIR; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "emulator: LocalStack $(curl -fsS "${endpoint}/_localstack/health" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("version", "?"))')"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "evidence: FirehoseLimits, FirehoseResponse and FirehoseSink* cases are deterministic" \
        "(the sink's write path against a model of PutRecordBatch); FirehoseLive and" \
        "FirehoseRecovery ran against the emulator, which delivers to S3 at once, without" \
        "Firehose's buffering, retries or retention; nothing here ran against AWS"
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

run firehose "${AWS_BIN}" --gtest_filter='Firehose*'
run recovery "${IT_BIN}" --gtest_filter='FirehoseRecovery.*'
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'FirehoseLimits.*=2' \
    --expect 'FirehoseResponse.*=2' \
    --expect 'FirehoseSinkConfig.*=3' \
    --expect 'FirehoseSinkRecords.*=5' \
    --expect 'FirehoseSinkWrites.*=5' \
    --expect 'FirehoseSinkResponses.*=1' \
    --expect 'FirehoseSinkFailures.*=2' \
    --expect 'FirehoseSinkCancel.*=2' \
    --expect 'FirehoseSinkLifecycle.*=5' \
    --expect 'FirehoseErrorClassification.*=1' \
    --expect 'FirehoseLive.*=5' \
    --expect 'FirehoseRecovery.*=3' \
    "${REPORT_DIR}/firehose.json" "${REPORT_DIR}/recovery.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
