#!/usr/bin/env bash
# The raw S3 connector's live gate: the text sink, the multipart two-phase
# commit sink and the C++ source, against SILO, a maintained fork of the MinIO
# server, pinned by digest as tests/integration/docker_minio.hpp pins it; then a
# check that nothing was skipped and that every suite ran in full.
#
#   scripts/s3-live.sh [build-dir]
#
# It starts one SILO container on a free loopback port, waits until the store
# answers its readiness route, and runs clink_s3_tests' raw object suites:
#   - without the store: S3FactoryRegistration (the factories, the strict
#     options, the records), S3SinkReal, S3SourceReal (the source's checkpoint
#     forms), S3Sink2PC (the committable's codec, a refused part) and
#     StreamEnvBuilder;
#   - against it: S3SourceLive (every page of a listing, the restores by key, a
#     store named by host), S3TextSinkLive (each subtask's parts and a restored
#     run's numbering), S3Sink2PCLive (a commit, a staged upload recovered by
#     another sink, an abort, a repeated commit, an empty interval, a later run
#     reusing a checkpoint id) and the sink contract suite for the 2PC sink.
# The cluster-level exactly-once suite for the 2PC sink, S3ExactlyOnceTest, runs
# in scripts/docker-suites-live.sh. The container goes, with its volume, when the
# gate ends.
#
# A live case skips when its store is not named and googletest counts that as a
# pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it has. A missing binary, a store that never
# answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR: the googletest report and output, provenance.txt
# (the commit, whether the tree was clean, the build's configuration, the store's
# image and version) and, on failure, the store's log. The store's credentials
# are fixed test values the gate sets, and it records none of them.
#
# Knobs:
#   BUILD_DIR    the build to take the binary from (argument or default build/)
#   REPORT_DIR   where the reports go (default <build-dir>/s3-live-reports)
#   PIN_RUNNER   a prefix for every test command (CI runs the binary inside the
#                toolchain image with `docker run --network host ... -e <each
#                variable below>`)
#
# What the tests read, all set here: CLINK_S3_TEST_ENDPOINT, CLINK_S3_TEST_BUCKET,
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_REGION,
# AWS_EC2_METADATA_DISABLED.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/s3-live-reports}"
IMAGE="pgsty/silo:RELEASE.2026-10-18T00-00-00Z@sha256:2636fc4155d9a7e41ff58cdb65ddd13ba9304e67184e13c25bc5cb34016269d7"

S3_BIN="$(find "${BUILD_DIR}" -name clink_s3_tests -type f -perm -u+x 2>/dev/null | head -1)"
if [ -z "${S3_BIN}" ]; then
    echo "s3-live: clink_s3_tests must be built under ${BUILD_DIR}" >&2
    exit 1
fi

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/provenance.txt"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container=clink-s3-live-silo
# shellcheck disable=SC2329 # run by the EXIT trap
cleanup() {
    if [ "${status}" -ne 0 ]; then
        docker logs "${container}" >"${REPORT_DIR}/silo.log" 2>&1 || true
    fi
    docker rm -fv "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

free_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

export AWS_ACCESS_KEY_ID=clink-s3-live AWS_SECRET_ACCESS_KEY=clink-s3-live-secret

# Docker's port allocator can hold a port the host shows free and then refuse
# to bind it, so a refused start is tried again on another port.
port=""
for attempt in 1 2 3; do
    candidate="$(free_port)"
    docker rm -fv "${container}" >/dev/null 2>&1 || true
    if docker run -d --name "${container}" -p "127.0.0.1:${candidate}:9000" \
        -e "MINIO_ROOT_USER=${AWS_ACCESS_KEY_ID}" -e "MINIO_ROOT_PASSWORD=${AWS_SECRET_ACCESS_KEY}" \
        "${IMAGE}" server /data >/dev/null; then
        port="${candidate}"
        break
    fi
    echo "s3-live: SILO did not start on port ${candidate} (attempt ${attempt})" >&2
done
if [ -z "${port}" ]; then
    echo "s3-live: SILO did not start" >&2
    status=1
    exit 1
fi
endpoint="http://127.0.0.1:${port}"

ready=0
for _ in $(seq 1 90); do
    if curl -fsS --max-time 5 "${endpoint}/minio/health/ready" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
if [ "${ready}" != 1 ]; then
    echo "s3-live: the store never answered its readiness route at ${endpoint}" >&2
    status=1
    exit 1
fi

export CLINK_S3_TEST_ENDPOINT="${endpoint}" CLINK_S3_TEST_BUCKET=clink-s3-live
export AWS_DEFAULT_REGION=us-east-1 AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    for key in CMAKE_BUILD_TYPE CLINK_WITH_AWS_S3 AWSSDK_DIR aws-cpp-sdk-s3_DIR; do
        grep -E "^${key}:" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || echo "${key}: (unset)"
    done
    echo "store: $(docker exec "${container}" silo --version 2>/dev/null | head -1 || echo unknown)"
    echo "image: $(docker inspect --format '{{.Config.Image}} {{.Image}}' "${container}")"
    echo "evidence: the live cases ran against SILO, a fork of the MinIO server, not against AWS"
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

echo "==> raw"
# shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
${PIN_RUNNER:-} "${S3_BIN}" --gtest_output="json:${REPORT_DIR}/raw.json" \
    --gtest_filter='S3FactoryRegistration.*:S3SinkReal.*:S3SourceReal.*:S3SourceLive.*:S3Sink2PC.*:S3Sink2PCLive.*:S3TextSinkLive.*:S3Sink2PC/SinkContractSuite/*:StreamEnvBuilder.*' \
    2>&1 | tee "${REPORT_DIR}/raw.log" || status=1
python3 "${ROOT}/scripts/check-gtest-reports.py" \
    --expect 'S3FactoryRegistration.*=7' \
    --expect 'S3SinkReal.*=3' \
    --expect 'S3SourceReal.*=3' \
    --expect 'S3SourceLive.*=6' \
    --expect 'S3Sink2PC.*=3' \
    --expect 'S3Sink2PCLive.*=6' \
    --expect 'S3TextSinkLive.*=1' \
    --expect 'S3Sink2PC/SinkContractSuite/*=8' \
    --expect 'StreamEnvBuilder.*=1' \
    "${REPORT_DIR}/raw.json" || status=1
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
