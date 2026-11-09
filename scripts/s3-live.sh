#!/usr/bin/env bash
# The S3 connector's live gate, against SILO, a maintained fork of the MinIO
# server, pinned by digest as tests/integration/docker_minio.hpp pins it: the raw
# object modes (the text sink, the multipart two-phase commit sink and the C++
# source), the Parquet modes, and Iceberg tables on an S3 warehouse; each phase
# followed by a check that nothing was skipped and that every suite ran in full.
#
#   scripts/s3-live.sh [build-dir]
#   S3_LIVE_PHASES=parquet scripts/s3-live.sh   # one phase
#
# It starts one SILO container on a free loopback port, waits until the store
# answers its readiness route, creates the test bucket, and runs
# clink_s3_tests' suites. The raw phase:
#   - without the store: S3FactoryRegistration (the factories, the strict
#     options, the records), S3SinkReal, S3SourceReal (the source's checkpoint
#     forms), S3Sink2PC (the committable's codec, a refused part) and
#     StreamEnvBuilder;
#   - against it: S3SourceLive (every page of a listing, the restores by key, a
#     store named by host), S3TextSinkLive (each subtask's parts and a restored
#     run's numbering), S3Sink2PCLive (a commit, a staged upload recovered by
#     another sink, an abort, a repeated commit, an empty interval, a later run
#     reusing a checkpoint id) and the sink contract suite for the 2PC sink.
# The parquet phase: ParquetS3Sink, ParquetS3Source and ParquetS3Factories
# without the store; ParquetRollingS3 (parts that survive a restore, a kill that
# leaves no partial object, the Row channel's typed columns), S3TypedSink, and
# ParquetS3Live, through the factories: the multi-object source's restore over
# the same objects and after one was removed, and the exactly-once sink's
# commit, its recovery of a prepared handle, and a recovery that finds neither
# the staged nor the committed object.
# The iceberg phase: clink_iceberg_tests in full but for the REST catalog case,
# which needs a catalog server: the sink's appends, two-phase commits,
# recoveries, partitions and upserts and the source's scans on a local
# warehouse, and IcebergS3Live, a table whose data and metadata live on the
# store, written and opened again.
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
#   S3_LIVE_PHASES  the phases to run, default "raw parquet iceberg"
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
S3_LIVE_PHASES="${S3_LIVE_PHASES:-raw parquet iceberg}"
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

# The Parquet suites write through Arrow's S3 filesystem, which does not create
# a bucket, so the gate does, signed as S3 expects.
if ! curl -fsS --max-time 10 -X PUT --aws-sigv4 "aws:amz:us-east-1:s3" \
    --user "${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}" \
    "${endpoint}/${CLINK_S3_TEST_BUCKET}" >/dev/null; then
    echo "s3-live: could not create the bucket ${CLINK_S3_TEST_BUCKET}" >&2
    status=1
    exit 1
fi

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

run() {  # <phase> <gtest filter> [binary, default clink_s3_tests]
    echo "==> $1"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "${3:-${S3_BIN}}" --gtest_output="json:${REPORT_DIR}/$1.json" \
        --gtest_filter="$2" 2>&1 | tee "${REPORT_DIR}/$1.log" || status=1
}

for phase in ${S3_LIVE_PHASES}; do
    case "${phase}" in
    raw)
        run raw 'S3FactoryRegistration.*:S3SinkReal.*:S3SourceReal.*:S3SourceLive.*:S3Sink2PC.*:S3Sink2PCLive.*:S3TextSinkLive.*:S3Sink2PC/SinkContractSuite/*:StreamEnvBuilder.*'
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
        ;;
    parquet)
        run parquet 'ParquetS3Sink.*:ParquetS3Source.*:ParquetS3Factories.*:ParquetRollingS3.*:S3TypedSink.*:ParquetS3Live.*'
        python3 "${ROOT}/scripts/check-gtest-reports.py" \
            --expect 'ParquetS3Sink.*=5' \
            --expect 'ParquetS3Source.*=3' \
            --expect 'ParquetS3Factories.*=1' \
            --expect 'ParquetRollingS3.*=2' \
            --expect 'S3TypedSink.*=1' \
            --expect 'ParquetS3Live.*=4' \
            "${REPORT_DIR}/parquet.json" || status=1
        ;;
    iceberg)
        ice_bin="$(find "${BUILD_DIR}" -name clink_iceberg_tests -type f -perm -u+x 2>/dev/null | head -1)"
        if [ -z "${ice_bin}" ]; then
            echo "s3-live: clink_iceberg_tests must be built under ${BUILD_DIR}" >&2
            status=1
            continue
        fi
        run iceberg '-IcebergRestLive.*' "${ice_bin}"
        python3 "${ROOT}/scripts/check-gtest-reports.py" \
            --expect 'IcebergSink.*=3' \
            --expect 'IcebergSink2PC.*=8' \
            --expect 'IcebergSinkEndOfInput.*=3' \
            --expect 'IcebergSinkPartitioned.*=2' \
            --expect 'IcebergUpsert.*=4' \
            --expect 'IcebergSource.*=4' \
            --expect 'IcebergFactoryRegistration.*=2' \
            --expect 'IcebergStateExport.*=1' \
            --expect 'IcebergS3Live.*=1' \
            "${REPORT_DIR}/iceberg.json" || status=1
        ;;
    *)
        echo "s3-live: no phase '${phase}' (raw, parquet, iceberg)" >&2
        status=1
        ;;
    esac
done
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
