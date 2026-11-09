#!/usr/bin/env bash
# The Parquet modes' live gate on the object stores other than S3, each against
# an emulator pinned by digest (the same images docker/integration-services.yml
# pins): Google Cloud Storage against fake-gcs-server and Azure Blob Storage
# against Azurite. scripts/s3-live.sh's parquet phase is S3's. Each phase runs
# the store's test binary in full, the cases that need no store and the live
# ones, then a check that nothing was skipped and that every suite ran in full.
#
#   scripts/object-store-parquet-live.sh [build-dir]
#   OBJECT_STORE_PHASES=azure scripts/object-store-parquet-live.sh   # one phase
#
# The gcs phase starts fake-gcs-server on a free loopback port, with the public
# host it names in upload redirects set to that port, and runs clink_gcs_tests:
# GcsFactoryRegistration, ParquetGcsSink and ParquetGcsSource without the store,
# and GcsParquetLive (a write and read back, the rolling sink never publishing
# an unfinished part, the Row channel's typed columns). The azure phase starts
# Azurite and runs clink_azure_tests, the same shape: AzureFactoryRegistration,
# ParquetAzureSink, ParquetAzureSource and AzureParquetLive. Each container goes
# when its phase ends.
#
# A live case skips when its endpoint is not named and googletest counts that as
# a pass, so scripts/check-gtest-reports.py fails the run on any skip and on a
# suite that ran fewer cases than it has. A missing binary, an emulator that
# never answers and a failed case fail it too.
#
# What it keeps in REPORT_DIR: each phase's googletest report and output, and
# provenance.txt (the commit, whether the tree was clean, each emulator's
# image), plus an emulator's log when its phase fails. Azurite's account and key
# are its published development defaults, which the tests carry.
#
# Knobs:
#   BUILD_DIR            the build to take the binaries from (argument or default build/)
#   REPORT_DIR           where the reports go (default <build-dir>/object-store-parquet-live-reports)
#   OBJECT_STORE_PHASES  the phases to run, default "gcs azure"
#   PIN_RUNNER           a prefix for every test command (CI runs the binaries inside
#                        the toolchain image with `docker run --network host ... -e
#                        CLINK_GCS_TEST_ENDPOINT -e CLINK_AZURE_TEST_ENDPOINT`)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${1:-${BUILD_DIR:-${ROOT}/build}}"
REPORT_DIR="${REPORT_DIR:-${BUILD_DIR}/object-store-parquet-live-reports}"
OBJECT_STORE_PHASES="${OBJECT_STORE_PHASES:-gcs azure}"
GCS_IMAGE="fsouza/fake-gcs-server@sha256:797ce226d62f947c009dc40246b30cfb456b8473d8241407f9d6f2c04e4d69ef"
AZURITE_IMAGE="mcr.microsoft.com/azure-storage/azurite@sha256:830430c1da1a2d537e08f3e6764dd1f5ae00cf0346bcaf625b968ec3f0971fd5"

find_bin() {
    find "${BUILD_DIR}" -name "$1" -type f -perm -u+x 2>/dev/null | head -1
}

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR:?}"/*.json "${REPORT_DIR:?}"/*.log "${REPORT_DIR:?}/provenance.txt"
# The toolchain image's user is not the runner's; let either write here.
chmod -R a+rwx "${REPORT_DIR}"

status=0
container=""
# shellcheck disable=SC2329 # run by the EXIT trap and at each phase's end
stop_container() {
    if [ -n "${container}" ]; then
        if [ "${status}" -ne 0 ]; then
            docker logs "${container}" >"${REPORT_DIR}/${container}.log" 2>&1 || true
        fi
        docker rm -fv "${container}" >/dev/null 2>&1 || true
        container=""
    fi
}
trap stop_container EXIT

free_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

# start <name> <container port> <image and arguments, where PORT stands for
# the host port>: starts the container on a free loopback port, trying again on
# another when Docker's allocator refuses one, and sets `port`.
start() {
    local name="$1" inner="$2"
    shift 2
    port=""
    for attempt in 1 2 3; do
        local candidate args=()
        candidate="$(free_port)"
        for a in "$@"; do
            args+=("${a//PORT/${candidate}}")
        done
        docker rm -fv "${name}" >/dev/null 2>&1 || true
        if docker run -d --name "${name}" -p "127.0.0.1:${candidate}:${inner}" "${args[@]}" >/dev/null; then
            port="${candidate}"
            container="${name}"
            return 0
        fi
        echo "object-store-parquet-live: ${name} did not start on port ${candidate} (attempt ${attempt})" >&2
    done
    return 1
}

# await <description> <command...>: up to 90 s for the command to succeed.
await() {
    local what="$1"
    shift
    for _ in $(seq 1 90); do
        if "$@" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "object-store-parquet-live: ${what} never answered" >&2
    return 1
}

run() {  # <phase> <binary>
    echo "==> $1"
    # shellcheck disable=SC2086 # PIN_RUNNER is a command prefix, split on purpose
    ${PIN_RUNNER:-} "$2" --gtest_output="json:${REPORT_DIR}/$1.json" \
        2>&1 | tee "${REPORT_DIR}/$1.log" || status=1
}

{
    echo "commit: $(git -C "${ROOT}" rev-parse HEAD)"
    echo "tree: $( [ -z "$(git -C "${ROOT}" status --porcelain --untracked-files=no)" ] && echo clean || echo "modified tracked files")"
    echo "build dir: ${BUILD_DIR}"
    grep -E "^(CMAKE_BUILD_TYPE|CLINK_WITH_GCS|CLINK_WITH_AZURE):" "${BUILD_DIR}/CMakeCache.txt" 2>/dev/null || true
    echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"${REPORT_DIR}/provenance.txt"

for phase in ${OBJECT_STORE_PHASES}; do
    case "${phase}" in
    gcs)
        bin="$(find_bin clink_gcs_tests)"
        if [ -z "${bin}" ]; then
            echo "object-store-parquet-live: clink_gcs_tests must be built under ${BUILD_DIR}" >&2
            status=1
            continue
        fi
        if ! start clink-gcs-live 4443 "${GCS_IMAGE}" -scheme http -host 0.0.0.0 -port 4443 \
            -public-host 127.0.0.1:PORT; then
            status=1
            continue
        fi
        if ! await "fake-gcs-server" curl -fsS --max-time 5 "http://127.0.0.1:${port}/storage/v1/b"; then
            status=1
            stop_container
            continue
        fi
        echo "gcs: ${GCS_IMAGE}" >>"${REPORT_DIR}/provenance.txt"
        export CLINK_GCS_TEST_ENDPOINT="127.0.0.1:${port}"
        run gcs "${bin}"
        python3 "${ROOT}/scripts/check-gtest-reports.py" \
            --expect 'GcsFactoryRegistration.*=3' \
            --expect 'ParquetGcsSink.*=2' \
            --expect 'ParquetGcsSource.*=2' \
            --expect 'GcsParquetLive.*=3' \
            "${REPORT_DIR}/gcs.json" || status=1
        stop_container
        ;;
    azure)
        bin="$(find_bin clink_azure_tests)"
        if [ -z "${bin}" ]; then
            echo "object-store-parquet-live: clink_azure_tests must be built under ${BUILD_DIR}" >&2
            status=1
            continue
        fi
        if ! start clink-azurite-live 10000 "${AZURITE_IMAGE}" azurite --blobHost 0.0.0.0 \
            --blobPort 10000 --skipApiVersionCheck; then
            status=1
            continue
        fi
        # Azurite answers an unauthenticated request with an error status, but
        # it answers: any HTTP reply means the blob service is up.
        if ! await "Azurite" curl -sS --max-time 5 -o /dev/null "http://127.0.0.1:${port}/"; then
            status=1
            stop_container
            continue
        fi
        echo "azure: ${AZURITE_IMAGE}" >>"${REPORT_DIR}/provenance.txt"
        export CLINK_AZURE_TEST_ENDPOINT="127.0.0.1:${port}"
        run azure "${bin}"
        python3 "${ROOT}/scripts/check-gtest-reports.py" \
            --expect 'AzureFactoryRegistration.*=3' \
            --expect 'ParquetAzureSink.*=2' \
            --expect 'ParquetAzureSource.*=2' \
            --expect 'AzureParquetLive.*=3' \
            "${REPORT_DIR}/azure.json" || status=1
        stop_container
        ;;
    *)
        echo "object-store-parquet-live: no phase '${phase}' (gcs, azure)" >&2
        status=1
        ;;
    esac
done
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status}" >>"${REPORT_DIR}/provenance.txt"
exit "${status}"
