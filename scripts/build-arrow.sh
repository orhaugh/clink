#!/usr/bin/env bash
# build-arrow.sh - compile + install Apache Arrow + Parquet FROM SOURCE at the pinned
# version (scripts/versions.env) into CLINK_DEPS_PREFIX, with the feature set clink + its
# deps need: Parquet, S3 (impls/s3 + the Iceberg S3 FileIO), GCS (impls/gcs - bundles
# google-cloud-cpp storage), Compute, IPC, every Parquet compression codec, and JSON
# (iceberg-cpp's avro module includes arrow/json - the host
# only got away without it because Homebrew's Arrow headers leaked in). Dropped vs a stock
# distro Arrow: Flight/Acero/Dataset/Gandiva (nothing links them) to keep the build bounded.
#
# Dependencies are BUNDLED (Arrow fetches + builds its own thrift/snappy/zstd/aws-sdk/...
# at Arrow-pinned versions) so the macOS host and the Debian image link byte-for-byte
# identical Arrow + transitive libs - that is the whole point of compiling from source.
#
# Both static AND shared Arrow are built (matches the prior Homebrew layout: clink core
# links arrow_shared, the iceberg-cpp bundle bakes in arrow_static). Idempotent: skips
# if the pinned version is already installed. Resumable: builds in a stable dir so a
# re-run continues incrementally after a failure.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${HERE}/versions.env"
PREFIX="${CLINK_DEPS_PREFIX:?CLINK_DEPS_PREFIX must be set}"

ver_file="${PREFIX}/lib/cmake/Arrow/ArrowConfigVersion.cmake"
if [ -f "${ver_file}" ] && grep -q "\"${ARROW_VERSION}\"" "${ver_file}" 2>/dev/null; then
    echo "build-arrow: Arrow ${ARROW_VERSION} already installed at ${PREFIX}; skipping."
    exit 0
fi

# Parallelism: cap the host (12-core MacBook) at 10 per project convention; use all
# cores in CI/Docker. Override with CLINK_BUILD_JOBS.
if [ -n "${CLINK_BUILD_JOBS:-}" ]; then
    JOBS="${CLINK_BUILD_JOBS}"
elif [ "$(uname -s)" = "Darwin" ]; then
    JOBS=10
else
    JOBS="$(nproc)"
fi

SRC_ROOT="${PREFIX}/src"
SRC_DIR="${SRC_ROOT}/arrow-apache-arrow-${ARROW_VERSION}"
BUILD_DIR="${SRC_DIR}/cpp/build"
mkdir -p "${SRC_ROOT}"

if [ ! -d "${SRC_DIR}/cpp" ]; then
    echo "build-arrow: fetching Apache Arrow ${ARROW_VERSION} source..."
    tarball="${SRC_ROOT}/arrow-${ARROW_VERSION}.tar.gz"
    curl -fsSL \
        "https://github.com/apache/arrow/archive/refs/tags/apache-arrow-${ARROW_VERSION}.tar.gz" \
        -o "${tarball}"
    tar -xzf "${tarball}" -C "${SRC_ROOT}"
    rm -f "${tarball}"
fi

# Arrow's S3 uses the SYSTEM aws-sdk-cpp + aws-c-* CRT, NOT Arrow 24's bundled AWS set:
# the bundled aws-c-http / aws-c-event-stream reference an aws_server_socket_channel_
# bootstrap_options.setup_callback field that the bundled aws-c-io version lacks (a
# mutually-inconsistent CRT pin) so the bundled AWS build does not compile. This is also
# exactly how Homebrew builds Arrow 24. The aws-sdk is a transport-layer dep only - it
# does not affect Arrow's data format / IPC / Parquet, so the data-path stays fully
# bundled + pinned (deterministic); only the S3 SDK is system. On macOS, point Arrow at
# Homebrew's aws-sdk; in Docker the image installs a system aws-sdk before this runs.
EXTRA_ARGS=()
if [ "$(uname -s)" = "Darwin" ]; then
    EXTRA_ARGS+=(-DCMAKE_PREFIX_PATH=/opt/homebrew)
fi

echo "build-arrow: configuring (BUNDLED data-path deps, SYSTEM aws-sdk, Parquet+S3+Compute) -> ${PREFIX}"
cmake -S "${SRC_DIR}/cpp" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DCMAKE_INSTALL_LIBDIR=lib \
    "${EXTRA_ARGS[@]}" \
    -DARROW_DEPENDENCY_SOURCE=BUNDLED \
    -DAWSSDK_SOURCE=SYSTEM \
    -DARROW_BUILD_SHARED=ON \
    -DARROW_BUILD_STATIC=ON \
    -DARROW_POSITION_INDEPENDENT_CODE=ON \
    -DARROW_PARQUET=ON \
    -DARROW_FILESYSTEM=ON \
    -DARROW_S3=ON \
    -DARROW_GCS=ON \
    -DARROW_COMPUTE=ON \
    -DARROW_JSON=ON \
    -DARROW_WITH_RAPIDJSON=ON \
    -DARROW_WITH_SNAPPY=ON \
    -DARROW_WITH_ZSTD=ON \
    -DARROW_WITH_LZ4=ON \
    -DARROW_WITH_ZLIB=ON \
    -DARROW_WITH_BROTLI=ON \
    -DARROW_WITH_BZ2=ON \
    -DARROW_WITH_UTF8PROC=ON \
    -DARROW_WITH_RE2=ON \
    -DARROW_DATASET=OFF \
    -DARROW_ACERO=OFF \
    -DARROW_FLIGHT=OFF \
    -DARROW_GANDIVA=OFF \
    -DARROW_BUILD_TESTS=OFF \
    -DARROW_BUILD_EXAMPLES=OFF \
    -DARROW_BUILD_BENCHMARKS=OFF \
    -DARROW_BUILD_UTILITIES=OFF \
    -DPARQUET_BUILD_EXECUTABLES=OFF

echo "build-arrow: building with ${JOBS} jobs (this is the long pole - bundled aws-sdk etc.)"
cmake --build "${BUILD_DIR}" --parallel "${JOBS}"
cmake --install "${BUILD_DIR}"
echo "build-arrow: installed Arrow + Parquet ${ARROW_VERSION} -> ${PREFIX}"
