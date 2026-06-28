#!/usr/bin/env bash
# build-iceberg-cpp.sh - compile + install Apache iceberg-cpp FROM SOURCE at the pinned
# tag (scripts/versions.env) into CLINK_DEPS_PREFIX, against the from-source Arrow that
# build-arrow.sh installed into the same prefix. This is the SINGLE recipe both the
# macOS host and the Debian image use, so the prebuilt iceberg static libs match.
#
# Critical flags (learned the hard way - see docs/lakehouse-research.md):
#   ICEBERG_BUILD_BUNDLE=ON      the Arrow/Parquet/Avro WRITE backend (MakeLocalFileIO,
#                                MakeS3FileIO, the parquet/avro writer factories,
#                                RegisterAll) lives ONLY in libiceberg_bundle.a.
#   ICEBERG_S3=ON                the S3 FileIO (s3:// warehouses); needs the bundle +
#                                Arrow built with S3 (build-arrow.sh does).
#   ICEBERG_BUILD_SQL_CATALOG/SQLITE  the offline-testable SQLite catalog.
#   ICEBERG_BUILD_REST=ON (default)   the REST catalog client (Polaris/Nessie/...).
#   CMAKE_DISABLE_FIND_PACKAGE_avro-cpp=ON  iceberg-cpp 0.3.0's avro_writer.cc does NOT
#                                compile against avro-cpp 1.12+ (codec API change); this
#                                forces its pinned, vendored Avro instead.
# Idempotent + resumable (stable build dir).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${HERE}/versions.env"
PREFIX="${CLINK_DEPS_PREFIX:?CLINK_DEPS_PREFIX must be set}"

if [ -f "${PREFIX}/lib/libiceberg_bundle.a" ] && \
   [ -f "${PREFIX}/lib/cmake/iceberg/iceberg-config.cmake" ]; then
    echo "build-iceberg-cpp: iceberg-cpp already installed at ${PREFIX}; skipping."
    exit 0
fi

if [ -n "${CLINK_BUILD_JOBS:-}" ]; then
    JOBS="${CLINK_BUILD_JOBS}"
elif [ "$(uname -s)" = "Darwin" ]; then
    JOBS=10
else
    JOBS="$(nproc)"
fi

SRC_ROOT="${PREFIX}/src"
SRC_DIR="${SRC_ROOT}/iceberg-cpp-${ICEBERG_CPP_TAG}"
BUILD_DIR="${SRC_DIR}/build"
mkdir -p "${SRC_ROOT}"

if [ ! -d "${SRC_DIR}/src" ]; then
    echo "build-iceberg-cpp: cloning iceberg-cpp ${ICEBERG_CPP_TAG}..."
    rm -rf "${SRC_DIR}"
    git clone --depth 1 --branch "${ICEBERG_CPP_TAG}" \
        https://github.com/apache/iceberg-cpp.git "${SRC_DIR}"
fi

echo "build-iceberg-cpp: configuring (bundle + S3 + sqlite catalog) against Arrow at ${PREFIX}"
# --compile-no-warning-as-error: iceberg-cpp 0.3.0 forces CMAKE_COMPILE_WARNING_AS_ERROR=ON
# (a plain set(), so -D cannot override it), and gcc 14 (Debian trixie) raises a
# -Werror=free-nonheap-object FALSE POSITIVE in json_serde.cc. It is a dependency, so we
# do not want its warnings to be errors; this flag overrides the property cross-platform.
cmake -S "${SRC_DIR}" -B "${BUILD_DIR}" \
    --compile-no-warning-as-error \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
    -DCMAKE_PREFIX_PATH="${PREFIX}" \
    -DICEBERG_BUILD_STATIC=ON \
    -DICEBERG_BUILD_SHARED=OFF \
    -DICEBERG_BUILD_BUNDLE=ON \
    -DICEBERG_S3=ON \
    -DICEBERG_BUILD_SQL_CATALOG=ON \
    -DICEBERG_SQL_SQLITE=ON \
    -DICEBERG_BUILD_TESTS=OFF \
    -DCMAKE_DISABLE_FIND_PACKAGE_avro-cpp=ON

echo "build-iceberg-cpp: building with ${JOBS} jobs"
cmake --build "${BUILD_DIR}" --parallel "${JOBS}"
cmake --install "${BUILD_DIR}"
echo "build-iceberg-cpp: installed iceberg-cpp ${ICEBERG_CPP_TAG} -> ${PREFIX}"
