#!/usr/bin/env bash
# build-arrow.sh - compile + install Apache Arrow + Parquet FROM SOURCE at the pinned
# version (scripts/versions.env) into CLINK_DEPS_PREFIX, with the feature set clink + its
# deps need: Parquet, S3 (impls/s3 + the Iceberg S3 FileIO), GCS (impls/gcs - bundles
# google-cloud-cpp storage), Azure (impls/azure - bundles azure-sdk-for-cpp; needs system
# libxml2 at build time), Compute, IPC, every Parquet compression codec, and JSON
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

# Arrow's object-store filesystems (S3, GCS, Azure) are ON for every normal build:
# impls/s3, impls/gcs, impls/azure, the Iceberg S3 FileIO and the disaggregated
# state backends all need them. CLINK_ARROW_OBJECT_STORES=OFF drops all three,
# and with them their whole dependency chains - bundled aws-sdk + aws-c-*, and the
# OpenSSL that google-cloud-cpp and the Azure SDK pull in.
#
# That is what the pyclink wheel wants. Those dylibs would otherwise be vendored
# into the wheel, and each Homebrew-built one carries the BUILD HOST's minimum
# macOS version, so delocate refuses them against a lower wheel floor. All three
# are impls/ connectors, which the wheel does not build anyway.
#
# The three move together on purpose: dropping S3 alone still leaves OpenSSL
# arriving via GCS and Azure, which was exactly the second half of this problem.
# clink's own Arrow-S3 usage is guarded on Arrow's ARROW_S3 macro (see
# include/clink/connectors/arrow_s3_lifecycle.hpp) so an object-store-less Arrow
# still links, and s3:// then fails with an explanation.
ARROW_OBJ_SETTING="${CLINK_ARROW_OBJECT_STORES:-ON}"
case "${ARROW_OBJ_SETTING}" in
    ON|OFF) ;;
    *) echo "build-arrow: CLINK_ARROW_OBJECT_STORES must be ON or OFF (got '${ARROW_OBJ_SETTING}')" >&2; exit 2 ;;
esac
# Feature stamp beside the install. The version check below cannot tell a
# full-featured prefix from a stripped one, and silently reusing the wrong one is
# a confusing way to lose whole filesystems, so a mismatch forces the rebuild.
stamp_file="${PREFIX}/.clink-arrow-features"
# The dependency-resolution mode is part of the identity too: with the object
# stores off Arrow is built with NO Homebrew prefix hint, so its brotli/zstd/etc
# are its own, compiled at the floor. A prefix built with the hint has different
# (and, for a portable wheel, wrong) transitive libraries even though the version
# and the object-store setting match, so the stamp must tell them apart.
if [ "${ARROW_OBJ_SETTING}" = "ON" ]; then
    ARROW_DEPS_MODE="system-hints"
else
    ARROW_DEPS_MODE="hermetic"
fi
stamp_want="arrow=${ARROW_VERSION} object_stores=${ARROW_OBJ_SETTING} deps=${ARROW_DEPS_MODE}"

ver_file="${PREFIX}/lib/cmake/Arrow/ArrowConfigVersion.cmake"
if [ -f "${ver_file}" ] && grep -q "\"${ARROW_VERSION}\"" "${ver_file}" 2>/dev/null; then
    if [ "$(cat "${stamp_file}" 2>/dev/null)" = "${stamp_want}" ]; then
        echo "build-arrow: Arrow ${ARROW_VERSION} (object stores=${ARROW_OBJ_SETTING}) already installed at ${PREFIX}; skipping."
        exit 0
    fi
    # A prefix installed before this stamp existed is a full-featured build (the
    # only thing that was ever produced), so honour it rather than rebuilding.
    if [ ! -f "${stamp_file}" ] && [ "${ARROW_OBJ_SETTING}" = "ON" ]; then
        echo "${stamp_want}" > "${stamp_file}"
        echo "build-arrow: Arrow ${ARROW_VERSION} already installed at ${PREFIX}; stamped object_stores=ON; skipping."
        exit 0
    fi
    echo "build-arrow: ${PREFIX} holds Arrow ${ARROW_VERSION} with different features" \
         "($(cat "${stamp_file}" 2>/dev/null || echo 'object_stores=ON (unstamped)')); rebuilding for ${stamp_want}."
fi

# Fast path: restore a prebuilt toolchain artifact (Arrow + Parquet +
# iceberg-cpp in one archive) instead of the source build, when one exists
# for this platform and pin. Set CLINK_DEPS_FROM_SOURCE=1 to force the
# source build. fetch-deps.sh exits 3 for "no artifact / guard declined"
# (fall back to source) and nonzero-else for hard failures such as a
# checksum mismatch, which must NOT be papered over by a silent rebuild.
# The existence check matters: contexts that copy scripts selectively
# (e.g. the Docker toolchain layer) may run this without fetch-deps.sh.
# The published archives are all full-featured builds, so an OFF request must not
# take this path - it would restore exactly what it is trying to avoid.
if [ "${ARROW_OBJ_SETTING}" = "OFF" ]; then
    echo "build-arrow: CLINK_ARROW_OBJECT_STORES=OFF; skipping the prebuilt fast path (archives carry them)."
elif [ "${CLINK_DEPS_FROM_SOURCE:-0}" != "1" ] && [ -f "${HERE}/fetch-deps.sh" ]; then
    fetch_rc=0
    "${HERE}/fetch-deps.sh" || fetch_rc=$?
    if [ "${fetch_rc}" -eq 0 ]; then
        exit 0
    elif [ "${fetch_rc}" -ne 3 ]; then
        echo "build-arrow: fetch-deps.sh failed hard (exit ${fetch_rc}); aborting." >&2
        exit "${fetch_rc}"
    fi
    echo "build-arrow: no usable prebuilt; building from source."
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
    # The Homebrew prefix hint exists for ONE reason: the system aws-sdk that
    # ARROW_S3 needs. With the object stores off there is nothing there Arrow
    # should use, and pointing at it is actively harmful for a portable build -
    # Arrow would resolve brotli / zstd / OpenSSL to Homebrew bottles, which on a
    # current runner are built for that runner's macOS and pin the resulting
    # library above the wheel's floor ("ld: warning: building for macOS-14.0, but
    # linking with dylib ... built for newer version 26.0", and then delocate
    # refuses them). Without the hint, ARROW_DEPENDENCY_SOURCE=BUNDLED compiles
    # those deps itself at CMAKE_OSX_DEPLOYMENT_TARGET.
    if [ "${ARROW_OBJ_SETTING}" = "ON" ]; then
        EXTRA_ARGS+=(-DCMAKE_PREFIX_PATH=/opt/homebrew)
    else
        echo "build-arrow: object stores off; NOT pointing Arrow at /opt/homebrew (deps stay bundled at the floor)."
    fi
    # Pin the macOS floor when set (CI builds portable wheels): the static Arrow
    # objects linked into a self-contained libclink must not pin minos above the
    # wheel's tag. Unset locally keeps the host default.
    if [ -n "${MACOSX_DEPLOYMENT_TARGET:-}" ]; then
        EXTRA_ARGS+=("-DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET}")
    fi
fi

echo "build-arrow: configuring (BUNDLED data-path deps, SYSTEM aws-sdk, Parquet+Compute, object stores=${ARROW_OBJ_SETTING}) -> ${PREFIX}"
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
    -DARROW_S3="${ARROW_OBJ_SETTING}" \
    -DARROW_GCS="${ARROW_OBJ_SETTING}" \
    -DARROW_AZURE="${ARROW_OBJ_SETTING}" \
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
echo "${stamp_want}" > "${stamp_file}"
echo "build-arrow: installed Arrow + Parquet ${ARROW_VERSION} (object stores=${ARROW_OBJ_SETTING}) -> ${PREFIX}"
