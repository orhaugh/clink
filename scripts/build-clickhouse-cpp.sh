#!/usr/bin/env bash
# Build the pinned clickhouse-cpp client (scripts/versions.env) as static archives, in
# Release, with OpenSSL, into ${CLICKHOUSE_CPP_PREFIX:-${CLINK_DEPS_PREFIX}/clickhouse-cpp}.
#
# Release whatever the caller's build type: the image builds its toolchain in Debug,
# and a Debug client is several times slower at encoding blocks, which would make
# every sink throughput figure a statement about the client's build rather than the
# sink. On the host it gets its own directory under the prefix rather than the
# prefix's lib/, so Homebrew's copy stays where it is and CMake picks this one
# deliberately (impls/clickhouse looks there first). The image passes
# CLICKHOUSE_CPP_PREFIX=/usr/local, the default search path, where the connector's
# ordinary probe finds it.
#
# abseil, lz4 and zstd come from the system (Homebrew on macOS, Debian's -dev packages
# in the image), the same copies the rest of the binary links; cityhash is bundled, as
# no platform packages it. OpenSSL is the one the rest of the build links: on macOS
# openssl@3 unless OPENSSL_ROOT_DIR says otherwise. Idempotent: a stamp skips a
# rebuild of the same version.
set -euo pipefail

# versions.env sits beside this script both in the tree (scripts/) and in the image
# layer that runs it (/tmp/clink-sys, where the Dockerfile copies the scripts flat).
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=versions.env
. "${HERE}/versions.env"
V="${CLICKHOUSE_CPP_VERSION}"
OUT="${CLICKHOUSE_CPP_PREFIX:-${CLINK_DEPS_PREFIX}/clickhouse-cpp}"
JOBS="${CLINK_BUILD_JOBS:-8}"

stamp="${OUT}/.clink-clickhouse-cpp-${V}"
if [ -f "${stamp}" ]; then
    echo "build-clickhouse-cpp: ${V} already built in ${OUT}"
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
TARBALL="${WORK}/clickhouse-cpp-${V}.tar.gz"
curl -fsSL --retry 3 -o "${TARBALL}" \
    "https://github.com/ClickHouse/clickhouse-cpp/archive/refs/tags/v${V}.tar.gz"
got="$( (sha256sum "${TARBALL}" 2>/dev/null || shasum -a 256 "${TARBALL}") | awk '{print $1}')"
if [ "${got}" != "${CLICKHOUSE_CPP_SHA256}" ]; then
    echo "build-clickhouse-cpp: checksum mismatch for v${V}: got ${got}, pinned ${CLICKHOUSE_CPP_SHA256}" >&2
    exit 1
fi
tar -xzf "${TARBALL}" -C "${WORK}"
SRC="${WORK}/clickhouse-cpp-${V}"

ssl_args=()
if [ -n "${OPENSSL_ROOT_DIR:-}" ]; then
    ssl_args+=("-DOPENSSL_ROOT_DIR=${OPENSSL_ROOT_DIR}")
elif [ "$(uname -s)" = "Darwin" ] && command -v brew >/dev/null 2>&1; then
    ssl_args+=("-DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)")
fi

cmake -S "${SRC}" -B "${WORK}/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${OUT}" \
    -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DWITH_OPENSSL=ON \
    -DWITH_SYSTEM_ABSEIL=ON \
    -DWITH_SYSTEM_LZ4=ON \
    -DWITH_SYSTEM_ZSTD=ON \
    -DWITH_SYSTEM_CITYHASH=OFF \
    -DDEBUG_DEPENDENCIES=OFF \
    "${ssl_args[@]}"
cmake --build "${WORK}/build" --parallel "${JOBS}"
cmake --install "${WORK}/build"
mkdir -p "${OUT}"
touch "${stamp}"
echo "build-clickhouse-cpp: installed static clickhouse-cpp ${V} (Release, TLS) -> ${OUT}"
