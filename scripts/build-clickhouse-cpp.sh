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
# Default mode: abseil, lz4 and zstd come from the system (Homebrew on macOS, Debian's
# -dev packages in the image), the same copies the rest of the binary links; cityhash
# is bundled, as no platform packages it. OpenSSL is the one the rest of the build
# links: on macOS openssl@3 unless OPENSSL_ROOT_DIR says otherwise.
#
# Bundled mode, CLICKHOUSE_CPP_BUNDLED_DEPS=1, is for the self-contained Linux libclink
# (the manylinux wheel), where no static abseil, lz4 or OpenSSL exists on the system.
# It needs OPENSSL_ROOT_DIR, which the wheel points at the OpenSSL that
# scripts/build-librdkafka.sh stages under ${CLINK_DEPS_PREFIX}/openssl-static, and
# links it statically. abseil, lz4, zstd and cityhash are the client's own copies, with
# two changes after install:
#   * the client's public headers include absl/numeric/int128.h, which the bundled
#     abseil does not install, so its headers are copied into include/ beside
#     lib/libabsl_int128.a;
#   * lib/liblz4.a and lib/libzstdstatic.a are deleted. The client keeps compiling
#     against its bundled headers, and its LZ4_* and ZSTD_* references resolve at the
#     libclink link against the one copy Arrow already brings, so Arrow's own frame
#     code never runs against another version's block functions. The client uses
#     only the stable API of both.
# impls/clickhouse recognises this layout by its stamp and links it with no system
# dependency. The mode is refused unless the install directory sits strictly inside
# CLINK_DEPS_PREFIX, and never into /usr/local, because switching modes wipes it.
#
# Idempotent: a stamp per mode (.clink-clickhouse-cpp-<version> by default,
# .clink-clickhouse-cpp-<version>-bundled in bundled mode) skips a rebuild of the same
# version in the same mode. A directory that holds the other mode's stamp is wiped
# before installing, so nothing of one layout survives into the other. The mode is
# recorded before anything is installed, as a pending marker
# (.clink-clickhouse-cpp-pending, or .clink-clickhouse-cpp-pending-bundled) that the
# stamp replaces at the end, so an install that stops part way still says which
# layout it left behind and a later run in the other mode wipes it.
set -euo pipefail

# versions.env sits beside this script both in the tree (scripts/) and in the image
# layer that runs it (/tmp/clink-sys, where the Dockerfile copies the scripts flat).
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=versions.env
. "${HERE}/versions.env"
V="${CLICKHOUSE_CPP_VERSION}"
OUT="${CLICKHOUSE_CPP_PREFIX:-${CLINK_DEPS_PREFIX}/clickhouse-cpp}"
JOBS="${CLINK_BUILD_JOBS:-8}"
BUNDLED="${CLICKHOUSE_CPP_BUNDLED_DEPS:-0}"

case "${BUNDLED}" in
    0|1) ;;
    *) echo "build-clickhouse-cpp: CLICKHOUSE_CPP_BUNDLED_DEPS must be 0 or 1 (got '${BUNDLED}')" >&2; exit 2 ;;
esac

# An absolute path with symlinks resolved, for a path that need not exist yet: the
# deepest existing ancestor is resolved and the rest appended.
resolve_path() {
    local p="$1" rest=""
    case "${p}" in /*) ;; *) p="$(pwd)/${p}" ;; esac
    while [ ! -d "${p}" ]; do
        rest="/$(basename "${p}")${rest}"
        p="$(dirname "${p}")"
    done
    printf '%s%s\n' "$(cd "${p}" && pwd -P)" "${rest}"
}

# Whether OUT may be wiped or take a bundled install: strictly inside
# CLINK_DEPS_PREFIX, never the prefix itself, never /usr/local. An unset or empty
# CLINK_DEPS_PREFIX, or one that resolves to /, makes nothing private.
out_is_private() {
    local out prefix
    [ -n "${CLINK_DEPS_PREFIX:-}" ] || return 1
    case "${OUT}" in *..*) return 1 ;; esac
    out="$(resolve_path "${OUT}")"
    prefix="$(resolve_path "${CLINK_DEPS_PREFIX}")"
    out="${out%/}"
    prefix="${prefix%/}"
    [ -n "${prefix}" ] || return 1
    [ "${out}" != "$(resolve_path /usr/local)" ] || return 1
    case "${out}" in "${prefix}"/?*) return 0 ;; *) return 1 ;; esac
}

if [ "${BUNDLED}" = "1" ]; then
    if [ -z "${OPENSSL_ROOT_DIR:-}" ]; then
        echo "build-clickhouse-cpp: bundled mode needs OPENSSL_ROOT_DIR (the wheel's is" \
             "\${CLINK_DEPS_PREFIX}/openssl-static, staged by scripts/build-librdkafka.sh)" >&2
        exit 2
    fi
    if ! out_is_private; then
        echo "build-clickhouse-cpp: bundled mode refuses ${OUT}: it must be a directory below" \
             "CLINK_DEPS_PREFIX ('${CLINK_DEPS_PREFIX:-}') and not be /usr/local" >&2
        exit 2
    fi
    stamp="${OUT}/.clink-clickhouse-cpp-${V}-bundled"
    pending="${OUT}/.clink-clickhouse-cpp-pending-bundled"
    mode=", bundled deps"
else
    stamp="${OUT}/.clink-clickhouse-cpp-${V}"
    pending="${OUT}/.clink-clickhouse-cpp-pending"
    mode=""
fi

if [ -f "${stamp}" ]; then
    echo "build-clickhouse-cpp: ${V}${mode:+ (${mode#, })} already built in ${OUT}"
    exit 0
fi

# The other mode's stamp, of any version, or its pending marker: its install, whole
# or partial, is wiped, not overlaid.
other_mode=0
for s in "${OUT}"/.clink-clickhouse-cpp-*; do
    [ -e "${s}" ] || continue
    case "${s}" in
        *-bundled) this_bundled=1 ;;
        *)         this_bundled=0 ;;
    esac
    if [ "${this_bundled}" != "${BUNDLED}" ]; then
        other_mode=1
    fi
done
if [ "${other_mode}" = "1" ]; then
    if ! out_is_private; then
        echo "build-clickhouse-cpp: ${OUT} holds the other mode's install but is not a" \
             "directory inside CLINK_DEPS_PREFIX ('${CLINK_DEPS_PREFIX:-}'); refusing to wipe it" >&2
        exit 2
    fi
    echo "build-clickhouse-cpp: ${OUT} holds the other mode's install; wiping it"
    rm -rf "${OUT}"
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

if [ "${BUNDLED}" = "1" ]; then
    dep_args=(-DOPENSSL_USE_STATIC_LIBS=TRUE
              -DWITH_SYSTEM_ABSEIL=OFF
              -DWITH_SYSTEM_LZ4=OFF
              -DWITH_SYSTEM_ZSTD=OFF)
else
    dep_args=(-DWITH_SYSTEM_ABSEIL=ON
              -DWITH_SYSTEM_LZ4=ON
              -DWITH_SYSTEM_ZSTD=ON)
fi

cmake -S "${SRC}" -B "${WORK}/build" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${OUT}" \
    -DBUILD_SHARED_LIBS=OFF \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DWITH_OPENSSL=ON \
    "${dep_args[@]}" \
    -DWITH_SYSTEM_CITYHASH=OFF \
    -DDEBUG_DEPENDENCIES=OFF \
    "${ssl_args[@]}"
cmake --build "${WORK}/build" --parallel "${JOBS}"
mkdir -p "${OUT}"
touch "${pending}"
cmake --install "${WORK}/build"
if [ "${BUNDLED}" = "1" ]; then
    cp -a "${SRC}/contrib/absl/absl" "${OUT}/include/"
    rm -f "${OUT}/lib/libzstdstatic.a" "${OUT}/lib/liblz4.a"
    for f in "${OUT}/lib/libabsl_int128.a" "${OUT}/include/absl/numeric/int128.h"; do
        if [ ! -f "${f}" ]; then
            echo "build-clickhouse-cpp: bundled install is missing ${f}" >&2
            exit 1
        fi
    done
fi
touch "${stamp}"
rm -f "${pending}"
echo "build-clickhouse-cpp: installed static clickhouse-cpp ${V} (Release, TLS${mode}) -> ${OUT}"
