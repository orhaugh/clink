#!/usr/bin/env bash
# Build a lean, self-contained libclink for the pyclink wheel and stage it where
# the wheel build can pick it up (CLINK_LIB). Lean = SQL on and the connector impls
# off unless a CLINK_WHEEL_* flag below turns one on, so the shared deps are
# Arrow's alone (lz4 / zstd / ...), which the wheel-repair step (delocate /
# auditwheel) then vendors in, or links statically on Linux.
#
# CLINK_HTTP_TLS=OFF is what keeps OpenSSL out of the HTTP subsystem. httplib
# enables HTTPS whenever it finds OpenSSL, and clink_core's http_server /
# http_client then reference it; the only OpenSSL on a macOS runner is a Homebrew
# bottle built for that runner's OS, which would pin the wheel above its floor.
# The wheel's HTTP subsystem therefore serves and fetches plain HTTP only. The
# Linux connectors (Kafka, ClickHouse) carry their own TLS through the static
# OpenSSL scripts/build-librdkafka.sh stages, which is not the HTTP subsystem's.
#
# Reused two ways:
#   * locally, to produce a libclink to point `python -m build` at via CLINK_LIB;
#   * in CI (.github/workflows/wheels.yml), to build the heavy library OUTSIDE the
#     wheel build and hand it to setup.py via CLINK_LIB, so the wheel build just
#     copies it rather than rebuilding clink_shared per platform.
#
# The pinned Arrow/Parquet toolchain must already exist (host ~/.clink-deps via
# scripts/build-arrow.sh, or /usr/local in the build image).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD="${CLINK_WHEEL_BUILD_DIR:-${ROOT}/build-libclink-wheel}"
# Stage target: an explicit $1, else CLINK_LIB, else inside the (gitignored)
# build dir - never the source tree, so a bare run leaves no stray artifact.
OUT="${1:-${CLINK_LIB:-${BUILD}/staged/libclink}}"
JOBS="${CLINK_BUILD_JOBS:-8}"

# Pin the macOS floor to match the wheel tag when MACOSX_DEPLOYMENT_TARGET is set
# (CI). CMake ignores CMAKE_OSX_DEPLOYMENT_TARGET off Apple, but only pass it when
# the floor is explicit so a bare local build keeps the host default. Written as a
# single ${VAR:+...} word so an unset floor expands to nothing under `set -u` on
# macOS's bash 3.2 (an empty "${ARR[@]}" would error there).
OSX_ARG=""
if [[ -n "${MACOSX_DEPLOYMENT_TARGET:-}" ]]; then
    OSX_ARG="-DCMAKE_OSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET}"
fi
# Optional: forward a zstd choice. clink_core links a resolved zstd LIBRARY PATH,
# and its find_library would otherwise pick up a Homebrew bottle - which on a
# current macOS runner is built for that runner's OS and pins libclink above the
# wheel's floor. Passing CLINK_ZSTD_LIBRARY=IGNORE makes CMake's if() treat it as
# not-found, so clink falls back to Arrow's bundled zstd (build-scope only, which
# is exactly right for a wheel: nothing downstream find_package()s this build).
ZSTD_ARG=""
ZSTD_INC_ARG=""
if [[ -n "${CLINK_ZSTD_LIBRARY:-}" ]]; then
    ZSTD_ARG="-DCLINK_ZSTD_LIBRARY=${CLINK_ZSTD_LIBRARY}"
fi
# Deps prefix, resolved the way CMake resolves it (scripts/versions.env).
DEPS_PREFIX="${CLINK_DEPS_PREFIX:-${HOME}/.clink-deps}"
# On Linux there is no system zstd header to fall back on: default to Arrow's own
# bundled zstd, which scripts/build-arrow.sh installs under <prefix>/zstd-bundled.
if [[ "$(uname -s)" == "Linux" && -z "${CLINK_ZSTD_LIBRARY:-}" &&
      -f "${DEPS_PREFIX}/zstd-bundled/lib/libzstd.a" ]]; then
    ZSTD_ARG="-DCLINK_ZSTD_LIBRARY=${DEPS_PREFIX}/zstd-bundled/lib/libzstd.a"
    ZSTD_INC_ARG="-DCLINK_ZSTD_INCLUDE_DIR=${DEPS_PREFIX}/zstd-bundled/include"
fi
# The connector impls a Linux wheel carries, each against static archives staged
# under the deps prefix. The impls turn on when either flag is set; every
# CLINK_WITH_* the flags do not name is switched off by name, read from the build
# itself so a connector added later cannot join the wheel on AUTO (RocksDB, the
# always-built state backend, is left as it is).
#
#   CLINK_WHEEL_KAFKA=1       Kafka, against the static librdkafka
#                             scripts/build-librdkafka.sh stages under
#                             <prefix>/rdkafka-static.
#   CLINK_WHEEL_CLICKHOUSE=1  ClickHouse with the native sink and its TLS, against
#                             the bundled-deps client scripts/build-clickhouse-cpp.sh
#                             installs under <prefix>/clickhouse-cpp
#                             (CLICKHOUSE_CPP_BUNDLED_DEPS=1) and the static OpenSSL
#                             scripts/build-librdkafka.sh stages under
#                             <prefix>/openssl-static. ClickHouse without Kafka is
#                             not a shipped configuration: the flag needs that
#                             OpenSSL, which only the librdkafka build stages, so the
#                             library holds one OpenSSL. After configure the build
#                             refuses unless the client's TLS factory probe linked
#                             against that OpenSSL, so a sink without TLS cannot ship
#                             silently.
WHEEL_KAFKA="${CLINK_WHEEL_KAFKA:-0}"
WHEEL_CLICKHOUSE="${CLINK_WHEEL_CLICKHOUSE:-0}"
SSL_STATIC="${DEPS_PREFIX}/openssl-static"
CH_PREFIX="${DEPS_PREFIX}/clickhouse-cpp"
IMPL_ARGS=("-DCLINK_BUILD_IMPLS=OFF")
KEEP=(CLINK_WITH_ROCKSDB)
PREFIX_PATH=("${DEPS_PREFIX}")
if [[ "${WHEEL_KAFKA}" == "1" ]]; then
    RDK="${DEPS_PREFIX}/rdkafka-static"
    if [[ ! -f "${RDK}/lib/librdkafka.a" ]]; then
        echo "build-libclink-wheel: CLINK_WHEEL_KAFKA=1 needs ${RDK} (run scripts/build-librdkafka.sh)" >&2
        exit 1
    fi
    KEEP+=(CLINK_WITH_KAFKA)
    PREFIX_PATH+=("${RDK}")
    IMPL_ARGS+=("-DCLINK_WITH_KAFKA=ON" "-DCMAKE_DISABLE_FIND_PACKAGE_RdKafka=ON")
fi
if [[ "${WHEEL_CLICKHOUSE}" == "1" ]]; then
    if [[ ! -f "${SSL_STATIC}/lib/libssl.a" ]]; then
        echo "build-libclink-wheel: CLINK_WHEEL_CLICKHOUSE=1 needs the static OpenSSL in" \
             "${SSL_STATIC} (run scripts/build-librdkafka.sh)" >&2
        exit 1
    fi
    if ! compgen -G "${CH_PREFIX}/.clink-clickhouse-cpp-*-bundled" >/dev/null; then
        echo "build-libclink-wheel: CLINK_WHEEL_CLICKHOUSE=1 needs the bundled-deps client in" \
             "${CH_PREFIX} (run scripts/build-clickhouse-cpp.sh with" \
             "CLICKHOUSE_CPP_BUNDLED_DEPS=1 OPENSSL_ROOT_DIR=${SSL_STATIC})" >&2
        exit 1
    fi
    KEEP+=(CLINK_WITH_CLICKHOUSE)
    PREFIX_PATH+=("${SSL_STATIC}")
    IMPL_ARGS+=("-DCLINK_WITH_CLICKHOUSE=ON" "-DOPENSSL_ROOT_DIR=${SSL_STATIC}"
                "-DOPENSSL_USE_STATIC_LIBS=TRUE")
fi
if [[ "${WHEEL_KAFKA}" == "1" || "${WHEEL_CLICKHOUSE}" == "1" ]]; then
    IMPL_ARGS[0]="-DCLINK_BUILD_IMPLS=ON"
    IMPL_ARGS+=("-DCMAKE_PREFIX_PATH=$(IFS=';'; echo "${PREFIX_PATH[*]}")")
    while read -r opt; do
        keep=0
        for k in "${KEEP[@]}"; do
            [[ "${opt}" == "${k}" ]] && keep=1
        done
        [[ "${keep}" == "1" ]] || IMPL_ARGS+=("-D${opt}=OFF")
    done < <(cd "${ROOT}" && grep -hoE 'CLINK_WITH_[A-Z0-9_]+' CMakeLists.txt impls/*/CMakeLists.txt | sort -u)
    # No impl's pkg-config tier may find a shared library (librdkafka, OpenSSL).
    export PKG_CONFIG_PATH=""
fi
# On Linux the static Arrow must be linked into clink_core itself
# (CLINK_STATIC_ARROW): GNU ld reads each archive once, so the shared Arrow on
# clink_core's interface would otherwise win and auditwheel would vendor it,
# putting two Arrow copies in one process. macOS keeps the dead-strip route.
# A link map beside libclink.so, which scripts/check-wheel-self-contained.py reads
# to see which archive supplied each lz4 definition.
STATIC_ARG=""
MAP_ARG=""
if [[ "$(uname -s)" == "Linux" ]]; then
    STATIC_ARG="-DCLINK_STATIC_ARROW=ON"
    MAP_ARG="-DCMAKE_SHARED_LINKER_FLAGS=-Wl,-Map=${BUILD}/libclink.so.map"
fi
# -U drops the SSL factory probe's cached answer from any earlier configure of a
# reused build directory, so the guard below reads only what this configure found.
cmake -S "${ROOT}" -B "${BUILD}" \
    -UCLINK_CLICKHOUSE_SSL_FACTORY_LINKS \
    -DCMAKE_BUILD_TYPE=Release \
    -DCLINK_BUILD_SQL=ON \
    "${IMPL_ARGS[@]}" \
    -DCLINK_BUILD_TESTS=OFF \
    -DCLINK_BUILD_EXAMPLES=OFF \
    -DCLINK_HTTP_TLS=OFF \
    ${OSX_ARG:+"${OSX_ARG}"} \
    ${ZSTD_ARG:+"${ZSTD_ARG}"} \
    ${ZSTD_INC_ARG:+"${ZSTD_INC_ARG}"} \
    ${STATIC_ARG:+"${STATIC_ARG}"} \
    ${MAP_ARG:+"${MAP_ARG}"}

# The ClickHouse guard: the native sink's TLS needs the client's SSLSocketFactory
# probe to have linked, and against the staged OpenSSL rather than any other one
# the configure step could find. Read from the cache, which is what the build uses;
# an empty probe entry means this configure never ran the probe.
if [[ "${WHEEL_CLICKHOUSE}" == "1" ]]; then
    cache="${BUILD}/CMakeCache.txt"
    cache_value() { sed -n "s/^$1:[A-Z]*=//p" "${cache}" | head -1; }
    probe="$(cache_value CLINK_CLICKHOUSE_SSL_FACTORY_LINKS)"
    ssl_inc="$(cache_value OPENSSL_INCLUDE_DIR)"
    ssl_lib="$(cache_value OPENSSL_SSL_LIBRARY)"
    guard_ok=1
    if [[ "${probe}" != "1" ]]; then
        echo "build-libclink-wheel: the ClickHouse SSL factory probe did not link" \
             "(CLINK_CLICKHOUSE_SSL_FACTORY_LINKS='${probe}')" >&2
        guard_ok=0
    fi
    for pair in "OPENSSL_INCLUDE_DIR=${ssl_inc}" "OPENSSL_SSL_LIBRARY=${ssl_lib}"; do
        case "${pair#*=}" in
            "${SSL_STATIC}"/*) ;;
            *) echo "build-libclink-wheel: ${pair%%=*} is '${pair#*=}', not inside ${SSL_STATIC}" >&2
               guard_ok=0 ;;
        esac
    done
    if [[ "${guard_ok}" != "1" ]]; then
        echo "build-libclink-wheel: refusing to build a ClickHouse wheel library without" \
             "the staged OpenSSL's TLS path" >&2
        exit 1
    fi
    echo "build-libclink-wheel: ClickHouse TLS probe linked against ${ssl_lib}"
fi

cmake --build "${BUILD}" --target clink_shared --parallel "${JOBS}"

LIB="$(find "${BUILD}" \( -name libclink.dylib -o -name libclink.so \) -type f | head -1)"
if [[ -z "${LIB}" ]]; then
    echo "build-libclink-wheel: no libclink built under ${BUILD}" >&2
    exit 1
fi
mkdir -p "$(dirname "${OUT}")"
cp "${LIB}" "${OUT}"
echo "build-libclink-wheel: staged ${LIB} -> ${OUT}"
if [[ -n "${MAP_ARG}" ]]; then
    if [[ ! -f "${BUILD}/libclink.so.map" ]]; then
        echo "build-libclink-wheel: the link map ${BUILD}/libclink.so.map was not written" >&2
        exit 1
    fi
    cp "${BUILD}/libclink.so.map" "${OUT}.map"
    echo "build-libclink-wheel: link map -> ${OUT}.map"
fi
