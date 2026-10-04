#!/usr/bin/env bash
# Build the pinned librdkafka as static archives for the self-contained Linux
# libclink (the manylinux pyclink wheel), into ${CLINK_DEPS_PREFIX}/rdkafka-static.
#
# Its own directory rather than the prefix's lib/: ordinary builds link the system
# librdkafka, and a static copy on the default search path would change what they
# pick. scripts/build-libclink-wheel.sh points the Kafka impl at this directory.
#
# mklove's --install-deps --source-deps-only builds OpenSSL, zstd and zlib from
# source and folds them, with the built-in lz4, into librdkafka-static.a, so the
# result needs nothing beyond glibc. GSSAPI (cyrus-sasl / krb5) and libcurl (OIDC
# token fetch) are left out. Needs a C toolchain, make, and perl (perl-core on a
# manylinux image) for the OpenSSL build. Idempotent: a stamp skips a rebuild of the
# same version.
#
# The OpenSSL that build compiles is also staged, headers and static archives only,
# into ${CLINK_DEPS_PREFIX}/openssl-static, for the ClickHouse client the wheel links
# beside Kafka (scripts/build-clickhouse-cpp.sh, bundled mode). It must be this same
# build: the archives folded into librdkafka-static.a and the ones the client links
# then hold identical objects, so one OpenSSL ends up in the library. A second
# OpenSSL build in the same link would mix objects from both.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=versions.env
source "${ROOT}/scripts/versions.env"
PREFIX="${CLINK_DEPS_PREFIX}"
OUT="${PREFIX}/rdkafka-static"
JOBS="${CLINK_BUILD_JOBS:-8}"
V="${LIBRDKAFKA_VERSION}"

# The stamp names the OpenSSL staging too, so a prefix built before it rebuilds.
stamp="${OUT}/.clink-librdkafka-${V}-openssl"
SSL_OUT="${PREFIX}/openssl-static"
if [ -f "${stamp}" ] && [ -f "${SSL_OUT}/include/openssl/opensslv.h" ]; then
    echo "build-librdkafka: ${V} already built in ${OUT}"
    exit 0
fi

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT
TARBALL="${WORK}/librdkafka-${V}.tar.gz"
curl -fsSL -o "${TARBALL}" "https://github.com/confluentinc/librdkafka/archive/refs/tags/v${V}.tar.gz"
got="$( (sha256sum "${TARBALL}" 2>/dev/null || shasum -a 256 "${TARBALL}") | awk '{print $1}')"
if [ "${got}" != "${LIBRDKAFKA_SHA256}" ]; then
    echo "build-librdkafka: checksum mismatch for v${V}: got ${got}, pinned ${LIBRDKAFKA_SHA256}" >&2
    exit 1
fi
tar -xzf "${TARBALL}" -C "${WORK}"
SRC="${WORK}/librdkafka-${V}"

(cd "${SRC}" && ./configure --prefix="${WORK}/install" --install-deps --source-deps-only \
    --enable-static --disable-gssapi --disable-curl --disable-lz4-ext)
make -C "${SRC}" -j"${JOBS}" libs
make -C "${SRC}" install-subdirs || make -C "${SRC}" install

# Stage the self-contained archives under the names the Kafka impl's library probe
# looks for, with the headers and nothing else: no .so and no pkg-config file, so the
# probe cannot fall back to a shared librdkafka.
rm -rf "${OUT}"
mkdir -p "${OUT}/lib" "${OUT}/include"
cp -a "${WORK}/install/include/librdkafka" "${OUT}/include/"
cp "${WORK}/install/lib/librdkafka-static.a" "${OUT}/lib/librdkafka.a"
cp "${WORK}/install/lib/librdkafka++.a" "${OUT}/lib/librdkafka++.a"

# Stage mklove's OpenSSL before the EXIT trap removes WORK. OpenSSL installs into
# usr/lib64 on x86_64 and usr/lib on aarch64; the archives always land in lib here.
DEST="${SRC}/mklove/deps/dest/usr"
SSL_LIB=""
for d in "${DEST}/lib64" "${DEST}/lib"; do
    if [ -f "${d}/libssl.a" ] && [ -f "${d}/libcrypto.a" ]; then
        SSL_LIB="${d}"
        break
    fi
done
if [ -z "${SSL_LIB}" ] || [ ! -f "${DEST}/include/openssl/opensslv.h" ]; then
    echo "build-librdkafka: mklove's OpenSSL is not where it was expected under ${DEST}" >&2
    exit 1
fi
rm -rf "${SSL_OUT}"
mkdir -p "${SSL_OUT}/lib" "${SSL_OUT}/include"
cp -a "${DEST}/include/openssl" "${SSL_OUT}/include/"
cp "${SSL_LIB}/libssl.a" "${SSL_LIB}/libcrypto.a" "${SSL_OUT}/lib/"
echo "build-librdkafka: staged $(grep -hoE 'OpenSSL [0-9]+\.[0-9]+\.[0-9]+' "${SSL_OUT}/include/openssl/opensslv.h" | head -1) -> ${SSL_OUT}"

touch "${stamp}"
echo "build-librdkafka: installed static librdkafka ${V} -> ${OUT}"
