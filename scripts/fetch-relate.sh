#!/usr/bin/env bash
# fetch-relate.sh - install Relate's C library (relate.h and librelate_ffi.a)
# into ${CLINK_DEPS_PREFIX}/relate-ffi, where impls/relate looks for it.
#
# Relate (github.com/relatedb/relate, Apache-2.0) publishes the library as one
# release asset per platform. Those assets are not listed in the release's
# signed SHA256SUMS, which covers the command-line binaries only, so the
# sha256 pinned in scripts/versions.env is what decides whether an archive is
# accepted: the release hosts the bytes, this repository decides what is
# trusted.
#
#   scripts/fetch-relate.sh                      download the pinned version
#   RELATE_FFI_TARBALL=/path/a.tar.gz scripts/fetch-relate.sh
#                                                install an archive already on
#                                                disk; it must match the pin
#
# Exit codes:
#   0  installed, or already installed at the pinned version
#   3  no pin for this platform: the connector is not built here
#   1  hard failure (download failed, checksum mismatch, unexpected contents)
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${HERE}/versions.env"
PREFIX="${CLINK_DEPS_PREFIX:?CLINK_DEPS_PREFIX must be set}/relate-ffi"
RELEASE_URL="${RELATE_FFI_RELEASE_URL:-https://github.com/relatedb/relate/releases/download/v${RELATE_FFI_VERSION}}"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@" | awk '{print $1}'
    else
        shasum -a 256 "$@" | awk '{print $1}'
    fi
}

case "$(uname -s)/$(uname -m)" in
    Darwin/arm64)                target="aarch64-apple-darwin" ;;
    Darwin/x86_64)               target="x86_64-apple-darwin" ;;
    Linux/aarch64 | Linux/arm64) target="aarch64-unknown-linux-gnu" ;;
    Linux/x86_64)                target="x86_64-unknown-linux-gnu" ;;
    *)
        echo "fetch-relate: no Relate library is published for $(uname -s)/$(uname -m)." >&2
        exit 3
        ;;
esac

pin_name="RELATE_FFI_SHA256_$(printf '%s' "${target}" | tr 'a-z-' 'A-Z_')"
pin="$(eval "printf '%s' \"\${${pin_name}:-}\"")"
if [ -z "${pin}" ]; then
    echo "fetch-relate: scripts/versions.env pins no sha256 for ${target} (${pin_name});" >&2
    echo "fetch-relate: the Relate connector is not built on this platform until it does." >&2
    exit 3
fi

stamp="${RELATE_FFI_VERSION} ${target} ${pin}"
if [ -f "${PREFIX}/VERSION" ] && [ "$(cat "${PREFIX}/VERSION")" = "${stamp}" ] &&
    [ -f "${PREFIX}/include/relate.h" ] && [ -f "${PREFIX}/lib/librelate_ffi.a" ]; then
    echo "fetch-relate: Relate ${RELATE_FFI_VERSION} (${target}) already installed at ${PREFIX}; nothing to do."
    exit 0
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/clink-relate-ffi.XXXXXX")"
trap 'rm -rf "${work}"' EXIT

asset="relate-ffi-${target}.tar.gz"
if [ -n "${RELATE_FFI_TARBALL:-}" ]; then
    echo "fetch-relate: using ${RELATE_FFI_TARBALL}"
    archive="${RELATE_FFI_TARBALL}"
else
    archive="${work}/${asset}"
    echo "fetch-relate: downloading ${RELEASE_URL}/${asset}"
    curl -fsSL --retry 3 -o "${archive}" "${RELEASE_URL}/${asset}"
fi

got="$(sha256 "${archive}")"
if [ "${got}" != "${pin}" ]; then
    echo "fetch-relate: ${archive} does not match the sha256 pinned for ${asset}" >&2
    echo "fetch-relate:   pinned ${pin}" >&2
    echo "fetch-relate:   got    ${got}" >&2
    exit 1
fi

mkdir -p "${work}/x"
tar -xzf "${archive}" -C "${work}/x"
if [ ! -f "${work}/x/include/relate.h" ] || [ ! -f "${work}/x/librelate_ffi.a" ]; then
    echo "fetch-relate: ${asset} does not hold include/relate.h and librelate_ffi.a" >&2
    exit 1
fi

rm -rf "${PREFIX}"
mkdir -p "${PREFIX}/include" "${PREFIX}/lib"
cp "${work}/x/include/relate.h" "${PREFIX}/include/relate.h"
cp "${work}/x/librelate_ffi.a" "${PREFIX}/lib/librelate_ffi.a"
printf '%s\n' "${stamp}" > "${PREFIX}/VERSION"
echo "fetch-relate: installed Relate ${RELATE_FFI_VERSION} (${target}) at ${PREFIX}"
