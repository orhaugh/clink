#!/usr/bin/env bash
# fetch-deps.sh - restore the prebuilt pinned toolchain (Arrow + Parquet +
# iceberg-cpp) into CLINK_DEPS_PREFIX instead of the 20-40 minute source
# build, when an artifact exists for this platform and version pin.
#
# Artifacts are hosted on the repo's `deps` GitHub release and verified
# against the sha256 pinned IN THIS REPO (scripts/deps-checksums.txt), so
# the release hosts bytes but the repo decides what is trusted. On macOS
# the archive's manifest records the Homebrew aws-sdk-cpp version it was
# compiled against, and a mismatch with the local keg refuses the archive
# (the AWS C++ SDK has no patch-level ABI stability).
#
# Exit codes - build-arrow.sh keys its fallback on these:
#   0  restored (or already present)
#   3  no prebuilt available for this platform/pin, or a manifest guard
#      declined it - falling back to the source build is correct
#   1  hard failure (download corruption, checksum mismatch) - do NOT
#      fall back silently; something is wrong
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${HERE}/versions.env"
PREFIX="${CLINK_DEPS_PREFIX:?CLINK_DEPS_PREFIX must be set}"
RELEASE_URL="${CLINK_DEPS_RELEASE_URL:-https://github.com/orhaugh/clink/releases/download/deps}"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@" | awk '{print $1}'
    else
        shasum -a 256 "$@" | awk '{print $1}'
    fi
}

# Already installed at the pinned version? Same check build-arrow.sh uses.
ver_file="${PREFIX}/lib/cmake/Arrow/ArrowConfigVersion.cmake"
if [ -f "${ver_file}" ] && grep -q "\"${ARROW_VERSION}\"" "${ver_file}" 2>/dev/null; then
    echo "fetch-deps: Arrow ${ARROW_VERSION} already installed at ${PREFIX}; nothing to do."
    exit 0
fi

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
[ "$arch" = "aarch64" ] && arch="arm64"
key="$(printf 'ARROW=%s\nICEBERG=%s\n' "$ARROW_VERSION" "$ICEBERG_CPP_TAG" | sha256 /dev/stdin | cut -c1-12)"
name="clink-deps-${key}-${os}-${arch}.tar.gz"

pinned="$(awk -v n="${name}" '$2 == n {print $1}' "${HERE}/deps-checksums.txt" 2>/dev/null || true)"
if [ -z "${pinned}" ]; then
    echo "fetch-deps: no pinned prebuilt for ${name}; source build required."
    exit 3
fi

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
echo "fetch-deps: downloading ${name} ..."
if ! curl -fsSL -o "${tmp}/${name}" "${RELEASE_URL}/${name}"; then
    echo "fetch-deps: download failed (release asset missing or offline); source build required."
    exit 3
fi

actual="$(sha256 "${tmp}/${name}")"
if [ "${actual}" != "${pinned}" ]; then
    echo "fetch-deps: CHECKSUM MISMATCH for ${name}" >&2
    echo "  pinned:  ${pinned}" >&2
    echo "  actual:  ${actual}" >&2
    echo "Refusing the archive. If the artifact was legitimately republished," >&2
    echo "update scripts/deps-checksums.txt; otherwise treat this as tampering." >&2
    exit 1
fi

mkdir -p "${tmp}/extract"
tar xzf "${tmp}/${name}" -C "${tmp}/extract"

# macOS guard: Arrow's dylibs bind /opt/homebrew/opt/aws-sdk-cpp - the
# LINKED keg symlink - and the SDK has no patch-level ABI stability, so a
# different linked keg SIGBUS-crashes every live S3 path. Refuse a
# mismatched archive rather than install a landmine.
if [ "${os}" = "darwin" ]; then
    want="$(awk -F= '$1 == "aws_sdk_cpp" {print $2}' "${tmp}/extract/.clink-deps-manifest" 2>/dev/null || true)"
    have="$(readlink "$(brew --prefix 2>/dev/null)/opt/aws-sdk-cpp" 2>/dev/null | xargs basename 2>/dev/null || true)"
    if [ -n "${want}" ] && [ "${have}" != "${want}" ]; then
        echo "fetch-deps: prebuilt archive was compiled against Homebrew aws-sdk-cpp ${want},"
        echo "but this machine's linked keg is: ${have:-none}."
        echo "Link the matching version (brew install aws-sdk-cpp / brew switch) or build"
        echo "from source; source build required."
        exit 3
    fi
fi

mkdir -p "${PREFIX}"
cp -R "${tmp}/extract/." "${PREFIX}/"
echo "fetch-deps: restored pinned toolchain (Arrow ${ARROW_VERSION}, iceberg-cpp ${ICEBERG_CPP_TAG}) -> ${PREFIX}"
