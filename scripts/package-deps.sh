#!/usr/bin/env bash
# package-deps.sh - tar the INSTALLED half of CLINK_DEPS_PREFIX (lib, include,
# share - never the resumable src/ build trees) into a relocatable prebuilt
# toolchain artifact:
#
#   clink-deps-<key>-<os>-<arch>.tar.gz
#
# <key> is a short hash of the pinned versions that define the artifact
# (ARROW_VERSION + ICEBERG_CPP_TAG from versions.env), so a version bump
# changes the name and can never be served a stale archive. A manifest
# rides inside the tarball recording the pins, the platform, and - on
# macOS, where Arrow links the SYSTEM (Homebrew) aws-sdk-cpp - the exact
# aws-sdk-cpp version the archive was compiled against; fetch-deps.sh
# refuses a macOS archive whose aws-sdk does not match the consumer's
# Homebrew keg (the AWS C++ SDK has no patch-level ABI stability, and a
# mismatch SIGBUS-crashes every live S3 path).
#
# Prints the artifact path and the checksum line to append to
# scripts/deps-checksums.txt (the repo-committed pin fetch-deps.sh
# verifies downloads against).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${HERE}/versions.env"
PREFIX="${CLINK_DEPS_PREFIX:?CLINK_DEPS_PREFIX must be set}"
OUT_DIR="${1:-.}"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$@" | awk '{print $1}'
    else
        shasum -a 256 "$@" | awk '{print $1}'
    fi
}

os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
[ "$arch" = "aarch64" ] && arch="arm64"
key="$(printf 'ARROW=%s\nICEBERG=%s\n' "$ARROW_VERSION" "$ICEBERG_CPP_TAG" | sha256 /dev/stdin | cut -c1-12)"
name="clink-deps-${key}-${os}-${arch}.tar.gz"

for d in lib include; do
    [ -d "${PREFIX}/${d}" ] || { echo "package-deps: ${PREFIX}/${d} missing - build the toolchain first" >&2; exit 1; }
done

manifest="${PREFIX}/.clink-deps-manifest"
{
    echo "arrow_version=${ARROW_VERSION}"
    echo "iceberg_cpp_tag=${ICEBERG_CPP_TAG}"
    echo "os=${os}"
    echo "arch=${arch}"
    if [ "$os" = "darwin" ]; then
        # Arrow's dylibs bind /opt/homebrew/opt/aws-sdk-cpp - the LINKED keg
        # symlink - so record what that resolves to, not just any installed
        # keg (brew keeps superseded versions around).
        aws_ver="$(readlink "$(brew --prefix)/opt/aws-sdk-cpp" 2>/dev/null | xargs basename 2>/dev/null)"
        [ -n "${aws_ver}" ] || { echo "package-deps: cannot determine the linked Homebrew aws-sdk-cpp keg" >&2; exit 1; }
        echo "aws_sdk_cpp=${aws_ver}"
    fi
} > "${manifest}"

mkdir -p "${OUT_DIR}"
tar_paths=(lib include .clink-deps-manifest)
[ -d "${PREFIX}/share" ] && tar_paths+=(share)
tar czf "${OUT_DIR}/${name}" -C "${PREFIX}" "${tar_paths[@]}"

sum="$(sha256 "${OUT_DIR}/${name}")"
echo "package-deps: wrote ${OUT_DIR}/${name} ($(du -h "${OUT_DIR}/${name}" | cut -f1 | tr -d ' '))"
echo "package-deps: append to scripts/deps-checksums.txt:"
echo "${sum}  ${name}"
