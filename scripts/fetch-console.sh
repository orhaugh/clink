#!/usr/bin/env bash
# fetch-console.sh - install the pinned clink-fe console into a prefix, at
# <prefix>/share/clink/console, where clink_node finds it beside its binaries
# and serves it on the coordinator's HTTP port.
#
#   scripts/fetch-console.sh <prefix>
#   CLINK_CONSOLE_TARBALL=<file> scripts/fetch-console.sh <prefix>
#
# The bundle is clink-fe's release asset for the version pinned in
# console.env (CLINK_CONSOLE_VERSION), checked against the sha256 pinned
# beside it (CLINK_CONSOLE_SHA256): the release hosts the bytes, this
# repository decides what is trusted. The pin is kept out of versions.env,
# whose hash keys the toolchain image and the dependency caches, so a
# console bump rebuilds neither. CLINK_CONSOLE_TARBALL installs a local
# copy of the same file instead of downloading it, under the same check.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${HERE}/console.env"
PREFIX="${1:?usage: fetch-console.sh <prefix>}"
VERSION="${CLINK_CONSOLE_VERSION:?console.env pins no CLINK_CONSOLE_VERSION}"
WANT="${CLINK_CONSOLE_SHA256:?console.env pins no CLINK_CONSOLE_SHA256}"
NAME="clink-console-v${VERSION}"
BASE_URL="${CLINK_CONSOLE_RELEASE_URL:-https://github.com/orhaugh/clink-fe/releases/download/v${VERSION}}"

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | awk '{print $1}'
    else
        shasum -a 256 "$1" | awk '{print $1}'
    fi
}

tmp="$(mktemp -d)"
trap 'rm -rf "${tmp}"' EXIT
if [[ -n "${CLINK_CONSOLE_TARBALL:-}" ]]; then
    cp "${CLINK_CONSOLE_TARBALL}" "${tmp}/${NAME}.tar.gz"
else
    curl -fsSL -o "${tmp}/${NAME}.tar.gz" "${BASE_URL}/${NAME}.tar.gz"
fi
have="$(sha256 "${tmp}/${NAME}.tar.gz")"
if [[ "${have}" != "${WANT}" ]]; then
    echo "fetch-console: ${NAME}.tar.gz has sha256 ${have}, console.env pins ${WANT}; refusing it" >&2
    exit 1
fi
tar xzf "${tmp}/${NAME}.tar.gz" -C "${tmp}"
if [[ ! -f "${tmp}/${NAME}/index.html" ]]; then
    echo "fetch-console: ${NAME}.tar.gz holds no ${NAME}/index.html" >&2
    exit 1
fi
dest="${PREFIX}/share/clink/console"
rm -rf "${dest}"
mkdir -p "$(dirname "${dest}")"
mv "${tmp}/${NAME}" "${dest}"
echo "fetch-console: clink console v${VERSION} installed at ${dest}"
