#!/usr/bin/env bash
# Build a lean, self-contained libclink for the pyclink wheel and stage it where
# the wheel build can pick it up (CLINK_LIB). Lean = SQL on, connector impls off,
# so the only shared deps are Arrow's (curl / xml2 / aws-sdk / openssl / lz4 /
# zstd), which the wheel-repair step (delocate / auditwheel) then vendors in.
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
cmake -S "${ROOT}" -B "${BUILD}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCLINK_BUILD_SQL=ON \
    -DCLINK_BUILD_IMPLS=OFF \
    -DCLINK_BUILD_TESTS=OFF \
    -DCLINK_BUILD_EXAMPLES=OFF \
    ${OSX_ARG:+"${OSX_ARG}"}
cmake --build "${BUILD}" --target clink_shared --parallel "${JOBS}"

LIB="$(find "${BUILD}" \( -name libclink.dylib -o -name libclink.so \) -type f | head -1)"
if [[ -z "${LIB}" ]]; then
    echo "build-libclink-wheel: no libclink built under ${BUILD}" >&2
    exit 1
fi
mkdir -p "$(dirname "${OUT}")"
cp "${LIB}" "${OUT}"
echo "build-libclink-wheel: staged ${LIB} -> ${OUT}"
