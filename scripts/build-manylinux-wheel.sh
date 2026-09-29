#!/usr/bin/env bash
# Build, repair, gate and smoke-test the Linux pyclink wheel, with the Kafka
# connector, inside quay.io/pypa/manylinux_2_28_<arch>. This is the whole job
# .github/workflows/wheels.yml runs, kept in one script so it runs the same way
# locally:
#
#   docker run --rm -v "$PWD:/src" -w /src quay.io/pypa/manylinux_2_28_aarch64 \
#       scripts/build-manylinux-wheel.sh /src/build-manylinux/wheelhouse
#
# Steps: the pinned Arrow from source with the object stores off
# (scripts/build-arrow.sh, which also installs Arrow's bundled zstd); the pinned
# librdkafka as static archives (scripts/build-librdkafka.sh); libclink with SQL
# and Kafka only, Arrow linked statically (scripts/build-libclink-wheel.sh with
# CLINK_WHEEL_KAFKA=1); the wheel; auditwheel repair; the self-contained gate
# (scripts/check-wheel-self-contained.py), which fails if anything was vendored
# or libclink needs more than glibc, libstdc++ and libgcc_s; and the smoke test in
# a fresh venv with the bundled library.
#
# CLINK_DEPS_PREFIX (default /opt/clink-deps) holds Arrow and librdkafka and is
# what CI caches; both builds are skipped when their stamps match.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-${ROOT}/build-manylinux/wheelhouse}"
export CLINK_DEPS_PREFIX="${CLINK_DEPS_PREFIX:-/opt/clink-deps}"
export CLINK_BUILD_JOBS="${CLINK_BUILD_JOBS:-$(nproc)}"
PY="${PYTHON:-/opt/python/cp311-cp311/bin/python}"
WORK="${CLINK_MANYLINUX_WORK:-${ROOT}/build-manylinux}"

if [[ ! -f /etc/redhat-release ]] || ! command -v auditwheel >/dev/null; then
    echo "build-manylinux-wheel: run this inside a quay.io/pypa/manylinux_2_28 image" >&2
    exit 2
fi
# perl for librdkafka's OpenSSL build; the manylinux image ships only perl-interpreter.
if ! perl -MIPC::Cmd -e 1 2>/dev/null; then
    dnf -y -q install perl-core
fi

echo "::group::Arrow ${CLINK_DEPS_PREFIX}"
CLINK_ARROW_OBJECT_STORES=OFF CLINK_DEPS_FROM_SOURCE=1 "${ROOT}/scripts/build-arrow.sh"
echo "::endgroup::"
echo "::group::librdkafka"
"${ROOT}/scripts/build-librdkafka.sh"
echo "::endgroup::"

echo "::group::libclink (SQL + Kafka, static Arrow)"
mkdir -p "${WORK}"
CLINK_WHEEL_KAFKA=1 CLINK_WHEEL_BUILD_DIR="${WORK}/libclink-build" \
    "${ROOT}/scripts/build-libclink-wheel.sh" "${WORK}/libclink.so"
strip --strip-unneeded "${WORK}/libclink.so"
echo "::endgroup::"

echo "::group::wheel + auditwheel"
# Build from a copy of python/, so the wheel build leaves nothing in the source tree.
rm -rf "${WORK}/pysrc" "${WORK}/dist"
cp -a "${ROOT}/python" "${WORK}/pysrc"
"${PY}" -m pip install -q build setuptools wheel
CLINK_LIB="${WORK}/libclink.so" "${PY}" -m build --wheel --no-isolation \
    --outdir "${WORK}/dist" "${WORK}/pysrc"
rm -rf "${OUT}"
auditwheel repair -w "${OUT}" "${WORK}"/dist/*.whl
auditwheel show "${OUT}"/*.whl
echo "::endgroup::"

"${PY}" "${ROOT}/scripts/check-wheel-self-contained.py" "${OUT}"/*.whl

echo "::group::smoke test (fresh venv, bundled library)"
rm -rf "${WORK}/venv"
"${PY}" -m venv "${WORK}/venv"
"${WORK}/venv/bin/pip" install -q "${OUT}"/*.whl pyarrow
(cd "${WORK}" && env -u CLINK_LIB "${WORK}/venv/bin/python" "${ROOT}/python/tests/wheel_smoke.py")
echo "::endgroup::"
ls -l "${OUT}"
