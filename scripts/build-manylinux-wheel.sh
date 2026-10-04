#!/usr/bin/env bash
# Build, repair, gate and smoke-test the Linux pyclink wheel, with the Kafka and
# ClickHouse connectors, inside quay.io/pypa/manylinux_2_28_<arch>. This is the
# whole job .github/workflows/wheels.yml runs, kept in one script so it runs the
# same way locally:
#
#   docker run --rm -v "$PWD:/src" -w /src quay.io/pypa/manylinux_2_28_aarch64 \
#       scripts/build-manylinux-wheel.sh /src/build-manylinux/wheelhouse
#
# Steps: the pinned Arrow from source with the object stores off
# (scripts/build-arrow.sh, which also installs Arrow's bundled zstd); the pinned
# librdkafka as static archives (scripts/build-librdkafka.sh, which also stages the
# OpenSSL it builds); the pinned ClickHouse client in bundled-deps mode against that
# OpenSSL (scripts/build-clickhouse-cpp.sh); libclink with SQL, Kafka and ClickHouse
# (the native sink with TLS), Arrow linked statically
# (scripts/build-libclink-wheel.sh with CLINK_WHEEL_KAFKA=1 and
# CLINK_WHEEL_CLICKHOUSE=1, which refuses to build unless the client's TLS path
# linked against the staged OpenSSL); the wheel; auditwheel repair; the
# self-contained gate (scripts/check-wheel-self-contained.py), which fails if
# anything was vendored, libclink needs more than glibc, libstdc++ and libgcc_s,
# exports anything but the C ABI, carries more than one OpenSSL, or took lz4
# definitions from more than one archive or from the ClickHouse client's; and the
# smoke test in a fresh venv with the bundled library.
#
# ClickHouse without Kafka is not a shipped configuration: the client links the
# OpenSSL the librdkafka build stages, so one OpenSSL ends up in the library and
# both connectors use it.
#
# CLINK_DEPS_PREFIX (default /opt/clink-deps) holds Arrow, librdkafka, the staged
# OpenSSL and the ClickHouse client, and is what CI caches; each build is skipped
# when its stamp matches.
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
SSL_STATIC="${CLINK_DEPS_PREFIX}/openssl-static"
echo "::group::clickhouse-cpp (bundled deps, staged OpenSSL)"
CLICKHOUSE_CPP_BUNDLED_DEPS=1 OPENSSL_ROOT_DIR="${SSL_STATIC}" \
    "${ROOT}/scripts/build-clickhouse-cpp.sh"
echo "::endgroup::"

echo "::group::libclink (SQL + Kafka + ClickHouse, static Arrow)"
mkdir -p "${WORK}"
CLINK_WHEEL_KAFKA=1 CLINK_WHEEL_CLICKHOUSE=1 CLINK_WHEEL_BUILD_DIR="${WORK}/libclink-build" \
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

"${PY}" "${ROOT}/scripts/check-wheel-self-contained.py" \
    --link-map "${WORK}/libclink.so.map" "${OUT}"/*.whl

echo "::group::smoke test (fresh venv, bundled library)"
rm -rf "${WORK}/venv"
"${PY}" -m venv "${WORK}/venv"
"${WORK}/venv/bin/pip" install -q "${OUT}"/*.whl pyarrow
(cd "${WORK}" && env -u CLINK_LIB "${WORK}/venv/bin/python" "${ROOT}/python/tests/wheel_smoke.py")
echo "::endgroup::"
ls -l "${OUT}"
