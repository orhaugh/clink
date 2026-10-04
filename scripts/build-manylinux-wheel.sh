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
# librdkafka as static archives (scripts/build-librdkafka.sh, which also stages the
# OpenSSL it builds); the pinned ClickHouse client in bundled-deps mode against that
# OpenSSL (scripts/build-clickhouse-cpp.sh); a configure-only check that libclink
# with Kafka and ClickHouse takes that client and its TLS path; libclink with SQL
# and Kafka only, Arrow linked statically (scripts/build-libclink-wheel.sh with
# CLINK_WHEEL_KAFKA=1); the wheel; auditwheel repair; the self-contained gate
# (scripts/check-wheel-self-contained.py), which fails if anything was vendored
# or libclink needs more than glibc, libstdc++ and libgcc_s; and the smoke test in
# a fresh venv with the bundled library.
#
# The shipped wheel does not carry the ClickHouse connector yet: the client is
# built and the configure check run so that the bundled layout is proven here, on
# both architectures, before the connector joins the wheel. ClickHouse without
# Kafka is not a shipped configuration: the client links the OpenSSL the librdkafka
# build stages, so one OpenSSL ends up in the library.
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

# Configure only: libclink with Kafka and ClickHouse the only connectors, set up the
# way the wheel's library is, must take the bundled client and its TLS path. Every
# other CLINK_WITH_* is switched off by name, as in scripts/build-libclink-wheel.sh.
echo "::group::libclink configure check (Kafka + ClickHouse, bundled client)"
CH_CHECK="${WORK}/clickhouse-check"
RDK="${CLINK_DEPS_PREFIX}/rdkafka-static"
ZSTD="${CLINK_DEPS_PREFIX}/zstd-bundled"
CH_ARGS=(-DCLINK_BUILD_IMPLS=ON -DCLINK_WITH_KAFKA=ON -DCLINK_WITH_CLICKHOUSE=ON
         -DCMAKE_DISABLE_FIND_PACKAGE_RdKafka=ON
         "-DCMAKE_PREFIX_PATH=${CLINK_DEPS_PREFIX};${RDK};${SSL_STATIC}")
while read -r opt; do
    case "${opt}" in CLINK_WITH_KAFKA|CLINK_WITH_CLICKHOUSE|CLINK_WITH_ROCKSDB) ;; *) CH_ARGS+=("-D${opt}=OFF") ;; esac
done < <(cd "${ROOT}" && grep -hoE 'CLINK_WITH_[A-Z0-9_]+' CMakeLists.txt impls/*/CMakeLists.txt | sort -u)
rm -rf "${CH_CHECK}"
mkdir -p "${WORK}"
PKG_CONFIG_PATH="" cmake -S "${ROOT}" -B "${CH_CHECK}" \
    -DCMAKE_BUILD_TYPE=Release -DCLINK_BUILD_SQL=ON "${CH_ARGS[@]}" \
    -DCLINK_BUILD_TESTS=OFF -DCLINK_BUILD_EXAMPLES=OFF -DCLINK_HTTP_TLS=OFF \
    -DCLINK_STATIC_ARROW=ON \
    "-DCLINK_ZSTD_LIBRARY=${ZSTD}/lib/libzstd.a" "-DCLINK_ZSTD_INCLUDE_DIR=${ZSTD}/include" \
    "-DOPENSSL_ROOT_DIR=${SSL_STATIC}" -DOPENSSL_USE_STATIC_LIBS=TRUE \
    > "${WORK}/clickhouse-check.log" 2>&1 || { tail -40 "${WORK}/clickhouse-check.log"; exit 1; }
grep -E 'clink::clickhouse' "${WORK}/clickhouse-check.log" || true
for want in 'clickhouse-cpp [0-9.]+ \(pinned, Release, TLS, bundled deps\)' \
            'enabled \(native sink with TLS\)'; do
    if ! grep -qE "clink::clickhouse - ${want}" "${WORK}/clickhouse-check.log"; then
        tail -40 "${WORK}/clickhouse-check.log"
        echo "build-manylinux-wheel: the ClickHouse configure check did not report '${want}'" >&2
        exit 1
    fi
done
rm -rf "${CH_CHECK}"
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
