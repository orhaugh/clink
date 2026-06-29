#!/usr/bin/env bash
# install-connector-deps.sh
# The CHEAP, volatile half of the clink build env: the optional connector CLIENT
# libraries that are plain apt packages, plus the Apache Pulsar prebuilt .deb. Each is
# gated by find_package() in CMakeLists.txt; if absent, the corresponding connector
# compiles to a throwing stub. We install all of them here so the production build path
# is exercised.
#
# This runs AFTER install-system-deps.sh (which has the base toolchain + from-source
# toolchain). Adding a new connector usually means adding one lib here, which re-runs
# only this fast Docker layer - the expensive from-source layer stays cached.
#
# NOTE: deps that the from-source builds link at COMPILE time (libssl-dev,
# libcurl4-openssl-dev, zlib1g-dev) live in install-system-deps.sh, NOT here.

set -euo pipefail

echo "▶ clink connector deps (apt packages + prebuilt Pulsar .deb)"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/versions.env"

export DEBIAN_FRONTEND=noninteractive

# -- Optional clink connector client libraries --
# Each maps to one clink connector and is what grows when a connector is added; none is a
# build dep of the from-source toolchain (install-system-deps.sh), so adding one here only
# re-runs this fast layer. (libssl/libcurl/libsqlite3/libzstd/zlib live in the system layer
# because the from-source builds need them at COMPILE time - do not move them here.)
apt-get update && apt-get install -y --no-install-recommends \
    librocksdb-dev \
    librdkafka-dev \
    librdkafka++1 \
    libpq-dev \
    libhiredis-dev \
    libmariadb-dev \
    liburing-dev \
    librabbitmq-dev \
    libnats-dev

# Kafka mock-broker tests rely on rdkafka_mock.h, which ships with
# librdkafka >= 1.3. Debian trixie has 2.x - verify so future image
# bumps don't silently disable mock-broker tests.
if [ ! -f /usr/include/librdkafka/rdkafka_mock.h ]; then
    echo "▶ WARNING: librdkafka does not ship rdkafka_mock.h; Kafka mock tests will be disabled"
fi

# -- Apache Pulsar C++ client (messaging transport connector) --
# Not in Debian apt. Apache publishes prebuilt .deb packages per arch; install the pinned
# version (scripts/versions.env) matching the host's Homebrew libpulsar. clink uses the C API
# (pulsar/c/), so no C++ ABI is shared with this prebuilt library. apt-get install -f resolves
# its runtime deps (libssl/libcurl/...), already present from install-system-deps.sh.
if [ ! -f "/usr/lib/libpulsar.so" ] && [ ! -f "/usr/lib/$(dpkg-architecture -qDEB_HOST_MULTIARCH 2>/dev/null)/libpulsar.so" ]; then
    echo "▶ Installing Apache Pulsar C++ client ${PULSAR_CLIENT_CPP_VERSION} (.deb)..."
    _deb_arch="$(dpkg --print-architecture)"  # amd64 | arm64
    case "${_deb_arch}" in
        amd64) _pulsar_arch="x86_64" ;;
        arm64) _pulsar_arch="arm64" ;;
        *)     _pulsar_arch="${_deb_arch}" ;;
    esac
    _pulsar_base="https://archive.apache.org/dist/pulsar/pulsar-client-cpp-${PULSAR_CLIENT_CPP_VERSION}/deb-${_pulsar_arch}"
    _pulsar_tmp="$(mktemp -d)"
    curl -fsSL "${_pulsar_base}/apache-pulsar-client.deb" -o "${_pulsar_tmp}/client.deb"
    curl -fsSL "${_pulsar_base}/apache-pulsar-client-dev.deb" -o "${_pulsar_tmp}/client-dev.deb"
    apt-get update >/dev/null 2>&1 || true
    dpkg -i "${_pulsar_tmp}/client.deb" "${_pulsar_tmp}/client-dev.deb" || apt-get install -y -f
    rm -rf "${_pulsar_tmp}"
    ldconfig
fi

rm -rf /var/lib/apt/lists/*

echo "▶ Connector deps ready."
