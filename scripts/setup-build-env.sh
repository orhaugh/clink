#!/usr/bin/env bash
# setup-build-env.sh
# Single source of truth for the clink build environment.
# Used by both docker/Dockerfile and CI.
#
# Usage: ./setup-build-env.sh [BUILD_TYPE]
#   BUILD_TYPE defaults to "Release" if not specified. Currently used
#   only to keep the API symmetric with 's setup script;
#   clink's optional deps are all distro-provided so there's no
#   per-build-type compilation step here yet.

set -euo pipefail

BUILD_TYPE="${1:-Release}"
echo "▶ Setting up clink build env (BUILD_TYPE=$BUILD_TYPE)"

# Pinned from-source toolchain: install to /usr/local (on the default search path) and
# read the pinned versions (single source of truth). The host equivalent lives in
# build_and_test.sh (installs to ~/.clink-deps).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export CLINK_DEPS_PREFIX=/usr/local
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/versions.env"

export DEBIAN_FRONTEND=noninteractive

# -- Base toolchain --
# build-essential pulls gcc/g++/make. clink targets C++23, which means
# we need gcc 13+ - Debian trixie (13) ships gcc 14 by default, fine.
apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    git \
    curl \
    build-essential \
    ca-certificates \
    pkg-config \
    clang-format \
    clang-tidy \
    lcov \
    gcovr

# -- Optional clink dependencies --
# Each is gated by find_package() in CMakeLists.txt; if absent, the
# corresponding connector compiles to a throwing stub. We install all of
# them here so the production build path is exercised.
apt-get install -y --no-install-recommends \
    libssl-dev \
    librocksdb-dev \
    librdkafka-dev \
    librdkafka++1 \
    libpq-dev \
    libhiredis-dev \
    libmariadb-dev \
    liburing-dev \
    libsqlite3-dev \
    libcurl4-openssl-dev \
    librabbitmq-dev \
    libzstd-dev

# -- AWS SDK for C++ (S3 transport for Arrow's S3FileSystem) --
# MUST be built BEFORE Arrow: Arrow's S3 uses the SYSTEM aws-sdk (Arrow 24's OWN bundled
# aws-c-* CRT is a mutually-inconsistent version set that does not compile - also why
# Homebrew builds Arrow against the system SDK). Build exactly the components Arrow's S3
# resolves (config, s3, transfer, identity-management -> pulls cognito-identity, sts,
# core), pinned to AWS_SDK_CPP_TAG so it tracks the host's Homebrew aws-sdk. impls/s3 and
# the Iceberg S3 FileIO ride the same SDK. ~15-25 min first build, Docker-layer-cached.
if [ ! -f "/usr/local/lib/libaws-cpp-sdk-s3.so" ] && \
   [ ! -f "/usr/local/lib/libaws-cpp-sdk-s3.a" ]; then
    echo "▶ Building aws-sdk-cpp ${AWS_SDK_CPP_TAG} (Arrow S3 components) from source..."
    apt-get install -y --no-install-recommends zlib1g-dev
    WORK_DIR=$(mktemp -d)
    git clone --depth 1 --branch "${AWS_SDK_CPP_TAG}" --recursive \
        https://github.com/aws/aws-sdk-cpp.git "$WORK_DIR/aws-sdk-cpp"
    cmake -S "$WORK_DIR/aws-sdk-cpp" -B "$WORK_DIR/aws-sdk-cpp/build" \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
          -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
          -DBUILD_ONLY="config;s3;transfer;identity-management;sts" \
          -DBUILD_SHARED_LIBS=ON \
          -DENABLE_TESTING=OFF \
          -DAUTORUN_UNIT_TESTS=OFF
    cmake --build "$WORK_DIR/aws-sdk-cpp/build" --target install -- -j"${CLINK_BUILD_JOBS:-$(nproc)}"
    cd /
    rm -rf "$WORK_DIR"
    ldconfig
fi

# -- Apache Arrow + Parquet (compiled from source, pinned) --
# clink fundamentally requires Arrow: the wire format is Arrow IPC, state-backend
# snapshots are Arrow IPC streams, and the Parquet/Iceberg connectors share the
# ArrowBatcher seam. Built FROM SOURCE at the exact version in scripts/versions.env
# (NOT the rolling APT repo) so the Debian image and the macOS host link byte-for-byte
# the same Arrow + transitive deps. Single source of truth: scripts/build-arrow.sh,
# shared with the host. Finds the system aws-sdk built just above. Docker-layer-cached.
"${SCRIPT_DIR}/build-arrow.sh"

# Kafka mock-broker tests rely on rdkafka_mock.h, which ships with
# librdkafka >= 1.3. Debian trixie has 2.x - verify so future image
# bumps don't silently disable mock-broker tests.
if [ ! -f /usr/include/librdkafka/rdkafka_mock.h ]; then
    echo "▶ WARNING: librdkafka does not ship rdkafka_mock.h; Kafka mock tests will be disabled"
fi

# -- ClickHouse C++ client --
# clickhouse-cpp isn't packaged by Debian. It's a small, header-+-cpp
# library; build from source. Cache by checking if already installed.
if [ ! -f "/usr/local/lib/libclickhouse-cpp-lib.a" ] && \
   [ ! -f "/usr/local/lib/libclickhouse-cpp-lib.so" ]; then
    echo "▶ Building clickhouse-cpp from source..."
    apt-get install -y --no-install-recommends liblz4-dev libabsl-dev
    WORK_DIR=$(mktemp -d)
    git clone --depth 1 --branch v2.5.1 \
        https://github.com/ClickHouse/clickhouse-cpp.git "$WORK_DIR/clickhouse-cpp"
    mkdir "$WORK_DIR/clickhouse-cpp/build"
    cd "$WORK_DIR/clickhouse-cpp/build"
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local \
          -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
          -DBUILD_SHARED_LIBS=ON \
          ..
    cmake --build . --target install -- -j"${CLINK_BUILD_JOBS:-$(nproc)}"
    cd /
    rm -rf "$WORK_DIR"
    ldconfig
fi

# -- Apache iceberg-cpp (Iceberg table-format sink) --
# Not packaged by Debian. Built FROM SOURCE at the pinned tag (scripts/versions.env)
# against the from-source Arrow installed above, via the SAME recipe the host uses
# (scripts/build-iceberg-cpp.sh): the bundle (Arrow/Parquet/Avro write backend +
# MakeS3FileIO), the SQLite catalog, the REST catalog, ICEBERG_S3=ON, and the pinned
# vendored Avro (iceberg-cpp 0.3.0 does not compile against avro-cpp 1.12+).
"${SCRIPT_DIR}/build-iceberg-cpp.sh"
ldconfig

rm -rf /var/lib/apt/lists/*

echo "▶ Build env ready."
