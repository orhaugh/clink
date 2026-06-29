#!/usr/bin/env bash
# install-system-deps.sh
# The EXPENSIVE, rarely-invalidated half of the clink build env: the base compile
# toolchain plus the from-source, version-pinned deps (aws-sdk, Arrow + Parquet,
# clickhouse-cpp, iceberg-cpp, DataStax cpp-driver). Split out of setup-build-env.sh
# so docker/Dockerfile can cache this as its own layer, independent of the cheap
# connector-apt layer (install-connector-deps.sh). Adding a connector's apt package
# then re-runs only that fast layer instead of recompiling Arrow et al (~40-60 min).
#
# Usage: ./install-system-deps.sh [BUILD_TYPE]   (BUILD_TYPE defaults to "Release")

set -euo pipefail

BUILD_TYPE="${1:-Release}"
echo "▶ clink system deps (from-source toolchain), BUILD_TYPE=$BUILD_TYPE"

# Pinned from-source toolchain: install to /usr/local (on the default search path) and
# read the pinned versions (single source of truth). The host equivalent lives in
# build_and_test.sh (installs to ~/.clink-deps).
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export CLINK_DEPS_PREFIX=/usr/local
# shellcheck disable=SC1091
. "${SCRIPT_DIR}/versions.env"

export DEBIAN_FRONTEND=noninteractive

# -- Base toolchain + from-source link/build deps --
# build-essential pulls gcc/g++/make. clink targets C++23, which means we need gcc 13+ -
# Debian trixie (13) ships gcc 14 by default, fine. The lib*-dev packages here are NOT in
# the connector layer because the from-source builds below need them at BUILD time, and
# the connector layer runs AFTER these builds:
#   libssl-dev           - aws-sdk, Arrow (S3 + GCS), cassandra link system OpenSSL
#   libcurl4-openssl-dev - aws-sdk, Arrow GCS (google-cloud-cpp) link system libcurl
#   libsqlite3-dev       - iceberg-cpp's SQLite catalog (find_package(SQLite3), build-time;
#                          invisible to ldd because libiceberg is a static lib)
#   libxml2-dev          - azure-sdk-for-cpp's storage libs (bundled by Arrow's ARROW_AZURE)
#                          parse XML storage responses via system libxml2 at build time
#   libzstd-dev, zlib1g-dev - compression libs the toolchain links
# All are stable - they do not change when a connector is added - so they cache here.
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
    gcovr \
    libssl-dev \
    libcurl4-openssl-dev \
    libsqlite3-dev \
    libxml2-dev \
    libzstd-dev \
    zlib1g-dev

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

# -- DataStax C/C++ driver for Cassandra / ScyllaDB --
# Not packaged by Debian and no prebuilt arm64 release; build the pinned tag (versions.env) from
# source via CMake. Needs libuv (event loop) + openssl/zlib (already present). clink uses the C
# API (cassandra.h), so the build is ABI-safe regardless of the compiler. Docker-layer-cached.
if [ ! -f "/usr/local/lib/libcassandra.so" ] && [ ! -f "/usr/lib/libcassandra.so" ]; then
    echo "▶ Building DataStax cpp-driver ${CASSANDRA_CPP_DRIVER_VERSION} from source..."
    apt-get install -y --no-install-recommends libuv1-dev zlib1g-dev
    WORK_DIR=$(mktemp -d)
    git clone --depth 1 --branch "${CASSANDRA_CPP_DRIVER_VERSION}" \
        https://github.com/apache/cassandra-cpp-driver.git "$WORK_DIR/cpp-driver"
    cmake -S "$WORK_DIR/cpp-driver" -B "$WORK_DIR/cpp-driver/build" \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
          -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
          -DCASS_BUILD_SHARED=ON \
          -DCASS_BUILD_STATIC=OFF \
          -DCASS_BUILD_EXAMPLES=OFF \
          -DCASS_BUILD_UNIT_TESTS=OFF \
          -DCASS_BUILD_INTEGRATION_TESTS=OFF
    cmake --build "$WORK_DIR/cpp-driver/build" --target install -- -j"${CLINK_BUILD_JOBS:-$(nproc)}"
    cd /
    rm -rf "$WORK_DIR"
    ldconfig
fi

rm -rf /var/lib/apt/lists/*

echo "▶ System deps ready."
