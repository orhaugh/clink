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

export DEBIAN_FRONTEND=noninteractive

# -- Base toolchain --
# build-essential pulls gcc/g++/make. clink targets C++23, which means
# we need gcc 13+ - Debian trixie (13) ships gcc 14 by default, fine.
apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    git \
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
    liburing-dev

# -- Apache Arrow + Parquet --
# clink fundamentally requires Arrow: the wire format is Arrow IPC,
# state-backend snapshots are Arrow IPC streams, and ParquetSink/Source
# share the same ArrowBatcher seam. Installed from Apache's official
# APT repo so the version matches the macOS-host Homebrew Arrow (24+);
# Debian trixie's bundled libarrow is too old for the current
# Parquet API surface we use (parquet::arrow::OpenFile-returning-Result
# was added in Arrow 21).
apt-get install -y --no-install-recommends ca-certificates lsb-release wget
arrow_codename=$(lsb_release --codename --short)
arrow_distro=$(lsb_release --id --short | tr 'A-Z' 'a-z')
arrow_deb="/tmp/apache-arrow-apt-source.deb"
wget -q -O "${arrow_deb}" \
    "https://apache.jfrog.io/artifactory/arrow/${arrow_distro}/apache-arrow-apt-source-latest-${arrow_codename}.deb"
apt-get install -y --no-install-recommends "${arrow_deb}"
apt-get update
apt-get install -y --no-install-recommends libarrow-dev libparquet-dev
rm -f "${arrow_deb}"

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
    cmake --build . --target install -- -j"$(nproc)"
    cd /
    rm -rf "$WORK_DIR"
    ldconfig
fi

# -- AWS SDK for C++ (S3 component only) --
# The SDK is not in Debian; build only the s3 component to keep the
# layer reasonable (~15-25 min on first build, cached after). We also
# need its aws-c-* dependencies which the SDK's CMake fetches as
# subprojects when --recursive is used.
if [ ! -f "/usr/local/lib/libaws-cpp-sdk-s3.so" ] && \
   [ ! -f "/usr/local/lib/libaws-cpp-sdk-s3.a" ]; then
    echo "▶ Building aws-sdk-cpp (s3 component) from source..."
    apt-get install -y --no-install-recommends \
        libcurl4-openssl-dev zlib1g-dev
    WORK_DIR=$(mktemp -d)
    git clone --depth 1 --branch 1.11.428 --recursive \
        https://github.com/aws/aws-sdk-cpp.git "$WORK_DIR/aws-sdk-cpp"
    mkdir "$WORK_DIR/aws-sdk-cpp/build"
    cd "$WORK_DIR/aws-sdk-cpp/build"
    cmake -DCMAKE_INSTALL_PREFIX=/usr/local \
          -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
          -DBUILD_ONLY="s3" \
          -DBUILD_SHARED_LIBS=ON \
          -DENABLE_TESTING=OFF \
          -DAUTORUN_UNIT_TESTS=OFF \
          ..
    cmake --build . --target install -- -j"$(nproc)"
    cd /
    rm -rf "$WORK_DIR"
    ldconfig
fi

rm -rf /var/lib/apt/lists/*

echo "▶ Build env ready."
