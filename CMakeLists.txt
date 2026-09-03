cmake_minimum_required(VERSION 3.24)
project(clink
        VERSION 0.8.0
        DESCRIPTION "Semantics-first, Arrow-native, stateful stream processing engine in modern C++"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Installed binaries must be able to find the pinned toolchain's shared
# libraries: Arrow and friends live under CLINK_DEPS_PREFIX (host default
# ~/.clink-deps), which is on no default loader path, and their install
# names are @rpath-relative. Build-tree binaries work because CMake gives
# them an automatic build rpath, and the Docker image works because its
# toolchain sits in /usr/local - but a host `cmake --install` produced a
# binary with no LC_RPATH at all on macOS, which dyld refuses to load.
# Carrying the external link directories into the installed rpath is the
# standard fix, and covers any other out-of-prefix dependency (Homebrew's
# aws-sdk, system librdkafka) the same way.
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

# CLINK_SANITIZER_BUILD - "this build carries a -fsanitize runtime".
#
# build_and_test.sh passes the sanitizer flags through CMAKE_CXX_FLAGS rather than a
# cache option, so nothing else in the build knows a sanitizer is on. A few tests
# need to, because they assert on the EXIT CODE of a spawned clink binary and a
# sanitizer build of that binary aborts during its own at-exit teardown - see
# tests/test_check_savepoint_cli.cpp. Detecting it here keeps the knowledge in one
# place instead of each test guessing from compiler-specific macros (there is no
# portable predefined macro for UBSan at all).
if(CMAKE_CXX_FLAGS MATCHES "-fsanitize")
    add_compile_definitions(CLINK_SANITIZER_BUILD=1)
    message(STATUS "clink: sanitizer build detected (CLINK_SANITIZER_BUILD=1)")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(CLINK_BUILD_TESTS    "Build clink tests"    ON)
option(CLINK_BUILD_EXAMPLES "Build clink examples" ON)
# libFuzzer discovery targets under fuzz/. OFF by default and hard-errors
# when the toolchain cannot link libFuzzer, rather than skipping quietly -
# someone who asked for fuzzers and silently got none would believe a
# fuzzing gate exists. The REGRESSION half (replaying fuzz/corpus/ through
# the same functions) is part of the normal test suite and needs none of
# this.
option(CLINK_BUILD_FUZZERS  "Build libFuzzer discovery targets" OFF)
# Instrument the WHOLE build for coverage-guided fuzzing, not just the fuzz TUs.
#
# The fuzz targets link clink_core, and without this the library is compiled with
# no fuzzer instrumentation at all - so libFuzzer's coverage feedback sees only
# the header-inline code in each target's own TU. The first campaign made that
# measurable: fault_spec covered FOUR edges while genuinely executing a grammar
# parser, because arm_from_spec lives in src/fault/*.cpp inside the library. The
# fuzzer still executed the real code (ASan would still catch a crash), but its
# mutation was blind - coverage-guided in name only.
#
# fuzzer-no-link instruments without pulling in libFuzzer's main, so the library
# stays linkable by every non-fuzz consumer in this build tree; each fuzz target
# then links -fsanitize=fuzzer on top. ASan+UBSan ride along so a find is a
# report rather than a plain SIGSEGV. Dedicated build directories only - this is
# not a flag combination to mix into a normal build.
if(CLINK_BUILD_FUZZERS)
    add_compile_options(-fsanitize=fuzzer-no-link,address,undefined -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=address,undefined)
endif()
# Connector/backend impls (kafka, postgres, s3, rocksdb, ...). ON by default.
# Turn OFF for a lean core + test-framework build: the install then carries only
# clink::core / clink::clink / clink::test_support and a clinkConfig that
# requires just Arrow/Parquet/Threads, so a downstream find_package(clink) needs
# no connector SDKs.
option(CLINK_BUILD_IMPLS    "Build the connector/backend impls" ON)
# HTTPS in the HTTP subsystem (server + client). ON links OpenSSL when available.
# OFF keeps plain HTTP only and takes no OpenSSL dependency - what a portable
# artifact needs, since the OpenSSL a build finds on macOS is a Homebrew bottle
# pinned to the build host's OS version. See the httplib block below.
option(CLINK_HTTP_TLS       "Enable HTTPS in the HTTP subsystem (links OpenSSL)" ON)
option(CLINK_BUILD_BENCH    "Build clink benchmarks (placeholder)" OFF)
option(CLINK_BUILD_HTTP     "Build the HTTP subsystem (cluster dashboard + JSON API)" ON)
# SQL frontend: parser (libpg_query), binder, logical/physical planner,
# Table API.
#
# ON by default because everything that SHIPS is built with it - both CI test
# jobs, the release binaries, the runtime image and the Python wheel all pass
# -DCLINK_BUILD_SQL=ON explicitly. A default that differs from what ships is the
# wrong default: it means the build a contributor gets from a bare `cmake -S . -B
# build` is not the build CI gates, and 984 tests silently do not run.
#
# That divergence is not theoretical. A Linux verification sweep on 2026-08-04
# reported a green "sql" label containing no SQL tests at all - `ctest -L sql` is
# a REGEX, so it also matches the `mysql` label, and with the frontend off those
# 58 connector cases were the entire "SQL" run. Use -L '^sql$' to mean the
# frontend.
#
# Pass -DCLINK_BUILD_SQL=OFF to skip it: worth about 7% of the object count plus
# libpg_query. The builds that legitimately want it off - sanitizers (the SQL
# runtime tests hang under ASan), fuzzing, and the live-connector runs - all pass
# that explicitly already.
option(CLINK_BUILD_SQL      "Build the SQL frontend (libpg_query + planner)" ON)
# WS4: route the clink::FlatMap state seam through ankerl::unordered_dense
# (open-addressing, metadata-byte probing). OFF aliases std::unordered_map (the
# behaviour-preserving default). The define and include must be GLOBAL because
# FlatMap appears in public headers (InMemoryStateBackend::State), so every TU
# (core, tests, plugins) must agree on the container type or the ABI mismatches.
# Default ON: ankerl::unordered_dense is a measured win on the hot keyed-state
# path (~7-15% on a windowed GROUP BY, host bench) with no behavioural change.
# Set OFF to fall back to std::unordered_map/set.
option(CLINK_USE_FLAT_HASH_MAP "Use ankerl::unordered_dense for the clink::FlatMap state seam" ON)
# Apache Arrow is REQUIRED. It is the canonical snapshot byte format,
# the wire format for every operator-to-operator data frame, and the
# substrate for ParquetSink/Source. There is no "Arrow OFF" build
# path - header dependencies make it non-conditional. The
# CLINK_HAS_ARROW define is still emitted PUBLIC on clink::core for
# user code that wants to gate Arrow-specific extensions; inside
# clink we treat it as always-defined.
option(CLINK_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)
# Deterministic fault injection inside clink's own runtime paths
# (include/clink/fault/fault_injection.hpp). AUTO follows CLINK_BUILD_TESTS:
# a test build carries the fault surface, a tests-off build does not. ON
# forces it in (needed for the multi-process integration harness, which arms
# faults in a spawned clink_node via CLINK_FAULT_INJECT); OFF compiles every
# CLINK_FAULT_POINT to nothing, so a shipped binary has no fault surface at
# all. The state is reported by `clink --capabilities`.
set(CLINK_ENABLE_FAULT_INJECTION "AUTO" CACHE STRING
    "Compile in runtime fault-injection points (AUTO|ON|OFF)")
set_property(CACHE CLINK_ENABLE_FAULT_INJECTION PROPERTY STRINGS AUTO ON OFF)
if(CLINK_ENABLE_FAULT_INJECTION STREQUAL "AUTO")
    set(CLINK_FAULT_INJECTION_ACTIVE ${CLINK_BUILD_TESTS})
else()
    set(CLINK_FAULT_INJECTION_ACTIVE ${CLINK_ENABLE_FAULT_INJECTION})
endif()
# Per-dependency tri-state: AUTO uses the dep when found, ON requires
# the dep at configure time, OFF skips the impl module entirely.
# Each impl reads its own knob from impls/<x>/CMakeLists.txt - the
# discovery details live there rather than here.
#
# RocksDB is the exception: it's shipped with every clink build
# (mirrors  bundling rocksdb-jni in the distribution). The impl
# module finds a system RocksDB if present, else FetchContent-builds
# from source - never silently skipped. ON is the only sensible
# value; the cache var is kept for symmetry with the other knobs.
set(CLINK_WITH_ROCKSDB    "ON"   CACHE STRING "RocksDB impl (always built; cache kept for parity)")
set(CLINK_WITH_KAFKA      "AUTO" CACHE STRING "Kafka impl (AUTO/ON/OFF)")
set(CLINK_WITH_AWS_S3     "AUTO" CACHE STRING "S3 impl (AUTO/ON/OFF)")
set(CLINK_WITH_AWS        "AUTO" CACHE STRING "AWS-family connectors: kinesis/firehose/dynamodb (AUTO/ON/OFF)")
set(CLINK_WITH_CLICKHOUSE "AUTO" CACHE STRING "ClickHouse impl (AUTO/ON/OFF)")
set(CLINK_WITH_POSTGRES   "AUTO" CACHE STRING "Postgres impl (AUTO/ON/OFF)")
set(CLINK_WITH_TLS        "AUTO" CACHE STRING "TLS impl (AUTO/ON/OFF)")
set(CLINK_WITH_ETCD       "AUTO" CACHE STRING "etcd HA coordinator (AUTO/ON/OFF)")
set(CLINK_WITH_AVRO       "AUTO" CACHE STRING "Avro codec impl (AUTO/ON/OFF)")
set(CLINK_WITH_HTTP       "AUTO" CACHE STRING "HTTP connector impl (AUTO/ON/OFF)")
set(CLINK_WITH_REDIS      "AUTO" CACHE STRING "Redis Streams connector impl (AUTO/ON/OFF)")
set(CLINK_WITH_MYSQL      "AUTO" CACHE STRING "MySQL/MariaDB connector impl (AUTO/ON/OFF)")
set(CLINK_WITH_MQTT       "AUTO" CACHE STRING "MQTT connector impl (AUTO/ON/OFF)")
set(CLINK_WITH_MONGODB    "AUTO" CACHE STRING "MongoDB connector impl (AUTO/ON/OFF)")
set(CLINK_WITH_ICEBERG    "AUTO" CACHE STRING "Apache Iceberg sink impl via iceberg-cpp (AUTO/ON/OFF)")
set(CLINK_WITH_RABBITMQ   "AUTO" CACHE STRING "RabbitMQ/AMQP connector impl via librabbitmq (AUTO/ON/OFF)")
set(CLINK_WITH_NATS       "AUTO" CACHE STRING "NATS JetStream connector impl via libnats (AUTO/ON/OFF)")
set(CLINK_WITH_PULSAR     "AUTO" CACHE STRING "Apache Pulsar connector impl via libpulsar (AUTO/ON/OFF)")
set(CLINK_WITH_CASSANDRA  "AUTO" CACHE STRING "Cassandra/ScyllaDB sink impl via the DataStax cpp-driver (AUTO/ON/OFF)")
set(CLINK_WITH_GCS        "AUTO" CACHE STRING "GCS Parquet connector via Arrow's GcsFileSystem (AUTO/ON/OFF)")
set(CLINK_WITH_AZURE      "AUTO" CACHE STRING "Azure Blob Parquet connector via Arrow's AzureFileSystem (AUTO/ON/OFF)")
set(CLINK_WITH_WEBHDFS    "AUTO" CACHE STRING "WebHDFS/HttpFS Parquet connector over the HTTP client (AUTO/ON/OFF)")
set(CLINK_WITH_WEBSOCKET  "AUTO" CACHE STRING "WebSocket source impl, RFC 6455 in-tree, OpenSSL for wss (AUTO/ON/OFF)")
set(CLINK_WITH_VECTOR_SEARCH "AUTO" CACHE STRING "VECTOR_SEARCH KNN impl: SimSIMD kernels + usearch HNSW, header-only (AUTO/ON/OFF)")
# ONNX Runtime local inference provider. OFF by default (heavy, optional dep): AUTO uses
# a locally-discoverable runtime but never downloads (offline-safe); ON fetches a pinned
# prebuilt runtime when none is found locally.
set(CLINK_WITH_ONNX       "OFF"  CACHE STRING "ONNX Runtime ML_PREDICT provider (OFF/AUTO/ON)")
# WebAssembly scalar UDFs (CREATE FUNCTION ... LANGUAGE wasm). Opt-in like ONNX:
# AUTO uses a locally-discoverable wasmtime C API but never downloads (offline-safe);
# ON fetches the pinned prebuilt c-api when none is found locally.
set(CLINK_WITH_WASM       "OFF"  CACHE STRING "WebAssembly (wasmtime) scalar UDFs (OFF/AUTO/ON)")
# ForSt state backend (github.com/ververica/ForSt, an LSM store of RocksDB
# lineage). Opt-in: ON clones and builds the pinned tag via ExternalProject
# (needs network + a RocksDB-scale first build), so there is no AUTO tier -
# nothing local to discover.
set(CLINK_WITH_FORST      "OFF"  CACHE STRING "ForSt state backend, built from the pinned upstream tag (OFF/ON)")
# jemalloc as the process allocator. Opt-in, and OFF by default on the evidence:
# measured on the two-node nexmark rig against the SAME clink binary with only the
# allocator changed, it gave +5% sustained throughput on the windowed query (q12),
# neutral on the stateless one (q0), neutral on CPU per event, and NO memory change.
# See benchmarks/nexmark_compare/cloud/README.md for the numbers.
#
# Worth having as an option because malloc is demonstrably on clink's critical path -
# capping glibc to two arenas (MALLOC_ARENA_MAX=2) halves throughput - and jemalloc's
# per-thread caches avoid that contention. Not worth a hard dependency for 5% on one
# query shape.
set(CLINK_WITH_JEMALLOC   "OFF"  CACHE STRING "Link jemalloc as the process allocator (OFF/AUTO/ON), Linux only")
foreach(_opt CLINK_WITH_ROCKSDB CLINK_WITH_KAFKA CLINK_WITH_AWS_S3 CLINK_WITH_AWS
             CLINK_WITH_CLICKHOUSE CLINK_WITH_POSTGRES CLINK_WITH_TLS
             CLINK_WITH_ETCD CLINK_WITH_AVRO CLINK_WITH_HTTP CLINK_WITH_REDIS CLINK_WITH_MYSQL
             CLINK_WITH_MQTT CLINK_WITH_MONGODB CLINK_WITH_ICEBERG CLINK_WITH_RABBITMQ
             CLINK_WITH_NATS CLINK_WITH_PULSAR CLINK_WITH_CASSANDRA CLINK_WITH_GCS CLINK_WITH_AZURE
             CLINK_WITH_WEBHDFS CLINK_WITH_WEBSOCKET CLINK_WITH_VECTOR_SEARCH CLINK_WITH_ONNX
             CLINK_WITH_WASM CLINK_WITH_FORST CLINK_WITH_JEMALLOC)
    set_property(CACHE ${_opt} PROPERTY STRINGS AUTO ON OFF)
endforeach()
if(CLINK_WITH_ROCKSDB STREQUAL "OFF")
    message(FATAL_ERROR
        "CLINK_WITH_ROCKSDB=OFF is not supported. RocksDB is a required "
        "backend (matches bundled rocksdb-jni model). If the system "
        "package isn't available, the impl falls back to FetchContent so "
        "the build always succeeds.")
endif()
option(CLINK_INTEGRATION_TESTS "Build integration tests that need external services (Docker, etc.)" OFF)

include(CompilerWarnings)
include(GNUInstallDirs)
include(cmake/ClinkInstall.cmake)
# Pulls in ClinkPluginDebugSplit.cmake and defines clink_add_job_module(),
# the one supported recipe for building a job/plugin module (also installed
# with the CMake package for out-of-tree consumers).
include(cmake/ClinkJobModule.cmake)

# Always emit compile_commands.json so clang-tidy / clangd / IDEs can
# resolve per-TU flags without extra plumbing. Cheap to leave on.
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build static libraries with -fPIC so the test-fixture plugin .so
# modules (clink_hello_plugin, clink_gateway_plugin) can consume
# them. On ELF (Linux) this is a hard requirement: shared objects
# cannot consume non-PIC static archives - the AArch64 linker refuses
# with "dangerous relocation: unsupported relocation". macOS Mach-O is
# lenient, which is why this didn't surface until we ran the sanitizer
# matrix in the Linux Docker image.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# Plugin ABI compatibility.
#
# CLINK_ABI_VERSION is the compatibility gate: a single integer baked into
# both clink and any plugin built against these headers. The loader accepts a
# plugin whose ABI version equals the cluster's and refuses otherwise, so a
# plugin survives any marketing-version bump (patch or feature) that does NOT
# change the ABI. BUMP IT BY HAND, and only on an intentional ABI break -
# anything that changes a layout crossing the dlopen boundary: a data member
# added to RuntimeContext / BuildContext / StreamElement / Batch / Record /
# PluginRegistry, a virtual added or reordered on Operator / Source / Sink, an
# enum's underlying type, an inline/template body compiled into plugins, or the
# CLINK_USE_FLAT_HASH_MAP choice (the FlatMap type must match across the seam).
# It is deliberately decoupled from project(VERSION); once the project reaches
# 1.0 it can simply track the semver major.
#
# The git commit hash is still baked in (below) but is now INFORMATIONAL - it
# is reported in logs / plugin metadata and used only by the opt-in strict mode
# (CLINK_STRICT_PLUGIN_ABI=1), which restores the historic exact-match gate.
# ---------------------------------------------------------------------------
set(CLINK_ABI_VERSION 1)
find_package(Git QUIET)
# An explicitly supplied hash wins over discovery. A source archive carries
# no .git, so a build from one could not name its own commit and reported
# "unknown" - which for a qualification image is the whole provenance chain,
# since results are only attributable to a revision if the binary can say
# which one it is. The builder passes -DCLINK_ABI_HASH=<sha> and takes
# responsibility for it matching the source it shipped.
set(CLINK_ABI_HASH_SUPPLIED "${CLINK_ABI_HASH}")
set(CLINK_ABI_HASH "unknown")
set(CLINK_ABI_HASH_CLEAN "false")
if(CLINK_ABI_HASH_SUPPLIED)
    set(CLINK_ABI_HASH "${CLINK_ABI_HASH_SUPPLIED}")
    set(CLINK_ABI_HASH_CLEAN "true")
elseif(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        OUTPUT_VARIABLE _git_hash
        OUTPUT_STRIP_TRAILING_WHITESPACE
        RESULT_VARIABLE _git_rc)
    if(_git_rc EQUAL 0 AND _git_hash)
        set(CLINK_ABI_HASH "${_git_hash}")
    endif()
    execute_process(
        COMMAND ${GIT_EXECUTABLE} status --porcelain
        WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
        OUTPUT_VARIABLE _git_dirty
        RESULT_VARIABLE _git_rc2)
    if(_git_rc2 EQUAL 0 AND _git_dirty STREQUAL "")
        set(CLINK_ABI_HASH_CLEAN "true")
    endif()
endif()
message(STATUS "clink: plugin ABI hash = ${CLINK_ABI_HASH} (clean: ${CLINK_ABI_HASH_CLEAN})")

# The structural ABI fingerprint (the DEFAULT plugin compatibility gate) is
# computed further down, after dependency detection: its material includes the
# pinned Arrow version and the resolved ABI-relevant build options, which are
# not known yet at this point. See "Plugin ABI fingerprint" below.

# WS4 open-addressing state container. Fetched + made global BEFORE any target
# so the CLINK_USE_FLAT_HASH_MAP define and the ankerl include reach every TU
# uniformly (clink::FlatMap is in public headers; a per-target define would let
# the container type differ across the ABI boundary). ankerl/unordered_dense is
# a single MIT header; the std::unordered_map fallback (flag OFF) needs nothing.
if(CLINK_USE_FLAT_HASH_MAP)
    include(FetchContent)
    FetchContent_Declare(
        unordered_dense
        GIT_REPOSITORY https://github.com/martinus/unordered_dense.git
        GIT_TAG v4.4.0
        GIT_SHALLOW TRUE
        SYSTEM
    )
    FetchContent_MakeAvailable(unordered_dense)
    add_compile_definitions(CLINK_USE_FLAT_HASH_MAP=1)
    include_directories(SYSTEM ${unordered_dense_SOURCE_DIR}/include)
    # The container choice is part of the ABI: clink::FlatMap appears in public
    # headers (InMemoryStateBackend::State), so a downstream find_package(clink)
    # consumer MUST compile those headers with the same choice, or the library
    # and the consumer disagree on the map layout (a crash, not a link error).
    # Ship the single ankerl header alongside clink's so <ankerl/unordered_dense.h>
    # resolves from the install prefix; the matching define is propagated as a
    # PUBLIC compile definition on clink_core below.
    install(DIRECTORY "${unordered_dense_SOURCE_DIR}/include/ankerl"
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

# simdjson backs config::parse (the JSON decode on every Row-channel hop:
# source decode AND each network-bridge wire decode), replacing the
# hand-rolled recursive parser. Pinned tag, source-only fetch: the
# AMALGAMATED single TU (singleheader/simdjson.cpp) is compiled directly
# into clink_core under clink's own flags rather than as a CMake
# subproject - the subproject build intermittently corrupted the heap on
# macOS arm64 (mfm_alloc/mfm_free aborts under the xzone allocator; the
# same version amalgamated under our flags is clean, verified with 20-run
# loops of the parser-heavy suites either way). The header stays PRIVATE
# to src/config/json.cpp, off the public surface and the plugin ABI.
set(CLINK_SIMDJSON_TAG "v3.12.2" CACHE STRING "simdjson version tag")
include(FetchContent)
FetchContent_Declare(
    simdjson_src
    GIT_REPOSITORY https://github.com/simdjson/simdjson.git
    GIT_TAG ${CLINK_SIMDJSON_TAG}
    GIT_SHALLOW TRUE
    SYSTEM
)
FetchContent_GetProperties(simdjson_src)
if(NOT simdjson_src_POPULATED)
    FetchContent_Populate(simdjson_src)
endif()
set(CLINK_SIMDJSON_AMALGAMATED_DIR "${simdjson_src_SOURCE_DIR}/singleheader")

# ---------------------------------------------------------------------------
# Instruction-set baseline (opt-in).
#
# Empty by default, which means clink's own code compiles for the toolchain's
# baseline - on x86-64 that is SSE2, so a hand-written autovectorisable loop gets
# 128-bit vectors on a machine that has 256- or 512-bit ones. The libraries doing
# the heaviest lifting (simdjson, Arrow) are unaffected either way: they compile
# every implementation and dispatch on the CPU at RUNTIME.
#
# Setting this raises the floor for OUR loops - e.g. x86-64-v3 for AVX2 (Haswell,
# 2013+) or x86-64-v4 for AVX-512. It is a PORTABILITY decision, not just a
# performance one: a binary built this way executes illegal instructions on an older
# CPU, and clink ships a container image and Python wheels. Left off by default for
# exactly that reason; set it for a deployment whose hardware floor is known.
#
# Applied globally (add_compile_options) rather than per-target, so bundled
# third-party code compiled in this tree gets the same baseline as the engine.
set(CLINK_ISA_BASELINE "" CACHE STRING
    "Raise the instruction-set baseline for compiled code, e.g. x86-64-v3 (AVX2) or \
x86-64-v4 (AVX-512). Empty = the toolchain default. Binaries will NOT run on CPUs \
below the chosen level.")
if(NOT CLINK_ISA_BASELINE STREQUAL "")
    include(CheckCXXCompilerFlag)
    set(_clink_isa_flag "-march=${CLINK_ISA_BASELINE}")
    check_cxx_compiler_flag("${_clink_isa_flag}" CLINK_ISA_FLAG_OK)
    if(NOT CLINK_ISA_FLAG_OK)
        message(FATAL_ERROR
            "CLINK_ISA_BASELINE=${CLINK_ISA_BASELINE} rejected by this compiler "
            "(tried ${_clink_isa_flag}). Use a level this toolchain knows, or leave "
            "the option empty.")
    endif()
    add_compile_options("${_clink_isa_flag}")
    message(STATUS "clink: ISA baseline - ${_clink_isa_flag} (binaries require this level)")
endif()

# ---------------------------------------------------------------------------
# Process allocator (opt-in).
#
# Linked into clink_core PUBLIC, so every binary built from this tree and every
# consumer of the install picks it up - an allocator that only some of the
# process uses is worse than none.
#
# LINUX ONLY, and a request on macOS is a hard error rather than a silent skip.
# jemalloc replaces malloc on Linux simply by being linked; on macOS the
# allocator is zone-based and interposition needs a different mechanism, so a
# link there would leave the system allocator in charge while the build reported
# success. A no-op that reports success is the exact failure mode this codebase
# has been bitten by twice (a dead in-process fast path, a shadowed sink), so it
# is refused instead.
# ---------------------------------------------------------------------------
set(CLINK_JEMALLOC_ACTIVE OFF)
if(NOT CLINK_WITH_JEMALLOC STREQUAL "OFF")
    if(APPLE)
        if(CLINK_WITH_JEMALLOC STREQUAL "ON")
            message(FATAL_ERROR
                "CLINK_WITH_JEMALLOC=ON is not supported on macOS: jemalloc does not "
                "replace the system allocator by linking alone there, so the build "
                "would succeed while leaving malloc unchanged. Use AUTO (skips) or "
                "build on Linux / in the Docker image.")
        endif()
        message(STATUS "clink: jemalloc - skipped on macOS (interposition unsupported)")
    else()
        find_library(CLINK_JEMALLOC_LIB NAMES jemalloc)
        find_path(CLINK_JEMALLOC_INCLUDE_DIR jemalloc/jemalloc.h)
        if(CLINK_JEMALLOC_LIB AND CLINK_JEMALLOC_INCLUDE_DIR)
            set(CLINK_JEMALLOC_ACTIVE ON)
            message(STATUS "clink: jemalloc - enabled (${CLINK_JEMALLOC_LIB})")
        elseif(CLINK_WITH_JEMALLOC STREQUAL "ON")
            message(FATAL_ERROR
                "CLINK_WITH_JEMALLOC=ON but jemalloc was not found. Install it "
                "(Debian/Ubuntu: libjemalloc-dev) or set the option to AUTO/OFF.")
        else()
            message(STATUS "clink: jemalloc - not found; using the system allocator")
        endif()
    endif()
endif()

# ---------------------------------------------------------------------------
# Core library - no external connector/state-backend deps.
#
# Everything in src/ here is the engine: cluster runtime, scheduler,
# checkpoint coordinator, JSON config, metrics, the plain-text /
# in-memory / file state backends. Vendor connectors and the RocksDB
# state backend live under impls/ and link in as separate static libs.
# ---------------------------------------------------------------------------
# Experiment gate for followups item 30. STATIC is the shipped model: each
# job .so links its own copy of the engine, so under RTLD_LOCAL every module
# gets private singletons. The 2026-08-13 audit argued nothing DEPENDS on
# that (per-job isolation is JobBundle's, keyed by job id, so it is finer
# than per-.so anyway) and that the duplication is a pure hazard - five
# defects, each fixed by hand-threading a host instance onto RunnerContext.
#
# ON builds the engine once and links every module against it. That
# experiment was RUN on 2026-08-13 and it FAILS - the audit's argument was
# about behaviour and missed LIFETIME. Kept, because one command reproduces
# the finding and a future attempt should start from the failure rather than
# from the reasoning:
#
#   cmake -S . -B build-shared -DCLINK_SHARED_ENGINE=ON \
#         -DCLINK_INTEGRATION_TESTS=ON && cmake --build build-shared
#
# What breaks, on macOS - the platform MOST likely to tolerate it, since its
# linker already coalesces these symbols: every failure sits on the
# dlopen -> register -> call-exported-symbol path. SchemaEvoCheck passes its
# assertions then SIGSEGVs at exit; check-savepoint's CLI subprocess exits
# 139; SideOutputRecovery fails a worker-kill assertion.
#
# The mechanism is one this tree already carries a scar from - see the
# RTLD_NODELETE comment in src/cluster/restore_compat_gate.cpp: a plugin's
# build_fn registers std::function closures whose CODE LIVES IN THE MODULE
# into "process-wide" registries. With a STATIC engine those registries are
# per-.so, so closure lifetime and registry lifetime are the same lifetime
# and the ordering is free. Make the engine shared and the registries become
# genuinely process-wide, outliving the modules whose closures they hold -
# a use-after-unload at teardown.
#
# So the static link IS load-bearing, for lifetime rather than for the
# behavioural isolation item 30 claimed. Any future attempt has to answer
# closure ownership across the boundary first (registry entries owning
# module handles, or a registration protocol that copies rather than
# captures) - the linkage change is the easy part.
option(CLINK_SHARED_ENGINE "Build clink_core as ONE shared library (item 30 experiment - KNOWN BROKEN, see comment)" OFF)
if(CLINK_SHARED_ENGINE)
    set(_clink_core_linkage SHARED)
    message(WARNING "clink::core - SHARED engine: a KNOWN-FAILING experiment (item 30). "
                    "Plugin-registered closures outlive nothing and the dlopen paths SIGSEGV. "
                    "See the comment above this option in CMakeLists.txt.")
else()
    set(_clink_core_linkage STATIC)
endif()
add_library(clink_core ${_clink_core_linkage}
    src/fault/fault_injection.cpp
    src/connectors/capability.cpp
    src/connectors/builtin_capabilities.cpp
    src/connectors/delivery_guarantee.cpp
    src/checkpoint/checkpoint_coordinator.cpp
    src/metrics/metrics_registry.cpp
    src/metrics/otlp_export.cpp
    src/metrics/prometheus.cpp
    src/metrics/system_metrics.cpp
    src/application/job_submitter.cpp
    src/runtime/event_bus.cpp
    src/runtime/local_executor.cpp
    src/runtime/local_submitter.cpp
    src/runtime/log_buffer.cpp
    src/runtime/logging.cpp
    src/runtime/network/connection.cpp
    src/runtime/network/network_socket.cpp
    src/state/in_memory_state_backend.cpp
    src/state/snapshot_arrow_writer.cpp
    src/state/snapshot_canonicalise.cpp
    src/state/changelog_state_backend.cpp
    src/state/state_backend_factory.cpp
    src/state/schema_version.cpp
    src/state/state_migration_on_restore.cpp
    src/cluster/coordination_store.cpp
    src/cluster/fenced_metadata.cpp
    src/cluster/ha_coordinator.cpp
    src/cluster/restore_compat_gate.cpp
    src/cluster/coordinator.cpp
    src/cluster/in_doubt_resolution.cpp
    src/cluster/worker.cpp
    src/cluster/worker_supervisor.cpp
    src/cluster/service_discovery.cpp
    src/cluster/job_graph.cpp
    src/cluster/job_planner.cpp
    src/cluster/operator_registry.cpp
    src/cluster/type_registry.cpp
    src/cluster/runner_registry.cpp
    src/cluster/rescale_coordinator.cpp
    src/cluster/rescale_dispatch.cpp
    src/cluster/autoscaler.cpp
    src/cluster/refresh_scheduler.cpp
    src/cluster/dag_builder_registry.cpp
    src/cluster/built_in_factories.cpp
    src/cluster/guarantee_gate.cpp
    src/cluster/connector_availability.cpp
    src/cluster/config_lint.cpp
    src/cluster/plugin_loader.cpp
    src/cluster/plugin_cache.cpp
    src/lineage/lineage_graph.cpp
    src/lineage/lineage_listener.cpp
    src/config/interned_name.cpp
    src/config/json.cpp
    src/connectors/openssl_atexit_guard.cpp
)
if(CLINK_BUILD_HTTP)
    target_sources(clink_core PRIVATE
        src/http/http_server.cpp
        src/http/http_client.cpp
        src/lineage/openlineage_exporter.cpp
    )
endif()
add_library(clink::core ALIAS clink_core)
set_target_properties(clink_core PROPERTIES EXPORT_NAME core)

# Propagate the FlatMap container choice as a PUBLIC (exported) compile
# definition so a downstream find_package(clink) consumer compiles clink's
# public headers with the SAME clink::FlatMap type the library was built with.
# The build tree already gets this via add_compile_definitions above; this line
# carries it across the install/export boundary (INTERFACE_COMPILE_DEFINITIONS).
if(CLINK_USE_FLAT_HASH_MAP)
    target_compile_definitions(clink_core PUBLIC CLINK_USE_FLAT_HASH_MAP=1)
endif()

# PUBLIC, and it must stay PUBLIC: fault_injection.hpp changes what the
# Registry class declares based on this macro, so a TU that sees it and a
# TU that does not would be an ODR violation waiting for a link-time
# surprise. Everything that links clink::core therefore agrees.
if(CLINK_FAULT_INJECTION_ACTIVE)
    target_compile_definitions(clink_core PUBLIC CLINK_FAULT_INJECTION=1)
endif()

# The capability manifest reports the version of the binary it is running
# in, so it needs the project version as a compile-time string.
target_compile_definitions(clink_core PUBLIC CLINK_VERSION_STRING="${PROJECT_VERSION}")

target_include_directories(clink_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_core PUBLIC cxx_std_23)

find_package(Threads REQUIRED)
target_link_libraries(clink_core PUBLIC Threads::Threads)
if(CLINK_JEMALLOC_ACTIVE)
    # PUBLIC so it reaches every executable and every consumer of the install: on Linux
    # jemalloc takes over malloc by being present in the link, and a process where only
    # part of the code allocates through it is worse than not using it at all.
    target_link_libraries(clink_core PUBLIC ${CLINK_JEMALLOC_LIB})
    target_include_directories(clink_core PUBLIC ${CLINK_JEMALLOC_INCLUDE_DIR})
    # PUBLIC: allocator_info.hpp is a header, so a consumer's TU must see the same
    # definition or it would report the wrong allocator.
    target_compile_definitions(clink_core PUBLIC CLINK_HAS_JEMALLOC=1)
endif()
# simdjson is an implementation detail of src/config/json.cpp: the
# amalgamated TU is a clink_core source (see the fetch block above) and
# only json.cpp sees the header.
target_sources(clink_core PRIVATE ${CLINK_SIMDJSON_AMALGAMATED_DIR}/simdjson.cpp)
target_include_directories(clink_core SYSTEM PRIVATE ${CLINK_SIMDJSON_AMALGAMATED_DIR})
if(CLINK_BUILD_HTTP)
    # httplib is header-only - PRIVATE link only adds the include path
    # for clink_core.cpp files that need it (just http_server.cpp).
    # Wrap in $<BUILD_INTERFACE:> so the install(EXPORT) step doesn't
    # try to record httplib::httplib (a non-installed FetchContent
    # INTERFACE target) in clinkTargets.cmake.
    target_link_libraries(clink_core PRIVATE $<BUILD_INTERFACE:httplib::httplib>)
    target_compile_definitions(clink_core PUBLIC CLINK_HAS_HTTP=1)
endif()

# ---------------------------------------------------------------------------
# Pinned, from-source third-party toolchain (scripts/versions.env). Arrow,
# Parquet and iceberg-cpp are compiled from source at exact versions into
# CLINK_DEPS_PREFIX (scripts/build-arrow.sh / build-iceberg-cpp.sh) on BOTH the
# macOS host and the Debian image, so every environment links byte-for-byte the
# same libraries. Prepend that prefix so find_package() resolves it ahead of any
# system/Homebrew copy, and read the pinned Arrow version to assert against drift.
# ---------------------------------------------------------------------------
set(_clink_versions_env "${CMAKE_CURRENT_SOURCE_DIR}/scripts/versions.env")
if(EXISTS "${_clink_versions_env}")
    file(STRINGS "${_clink_versions_env}" _clink_ver_lines REGEX "^ARROW_VERSION=")
    foreach(_l IN LISTS _clink_ver_lines)
        if(_l MATCHES "^ARROW_VERSION=(.+)$")
            set(CLINK_PINNED_ARROW_VERSION "${CMAKE_MATCH_1}")
        endif()
    endforeach()
endif()
if(DEFINED ENV{CLINK_DEPS_PREFIX})
    set(_clink_deps_prefix "$ENV{CLINK_DEPS_PREFIX}")
elseif(DEFINED ENV{HOME})
    set(_clink_deps_prefix "$ENV{HOME}/.clink-deps")
endif()
if(_clink_deps_prefix AND IS_DIRECTORY "${_clink_deps_prefix}")
    list(PREPEND CMAKE_PREFIX_PATH "${_clink_deps_prefix}")
    message(STATUS "clink::core - pinned toolchain prefix: ${_clink_deps_prefix}")
endif()

# Apache Arrow - required (the wire format, state snapshots and the Parquet/
# Iceberg connectors are all Arrow-native). Surfaces via the CLINK_HAS_ARROW
# preprocessor define and the Arrow::arrow_shared imported target.
find_package(Arrow REQUIRED)
if(CLINK_PINNED_ARROW_VERSION AND NOT ARROW_VERSION VERSION_EQUAL "${CLINK_PINNED_ARROW_VERSION}")
    message(FATAL_ERROR
        "Arrow ${ARROW_VERSION} resolved, but scripts/versions.env pins "
        "${CLINK_PINNED_ARROW_VERSION}. Build the pinned toolchain "
        "(scripts/build-arrow.sh) and put CLINK_DEPS_PREFIX on the search path, "
        "or update scripts/versions.env if the bump is intentional.")
endif()
target_link_libraries(clink_core PUBLIC Arrow::arrow_shared)
target_compile_definitions(clink_core PUBLIC CLINK_HAS_ARROW=1)
message(STATUS "clink::core - Arrow v${ARROW_VERSION} (pinned ${CLINK_PINNED_ARROW_VERSION})")

# Arrow's COMPUTE kernels (comparison, arithmetic, boolean, ...) live in a SEPARATE
# library from Arrow 15 onwards, and are registered on demand rather than at load. So
# linking libarrow alone gives a function registry holding 13 entries - `filter` and a
# handful of selection kernels - and `greater_equal` is simply absent.
#
# That was previously recorded in the engine as a package limitation: a comment in
# columnar_filter_operator.hpp said arrow::compute::Initialize() "is not an exported
# symbol in this package (verified against Arrow::arrow_shared)", and the comparison
# masks were hand-rolled as a consequence. The check was against the wrong library.
# Initialize() IS exported, from libarrow_compute, and calling it takes the registry
# from 13 functions to 305 - measured against both the Homebrew and the pinned Arrow 24
# prefixes, with greater_equal then returning correct results.
#
# Optional rather than REQUIRED: a build against an Arrow that does not ship the compute
# library keeps working, and every caller of a kernel is expected to have a non-kernel
# path anyway (see clink::arrow_compute_available()).
find_package(ArrowCompute QUIET)
# Recorded into clinkConfig.cmake so an installed clink tells its consumers whether they
# must resolve ArrowCompute too. Without that, the exported link interface names a target
# the consumer never found and their configure step fails at generate time.
set(CLINK_ARROW_COMPUTE_FOUND OFF)
if(TARGET ArrowCompute::arrow_compute_shared)
    set(CLINK_ARROW_COMPUTE_FOUND ON)
    target_link_libraries(clink_core PUBLIC ArrowCompute::arrow_compute_shared)
    target_compile_definitions(clink_core PUBLIC CLINK_HAS_ARROW_COMPUTE=1)
    message(STATUS "clink::core - Arrow compute kernels available (arrow_compute)")
else()
    message(STATUS "clink::core - Arrow compute kernels NOT available; hand-rolled paths only")
endif()

# Parquet ships alongside Arrow in every standard distribution
# (Homebrew, apt, conda). Required by the ParquetSink/Source built-ins
# and the snapshot/wire layers that share the same batcher seam.
find_package(Parquet REQUIRED)
target_link_libraries(clink_core PUBLIC Parquet::parquet_shared)
target_compile_definitions(clink_core PUBLIC CLINK_HAS_PARQUET=1)
message(STATUS "clink::core - Parquet enabled")

# ---------------------------------------------------------------------------
# Plugin ABI fingerprint: the DEFAULT plugin compatibility gate.
#
# A SHA256 over the DECLARED extension surface - the tracked manifest
# scripts/plugin-abi-surface.txt, generated as the transitive include closure
# of the plugin authoring entry points (scripts/gen-plugin-abi-surface.py) -
# plus the ABI-relevant build options named by the manifest's [options]
# section, the pinned Arrow version and CLINK_ABI_VERSION. It rotates when
# the surface a plugin can actually reach across the dlopen boundary changes,
# and NOT on edits to host-side headers (sql/, embed/, queryable_state/, ...)
# or to .cpp / tests / docs / build scaffolding - so a cluster rebuild keeps
# loading existing plugins unless the contract genuinely moved. A header
# entering or leaving the closure shows up as a reviewed manifest diff, gated
# by the --check mode in CI and the pre-commit hook. CLINK_ABI_VERSION folds
# in as a manual force-rotate for a semantic break the header text cannot
# capture. The generated header is in the BINARY dir, so it is not self-hashed
# (its .hpp.in template is hashed instead, via the manifest).
# ---------------------------------------------------------------------------
set(_clink_abi_manifest "${CMAKE_CURRENT_SOURCE_DIR}/scripts/plugin-abi-surface.txt")
if(NOT EXISTS "${_clink_abi_manifest}")
    message(FATAL_ERROR
        "clink: scripts/plugin-abi-surface.txt is missing. Regenerate it with "
        "scripts/gen-plugin-abi-surface.py.")
endif()
file(STRINGS "${_clink_abi_manifest}" _clink_abi_manifest_lines)
set(_clink_abi_headers "")
set(_clink_abi_options "")
set(_clink_abi_section "")
foreach(_l IN LISTS _clink_abi_manifest_lines)
    if(_l MATCHES "^#" OR _l STREQUAL "")
        continue()
    elseif(_l STREQUAL "[headers]")
        set(_clink_abi_section "headers")
    elseif(_l STREQUAL "[options]")
        set(_clink_abi_section "options")
    elseif(_clink_abi_section STREQUAL "headers")
        list(APPEND _clink_abi_headers "${_l}")
    elseif(_clink_abi_section STREQUAL "options")
        list(APPEND _clink_abi_options "${_l}")
    else()
        message(FATAL_ERROR
            "clink: unrecognised line in scripts/plugin-abi-surface.txt: '${_l}'")
    endif()
endforeach()

# Each [options] token maps to the build's resolved value, normalised to 0/1
# so ON/TRUE/1 spellings cannot diverge. An unmapped token means a surface
# header gained a new conditional-compilation knob: decide whether it is
# ABI-relevant and add the mapping here - refusing to guess is the point.
set(_clink_abi_material "abi_version=${CLINK_ABI_VERSION};arrow_version=${CLINK_PINNED_ARROW_VERSION};")
foreach(_opt IN LISTS _clink_abi_options)
    if(_opt STREQUAL "CLINK_USE_FLAT_HASH_MAP")
        set(_clink_opt_value "${CLINK_USE_FLAT_HASH_MAP}")
    elseif(_opt STREQUAL "CLINK_FAULT_INJECTION")
        # fault_injection.hpp swaps a class declaration on this macro, so a
        # test-configured build (AUTO -> ON) and a runtime build (OFF) are
        # genuinely different ABIs.
        set(_clink_opt_value "${CLINK_FAULT_INJECTION_ACTIVE}")
    elseif(_opt STREQUAL "CLINK_HAS_ARROW")
        set(_clink_opt_value ON)  # find_package(Arrow REQUIRED) above
    elseif(_opt STREQUAL "CLINK_HAS_PARQUET")
        set(_clink_opt_value ON)  # find_package(Parquet REQUIRED) above
    elseif(_opt STREQUAL "CLINK_HAS_ARROW_COMPUTE")
        set(_clink_opt_value "${CLINK_ARROW_COMPUTE_FOUND}")
    else()
        message(FATAL_ERROR
            "clink: plugin-abi-surface.txt declares option '${_opt}' with no "
            "value mapping in CMakeLists.txt. Add one where the fingerprint "
            "material is assembled.")
    endif()
    if(_clink_opt_value)
        string(APPEND _clink_abi_material "${_opt}=1;")
    else()
        string(APPEND _clink_abi_material "${_opt}=0;")
    endif()
endforeach()

set(CLINK_ABI_SURFACE_MANIFEST "")
foreach(_clink_h IN LISTS _clink_abi_headers)
    set(_clink_h_path "${CMAKE_CURRENT_SOURCE_DIR}/${_clink_h}")
    if(NOT EXISTS "${_clink_h_path}")
        message(FATAL_ERROR
            "clink: scripts/plugin-abi-surface.txt lists a missing header: "
            "${_clink_h}. Regenerate the manifest with "
            "scripts/gen-plugin-abi-surface.py and review the diff.")
    endif()
    file(SHA256 "${_clink_h_path}" _clink_h_hash)
    string(APPEND _clink_abi_material "${_clink_h_hash}")
    string(APPEND CLINK_ABI_SURFACE_MANIFEST "${_clink_h}=${_clink_h_hash}\n")
    # A content edit to any hashed header must re-run configure, or a stale
    # fingerprint would be baked into the next build.
    set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_clink_h_path}")
endforeach()
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_clink_abi_manifest}")
string(SHA256 CLINK_ABI_FINGERPRINT "${_clink_abi_material}")
list(LENGTH _clink_abi_headers _clink_abi_header_count)
message(STATUS "clink: plugin ABI fingerprint = ${CLINK_ABI_FINGERPRINT} "
               "(abi v${CLINK_ABI_VERSION}, ${_clink_abi_header_count} surface headers)")

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/include/clink/plugin/abi_version.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/clink/plugin/abi_version.hpp"
    @ONLY)
# The per-header manifest, baked in beside the fingerprint so a refusal can
# name the differing headers (see abi_surface.hpp.in).
configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/include/clink/plugin/abi_surface.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/clink/plugin/abi_surface.hpp"
    @ONLY)

# ---------------------------------------------------------------------------
# Logging: spdlog behind the clink::logging module. clink does not use fmt
# directly, so spdlog uses its OWN bundled, header-only fmt (it includes
# <spdlog/fmt/bundled/...>, never <fmt/...>). That deliberately sidesteps the
# Homebrew-fmt-shadow hazard: a stray /opt/homebrew/include on the include
# path cannot shadow a bundled header that is reached by its full spdlog path,
# and there is no external libfmt to link. Pinned tag for reproducibility.
# SPDLOG_INSTALL OFF so the FetchContent target is not in clink's export set.
#
# spdlog is linked PRIVATE and header-only: only logging.cpp and the
# build-private logging headers include <spdlog/...>; the installed public
# headers (runtime_context.hpp, job_config.hpp, runner_registry.hpp) merely
# forward-declare spdlog::logger and pass it by raw pointer. PRIVATE keeps
# spdlog/fmt include dirs out of the dlopen'd plugin .so MODULE targets, the
# header-only target means there is no compiled spdlog symbol to leak, and the
# $<BUILD_INTERFACE:> wrap keeps the non-installed FetchContent target out of
# clink's install(EXPORT) set (clink_core is a static lib, so a bare PRIVATE
# dep would otherwise propagate as LINK_ONLY into the export interface). The
# net effect: a plugin never instantiates a second, sink-less per-.so spdlog
# registry (the RTLD_LOCAL hazard the operator-logging seam is built to avoid).
# ---------------------------------------------------------------------------
include(FetchContent)
set(SPDLOG_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    spdlog
    GIT_REPOSITORY https://github.com/gabime/spdlog.git
    GIT_TAG v1.15.2
    GIT_SHALLOW TRUE
    SYSTEM
)
FetchContent_MakeAvailable(spdlog)

# GCC-only: re-export the header-only template includes as SYSTEM so spdlog's
# (and its bundled fmt's) header-only template warnings cannot trip -Werror.
# NOT on macOS, where -isystem lets a Homebrew spdlog shadow the fetched one.
# Use the concrete target (spdlog_header_only), NOT the spdlog::spdlog_header_only
# ALIAS: set_property rejects alias targets.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND TARGET spdlog_header_only)
    get_target_property(_inc spdlog_header_only INTERFACE_INCLUDE_DIRECTORIES)
    if(_inc)
        set_property(TARGET spdlog_header_only APPEND PROPERTY
            INTERFACE_SYSTEM_INCLUDE_DIRECTORIES ${_inc})
    endif()
endif()

target_link_libraries(clink_core PRIVATE $<BUILD_INTERFACE:spdlog::spdlog_header_only>)
target_compile_definitions(clink_core PRIVATE CLINK_HAS_SPDLOG=1)
# Force the pinned FetchContent spdlog ahead of any system copy. find_package(Arrow)
# adds /opt/homebrew/include as an -isystem dir; a contributor with `brew install
# spdlog` would otherwise have <spdlog/...> resolve to that (newer, FMT_EXTERNAL)
# copy instead of the pinned 1.15.2 + bundled fmt. A plain -I (BEFORE) is searched
# ahead of every -isystem dir, so this restores the version pin on macOS. Build
# scope only; the header is never installed/exported.
target_include_directories(clink_core PRIVATE BEFORE
    $<BUILD_INTERFACE:${spdlog_SOURCE_DIR}/include>)

# zstd for the compressing rotating file sink. PRIVATE - only the sink TU
# needs the symbols - but clink_core is STATIC, so the dep must still reach a
# downstream link line ($<LINK_ONLY:...> in the export). Link the resolved
# LIBRARY PATH, not an imported target: an absolute path exports verbatim and
# needs no find_dependency downstream, whereas a zstd:: target would demand
# one (system zstd does not reliably ship a CMake config) and relying on
# Arrow's transitive link only works where libarrow exports its bundled zstd
# symbols (true on macOS, not in the Linux image, where a downstream
# find_package(clink) link then fails on ZSTD_CStreamInSize).
find_library(CLINK_ZSTD_LIBRARY NAMES zstd
    HINTS /opt/homebrew/lib /usr/local/lib)
if(CLINK_ZSTD_LIBRARY)
    target_link_libraries(clink_core PRIVATE "${CLINK_ZSTD_LIBRARY}")
    message(STATUS "clink::core - zstd at ${CLINK_ZSTD_LIBRARY}")
else()
    # No system zstd library found; fall back to an imported target if Arrow's
    # static-build branch (or a system CMake config) created one, build-scope
    # only so the unexportable target name never reaches clinkTargets.cmake.
    find_package(zstd QUIET)
    if(TARGET zstd::libzstd_shared)
        target_link_libraries(clink_core PRIVATE $<BUILD_INTERFACE:zstd::libzstd_shared>)
        message(STATUS "clink::core - zstd via zstd::libzstd_shared (build-scope only)")
    elseif(TARGET zstd::libzstd_static)
        target_link_libraries(clink_core PRIVATE $<BUILD_INTERFACE:zstd::libzstd_static>)
        message(STATUS "clink::core - zstd via zstd::libzstd_static (build-scope only)")
    else()
        message(STATUS "clink::core - zstd not found; relying on Arrow transitive link")
    endif()
endif()
# The zstd HEADER must be reachable: compressing_rotating_file_sink.hpp is a PUBLIC
# header that #includes <zstd.h>, so every consumer (clink_core_tests, impls, clink_node)
# needs it on its include path. With the from-source Arrow this is no longer implicit
# (the prior Homebrew Arrow added /opt/homebrew/include globally; the bundled-zstd target
# our Arrow exposes has no usable include dir). Locate the system header explicitly and
# add it PUBLIC (BUILD_INTERFACE - a host path, not part of the installed contract).
find_path(CLINK_ZSTD_INCLUDE_DIR NAMES zstd.h
    HINTS /opt/homebrew/include /usr/local/include /usr/include)
if(CLINK_ZSTD_INCLUDE_DIR)
    # SYSTEM (= -isystem, low search priority) so it cannot shadow the pinned FetchContent
    # spdlog with a Homebrew spdlog (which links an external fmt). This mirrors how the
    # prior Homebrew find_package(Arrow) added /opt/homebrew/include - as a system dir.
    target_include_directories(clink_core SYSTEM PUBLIC "$<BUILD_INTERFACE:${CLINK_ZSTD_INCLUDE_DIR}>")
    message(STATUS "clink::core - zstd.h at ${CLINK_ZSTD_INCLUDE_DIR}")
else()
    message(WARNING "clink::core - zstd.h not found; the compressing log sink will not compile")
endif()

# ---------------------------------------------------------------------------
# C++23 std::stacktrace - optional. When available it lets the executor attach
# a capture-site stack trace to an operator failure (surfaced in JobDetail's
# subtask_errors). GCC 13/14 ship it behind a backtrace library; libc++
# (macOS) generally lacks it, in which case failures simply carry no trace.
# ---------------------------------------------------------------------------
include(CheckCXXSourceCompiles)
set(_st_code "#include <stacktrace>\nint main(){ return static_cast<int>(std::stacktrace::current().size()); }")
set(_st_saved_flags "${CMAKE_REQUIRED_FLAGS}")
set(CMAKE_REQUIRED_FLAGS "-std=c++23")
set(_clink_stacktrace_lib "")
set(_clink_have_stacktrace FALSE)
set(CMAKE_REQUIRED_LIBRARIES "")
check_cxx_source_compiles("${_st_code}" CLINK_STACKTRACE_NOLIB)
if(CLINK_STACKTRACE_NOLIB)
    set(_clink_have_stacktrace TRUE)
else()
    set(CMAKE_REQUIRED_LIBRARIES "stdc++exp")
    check_cxx_source_compiles("${_st_code}" CLINK_STACKTRACE_EXP)
    if(CLINK_STACKTRACE_EXP)
        set(_clink_have_stacktrace TRUE)
        set(_clink_stacktrace_lib "stdc++exp")
    else()
        set(CMAKE_REQUIRED_LIBRARIES "stdc++_libbacktrace")
        check_cxx_source_compiles("${_st_code}" CLINK_STACKTRACE_BT)
        if(CLINK_STACKTRACE_BT)
            set(_clink_have_stacktrace TRUE)
            set(_clink_stacktrace_lib "stdc++_libbacktrace")
        endif()
    endif()
endif()
set(CMAKE_REQUIRED_LIBRARIES "")
set(CMAKE_REQUIRED_FLAGS "${_st_saved_flags}")
if(_clink_have_stacktrace)
    target_compile_definitions(clink_core PRIVATE CLINK_HAS_STACKTRACE=1)
    if(_clink_stacktrace_lib)
        target_link_libraries(clink_core PUBLIC ${_clink_stacktrace_lib})
    endif()
    message(STATUS "clink::core - std::stacktrace enabled (lib: ${_clink_stacktrace_lib})")
else()
    message(STATUS "clink::core - std::stacktrace not available; operator failure traces disabled")
endif()

# liburing - Linux-only optional. Provides the io_uring submission /
# completion ring used by Phase 28e's IoUringReactor: a coroutine
# scheduler that drives async I/O without per-request threads.
# Without it, the io_uring_reactor source and tests are excluded
# from the build entirely; macOS / non-Linux builds keep the rest
# of Phase 28 (Task<T>, AsyncLookupOperator, retry/circuit-breaker)
# working as a coroutine contract layer with a synchronous backend.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_library(LIBURING_LIBRARY uring)
    find_path(LIBURING_INCLUDE_DIR liburing.h)
    if(LIBURING_LIBRARY AND LIBURING_INCLUDE_DIR)
        target_link_libraries(clink_core PUBLIC ${LIBURING_LIBRARY})
        target_include_directories(clink_core PUBLIC ${LIBURING_INCLUDE_DIR})
        target_compile_definitions(clink_core PUBLIC CLINK_HAS_URING=1)
        target_sources(clink_core PRIVATE
            src/async/io_uring_reactor.cpp
            src/async/http_pool.cpp)
        message(STATUS "clink::core - liburing enabled (${LIBURING_LIBRARY})")
    else()
        message(STATUS "clink::core - liburing not found; io_uring reactor disabled")
    endif()
endif()

clink_set_warnings(clink_core)

# ---------------------------------------------------------------------------
# GoogleTest. Fetched at root scope so every subdirectory's tests/ - both
# the core test suite under tests/ and the per-impl test exes under
# impls/<x>/tests/ - can link GTest::gtest_main without re-fetching.
# Gated on CLINK_BUILD_TESTS to keep build-no-tests fast.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.15.2
        GIT_SHALLOW TRUE
        SYSTEM
    )
    set(BUILD_GMOCK    OFF CACHE BOOL "" FORCE)
    set(INSTALL_GTEST  OFF CACHE BOOL "" FORCE)
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    enable_testing()
    include(GoogleTest)
endif()

# ---------------------------------------------------------------------------
# cpp-httplib: single-header HTTP/1.1 server. Backs the clink HTTP
# dashboard subsystem (JSON API + embedded SPA). Gated on
# CLINK_BUILD_HTTP so embedded / airgapped builds can omit it.
# Pinned tag for reproducibility.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_HTTP)
    include(FetchContent)
    # httplib enables HTTPS whenever it FINDS OpenSSL, which then makes
    # clink_core's http_server / http_client reference OpenSSL symbols and pulls
    # libssl/libcrypto onto every consumer's link line. That is right for a normal
    # build and wrong for a portable artifact: on macOS the only OpenSSL found is
    # Homebrew's, whose bottle carries the BUILD HOST's minimum macOS version, so a
    # wheel vendoring it cannot honestly claim a lower floor (delocate refuses:
    # "libcrypto.3.dylib has a minimum target of 26.0"). CLINK_HTTP_TLS=OFF drops
    # HTTPS from the HTTP subsystem and with it the OpenSSL dependency; plain HTTP
    # is unaffected. The pyclink wheel sets it (scripts/build-libclink-wheel.sh).
    if(NOT CLINK_HTTP_TLS)
        set(HTTPLIB_USE_OPENSSL_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
        message(STATUS "clink::core - HTTP subsystem without TLS (CLINK_HTTP_TLS=OFF)")
    endif()
    FetchContent_Declare(
        httplib
        GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
        GIT_TAG v0.18.5
        GIT_SHALLOW TRUE
        SYSTEM
    )
    # httplib is header-only; the library target (`httplib::httplib`)
    # carries the include path and feature flags without producing a
    # .a - link cost is zero for any consumer that doesn't include
    # the header.
    set(HTTPLIB_COMPILE OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(httplib)

    # When httplib resolves OpenSSL (CLINK_HTTP_TLS=ON and OpenSSL present),
    # clink_core's http_server / http_client objects reference OpenSSL
    # symbols. httplib::httplib itself rides clink_core as BUILD_INTERFACE
    # only (a non-installed FetchContent target cannot appear in the export
    # set), which would silently strip the OpenSSL usage requirement from
    # the INSTALLED package - leaving every find_package(clink) consumer of
    # the HTTP surface with undefined OpenSSL symbols at link (found by the
    # card-sentry queryable-state scene). The OpenSSL imported targets ARE
    # exportable: link them directly and teach the generated
    # clinkConfig.cmake to resolve them. TLS-off builds (the portable
    # wheel) take no OpenSSL dependency, exactly as before.
    #
    # Gate on clink's own terms, not httplib's HTTPLIB_IS_USING_OPENSSL -
    # that variable is set inside the FetchContent subdirectory scope and
    # is invisible here. Our find_package resolves the same OpenSSL
    # httplib's did (one configure, one result); "if available" semantics
    # match by using a non-REQUIRED find gated on CLINK_HTTP_TLS.
    if(CLINK_HTTP_TLS)
        find_package(OpenSSL)
        if(OpenSSL_FOUND)
            target_link_libraries(clink_core PRIVATE OpenSSL::SSL OpenSSL::Crypto)
            # PUBLIC: the capability manifest reports whether THIS binary
            # carries TLS, so the fact has to reach any TU that renders it.
            target_compile_definitions(clink_core PUBLIC CLINK_HAS_OPENSSL=1)
            set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
                "# clink_core was built with HTTPS (CLINK_HTTP_TLS=ON): its archive\n")
            set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
                "# references OpenSSL symbols, so every consumer must resolve them.\n")
            set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
                "find_dependency(OpenSSL)\n")
            # On macOS httplib's TLS client loads its trust anchors from the
            # system Keychain, so http_client.cpp references CoreFoundation and
            # Security. Same export gap as OpenSSL and zlib, with one
            # difference: a framework is a link flag, not an imported target, so
            # PRIVATE linking is the whole fix - CMake records it in
            # clink_core's INTERFACE_LINK_LIBRARIES as $<LINK_ONLY:...>, which a
            # consumer of the static archive picks up, and there is no
            # find_dependency to add. Without it a find_package(clink) consumer
            # that touches the HTTP client fails at link with undefined
            # _CFArrayGetCount / _SecItemCopyMatching.
            if(APPLE)
                target_link_libraries(clink_core PRIVATE
                    "-framework CoreFoundation" "-framework Security")
            endif()
        endif()
    endif()

    # httplib also enables gzip whenever it FINDS zlib
    # (HTTPLIB_USE_ZLIB_IF_AVAILABLE, on by default), which makes clink_core's
    # http_server / http_client reference zlib's inflate/deflate. Same export gap
    # as OpenSSL above: httplib::httplib rides clink_core as BUILD_INTERFACE only,
    # so the installed package drops the zlib usage requirement, and a
    # find_package(clink) consumer of the HTTP surface fails at link with undefined
    # inflate/deflate. This bites the prebuilt-SDK path (static clink_core.a, whose
    # deps the consumer's final link must supply); a source build on macOS resolved
    # zlib transitively and hid it. Unlike OpenSSL this takes no gate: httplib uses
    # zlib whenever present, and zlib is safe to require - it is ubiquitous and
    # find_dependency(Arrow) already pulls it onto every consumer. Resolve the same
    # zlib httplib did and export it.
    find_package(ZLIB)
    if(ZLIB_FOUND)
        target_link_libraries(clink_core PRIVATE ZLIB::ZLIB)
        set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
            "# clink_core's bundled httplib was built with gzip (zlib present): its\n")
        set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
            "# archive references zlib symbols, so every consumer must resolve them.\n")
        set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
            "find_dependency(ZLIB)\n")
    endif()

    # And Brotli, for exactly the same reason and by the same mechanism:
    # httplib enables Brotli whenever it finds it
    # (HTTPLIB_USE_BROTLI_IF_AVAILABLE), so clink_core's http_server
    # references BrotliDecoderDecompressStream / BrotliEncoderCompressStream.
    # Third instance of this gap after OpenSSL and zlib, and it broke CI's
    # "Install + downstream consume" job the moment a runner image started
    # shipping libbrotli-dev: the build finds Brotli and uses it, the
    # installed package does not say so, and the consumer's link fails.
    #
    # Unlike OpenSSL and zlib, Brotli ships no CMake config or FindBrotli
    # module, so there is nothing to find_dependency() and nothing to
    # target_link_libraries() by imported target. pkg-config is the only
    # portable handle. Link the libraries directly - CMake records them in
    # INTERFACE_LINK_LIBRARIES as $<LINK_ONLY:...>, which is what a consumer
    # of the static archive needs - and emit no find_dependency, because
    # there is no package for a consumer to find. Same shape as the
    # CoreFoundation/Security frameworks above: a link flag, not a package.
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(BROTLI QUIET libbrotlienc libbrotlidec libbrotlicommon)
        if(BROTLI_FOUND)
            target_link_libraries(clink_core PRIVATE ${BROTLI_LINK_LIBRARIES})
            message(STATUS "clink::core - httplib Brotli support: linking ${BROTLI_LIBRARIES}")
        endif()
    endif()
endif()

# ---------------------------------------------------------------------------
# Impl modules (Kafka, Postgres, ClickHouse, S3, RocksDB, TLS).
# Each subdir handles its own dependency discovery and only defines a
# clink::<x> target when its dep is available. Each impl also owns
# the tests for its own surface area under impls/<x>/tests/, built
# automatically when CLINK_BUILD_TESTS is on.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_IMPLS)
    add_subdirectory(impls)
endif()

# ---------------------------------------------------------------------------
# clink_sql - SQL frontend (parser + binder + planner). Vendored
# libpg_query handles the PG-compatible parse tree; everything above
# the parse tree is clink-owned. Gated on CLINK_BUILD_SQL because
# the libpg_query build (~30s of make) is wasted work for builds that
# don't need SQL.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_SQL)
    include(LibPgQuery)

    add_library(clink_sql STATIC
        src/sql/parser.cpp
        src/sql/preparse.cpp
        src/sql/bounded_state.cpp
        src/sql/table_option_check.cpp
        src/sql/ast_builder.cpp
        src/sql/type.cpp
        src/sql/catalog.cpp
        src/sql/logical_plan.cpp
        src/sql/binder.cpp
        src/sql/physical_plan.cpp
        src/sql/column_lineage.cpp
        src/sql/materialized_view.cpp
        src/sql/view.cpp
        src/sql/optimizer.cpp
        src/sql/analyze.cpp
        src/sql/script_runner.cpp
        src/sql/replay.cpp
        src/sql/install.cpp
        src/sql/json_string_to_row_columnar.cpp
        src/sql/async_function_registry.cpp
        src/sql/table_api.cpp
    )
    add_library(clink::sql ALIAS clink_sql)
    set_target_properties(clink_sql PROPERTIES EXPORT_NAME sql)
    target_include_directories(clink_sql PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    )
    target_compile_features(clink_sql PUBLIC cxx_std_23)
    # simdjson, for src/sql/json_string_to_row_columnar.cpp's on-demand decoder.
    # PRIVATE and SYSTEM, exactly as clink_core has it: the header stays an
    # implementation detail and never reaches a consumer's include path.
    #
    # Needed EXPLICITLY. clink_core's simdjson include is PRIVATE, so it does not
    # propagate here, and the local build only compiled because Arrow vendors its
    # own simdjson.h into the deps prefix - so this TU was silently building
    # against ARROW's copy rather than the version pinned in
    # CLINK_SIMDJSON_TAG. The container has no such accident and failed with
    # "fatal error: simdjson.h: No such file or directory".
    target_include_directories(clink_sql SYSTEM PRIVATE ${CLINK_SIMDJSON_AMALGAMATED_DIR})
    target_link_libraries(clink_sql
        PUBLIC clink::core
        PRIVATE libpg_query::libpg_query
    )
    target_compile_definitions(clink_sql PUBLIC CLINK_HAS_SQL=1)
    clink_set_warnings(clink_sql)
    message(STATUS "clink::sql - SQL frontend enabled")

    # clink_submit_sql: SQL-to-JobGraphSpec compiler CLI. Phase 1 emits
    # JSON; Phase 2 added an HTTP submit path.
    add_executable(clink_submit_sql tools/clink_submit_sql.cpp)
    target_link_libraries(clink_submit_sql PRIVATE clink::sql)
    # CREATE FUNCTION ... LANGUAGE wasm executes client-side (the script
    # runner loads + packages the module before the spec is POSTed), so the
    # submitting CLI needs the loader installed, not just the cluster.
    if(TARGET clink::wasm)
        target_link_libraries(clink_submit_sql PRIVATE clink::wasm)
        target_compile_definitions(clink_submit_sql PRIVATE CLINK_LINKED_WASM)
    endif()
    clink_set_warnings(clink_submit_sql)
    install(TARGETS clink_submit_sql
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

    # clink::embed - the whole runtime in one process (in-process coordinator + worker
    # driving the shared SQL script runner). The execution core behind
    # `clink run <file>.sql`; the embeddable C ABI wraps the same class.
    add_library(clink_embed STATIC
        src/embed/embedded_engine.cpp
        src/embed/collect_hub.cpp
    )
    add_library(clink::embed ALIAS clink_embed)
    set_target_properties(clink_embed PROPERTIES EXPORT_NAME embed)
    target_compile_features(clink_embed PUBLIC cxx_std_23)
    target_link_libraries(clink_embed PUBLIC clink::sql)
    # WASM scalar UDFs in embedded execution (CREATE FUNCTION ... LANGUAGE wasm).
    if(TARGET clink::wasm)
        target_link_libraries(clink_embed PRIVATE clink::wasm)
        target_compile_definitions(clink_embed PRIVATE CLINK_EMBED_LINKED_WASM)
    endif()
    clink_set_warnings(clink_embed)
    message(STATUS "clink::embed - embedded engine enabled")
endif()

# ---------------------------------------------------------------------------
# Back-compat aggregator: clink::clink links everything that got
# built (core + every available impl). Existing callsites that link
# clink::clink keep working without changes; new code should prefer
# the explicit per-target deps clink::core / clink::kafka / etc.
# ---------------------------------------------------------------------------
# The underlying target is `clink_all`; the historical alias name
# `clink::clink` is preserved so every existing call site keeps
# working. (Renamed away from the bare `clink` target name so the
# client binary can take it - see the clink executable
# below.)
add_library(clink_all INTERFACE)
target_link_libraries(clink_all INTERFACE clink::core)
if(CLINK_BUILD_IMPLS)
    # clink::impls_all only exists when the impls subtree was added.
    target_link_libraries(clink_all INTERFACE clink::impls_all)
endif()
add_library(clink::clink ALIAS clink_all)
set_target_properties(clink_all PROPERTIES EXPORT_NAME clink)

# libclink - the embeddable C ABI (include/clink/embed/clink.h) over
# EmbeddedEngine, with connector='collect' results exported through the
# Arrow C stream interface. Links the whole engine plus every built impl
# statically into one shared library, so an embedding process needs only
# the C header and this artifact.
# Flight SQL endpoint over the embedded engine - enabled when the linked
# Arrow ships Flight SQL (Homebrew and the Docker image both do).
if(TARGET clink::embed)
    find_package(ArrowFlightSql QUIET)
    if(ArrowFlightSql_FOUND AND TARGET ArrowFlightSql::arrow_flight_sql_shared)
        add_library(clink_flightsql STATIC src/embed/flight_sql_server.cpp)
        target_link_libraries(clink_flightsql
            PUBLIC clink::embed ArrowFlightSql::arrow_flight_sql_shared)
        add_library(clink::flightsql ALIAS clink_flightsql)
        clink_set_warnings(clink_flightsql)
        message(STATUS "clink::flightsql - Flight SQL endpoint enabled")
    else()
        message(STATUS "clink::flightsql - Arrow Flight SQL not found; endpoint not built")
    endif()
endif()

if(TARGET clink::embed)
    add_library(clink_shared SHARED src/embed/clink_c.cpp)
    set_target_properties(clink_shared PROPERTIES OUTPUT_NAME clink EXPORT_NAME shared)
    # Self-contained artifact: satisfy Arrow/Parquet symbols from the STATIC
    # libraries (listed first, so the linker resolves from them) and strip
    # the then-unreferenced libarrow/libparquet dylib load commands. The
    # embedding process then carries its own private Arrow - no clash with a
    # host that loads a different Arrow (pyarrow), in either load order.
    if(TARGET Parquet::parquet_static AND TARGET Arrow::arrow_static)
        target_link_libraries(clink_shared PRIVATE Parquet::parquet_static Arrow::arrow_static)
        # Arrow's static bundle carries cloud-SDK objects with transitive system
        # deps Arrow's interface does not declare: google-cloud-cpp (GCS) needs
        # libcurl and Azure Storage needs libxml2. A full build only links because
        # a connector impl (s3/aws/azure/gcs) drags them in; a lean build
        # (CLINK_BUILD_IMPLS=OFF, e.g. the pyclink wheel) links no impls, so
        # satisfy them explicitly whenever they are available.
        #
        # Conditioned on what the linked Arrow ACTUALLY contains (ArrowOptions.cmake
        # exports ARROW_GCS / ARROW_AZURE), because these are not free: on macOS the
        # only libcurl a lean build finds is Homebrew's, which drags in Homebrew's
        # OpenSSL, and every Homebrew bottle carries the build host's minimum macOS
        # version. Against an Arrow built without the object stores that pinned
        # libclink above the pyclink wheel's floor for libraries nothing referenced.
        if(ARROW_GCS)
            find_package(CURL QUIET)
            if(TARGET CURL::libcurl)
                target_link_libraries(clink_shared PRIVATE CURL::libcurl)
            endif()
        endif()
        if(ARROW_AZURE)
            find_package(LibXml2 QUIET)
            if(TARGET LibXml2::LibXml2)
                target_link_libraries(clink_shared PRIVATE LibXml2::LibXml2)
            endif()
        endif()
        if(APPLE)
            target_link_options(clink_shared PRIVATE "LINKER:-dead_strip_dylibs")
        endif()
        message(STATUS "libclink - self-contained (static Arrow/Parquet)")
    else()
        message(STATUS "libclink - static Arrow/Parquet unavailable; linking shared Arrow")
    endif()
    target_link_libraries(clink_shared PRIVATE clink::embed clink::clink)
    # Export ONLY the clink_* C API from the dynamic symbol table. Without
    # this the artifact exports every engine / third-party symbol it links
    # (~20k, including bundled Arrow objects and abseil), and a host that
    # also loads its own Arrow (pyarrow!) can resolve symbols across copies
    # - the load-order crash pyclink used to work around. With the trimmed
    # table the C API is the whole contract and load order stops mattering.
    if(APPLE)
        set(_clink_exported_symbols "${CMAKE_CURRENT_BINARY_DIR}/libclink.exported_symbols")
        file(WRITE "${_clink_exported_symbols}" "_clink_*\n")
        target_link_options(clink_shared PRIVATE
            "LINKER:-exported_symbols_list,${_clink_exported_symbols}")
        set_target_properties(clink_shared PROPERTIES LINK_DEPENDS
            "${_clink_exported_symbols}")
    elseif(UNIX)
        # GNU-ld version script. Guarded to UNIX: MSVC's link.exe rejects it, and
        # a Windows DLL would need a .def / WINDOWS_EXPORT_ALL_SYMBOLS instead -
        # libclink has no Windows build path yet (see python/setup.py).
        set(_clink_version_script "${CMAKE_CURRENT_BINARY_DIR}/libclink.version")
        file(WRITE "${_clink_version_script}" "{ global: clink_*; local: *; };\n")
        target_link_options(clink_shared PRIVATE
            "LINKER:--version-script=${_clink_version_script}")
        set_target_properties(clink_shared PROPERTIES LINK_DEPENDS
            "${_clink_version_script}")
    endif()
    clink_set_warnings(clink_shared)
    install(TARGETS clink_shared LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
    message(STATUS "libclink - embeddable C ABI enabled")
endif()

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
# clink_node is the worker: it statically registers the connector/backend impls
# at startup, so it is only built when the impls are. A lean CLINK_BUILD_IMPLS=OFF
# build ships the library + client tools without the runnable node.
if(CLINK_BUILD_IMPLS)
add_executable(clink_node tools/clink_node.cpp)
target_link_libraries(clink_node PRIVATE clink::clink)
clink_set_warnings(clink_node)

# Exactly ONE Arrow runtime may exist in this binary. clink_node loads
# libarrow as a dylib via clink::core; a dependency that additionally drags
# libarrow.a into the link (iceberg-cpp's exported config did, whenever
# Arrow::arrow_static exists in the consumer) puts TWO memory-pool instances
# in one process, and buffers allocated by one and Reallocate'd by the other
# lose their contents without crashing (F87/F89: zeroed snapshot op_id
# columns, zeroed wire-frame heads). The check greps the binary for a
# DEFINED arrow::default_memory_pool, which only a second static Arrow can
# introduce.
if(CLINK_BUILD_TESTS)
    add_test(NAME single_arrow_runtime
        COMMAND "${CMAKE_COMMAND}" -DBINARY=$<TARGET_FILE:clink_node>
                -P "${CMAKE_CURRENT_SOURCE_DIR}/cmake/CheckSingleArrowRuntime.cmake")
    set_tests_properties(single_arrow_runtime PROPERTIES LABELS "core")
endif()

# Tell clink_node which impls it has at compile time so it can wire
# up clink::<x>::install() calls without #ifdef on macros that
# leaked from libclink's old build. Each impl sets a CLINK_LINKED_<X>
# define at the consumer-link boundary.
foreach(_impl kafka postgres clickhouse s3 aws rocksdb tls etcd http_connector redis mysql mqtt mongodb iceberg rabbitmq nats pulsar cassandra gcs azure webhdfs websocket vector_search onnx wasm forst)
    if(TARGET clink::${_impl})
        string(TOUPPER ${_impl} _IMPL_UC)
        target_compile_definitions(clink_node PRIVATE CLINK_LINKED_${_IMPL_UC})
    endif()
endforeach()
# Link clink_tls into clink_node when present so the TLS connection
# factories are reachable. Without this, --tls-cert flags would compile
# but the binary couldn't actually serve TLS.
if(TARGET clink::tls)
    target_link_libraries(clink_node PRIVATE clink::tls)
endif()
# TLS dialer for the Kafka prepared-transaction resume. Wired HERE, after
# every impl exists, because impls/tls is processed after impls/kafka and
# an if(TARGET clink_tls) inside the kafka lists is false forever.
# Export-safe: clink_tls sits in the same export set and libclink already
# carries the OpenSSL closure. The define gates the resolver's env-driven
# TLS branch; without it a TLS request is refused loudly rather than
# silently downgraded to plaintext.
if(TARGET clink::tls AND TARGET clink_kafka)
    target_link_libraries(clink_kafka PRIVATE clink::tls)
    target_compile_definitions(clink_kafka PRIVATE CLINK_KAFKA_RESUME_TLS=1)
    # SCRAM-SHA-256 for the resume rides the same gate: clink::tls is what
    # brings OpenSSL, whose HMAC/PBKDF2/SHA-256 are the mechanism's whole
    # crypto surface. OpenSSL itself comes via the tls target's link line.
    target_sources(clink_kafka PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/impls/kafka/src/scram.cpp)
    if(TARGET clink_kafka_tests)
        target_link_libraries(clink_kafka_tests PRIVATE clink::tls)
        target_compile_definitions(clink_kafka_tests PRIVATE CLINK_KAFKA_RESUME_TLS=1)
    endif()
endif()
# Same for clink_etcd: when the etcd impl is built, clink_node
# links it so --etcd-endpoints actually drives multi-machine HA.
if(TARGET clink::etcd)
    target_link_libraries(clink_node PRIVATE clink::etcd)
endif()
# clink::rocksdb_s3: register the remote-state schemes (s3+rocksdb://,
# changelog+s3+rocksdb://, changelog+s3://) at startup. Needs both rocksdb and
# s3, so it links separately from the base impls.
if(TARGET clink::rocksdb_s3)
    target_link_libraries(clink_node PRIVATE clink::rocksdb_s3)
    target_compile_definitions(clink_node PRIVATE CLINK_LINKED_ROCKSDB_S3)
endif()
# clink::forst_s3: the ForSt analogues (s3+forst://, changelog+s3+forst://).
# Needs both forst (opt-in) and s3.
if(TARGET clink::forst_s3)
    target_link_libraries(clink_node PRIVATE clink::forst_s3)
    target_compile_definitions(clink_node PRIVATE CLINK_LINKED_FORST_S3)
endif()
# clink::sql: register the SQL Row channel + Row-typed factories at
# clink_node startup. Mirrors the tls / etcd link-after-define pattern.
if(TARGET clink::sql)
    target_link_libraries(clink_node PRIVATE clink::sql)
    target_compile_definitions(clink_node PRIVATE CLINK_LINKED_SQL)
endif()
endif()  # CLINK_BUILD_IMPLS (clink_node)

# Subcommand entry points shared by both the unified `clink` CLI
# below and the per-name back-compat shims. Bundle them into an
# OBJECT library so each TU is compiled exactly once and linked into
# both flavors.
# clink::test_support - the public testing framework for library
# consumers (operator harnesses, output capture, deterministic time;
# docs/internals/testing-framework.md). Header-only over clink::core.
# Production binaries must not link it; test executables do.
add_library(clink_test_support INTERFACE)
add_library(clink::test_support ALIAS clink_test_support)
# Export under the alias name so a downstream find_package(clink) links
# clink::test_support (not clink::clink_test_support), matching the in-tree alias.
set_target_properties(clink_test_support PROPERTIES EXPORT_NAME test_support)
# BUILD_INTERFACE for in-tree consumers; INSTALL_INTERFACE so the exported
# target points at the installed headers (not the absolute source path) and a
# downstream find_package(clink) can link clink::test_support directly.
target_include_directories(clink_test_support INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(clink_test_support INTERFACE clink_core)
target_compile_features(clink_test_support INTERFACE cxx_std_23)

add_library(clink_cli_cmds OBJECT
    tools/clink_submit_job.cpp
    tools/clink_app.cpp
    tools/clink_cancel_job.cpp
    tools/clink_rescale_job.cpp
    tools/clink_rescale_op.cpp
    tools/clink_savepoint.cpp
    tools/clink_stop_job.cpp
    tools/clink_check_savepoint.cpp
    tools/clink_state_diff.cpp
    tools/clink_state_sweep.cpp
    tools/clink_capture_sync.cpp
    tools/clink_list.cpp
    tools/clink_checkpoint_verify.cpp
    tools/clink_lint.cpp
)
target_link_libraries(clink_cli_cmds PUBLIC clink::core)
# `clink run <file>.sql` and `clink replay` need the SQL frontend +
# embedded engine; a SQL-off build links stubs that error with a rebuild
# hint instead.
if(TARGET clink::embed)
    target_sources(clink_cli_cmds PRIVATE tools/clink_run_sql.cpp tools/clink_replay.cpp
                                          tools/clink_state_query.cpp)
    target_link_libraries(clink_cli_cmds PUBLIC clink::embed)
    # `clink run --coordinator-host` bypasses EmbeddedEngine (which installs the wasm
    # loader itself), so the CLI installs it for the remote-submit path.
    if(TARGET clink::wasm)
        target_link_libraries(clink_cli_cmds PUBLIC clink::wasm)
        target_compile_definitions(clink_cli_cmds PUBLIC CLINK_LINKED_WASM)
    endif()
else()
    target_sources(clink_cli_cmds PRIVATE tools/clink_run_sql_stub.cpp
                                          tools/clink_replay_stub.cpp
                                          tools/clink_state_query_stub.cpp)
endif()
# Flight SQL is independent of the embed gate. clink::flightsql exists only
# when the embedded engine is built AND Arrow ships Flight SQL, so it is
# always absent in a SQL-off build - which must still link the stub (it used
# to be nested in the embed branch, so a SQL-off `clink` failed to link on
# the undefined clink_cmd_flight_sql).
if(TARGET clink::flightsql)
    target_sources(clink_cli_cmds PRIVATE tools/clink_flight.cpp)
    target_link_libraries(clink_cli_cmds PUBLIC clink::flightsql)
else()
    target_sources(clink_cli_cmds PRIVATE tools/clink_flight_stub.cpp)
endif()
if(TARGET clink::tls)
    target_compile_definitions(clink_cli_cmds PUBLIC CLINK_LINKED_TLS)
endif()
# `clink state-cat` / `state-export` render RocksDB checkpoint dirs
# through the Arrow export when the RocksDB impl is in the build.
if(TARGET clink::rocksdb)
    target_link_libraries(clink_cli_cmds PUBLIC clink::rocksdb)
    target_compile_definitions(clink_cli_cmds PUBLIC CLINK_LINKED_ROCKSDB)
endif()
# `clink state-export --format=iceberg` commits state as an Iceberg
# table snapshot when the Iceberg impl is in the build.
if(TARGET clink::iceberg)
    target_link_libraries(clink_cli_cmds PUBLIC clink::iceberg)
    target_compile_definitions(clink_cli_cmds PUBLIC CLINK_LINKED_ICEBERG)
endif()
# `clink state-sweep` reclaims unreferenced objects from a disaggregated
# store. The reclaimer existed with no caller anywhere - no CLI, no
# endpoint, no periodic task - so a remote-read:// store grew with update
# volume for ever. This is the admin trigger its own comments describe.
if(TARGET clink::s3)
    target_link_libraries(clink_cli_cmds PUBLIC clink::s3)
    target_compile_definitions(clink_cli_cmds PUBLIC CLINK_LINKED_S3)
endif()
# The embedded runner (`clink run <file>.sql`, `clink flight-sql`) installs
# every linked connector via install_defaults, so embedded SQL reaches the
# same connector catalogue a cluster does. The final binary links
# clink::clink anyway; this hands the object lib's TUs the CLINK_HAS_*
# defines + include paths so the header-inline install sees them.
target_link_libraries(clink_cli_cmds PUBLIC clink::clink)
clink_set_warnings(clink_cli_cmds)

# `clink` - client CLI. One binary, multiple
# subcommands: run, run-application, cancel, savepoint, rescale, list.
# Mirrors ` run / cancel / savepoint / list` so muscle memory
# carries over from . The server daemon stays in its own binary
# (clink_node) - analogous to coordinator.sh / worker.sh
# split.
add_executable(clink tools/clink_cli.cpp)
target_link_libraries(clink PRIVATE clink_cli_cmds clink::core ${CMAKE_DL_LIBS})
# `clink --capabilities` reports whether THIS binary carries the SQL
# frontend. Only the binary knows what it linked, so the fact is supplied
# here rather than sniffed from a lower layer.
if(TARGET clink::embed)
    target_compile_definitions(clink PRIVATE CLINK_CLI_HAS_SQL=1)
endif()
if(TARGET clink::tls)
    target_link_libraries(clink PRIVATE clink::tls)
endif()
clink_set_warnings(clink)

# Back-compat shim executables. Each is a 3-line dispatcher into the
# matching clink_cmd_* function. Kept so existing integration-test
# fixtures and scripts that hardcoded the per-binary name keep working
# without modification. New code should prefer `clink <subcommand>`.
foreach(_pair
        "clink_submit_job;clink_submit_job_shim.cpp"
        "clink_app;clink_app_shim.cpp"
        "clink_cancel_job;clink_cancel_job_shim.cpp"
        "clink_rescale_job;clink_rescale_job_shim.cpp"
        "clink_savepoint;clink_savepoint_shim.cpp")
    string(REPLACE ";" ";" _parts "${_pair}")
    list(GET _parts 0 _bin)
    list(GET _parts 1 _src)
    add_executable(${_bin} tools/${_src})
    target_link_libraries(${_bin} PRIVATE clink_cli_cmds clink::core ${CMAKE_DL_LIBS})
    if(TARGET clink::tls)
        target_link_libraries(${_bin} PRIVATE clink::tls)
    endif()
    clink_set_warnings(${_bin})
endforeach()

# QUAL-11's schema-evolution plugin jobs (three .so variants from one
# source). Off by default; the qualification image build enables it so
# the jobs are built at exactly the image's engine revision (the plugin
# ABI fingerprint is the git SHA). Added BEFORE tests/ so the
# integration suite's if(TARGET ...) wiring can see the targets.
option(CLINK_BUILD_QUAL11_JOBS "Build the QUAL-11 schema-evolution job plugins" OFF)
if(CLINK_BUILD_QUAL11_JOBS)
    add_subdirectory(qualification/qual11)
endif()

# ---------------------------------------------------------------------------
# Core test suite. Per-impl tests live under impls/<x>/tests/.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

# fuzz/ carries the seed generator (needed by the corpus-replay test in
# tests/) AND the libFuzzer discovery targets. Gated on EITHER flag rather
# than nested inside the tests block: a fuzz-only build turns tests off, and
# a tests-only build still needs the seeds.
if(CLINK_BUILD_TESTS OR CLINK_BUILD_FUZZERS)
    add_subdirectory(fuzz)
endif()

# Embedded first-row latency regression gate (roadmap D5). RELEASE ONLY: a Debug
# build is several times slower (measured ~0.5s vs ~0.15s), so a budget is
# meaningless there. Needs the clink CLI and a Python 3 driver. The budget has
# generous headroom over the measured Release median (~155 ms, first row to
# stdout, 12-core Apple Silicon) so it catches a gross regression - the kind that
# turned "~100 ms" into "~0.4 s" - without flaking on slower CI machines.
if(CLINK_BUILD_TESTS AND CMAKE_BUILD_TYPE STREQUAL "Release" AND TARGET clink)
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_Interpreter_FOUND)
        add_test(NAME embedded_first_row_budget
            COMMAND "${Python3_EXECUTABLE}"
                "${CMAKE_CURRENT_SOURCE_DIR}/benchmarks/embedded_footprint.py"
                "$<TARGET_FILE:clink>" --runs 10 --large 10000 --budget-ms 600)
        set_tests_properties(embedded_first_row_budget PROPERTIES LABELS "bench")
    endif()
endif()

# ---------------------------------------------------------------------------
# Examples
# ---------------------------------------------------------------------------
if(CLINK_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()


# ---------------------------------------------------------------------------
# Benchmarks (placeholder; off by default)
# ---------------------------------------------------------------------------
if(CLINK_BUILD_BENCH)
    add_subdirectory(benchmarks)
endif()

# ---------------------------------------------------------------------------
# Install rules - make clink consumable via find_package(clink REQUIRED)
# from a downstream CMake project. Each impl that successfully built has
# already registered itself with the install machinery via
# clink_install_impl (in impls/<x>/CMakeLists.txt). This block installs
# the core library, the aggregator INTERFACE target, the CLI binaries,
# the public headers, and the generated headers, then finalizes the
# package config under <prefix>/lib/cmake/clink/.
# ---------------------------------------------------------------------------
# Export clink's own headers as NON-system includes (-I, not -isystem). CMake
# marks imported-target includes as SYSTEM by default, which suppresses consumer
# warnings AND, under Apple clang, mis-handles some of clink's headers (naming
# TimerService::TimerSet from the test harness fails under -isystem but is clean
# under -I). clink is the consumer's direct dependency, so its headers should be
# first-class. EXPORT_NO_SYSTEM needs CMake >= 3.25; on older CMake the consumer
# keeps the -isystem default (harmless except the Apple-clang harness case).
if(NOT CMAKE_VERSION VERSION_LESS 3.25)
    set_target_properties(clink_core clink_all clink_test_support
        PROPERTIES EXPORT_NO_SYSTEM ON)
endif()

# clink_test_support is INTERFACE (header-only), so the ARCHIVE/LIBRARY/RUNTIME
# destinations are inert for it - it is here to land in clinkTargets.cmake so a
# downstream find_package(clink) can link clink::test_support.
install(TARGETS clink_core clink_all clink_test_support
    EXPORT ${CLINK_EXPORT_NAME}
    ARCHIVE     DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY     DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME     DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS clink
        clink_submit_job clink_app clink_cancel_job
        clink_rescale_job clink_savepoint
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
# The worker node only exists (and installs) in an impls build.
if(CLINK_BUILD_IMPLS)
    install(TARGETS clink_node RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()

install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING
        PATTERN "*.hpp"
        PATTERN "*.h"
        PATTERN "*.inl"
        PATTERN "*.hpp.in" EXCLUDE)

install(DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/include/"
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING
        PATTERN "*.hpp"
        PATTERN "*.h")

clink_install_finalize()

# ---------------------------------------------------------------------------
# Tooling: clang-format / clang-tidy targets + git hooks
# ---------------------------------------------------------------------------
include(cmake/ClangTools.cmake)
include(cmake/GitHooks.cmake)
