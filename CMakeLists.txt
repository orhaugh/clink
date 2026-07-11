cmake_minimum_required(VERSION 3.24)
project(clink
        VERSION 0.0.1
        DESCRIPTION "Semantics-first, Arrow-native, stateful stream processing engine in modern C++"
        LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

option(CLINK_BUILD_TESTS    "Build clink tests"    ON)
option(CLINK_BUILD_EXAMPLES "Build clink examples" ON)
option(CLINK_BUILD_BENCH    "Build clink benchmarks (placeholder)" OFF)
option(CLINK_BUILD_HTTP     "Build the HTTP subsystem (cluster dashboard + JSON API)" ON)
# SQL frontend: parser (libpg_query), binder, logical/physical planner,
# Table API. Off during initial development to keep the default build
# fast; flip ON to opt in.
option(CLINK_BUILD_SQL      "Build the SQL frontend (libpg_query + planner)" OFF)
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
set(CLINK_WITH_VECTOR_SEARCH "AUTO" CACHE STRING "VECTOR_SEARCH KNN impl: SimSIMD kernels + usearch HNSW, header-only (AUTO/ON/OFF)")
# ONNX Runtime local inference provider. OFF by default (heavy, optional dep): AUTO uses
# a locally-discoverable runtime but never downloads (offline-safe); ON fetches a pinned
# prebuilt runtime when none is found locally.
set(CLINK_WITH_ONNX       "OFF"  CACHE STRING "ONNX Runtime ML_PREDICT provider (OFF/AUTO/ON)")
# WebAssembly scalar UDFs (CREATE FUNCTION ... LANGUAGE wasm). Opt-in like ONNX:
# AUTO uses a locally-discoverable wasmtime C API but never downloads (offline-safe);
# ON fetches the pinned prebuilt c-api when none is found locally.
set(CLINK_WITH_WASM       "OFF"  CACHE STRING "WebAssembly (wasmtime) scalar UDFs (OFF/AUTO/ON)")
foreach(_opt CLINK_WITH_ROCKSDB CLINK_WITH_KAFKA CLINK_WITH_AWS_S3 CLINK_WITH_AWS
             CLINK_WITH_CLICKHOUSE CLINK_WITH_POSTGRES CLINK_WITH_TLS
             CLINK_WITH_ETCD CLINK_WITH_AVRO CLINK_WITH_HTTP CLINK_WITH_REDIS CLINK_WITH_MYSQL
             CLINK_WITH_MQTT CLINK_WITH_MONGODB CLINK_WITH_ICEBERG CLINK_WITH_RABBITMQ
             CLINK_WITH_NATS CLINK_WITH_PULSAR CLINK_WITH_CASSANDRA CLINK_WITH_GCS CLINK_WITH_AZURE
             CLINK_WITH_WEBHDFS CLINK_WITH_VECTOR_SEARCH CLINK_WITH_ONNX CLINK_WITH_WASM)
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
set(CLINK_ABI_HASH "unknown")
set(CLINK_ABI_HASH_CLEAN "false")
if(GIT_FOUND AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/.git")
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

# Structural ABI fingerprint: a hash of the public header content plus the
# ABI-relevant compile options and CLINK_ABI_VERSION. This is the DEFAULT
# compatibility gate. It rotates when the plugin ABI/behaviour surface actually
# changes - a public header edit (layout, virtual, inline/template body, macro),
# a toggled ABI option, or a manual CLINK_ABI_VERSION bump - but NOT on the
# majority of commits that touch only .cpp / tests / docs / build scaffolding.
# So a cluster patch rebuild keeps loading existing plugins, while a real ABI
# change auto-invalidates them without anyone remembering to bump a number.
# Errs toward over-invalidation (hashes the whole public include tree, not a
# curated ABI subset) - the safe direction for a load gate. CLINK_ABI_VERSION
# folds in as a manual force-rotate for a semantic break the header text can't
# capture. The generated header is in the BINARY dir, so it is not self-hashed.
file(GLOB_RECURSE _clink_abi_headers CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/include/clink/*.hpp")
list(SORT _clink_abi_headers)
set(_clink_abi_material "abi_version=${CLINK_ABI_VERSION};flat_hash_map=${CLINK_USE_FLAT_HASH_MAP};")
foreach(_clink_h IN LISTS _clink_abi_headers)
    file(SHA256 "${_clink_h}" _clink_h_hash)
    string(APPEND _clink_abi_material "${_clink_h_hash}")
endforeach()
string(SHA256 CLINK_ABI_FINGERPRINT "${_clink_abi_material}")
message(STATUS "clink: plugin ABI fingerprint = ${CLINK_ABI_FINGERPRINT} (abi v${CLINK_ABI_VERSION})")

configure_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/include/clink/plugin/abi_version.hpp.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/clink/plugin/abi_version.hpp"
    @ONLY)

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
# Core library - no external connector/state-backend deps.
#
# Everything in src/ here is the engine: cluster runtime, scheduler,
# checkpoint coordinator, JSON config, metrics, the plain-text /
# in-memory / file state backends. Vendor connectors and the RocksDB
# state backend live under impls/ and link in as separate static libs.
# ---------------------------------------------------------------------------
add_library(clink_core STATIC
    src/checkpoint/checkpoint_coordinator.cpp
    src/metrics/metrics_registry.cpp
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
    src/cluster/ha_coordinator.cpp
    src/cluster/restore_compat_gate.cpp
    src/cluster/job_manager.cpp
    src/cluster/task_manager.cpp
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
    src/cluster/plugin_loader.cpp
    src/cluster/plugin_cache.cpp
    src/lineage/lineage_graph.cpp
    src/lineage/lineage_listener.cpp
    src/config/json.cpp
    src/connectors/openssl_atexit_guard.cpp
)
if(CLINK_BUILD_HTTP)
    target_sources(clink_core PRIVATE
        src/http/http_server.cpp
        src/http/http_client.cpp
        src/lineage/openlineage_exporter.cpp
    )
    # Embed the dashboard SPA. configure_file runs at configure time, so
    # re-run `cmake -S . -B build` after editing dashboard.html to
    # refresh the generated header. (For routine iteration on the SPA
    # the dev path is usually to mount static/dashboard.html via a
    # local web server instead.)
    file(READ "${CMAKE_CURRENT_SOURCE_DIR}/static/dashboard.html" DASHBOARD_HTML_CONTENT)
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/include/clink/http/dashboard_assets.hpp.in"
        "${CMAKE_CURRENT_BINARY_DIR}/include/clink/http/dashboard_assets.hpp"
        @ONLY)
endif()
add_library(clink::core ALIAS clink_core)
set_target_properties(clink_core PROPERTIES EXPORT_NAME core)

target_include_directories(clink_core PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_core PUBLIC cxx_std_23)

find_package(Threads REQUIRED)
target_link_libraries(clink_core PUBLIC Threads::Threads)
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

# Parquet ships alongside Arrow in every standard distribution
# (Homebrew, apt, conda). Required by the ParquetSink/Source built-ins
# and the snapshot/wire layers that share the same batcher seam.
find_package(Parquet REQUIRED)
target_link_libraries(clink_core PUBLIC Parquet::parquet_shared)
target_compile_definitions(clink_core PUBLIC CLINK_HAS_PARQUET=1)
message(STATUS "clink::core - Parquet enabled")

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

# zstd for the compressing rotating file sink. Arrow already links zstd
# transitively (zstd::libzstd_* is created under its static-build branch),
# but a shared-only Arrow skips that, so resolve the target explicitly and
# fall back to Arrow's transitive link. PRIVATE - only the sink TU needs the
# symbols.
find_package(zstd QUIET)
if(TARGET zstd::libzstd_shared)
    target_link_libraries(clink_core PRIVATE zstd::libzstd_shared)
    message(STATUS "clink::core - zstd via zstd::libzstd_shared")
elseif(TARGET zstd::libzstd_static)
    target_link_libraries(clink_core PRIVATE zstd::libzstd_static)
    message(STATUS "clink::core - zstd via zstd::libzstd_static")
else()
    message(STATUS "clink::core - zstd target not found directly; relying on Arrow transitive link")
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
endif()

# ---------------------------------------------------------------------------
# Impl modules (Kafka, Postgres, ClickHouse, S3, RocksDB, TLS).
# Each subdir handles its own dependency discovery and only defines a
# clink::<x> target when its dep is available. Each impl also owns
# the tests for its own surface area under impls/<x>/tests/, built
# automatically when CLINK_BUILD_TESTS is on.
# ---------------------------------------------------------------------------
add_subdirectory(impls)

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

    # clink::embed - the whole runtime in one process (in-process JM + TM
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
target_link_libraries(clink_all INTERFACE clink::core clink::impls_all)
add_library(clink::clink ALIAS clink_all)
set_target_properties(clink_all PROPERTIES EXPORT_NAME clink)

# libclink - the embeddable C ABI (include/clink/embed/clink.h) over
# EmbeddedEngine, with connector='collect' results exported through the
# Arrow C stream interface. Links the whole engine plus every built impl
# statically into one shared library, so an embedding process needs only
# the C header and this artifact.
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
    else()
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
add_executable(clink_node tools/clink_node.cpp)
target_link_libraries(clink_node PRIVATE clink::clink)
clink_set_warnings(clink_node)

# Tell clink_node which impls it has at compile time so it can wire
# up clink::<x>::install() calls without #ifdef on macros that
# leaked from libclink's old build. Each impl sets a CLINK_LINKED_<X>
# define at the consumer-link boundary.
foreach(_impl kafka postgres clickhouse s3 aws rocksdb tls etcd http_connector redis mysql mqtt mongodb iceberg rabbitmq nats pulsar cassandra gcs azure webhdfs vector_search onnx wasm)
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
# clink::sql: register the SQL Row channel + Row-typed factories at
# clink_node startup. Mirrors the tls / etcd link-after-define pattern.
if(TARGET clink::sql)
    target_link_libraries(clink_node PRIVATE clink::sql)
    target_compile_definitions(clink_node PRIVATE CLINK_LINKED_SQL)
endif()

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
target_include_directories(clink_test_support INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(clink_test_support INTERFACE clink_core)
target_compile_features(clink_test_support INTERFACE cxx_std_23)

add_library(clink_cli_cmds OBJECT
    tools/clink_submit_job.cpp
    tools/clink_app.cpp
    tools/clink_cancel_job.cpp
    tools/clink_rescale_job.cpp
    tools/clink_rescale_op.cpp
    tools/clink_savepoint.cpp
    tools/clink_check_savepoint.cpp
    tools/clink_state_diff.cpp
    tools/clink_capture_sync.cpp
    tools/clink_list.cpp
)
target_link_libraries(clink_cli_cmds PUBLIC clink::core)
# `clink run <file>.sql` and `clink replay` need the SQL frontend +
# embedded engine; a SQL-off build links stubs that error with a rebuild
# hint instead.
if(TARGET clink::embed)
    target_sources(clink_cli_cmds PRIVATE tools/clink_run_sql.cpp tools/clink_replay.cpp
                                          tools/clink_state_query.cpp)
    target_link_libraries(clink_cli_cmds PUBLIC clink::embed)
    # `clink run --jm-host` bypasses EmbeddedEngine (which installs the wasm
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
clink_set_warnings(clink_cli_cmds)

# `clink` - client CLI. One binary, multiple
# subcommands: run, run-application, cancel, savepoint, rescale, list.
# Mirrors ` run / cancel / savepoint / list` so muscle memory
# carries over from . The server daemon stays in its own binary
# (clink_node) - analogous to jobmanager.sh / taskmanager.sh
# split.
add_executable(clink tools/clink_cli.cpp)
target_link_libraries(clink PRIVATE clink_cli_cmds clink::core ${CMAKE_DL_LIBS})
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

# ---------------------------------------------------------------------------
# Core test suite. Per-impl tests live under impls/<x>/tests/.
# ---------------------------------------------------------------------------
if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
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
install(TARGETS clink_core clink_all
    EXPORT ${CLINK_EXPORT_NAME}
    ARCHIVE     DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY     DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME     DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(TARGETS clink clink_node
        clink_submit_job clink_app clink_cancel_job
        clink_rescale_job clink_savepoint
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

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
