# clink::forst - ForSt-backed keyed state (optional, default OFF).
#
# ForSt (https://github.com/ververica/ForSt) is an LSM key-value store of
# RocksDB lineage aimed at disaggregated deployments. clink builds it from
# a pinned upstream tag and links it statically. Two upstream facts shape
# the wiring below:
#
#   * ForSt's CMake project is still named `rocksdb` and defines a
#     `rocksdb` library target - the same names the bundled RocksDB
#     backend's FetchContent build uses, so the two cannot coexist as
#     subprojects of one CMake tree. ForSt is therefore built via
#     ExternalProject (an isolated sub-build with its own cache) and
#     imported as a static archive.
#   * ForSt compiles under its own C++ namespace (`forstdb`), so the
#     archive links cleanly alongside the bundled RocksDB in one binary -
#     no symbol collisions between the two engines.
#
# Opt-in: -DCLINK_WITH_FORST=ON. The first build clones and compiles the
# engine (RocksDB-scale, ~5-8 min at --parallel 10, needs network);
# later builds reuse the ExternalProject stamp. Default OFF keeps the
# standard build byte-identical and network-free.

if(NOT CLINK_WITH_FORST)
    message(STATUS "clink::forst - disabled (CLINK_WITH_FORST=OFF)")
    return()
endif()

set(CLINK_BUNDLED_FORST_TAG "v0.1.4-beta"
    CACHE STRING "Pinned ForSt upstream tag to fetch and link statically")

include(ExternalProject)
find_package(Threads REQUIRED)

set(_forst_src   "${CMAKE_CURRENT_BINARY_DIR}/forst-src")
set(_forst_build "${CMAKE_CURRENT_BINARY_DIR}/forst-build")
set(_forst_lib   "${_forst_build}/librocksdb.a")

# The engine inherits clink's compiler and flags (so sanitizer builds stay
# coherent) but is otherwise configured to build just the static core: no
# tools, no tests, no JNI (ForSt's remote filesystem is a JNI bridge to a
# hosting JVM; the C++ core is a local engine - clink supplies remote
# storage at a higher layer), no compression libs (parity with the bundled
# RocksDB build), no liburing (clink's io_uring integration is a separate
# subsystem). PORTABLE=1 targets the baseline CPU, matching the bundled
# RocksDB's portable build.
set(_forst_build_type "${CMAKE_BUILD_TYPE}")
if(NOT _forst_build_type)
    set(_forst_build_type Release)
endif()
set(_forst_cache_args
    "-DCMAKE_BUILD_TYPE:STRING=${_forst_build_type}"
    "-DCMAKE_C_COMPILER:FILEPATH=${CMAKE_C_COMPILER}"
    "-DCMAKE_CXX_COMPILER:FILEPATH=${CMAKE_CXX_COMPILER}"
    "-DCMAKE_C_FLAGS:STRING=${CMAKE_C_FLAGS}"
    "-DCMAKE_CXX_FLAGS:STRING=${CMAKE_CXX_FLAGS}"
    "-DCMAKE_POSITION_INDEPENDENT_CODE:BOOL=ON"
    "-DROCKSDB_BUILD_SHARED:BOOL=OFF"
    "-DFAIL_ON_WARNINGS:BOOL=OFF"
    "-DPORTABLE:STRING=1"
    "-DUSE_RTTI:STRING=ON"
    "-DWITH_GFLAGS:BOOL=OFF"
    "-DWITH_TESTS:BOOL=OFF"
    "-DWITH_ALL_TESTS:BOOL=OFF"
    "-DWITH_BENCHMARK_TOOLS:BOOL=OFF"
    "-DWITH_TOOLS:BOOL=OFF"
    "-DWITH_CORE_TOOLS:BOOL=OFF"
    "-DWITH_TRACE_TOOLS:BOOL=OFF"
    "-DWITH_JNI:BOOL=OFF"
    "-DWITH_LIBURING:BOOL=OFF"
    "-DWITH_SNAPPY:BOOL=OFF"
    "-DWITH_LZ4:BOOL=OFF"
    "-DWITH_ZLIB:BOOL=OFF"
    "-DWITH_ZSTD:BOOL=OFF"
    "-DWITH_BZ2:BOOL=OFF")
if(CMAKE_C_COMPILER_LAUNCHER)
    list(APPEND _forst_cache_args
        "-DCMAKE_C_COMPILER_LAUNCHER:STRING=${CMAKE_C_COMPILER_LAUNCHER}")
endif()
if(CMAKE_CXX_COMPILER_LAUNCHER)
    list(APPEND _forst_cache_args
        "-DCMAKE_CXX_COMPILER_LAUNCHER:STRING=${CMAKE_CXX_COMPILER_LAUNCHER}")
endif()

ExternalProject_Add(forst_ep
    GIT_REPOSITORY https://github.com/ververica/ForSt.git
    GIT_TAG        ${CLINK_BUNDLED_FORST_TAG}
    GIT_SHALLOW    TRUE
    GIT_CONFIG     advice.detachedHead=false
    SOURCE_DIR     ${_forst_src}
    BINARY_DIR     ${_forst_build}
    # Fetch-time patches, all pinned behaviour (the tag is fixed, so the
    # line shapes cannot drift) and all idempotent (re-running on a
    # patched file is a no-op):
    #
    #  1. Upstream packaging quirk (v0.1.4-beta): the core SOURCES list
    #     hardcodes the env/flink/*.cc JNI bridge, so a plain C++ build
    #     dies on a missing jni.h even with WITH_JNI=OFF. Those TUs are
    #     the JVM-hosted remote-filesystem glue - dead weight in a native
    #     embed - so strip them from the source list.
    #  2. The engine keeps two symbols OUTSIDE its C++ namespace, which
    #     collide when this archive links beside the bundled RocksDB in
    #     one binary (clink_node carries both engines):
    #       * `pmull_runtime_flag` (C-linkage arm64 crc32 runtime probe):
    #         a hard duplicate-symbol link error - rename it. The [^_]
    #         guard keeps the rename idempotent.
    #       * the `ROCKSDB_XXH*` xxhash family (XXH_NAMESPACE): identical
    #         strong definitions in both archives that today survive only
    #         by first-member-wins archive semantics - re-prefix to
    #         FORSTDB_ so the two engines' symbol tables are fully
    #         disjoint instead of coincidentally compatible.
    PATCH_COMMAND  sed -i.bak "\\|env/flink/|d" <SOURCE_DIR>/CMakeLists.txt
    COMMAND        sed -i.bak "s/\\([^_]\\)pmull_runtime_flag/\\1forstdb_pmull_runtime_flag/g" <SOURCE_DIR>/util/crc32c.cc <SOURCE_DIR>/util/crc32c_arm64.cc
    COMMAND        sed -i.bak "s/^#define XXH_NAMESPACE ROCKSDB_$/#define XXH_NAMESPACE FORSTDB_/" <SOURCE_DIR>/util/xxhash.h
    CMAKE_CACHE_ARGS ${_forst_cache_args}
    # Build only the static engine target; the sub-project's tools and
    # tests are configured off above, and --target keeps even an
    # accidental default target from building. Parallelism is capped
    # (never a bare -j).
    BUILD_COMMAND  ${CMAKE_COMMAND} --build ${_forst_build} --target rocksdb --parallel 10
    BUILD_BYPRODUCTS ${_forst_lib}
    INSTALL_COMMAND ""
    UPDATE_COMMAND  ""
    LOG_DOWNLOAD  ON
    LOG_CONFIGURE ON
    LOG_BUILD     ON
    LOG_OUTPUT_ON_FAILURE ON
)

# Imported view of the engine archive. Deliberately carries NO interface
# include dirs: ForSt keeps RocksDB's `rocksdb/` header layout, so with
# the bundled RocksDB (and possibly a system RocksDB) in the same build,
# include ORDER decides which engine's headers a TU sees. Interface
# includes from linked targets land after the consuming target's own
# dirs - too late. The include dir is instead prepended directly to
# clink_forst below (SYSTEM BEFORE), which guarantees ForSt's headers
# win inside this impl's TUs and leak nowhere else.
file(MAKE_DIRECTORY "${_forst_src}/include")
add_library(clink_forst_engine STATIC IMPORTED GLOBAL)
set_target_properties(clink_forst_engine PROPERTIES
    IMPORTED_LOCATION "${_forst_lib}"
    INTERFACE_LINK_LIBRARIES Threads::Threads)

add_library(clink_forst STATIC
    src/forst_state_backend.cpp
    src/register_state_backend.cpp
)
add_library(clink::forst ALIAS clink_forst)
add_dependencies(clink_forst forst_ep)

target_include_directories(clink_forst PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
# ForSt's headers, FIRST in the system-include order (see the imported-
# target comment above). PRIVATE: the backend is pImpl'd, so no engine
# type reaches this impl's public headers and consumers never need them.
target_include_directories(clink_forst SYSTEM BEFORE PRIVATE
    "${_forst_src}/include")
target_compile_features(clink_forst PUBLIC cxx_std_23)
target_compile_definitions(clink_forst PUBLIC CLINK_HAS_FORST)
target_link_libraries(clink_forst
    PUBLIC  clink::core
    PRIVATE clink_forst_engine
)
clink_set_warnings(clink_forst)

# Install rules. The engine archive is imported (built by the
# ExternalProject), so it cannot ride the export set the way the bundled
# RocksDB target does. The archive is installed under a private name and
# clinkConfig.cmake recreates the `clink_forst_engine` imported target -
# the name the exported clink::forst's LINK_ONLY reference resolves
# against - before including the targets file.
clink_install_impl(
    TARGET clink_forst
    NAME   forst)
install(FILES "${_forst_lib}"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RENAME libclink_forst_engine.a)

message(STATUS
    "clink::forst - bundled ForSt (${CLINK_BUNDLED_FORST_TAG}, static, ExternalProject)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()
