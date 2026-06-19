# clink::rocksdb - RocksDB-backed keyed state.
#
# Self-registers a "rocksdb" scheme with clink::core's
# StateBackendFactory at install() time (see register_state_backend.cpp).
# Applications that link clink::rocksdb can then refer to a backend
# URI of the form "rocksdb:///path/to/dir" without additional wiring.
#
# RocksDB is the default durable state backend and ships with every
# clink build (mirrors bundled rocksdb-jni). Rather than
# depending on whatever the system happens to have - which drifts (the
# Homebrew 11.x vs Debian 8.x DB::Open ABI split is the canonical
# example) - we pin a known-good upstream tag, FetchContent it,
# build it statically, and link it into our archive. The resulting
# clink_node binary has no runtime libRocksDB.so dependency.
#
# Bumping the pin is a deliberate act: update CLINK_BUNDLED_ROCKSDB_TAG
# below, rebuild, validate. The first build of any clean tree compiles
# RocksDB (~5-8 min at --parallel 10); subsequent incremental builds
# reuse the cached object files under build/_deps/.

set(CLINK_BUNDLED_ROCKSDB_TAG "v11.1.1"
    CACHE STRING "Pinned RocksDB upstream tag to fetch and link statically")

include(FetchContent)

# Disable rocksdb's optional extras so we build only the core library.
# Each set() must precede FetchContent_MakeAvailable so it lands in the
# subproject's CMake cache before its CMakeLists runs.
set(WITH_TESTS              OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS    OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS              OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS         OFF CACHE BOOL "" FORCE)
set(WITH_GFLAGS             OFF CACHE BOOL "" FORCE)
set(WITH_SNAPPY             OFF CACHE BOOL "" FORCE)
set(WITH_LZ4                OFF CACHE BOOL "" FORCE)
set(WITH_ZLIB               OFF CACHE BOOL "" FORCE)
set(WITH_ZSTD               OFF CACHE BOOL "" FORCE)
set(WITH_BZ2                OFF CACHE BOOL "" FORCE)
set(WITH_JNI                OFF CACHE BOOL "" FORCE)
set(WITH_TRACE_TOOLS        OFF CACHE BOOL "" FORCE)
set(WITH_ALL_TESTS          OFF CACHE BOOL "" FORCE)
set(USE_RTTI                ON  CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_SHARED    OFF CACHE BOOL "" FORCE)
set(FAIL_ON_WARNINGS        OFF CACHE BOOL "" FORCE)
set(PORTABLE                ON  CACHE BOOL "" FORCE)

FetchContent_Declare(rocksdb
    GIT_REPOSITORY https://github.com/facebook/rocksdb.git
    GIT_TAG        ${CLINK_BUNDLED_ROCKSDB_TAG}
    GIT_SHALLOW    TRUE
    # SYSTEM puts rocksdb's headers behind -isystem so its compiler
    # warnings don't pollute our -Werror surface.
    SYSTEM
    EXCLUDE_FROM_ALL
)
FetchContent_MakeAvailable(rocksdb)

add_library(clink_rocksdb STATIC
    src/rocksdb_state_backend.cpp
    src/rocksdb_materialization_store.cpp
    src/register_state_backend.cpp
)
add_library(clink::rocksdb ALIAS clink_rocksdb)

target_include_directories(clink_rocksdb PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_rocksdb PUBLIC cxx_std_23)
target_compile_definitions(clink_rocksdb PUBLIC CLINK_HAS_ROCKSDB)
target_link_libraries(clink_rocksdb
    PUBLIC  clink::core
    PRIVATE rocksdb  # bundled static target produced by FetchContent
)
clink_set_warnings(clink_rocksdb)

# Install rules. RocksDB is bundled (FetchContent + static link), so no
# find_dependency() is needed - but we must install the bundled
# `rocksdb` static archive alongside clink_rocksdb so the exported
# target's PRIVATE link reference resolves on the downstream side.
clink_install_impl(
    TARGET clink_rocksdb
    NAME   rocksdb)

if(TARGET rocksdb)
    # Avoid colliding with clink::rocksdb (the impl target). The
    # bundled archive is an implementation detail of clink_rocksdb so we
    # expose it under a private-looking name.
    set_target_properties(rocksdb PROPERTIES EXPORT_NAME bundled_rocksdb)
    install(TARGETS rocksdb
        EXPORT ${CLINK_EXPORT_NAME}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

message(STATUS
    "clink::rocksdb - bundled (${CLINK_BUNDLED_ROCKSDB_TAG}, static)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()
