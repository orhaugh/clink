# clink::iceberg - Apache Iceberg table-format SINK built against the native Apache
# iceberg-cpp library (https://github.com/apache/iceberg-cpp). Dep-gated: builds
# only when iceberg-cpp is found via its installed CMake config package
# (find_package(iceberg CONFIG)). iceberg-cpp is not in brew/apt - it is built +
# installed from source (see docs/lakehouse-research.md for the recipe;
# scripts/setup-build-env.sh installs it for CI/Docker). It MUST be built against the
# same Arrow as clink (it pins Arrow 24.0.0, == clink's), so there is no ABI clash.
#
# The module reuses the SQL Row columnar batcher for the typed Parquet data files
# (so it depends on clink::sql), and uses iceberg-cpp for the manifests / table
# metadata / catalog commit. SINK only in v1 (append-only, single-writer).
#
# It MUST link the SAME Arrow INSTANCE as clink::core, not merely the same
# version - see the imported-target rewrite below. A second static Arrow in
# the same process corrupts memory-pool handoffs (F87/F89).

if(CLINK_WITH_ICEBERG STREQUAL "OFF")
    message(STATUS "clink::iceberg - CLINK_WITH_ICEBERG=OFF; target not defined")
    return()
endif()

# iceberg-cpp's bundle compiles arrow_s3_file_io.cc unconditionally, so it
# references Arrow's S3 filesystem symbols whether or not a job ever touches
# s3://. Against an Arrow built without the object stores
# (CLINK_ARROW_OBJECT_STORES=OFF) that link can never succeed - every binary
# pulling clink::iceberg dies with undefined arrow::fs::S3* references - so
# gate the impl on the Arrow feature exactly as the GCS/Azure impls gate on
# theirs. ARROW_S3 is set by find_package(Arrow) via ArrowOptions.cmake.
if(NOT ARROW_S3)
    if(CLINK_WITH_ICEBERG STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_ICEBERG=ON but the pinned Arrow was built without "
                            "ARROW_S3 (CLINK_ARROW_OBJECT_STORES=OFF?); iceberg-cpp's bundle "
                            "cannot link against a no-S3 Arrow")
    endif()
    message(STATUS "clink::iceberg - the pinned Arrow has no S3 filesystem; target not defined")
    return()
endif()

find_package(iceberg CONFIG QUIET)
# The bundle ("battery-included") lib is what carries the Arrow / Parquet / Avro write
# backend (MakeLocalFileIO, the Parquet/Avro writer factories, RegisterAll). The
# component libs (iceberg_static / iceberg_data_static) do NOT contain it, so iceberg-cpp
# MUST be built with ICEBERG_BUILD_BUNDLE=ON + ICEBERG_BUILD_SQL_CATALOG=ON +
# ICEBERG_SQL_SQLITE=ON (see docs/lakehouse-research.md / scripts/setup-build-env.sh).
if(NOT iceberg_FOUND OR NOT TARGET iceberg::iceberg_bundle_static OR
   NOT TARGET iceberg::iceberg_sql_catalog_static)
    if(CLINK_WITH_ICEBERG STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_ICEBERG=ON but iceberg-cpp was not found with the bundle "
                            "+ SQL catalog targets (iceberg::iceberg_bundle_static + "
                            "iceberg::iceberg_sql_catalog_static). Build+install it per "
                            "docs/lakehouse-research.md and set CMAKE_PREFIX_PATH to its prefix.")
    endif()
    message(STATUS "clink::iceberg - iceberg-cpp (bundle + sql catalog) not found; "
                   "target not defined")
    return()
endif()

# iceberg-cpp's exported config links `Arrow::arrow_static` whenever that
# target EXISTS in the consuming project - and clink's find_package(Arrow)
# defines both the shared and static targets. Left alone, every binary that
# links clink::iceberg next to clink::core carries TWO complete Arrow
# runtimes: the static one smuggled in here (libarrow.a +
# libarrow_bundled_dependencies.a, with its own bundled mimalloc and its own
# arrow::default_memory_pool) and the shared libarrow.dylib everything else
# uses. Which copy a given call binds to is a link-order accident, and a
# buffer allocated by one runtime's memory pool then Reallocate'd through
# the other's LOSES ITS CONTENTS without crashing - found as F89 (a state
# snapshot's op_id column arriving zeroed, so restores came up silently
# empty) and as F87 (outbound Arrow IPC wire frames with zeroed heads under
# Debug). Same-version Arrow is NOT the same-instance Arrow the comment at
# the top of this file once assumed.
#
# Rewrite the imported targets' interfaces so the generator expressions
# resolve to the SHARED Arrow/Parquet - one runtime per process. iceberg-cpp
# consumes Arrow's public API only, so linking it against the dylib is
# sound. (libclink, the self-contained C-ABI artefact, keeps static Arrow -
# it never loads the dylib, so there is exactly one instance there too.)
foreach(_ice_tgt IN ITEMS iceberg::iceberg_static iceberg::iceberg_data_static
                          iceberg::iceberg_rest_static iceberg::iceberg_sql_catalog_static
                          iceberg::iceberg_bundle_static)
    if(TARGET ${_ice_tgt})
        get_target_property(_ice_link ${_ice_tgt} INTERFACE_LINK_LIBRARIES)
        if(_ice_link)
            string(REPLACE "Arrow::arrow_static" "Arrow::arrow_shared" _ice_link "${_ice_link}")
            string(REPLACE "Parquet::parquet_static" "Parquet::parquet_shared" _ice_link
                           "${_ice_link}")
            set_target_properties(${_ice_tgt} PROPERTIES INTERFACE_LINK_LIBRARIES "${_ice_link}")
        endif()
    endif()
endforeach()

add_library(clink_iceberg STATIC
    src/register_factories.cpp
    src/iceberg_row_sink.cpp
)
add_library(clink::iceberg ALIAS clink_iceberg)

target_include_directories(clink_iceberg PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_iceberg PUBLIC cxx_std_23)
target_compile_definitions(clink_iceberg PUBLIC CLINK_HAS_ICEBERG)
# The SQL Row type + the row columnar batcher (parse_row_schema /
# make_row_columnar_arrow_batcher) are header-only under include/clink/sql, reachable
# via clink::core's include tree - so no clink::sql LINK is needed (and impls/ is
# processed before clink_sql is defined). iceberg-cpp static libs provide the
# metadata/catalog/commit layer; the imported iceberg targets carry their includes as
# SYSTEM, so clink's -Werror does not fire on them.
target_link_libraries(clink_iceberg
    PUBLIC  clink::core
    PRIVATE iceberg::iceberg_bundle_static
            iceberg::iceberg_sql_catalog_static
            iceberg::iceberg_rest_static
)

# The libxml2 workaround that used to live here existed only because the
# static Arrow bundle (with azure-sdk baked in) reached the link via
# iceberg's targets. With the interface rewritten to arrow_shared above,
# azure-sdk and its XML layer live inside libarrow.dylib and no explicit
# libxml2 link is needed.
set(_iceberg_find_deps "find_dependency(iceberg CONFIG)")

clink_set_warnings(clink_iceberg)
clink_install_impl(
    TARGET   clink_iceberg
    NAME     iceberg
    FIND_DEP ${_iceberg_find_deps})

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::iceberg - enabled")
