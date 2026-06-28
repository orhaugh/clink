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

if(CLINK_WITH_ICEBERG STREQUAL "OFF")
    message(STATUS "clink::iceberg - CLINK_WITH_ICEBERG=OFF; target not defined")
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
)

clink_set_warnings(clink_iceberg)
clink_install_impl(
    TARGET   clink_iceberg
    NAME     iceberg
    FIND_DEP "find_dependency(iceberg CONFIG)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::iceberg - enabled")
