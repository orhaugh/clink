# clink::postgres - Postgres SELECT source + logical-replication CDC source.
#
# DRAFT: source files referenced below do not yet exist. Migration plan:
#   src/connectors/postgres_source.cpp     -> impls/postgres/src/postgres_source.cpp
#   src/connectors/postgres_cdc_source.cpp -> impls/postgres/src/postgres_cdc_source.cpp
#   src/connectors/postgres_decoder.cpp    -> impls/postgres/src/postgres_decoder.cpp
#   include/clink/connectors/{postgres_*,cdc_event}.hpp ->
#       impls/postgres/include/clink/connectors/
# Plus extract the StringPostgres{Source,CdcSource} adapters and the
# postgres_text_source / postgres_cdc_text_source registrations from
# src/cluster/built_in_factories.cpp into
# impls/postgres/src/{string_adapters,register_factories}.cpp.

if(CLINK_WITH_POSTGRES STREQUAL "OFF")
    message(STATUS "clink::postgres - CLINK_WITH_POSTGRES=OFF; target not defined")
    return()
endif()

set(_pg_found FALSE)
find_package(PostgreSQL QUIET)
if(PostgreSQL_FOUND)
    set(_pg_found TRUE)
endif()

if(NOT _pg_found)
    if(CLINK_WITH_POSTGRES STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_POSTGRES=ON but PostgreSQL not found")
    endif()
    message(STATUS "clink::postgres - libpq not found; target not defined")
    return()
endif()

add_library(clink_postgres STATIC
    src/postgres_source.cpp
    src/postgres_sink.cpp
    src/postgres_cdc_source.cpp
    src/postgres_decoder.cpp
    src/register_factories.cpp
)
add_library(clink::postgres ALIAS clink_postgres)

target_include_directories(clink_postgres PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_postgres PUBLIC cxx_std_23)
target_compile_definitions(clink_postgres PUBLIC CLINK_HAS_POSTGRES)
target_link_libraries(clink_postgres
    PUBLIC  clink::core
    PRIVATE PostgreSQL::PostgreSQL
)
clink_set_warnings(clink_postgres)

clink_install_impl(
    TARGET   clink_postgres
    NAME     postgres
    FIND_DEP "find_dependency(PostgreSQL)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::postgres - enabled (${PostgreSQL_VERSION_STRING})")
