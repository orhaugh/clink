# clink::clickhouse - ClickHouse insert sink (TSV / JSONEachRow).
#
# DRAFT: source files referenced below do not yet exist. Migration plan:
#   src/connectors/clickhouse_sink.cpp       -> impls/clickhouse/src/clickhouse_sink.cpp
#   include/clink/connectors/clickhouse_sink.hpp ->
#       impls/clickhouse/include/clink/connectors/clickhouse_sink.hpp
# Plus extract the clickhouse_sink factory registration from
# src/cluster/built_in_factories.cpp into
# impls/clickhouse/src/register_factories.cpp.

if(CLINK_WITH_CLICKHOUSE STREQUAL "OFF")
    message(STATUS "clink::clickhouse - CLINK_WITH_CLICKHOUSE=OFF; target not defined")
    return()
endif()

set(_ch_found FALSE)
set(_ch_libs)

find_package(clickhouse-cpp CONFIG QUIET)
if(clickhouse-cpp_FOUND)
    set(_ch_found TRUE)
    set(_ch_libs clickhouse-cpp-lib)
else()
    find_path(_ch_inc NAMES clickhouse/client.h
        HINTS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(_ch_lib NAMES clickhouse-cpp-lib
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    # clickhouse-cpp's Homebrew bottle (and source build) ships
    # libcityhash.a as a sibling static archive - clickhouse-cpp uses
    # cityhash internally but doesn't bundle the symbols, so any
    # downstream linking clickhouse-cpp must also link cityhash.
    find_library(_ch_cityhash NAMES cityhash
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    # clickhouse-cpp's CompressedInput/Output use LZ4 + ZSTD; the
    # archive has unresolved references to those that the final binary
    # must satisfy.
    find_library(_ch_lz4 NAMES lz4
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    find_library(_ch_zstd NAMES zstd
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    if(_ch_inc AND _ch_lib)
        set(_ch_found TRUE)
        add_library(_clink_clickhouse_external INTERFACE)
        target_include_directories(_clink_clickhouse_external INTERFACE ${_ch_inc})
        target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_lib})
        if(_ch_cityhash)
            target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_cityhash})
        endif()
        if(_ch_lz4)
            target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_lz4})
        endif()
        if(_ch_zstd)
            target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_zstd})
        endif()
        # clickhouse-cpp's client TLS path links against OpenSSL.
        find_package(OpenSSL QUIET)
        if(OpenSSL_FOUND)
            target_link_libraries(_clink_clickhouse_external INTERFACE
                OpenSSL::SSL OpenSSL::Crypto)
        endif()
        set(_ch_libs _clink_clickhouse_external)
    endif()
endif()

if(NOT _ch_found)
    if(CLINK_WITH_CLICKHOUSE STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_CLICKHOUSE=ON but clickhouse-cpp not found")
    endif()
    message(STATUS "clink::clickhouse - clickhouse-cpp not found; target not defined")
    return()
endif()

add_library(clink_clickhouse STATIC
    src/clickhouse_sink.cpp
    src/clickhouse_source.cpp
    src/register_factories.cpp
)
add_library(clink::clickhouse ALIAS clink_clickhouse)

target_include_directories(clink_clickhouse PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_clickhouse PUBLIC cxx_std_23)
target_compile_definitions(clink_clickhouse PUBLIC CLINK_HAS_CLICKHOUSE)
target_link_libraries(clink_clickhouse
    PUBLIC  clink::core
    PRIVATE ${_ch_libs}
)
clink_set_warnings(clink_clickhouse)

if(TARGET _clink_clickhouse_external)
    install(TARGETS _clink_clickhouse_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(
        TARGET clink_clickhouse
        NAME   clickhouse)
else()
    clink_install_impl(
        TARGET   clink_clickhouse
        NAME     clickhouse
        FIND_DEP "find_dependency(clickhouse-cpp CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::clickhouse - enabled")
