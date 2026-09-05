# clink::avro - codec helpers for Avro-encoded records.
#
# This impl ships only codec templates: there are no built-in
# sources or sinks here. Users compose Avro codecs with Kafka/file
# sources via PluginRegistry::register_type<T>(name,
# clink::avro::binary_codec<T>()) and the existing typed source/sink
# fluent helpers (`clink::kafka::message_source` etc.).
#
# Gate: CLINK_WITH_AVRO=AUTO|ON|OFF. AUTO probes for AvroCpp via
# find_package; OFF skips entirely; ON requires AvroCpp.

if(CLINK_WITH_AVRO STREQUAL "OFF")
    message(STATUS "clink::avro - CLINK_WITH_AVRO=OFF; target not defined")
    return()
endif()

set(_avro_found FALSE)
# GLOBAL: impls/schema_registry (a sibling directory) links clink::avro and
# so needs the imported AvroCpp target visible outside this directory.
set(CMAKE_FIND_PACKAGE_TARGETS_GLOBAL ON)
find_package(AvroCpp QUIET CONFIG)
if(AvroCpp_FOUND)
    set(_avro_found TRUE)
endif()
if(NOT _avro_found)
    # The pinned toolchain (scripts/versions.env AVRO_CPP_VERSION: the Debian image
    # builds the release tarball into /usr/local, the macOS host uses Homebrew's
    # avro-cpp) installs an `avro-cpp` package config, not an `AvroCpp` one, and
    # older Homebrew bottles installed none - probe for the header + library
    # directly, which covers every one of those layouts.
    find_path(_avro_include avro/Specific.hh PATHS /opt/homebrew/include /usr/local/include)
    find_library(_avro_lib avrocpp PATHS /opt/homebrew/lib /usr/local/lib)
    if(_avro_include AND _avro_lib)
        set(_avro_found TRUE)
        add_library(AvroCpp::AvroCpp UNKNOWN IMPORTED GLOBAL)
        set_target_properties(AvroCpp::AvroCpp PROPERTIES
            IMPORTED_LOCATION "${_avro_lib}"
            INTERFACE_INCLUDE_DIRECTORIES "${_avro_include}")
    endif()
endif()

if(NOT _avro_found)
    if(CLINK_WITH_AVRO STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_AVRO=ON but AvroCpp not found")
    endif()
    message(STATUS "clink::avro - AvroCpp not found; target not defined")
    return()
endif()

add_library(clink_avro STATIC
    src/register_factories.cpp
)
add_library(clink::avro ALIAS clink_avro)

target_include_directories(clink_avro PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_avro PUBLIC cxx_std_23)
target_compile_definitions(clink_avro PUBLIC CLINK_HAS_AVRO)
target_link_libraries(clink_avro
    PUBLIC  clink::core AvroCpp::AvroCpp
)
clink_set_warnings(clink_avro)

clink_install_impl(
    TARGET   clink_avro
    NAME     avro
    FIND_DEP "find_dependency(AvroCpp)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::avro - enabled")
