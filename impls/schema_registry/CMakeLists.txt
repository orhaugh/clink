# clink::schema_registry - the Confluent Schema Registry wire format and the
# registry-framed value formats (Avro, Protobuf, JSON Schema) a broker
# connector decodes into JSON text and encodes from it, plus the registry
# REST client behind them.
#
# Not a connector: it registers no sources or sinks. impls/kafka links it
# (guarded on `if(TARGET clink::schema_registry)`) and exposes the formats
# as the Kafka connector's format='avro' / 'protobuf' / 'json-schema'.
#
# Always built when clink::http_connector is (the registry client rides its
# TLS-capable HttpRequest). The Avro format is compiled in when clink::avro
# is (CLINK_WITH_AVRO), the Protobuf format when libprotobuf + libprotoc are
# found (CLINK_WITH_PROTOBUF=AUTO|ON|OFF); JSON Schema needs nothing extra.
# `supported_formats()` reports what this build compiled in, so a format a
# build lacks is refused by name at plan / build time rather than at runtime.

if(NOT TARGET clink::http_connector)
    message(STATUS "clink::schema_registry - clink::http_connector absent; target not defined")
    return()
endif()

set(_sr_sources
    src/wire_format.cpp
    src/client.cpp
    src/formats.cpp
    src/json_schema_format.cpp
    src/schema_derivation.cpp
    src/register_factories.cpp
)
set(_sr_links)
set(_sr_defs)
set(_sr_find_deps)

if(TARGET clink::avro)
    list(APPEND _sr_sources src/avro_format.cpp)
    list(APPEND _sr_links clink::avro)
    list(APPEND _sr_defs CLINK_SCHEMA_REGISTRY_HAS_AVRO)
endif()

set(_sr_protobuf FALSE)
if(NOT CLINK_WITH_PROTOBUF STREQUAL "OFF")
    # GLOBAL: the imported protobuf targets ride on this static library's
    # link interface into impls/kafka (a sibling directory).
    set(CMAKE_FIND_PACKAGE_TARGETS_GLOBAL ON)
    # CONFIG first (Homebrew protobuf >= 22 needs its abseil dependencies, which
    # only the config package carries), then the CMake module (Debian's 3.21).
    find_package(Protobuf CONFIG QUIET)
    if(NOT Protobuf_FOUND)
        find_package(Protobuf QUIET)
    endif()
    if(Protobuf_FOUND AND TARGET protobuf::libprotobuf AND TARGET protobuf::libprotoc)
        set(_sr_protobuf TRUE)
        list(APPEND _sr_sources src/protobuf_format.cpp)
        list(APPEND _sr_links protobuf::libprotobuf protobuf::libprotoc)
        list(APPEND _sr_defs CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF)
        list(APPEND _sr_find_deps "find_dependency(Protobuf)")
        message(STATUS "clink::schema_registry - protobuf ${Protobuf_VERSION} (libprotoc found)")
    elseif(CLINK_WITH_PROTOBUF STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_PROTOBUF=ON but libprotobuf + libprotoc were not found")
    else()
        message(STATUS "clink::schema_registry - protobuf compiler library not found; "
                       "format='protobuf' not compiled in")
    endif()
endif()

# Visible to sibling test directories (impls/kafka/tests gates its Protobuf
# live test on it), like CLINK_KAFKA_LIBS_EXPORT.
set(CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF ${_sr_protobuf} CACHE INTERNAL "registry-framed Protobuf compiled in")

add_library(clink_schema_registry STATIC ${_sr_sources})
add_library(clink::schema_registry ALIAS clink_schema_registry)

target_include_directories(clink_schema_registry PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_schema_registry PUBLIC cxx_std_23)
target_compile_definitions(clink_schema_registry
    PUBLIC  CLINK_HAS_SCHEMA_REGISTRY
    PRIVATE ${_sr_defs})
target_link_libraries(clink_schema_registry
    PUBLIC  clink::core
    PRIVATE clink::http_connector ${_sr_links}
)
clink_set_warnings(clink_schema_registry)

if(_sr_find_deps)
    clink_install_impl(
        TARGET   clink_schema_registry
        NAME     schema_registry
        FIND_DEP ${_sr_find_deps})
else()
    clink_install_impl(
        TARGET clink_schema_registry
        NAME   schema_registry)
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

set(_sr_formats "json-schema")
if(TARGET clink::avro)
    string(PREPEND _sr_formats "avro, ")
endif()
if(_sr_protobuf)
    string(APPEND _sr_formats ", protobuf")
endif()
message(STATUS "clink::schema_registry - enabled (${_sr_formats})")
