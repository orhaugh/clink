# clink::neo4j - a sink that writes rows to Neo4j through the Query API
# (POST /db/<database>/query/v2), one implicit transaction per batch, with the
# rows as a typed $rows parameter, and a read-only lookup that enriches SQL rows
# with one parameterised read per key. It reuses clink::http_connector's HTTP
# client for transport and adds no dependency of its own; that client is the
# gate.

if(CLINK_WITH_NEO4J STREQUAL "OFF")
    message(STATUS "clink::neo4j - CLINK_WITH_NEO4J=OFF; target not defined")
    return()
endif()

if(NOT TARGET clink::http_connector)
    if(CLINK_WITH_NEO4J STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_NEO4J=ON but clink::http_connector is not available "
                            "(needs CLINK_WITH_HTTP and the vendored httplib)")
    endif()
    message(STATUS "clink::neo4j - clink::http_connector absent; target not defined")
    return()
endif()

add_library(clink_neo4j STATIC
    src/connection.cpp
    src/lookup.cpp
    src/neo4j_sink.cpp
    src/options.cpp
    src/register_factories.cpp
    src/reply.cpp
    src/types.cpp
)
add_library(clink::neo4j ALIAS clink_neo4j)

target_include_directories(clink_neo4j
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_features(clink_neo4j PUBLIC cxx_std_23)
target_compile_definitions(clink_neo4j PUBLIC CLINK_HAS_NEO4J)
target_link_libraries(clink_neo4j PUBLIC clink::core clink::http_connector)
clink_set_warnings(clink_neo4j)

clink_install_impl(
    TARGET clink_neo4j
    NAME   neo4j)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::neo4j - enabled")
