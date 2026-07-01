# clink::http_connector - HTTP(S) bulk-sink connectors: a generic webhook
# `http_sink` plus the shared BatchedHttpBulkSink base that the
# Elasticsearch/OpenSearch/Prometheus/Splunk sinks (INC 2) build on.
#
# No external dependency to gate on: cpp-httplib is vendored at the top level
# (FetchContent) and is header-only, so this module always builds when httplib
# is available.
#
# https: handled by the vendored httplib's OWN CMake. When it finds OpenSSL it
# sets CPPHTTPLIB_OPENSSL_SUPPORT on the httplib::httplib INTERFACE target, so
# the define (and the OpenSSL link) propagate IDENTICALLY to every consumer
# (clink_core's http_client.cpp and this impl) - no ODR hazard, and https just
# works. We deliberately do NOT define CPPHTTPLIB_OPENSSL_SUPPORT here (that
# would be the inconsistent-per-TU mistake). On a build env without OpenSSL the
# define is absent everywhere, HttpRequest::tls_supported() returns false, and
# the sink rejects https URLs with a clear message. A live-TLS integration test
# is deferred to INC 2 (where the Elasticsearch/Splunk sinks exercise it).

if(CLINK_WITH_HTTP STREQUAL "OFF")
    message(STATUS "clink::http_connector - CLINK_WITH_HTTP=OFF; target not defined")
    return()
endif()

if(NOT TARGET httplib::httplib)
    if(CLINK_WITH_HTTP STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_HTTP=ON but the vendored httplib target is absent")
    endif()
    message(STATUS "clink::http_connector - httplib target absent; target not defined")
    return()
endif()

add_library(clink_http_connector STATIC
    src/http_request.cpp
    src/http_model_provider.cpp
    src/register_factories.cpp
)
add_library(clink::http_connector ALIAS clink_http_connector)

target_include_directories(clink_http_connector PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_http_connector PUBLIC cxx_std_23)
target_compile_definitions(clink_http_connector PUBLIC CLINK_HAS_HTTP_CONNECTOR)
target_link_libraries(clink_http_connector
    PUBLIC  clink::core
    PRIVATE $<BUILD_INTERFACE:httplib::httplib>
)
clink_set_warnings(clink_http_connector)

clink_install_impl(
    TARGET clink_http_connector
    NAME   http_connector)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::http_connector - enabled (https available iff httplib was built with OpenSSL)")
