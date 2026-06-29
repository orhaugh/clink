# clink::webhdfs - Parquet over HDFS via the WebHDFS / HttpFS REST API. No JVM and no libhdfs:
# it reuses clink::http_connector's HTTP client for transport and clink::core's Arrow/Parquet for
# encoding. Built only when clink::http_connector is available (CLINK_WITH_HTTP + the vendored
# httplib); that is the dependency gate (this module adds none of its own).

if(CLINK_WITH_WEBHDFS STREQUAL "OFF")
    message(STATUS "clink::webhdfs - CLINK_WITH_WEBHDFS=OFF; target not defined")
    return()
endif()

if(NOT TARGET clink::http_connector)
    if(CLINK_WITH_WEBHDFS STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_WEBHDFS=ON but clink::http_connector is not available "
                            "(needs CLINK_WITH_HTTP and the vendored httplib)")
    endif()
    message(STATUS "clink::webhdfs - clink::http_connector absent; target not defined")
    return()
endif()

add_library(clink_webhdfs STATIC
    src/register_factories.cpp
)
add_library(clink::webhdfs ALIAS clink_webhdfs)

target_include_directories(clink_webhdfs PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_webhdfs PUBLIC cxx_std_23)
target_compile_definitions(clink_webhdfs PUBLIC CLINK_HAS_WEBHDFS)
target_link_libraries(clink_webhdfs PUBLIC clink::core clink::http_connector)
clink_set_warnings(clink_webhdfs)

clink_install_impl(
    TARGET clink_webhdfs
    NAME   webhdfs)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::webhdfs - enabled")
