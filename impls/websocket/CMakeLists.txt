# clink::websocket - WebSocket source (websocket_source_string): RFC 6455
# spoken directly over POSIX sockets, no client library. The one optional
# dependency is OpenSSL, for wss:// - found: TLS is compiled in
# (CLINK_WEBSOCKET_TLS); not found: the impl still builds and ws:// works,
# and a wss:// URL fails at open() with an error saying the build lacks TLS.
# Zero new packages either way, which is what lets this impl default to ON
# wherever clink builds.

if(CLINK_WITH_WEBSOCKET STREQUAL "OFF")
    message(STATUS "clink::websocket - CLINK_WITH_WEBSOCKET=OFF; target not defined")
    return()
endif()

add_library(clink_websocket STATIC
    src/register_factories.cpp
    src/ws_client.cpp
)
add_library(clink::websocket ALIAS clink_websocket)

target_include_directories(clink_websocket PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_websocket PUBLIC cxx_std_23)
target_compile_definitions(clink_websocket PUBLIC CLINK_HAS_WEBSOCKET)
target_link_libraries(clink_websocket PUBLIC clink::core)

# wss:// support: OpenSSL is confined to ws_client.cpp (PRIVATE), so nothing
# downstream inherits its headers or link line.
find_package(OpenSSL QUIET)
if(OpenSSL_FOUND)
    target_compile_definitions(clink_websocket PRIVATE CLINK_WEBSOCKET_TLS)
    target_link_libraries(clink_websocket PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    set(_websocket_tls "wss enabled")
else()
    set(_websocket_tls "ws only (OpenSSL not found)")
endif()
clink_set_warnings(clink_websocket)

if(OpenSSL_FOUND)
    clink_install_impl(
        TARGET   clink_websocket
        NAME     websocket
        FIND_DEP "find_dependency(OpenSSL)")
else()
    clink_install_impl(TARGET clink_websocket NAME websocket)
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::websocket - enabled (${_websocket_tls})")
