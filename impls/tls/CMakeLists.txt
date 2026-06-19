# clink::tls - OpenSSL-backed TLS contexts for the cluster transport.
#
# Provides TlsServerContext / TlsClientContext concrete classes.
# clink::core's network layer references these through a non-templated
# void* native handle, so the rest of the runtime continues to build
# when this target is absent. With this target present, the cluster
# transport can be told to use mTLS via JobConfig.
#
# DRAFT: source files referenced below do not yet exist. Migration plan:
#   src/runtime/network/tls_socket.cpp -> impls/tls/src/tls_socket.cpp
#   include/clink/runtime/network/tls_socket.hpp ->
#       impls/tls/include/clink/runtime/network/tls_socket.hpp
#
# DESIGN NOTE: the existing cluster network bridge currently
# #ifdef-checks CLINK_HAS_OPENSSL to decide whether to call into the
# TLS classes. After this move, the bridge needs a small seam - either
#   (a) a runtime virtual interface that the TLS module installs into,
#       similar to the StateBackendFactory pattern; or
#   (b) a weakly-linked symbol the TLS module provides, with the bridge
#       falling back to plaintext when unresolved.
# (a) is cleaner; flag this as a follow-up before doing the move.

if(CLINK_WITH_TLS STREQUAL "OFF")
    message(STATUS "clink::tls - CLINK_WITH_TLS=OFF; target not defined")
    return()
endif()

find_package(OpenSSL QUIET)
if(NOT OpenSSL_FOUND)
    message(STATUS "clink::tls - OpenSSL not found; target not defined")
    return()
endif()

add_library(clink_tls STATIC
    src/tls_socket.cpp
    src/tls_connection.cpp
)
add_library(clink::tls ALIAS clink_tls)

target_include_directories(clink_tls PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_tls PUBLIC cxx_std_23)
target_compile_definitions(clink_tls PUBLIC CLINK_HAS_OPENSSL CLINK_HAS_TLS)
target_link_libraries(clink_tls
    PUBLIC  clink::core
    PRIVATE OpenSSL::SSL OpenSSL::Crypto
)
clink_set_warnings(clink_tls)

clink_install_impl(
    TARGET   clink_tls
    NAME     tls
    FIND_DEP "find_dependency(OpenSSL)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::tls - enabled (${OPENSSL_VERSION})")
