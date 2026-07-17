# clink::etcd - etcd-backed HaCoordinator.
#
# Replaces the FileHaCoordinator's shared-filesystem assumption with
# an etcd v3 cluster, so multi-machine HA works without an NFS mount.
# The leader-election protocol uses a TTL'd lease + atomic Put (the
# etcdv3 equivalent of "create-if-absent"): whichever coordinator creates the
# leader key first wins, holds it via lease KeepAlive, and the lease
# expires automatically if the process dies.
#
# Standbys Watch the leader key and try to acquire on every
# DELETE event. workers use the same Get() against the leader key to
# discover the active endpoint.
#
# Optional dep: etcd-cpp-apiv3 (https://github.com/etcd-cpp-apiv3/etcd-cpp-apiv3).
# When missing, the target is not defined and clink_node falls back
# to FileHaCoordinator. Install via:
#   - macOS    : brew install etcd-cpp-apiv3 (third-party tap) or
#                build from source
#   - Linux    : your distro's package or build from source
#   - Docker   : the ci image bakes it in

if(CLINK_WITH_ETCD STREQUAL "OFF")
    message(STATUS "clink::etcd - CLINK_WITH_ETCD=OFF; target not defined")
    return()
endif()

# etcd-cpp-apiv3 installs a CMake config package named "etcd-cpp-api" (NOT
# -apiv3), which exports the IMPORTED target "etcd-cpp-api". Some builds also
# name it plain "etcd"; try both.
find_package(etcd-cpp-api CONFIG QUIET)
if(NOT etcd-cpp-api_FOUND)
    find_package(etcd CONFIG QUIET)
endif()
if(NOT etcd-cpp-api_FOUND AND NOT etcd_FOUND)
    if(CLINK_WITH_ETCD STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_ETCD=ON but etcd-cpp-apiv3 not found")
    endif()
    message(STATUS "clink::etcd - etcd-cpp-api not found; target not defined")
    return()
endif()

add_library(clink_etcd STATIC
    src/etcd_ha_coordinator.cpp
)
add_library(clink::etcd ALIAS clink_etcd)

target_include_directories(clink_etcd PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_etcd PUBLIC cxx_std_23)
target_compile_definitions(clink_etcd PRIVATE CLINK_HAS_ETCD)
target_link_libraries(clink_etcd
    PUBLIC  clink::core
    PRIVATE etcd-cpp-api
)
clink_set_warnings(clink_etcd)

clink_install_impl(
    TARGET   clink_etcd
    NAME     etcd
    FIND_DEP "find_dependency(etcd-cpp-api CONFIG)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::etcd - enabled")
