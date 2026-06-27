# clink::redis - Redis Streams connectors (redis_source via XREADGROUP consumer
# groups, redis_sink via XADD) built against hiredis. Dep-gated: builds only when
# hiredis is found.

if(CLINK_WITH_REDIS STREQUAL "OFF")
    message(STATUS "clink::redis - CLINK_WITH_REDIS=OFF; target not defined")
    return()
endif()

# Discover hiredis three ways, most-portable first:
#   1. pkg-config imported target (apt libhiredis-dev ships hiredis.pc)
#   2. hiredis' own CONFIG package (hiredis::hiredis)
#   3. a manual header/lib probe (Homebrew, or a bare Linux install with no
#      pkg-config tool - which is this dev host's case)
find_package(PkgConfig QUIET)
set(_redis_target "")
if(PkgConfig_FOUND)
    pkg_check_modules(HIREDIS QUIET IMPORTED_TARGET hiredis)
    if(HIREDIS_FOUND)
        set(_redis_target PkgConfig::HIREDIS)
    endif()
endif()
if(NOT _redis_target)
    find_package(hiredis CONFIG QUIET)
    if(hiredis_FOUND AND TARGET hiredis::hiredis)
        set(_redis_target hiredis::hiredis)
    endif()
endif()
if(NOT _redis_target)
    find_path(_redis_inc NAMES hiredis/hiredis.h
        HINTS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(_redis_lib NAMES hiredis
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    if(_redis_inc AND _redis_lib)
        add_library(clink_hiredis_external INTERFACE)
        target_include_directories(clink_hiredis_external INTERFACE ${_redis_inc})
        target_link_libraries(clink_hiredis_external INTERFACE ${_redis_lib})
        set(_redis_target clink_hiredis_external)
    endif()
endif()
if(NOT _redis_target)
    if(CLINK_WITH_REDIS STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_REDIS=ON but hiredis was not found")
    endif()
    message(STATUS "clink::redis - hiredis not found; target not defined")
    return()
endif()

add_library(clink_redis STATIC
    src/register_factories.cpp
)
add_library(clink::redis ALIAS clink_redis)

target_include_directories(clink_redis PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_redis PUBLIC cxx_std_23)
target_compile_definitions(clink_redis PUBLIC CLINK_HAS_REDIS)
target_link_libraries(clink_redis
    PUBLIC  clink::core
    PRIVATE ${_redis_target}
)
clink_set_warnings(clink_redis)

# The manual-probe INTERFACE wrapper is a local target and must be exported with
# the impl so an installed clinkConfig can resolve it; the package arms instead
# emit a find_dependency line.
if(TARGET clink_hiredis_external)
    install(TARGETS clink_hiredis_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(TARGET clink_redis NAME redis)
else()
    clink_install_impl(
        TARGET   clink_redis
        NAME     redis
        FIND_DEP "find_dependency(hiredis CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::redis - enabled")
