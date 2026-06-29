# clink::nats - NATS JetStream source + sink via libnats (nats.c).
# libnats is a transport-layer client (like librdkafka / librabbitmq): distro-packaged on
# Debian (libnats-dev) and Homebrew (cnats) rather than pinned from source.

if(CLINK_WITH_NATS STREQUAL "OFF")
    message(STATUS "clink::nats - CLINK_WITH_NATS=OFF; target not defined")
    return()
endif()

set(_nats_found FALSE)
unset(_nats_inc CACHE)
unset(_nats_lib CACHE)

# Discovery: pkg-config (libnats.pc ships on both Debian + Homebrew) then a manual probe.
# Both paths funnel into ONE INTERFACE target so the install/export is uniform.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(NATS QUIET libnats)
    if(NATS_FOUND)
        set(_nats_found TRUE)
        add_library(_clink_nats_external INTERFACE)
        target_include_directories(_clink_nats_external INTERFACE ${NATS_INCLUDE_DIRS})
        target_link_libraries(_clink_nats_external INTERFACE ${NATS_LINK_LIBRARIES})
    endif()
endif()
if(NOT _nats_found)
    find_path(_nats_inc NAMES nats/nats.h
        HINTS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(_nats_lib NAMES nats
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    if(_nats_inc AND _nats_lib)
        set(_nats_found TRUE)
        add_library(_clink_nats_external INTERFACE)
        target_include_directories(_clink_nats_external INTERFACE ${_nats_inc})
        target_link_libraries(_clink_nats_external INTERFACE ${_nats_lib})
    endif()
endif()

if(NOT _nats_found)
    if(CLINK_WITH_NATS STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_NATS=ON but libnats (nats.c) not found "
                            "(Debian: libnats-dev; macOS: brew install cnats)")
    endif()
    message(STATUS "clink::nats - libnats not found; target not defined")
    return()
endif()

add_library(clink_nats STATIC
    src/nats_source.cpp
    src/nats_sink.cpp
    src/register_factories.cpp
)
add_library(clink::nats ALIAS clink_nats)

target_include_directories(clink_nats PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_nats PUBLIC cxx_std_23)
target_compile_definitions(clink_nats PUBLIC CLINK_HAS_NATS)
target_link_libraries(clink_nats
    PUBLIC  clink::core
    PRIVATE _clink_nats_external
)
clink_set_warnings(clink_nats)

# Re-export the nats lib so the live test can drive js_AddStream (create the JetStream stream)
# directly, the way the kafka tests drive rdkafka_mock.
set(CLINK_NATS_LIBS_EXPORT _clink_nats_external CACHE INTERNAL "nats libs for tests")

install(TARGETS _clink_nats_external EXPORT ${CLINK_EXPORT_NAME})
clink_install_impl(
    TARGET clink_nats
    NAME   nats)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::nats - enabled")
