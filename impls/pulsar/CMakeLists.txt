# clink::pulsar - Apache Pulsar source + sink via the libpulsar C API (pulsar/c/).
# libpulsar is a transport-layer client: Homebrew (libpulsar) on the host, and the prebuilt
# Apache pulsar-client-cpp .deb in the Debian image (see scripts/setup-build-env.sh). We use the
# C API, so no C++ ABI is shared with the prebuilt library. No pkg-config ships, so probe.

if(CLINK_WITH_PULSAR STREQUAL "OFF")
    message(STATUS "clink::pulsar - CLINK_WITH_PULSAR=OFF; target not defined")
    return()
endif()

set(_pulsar_found FALSE)
unset(_pulsar_inc CACHE)
unset(_pulsar_lib CACHE)
find_path(_pulsar_inc NAMES pulsar/c/client.h
    HINTS /opt/homebrew/include /usr/local/include /usr/include)
find_library(_pulsar_lib NAMES pulsar
    HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
if(_pulsar_inc AND _pulsar_lib)
    set(_pulsar_found TRUE)
    add_library(_clink_pulsar_external INTERFACE)
    target_include_directories(_clink_pulsar_external INTERFACE ${_pulsar_inc})
    target_link_libraries(_clink_pulsar_external INTERFACE ${_pulsar_lib})
endif()

if(NOT _pulsar_found)
    if(CLINK_WITH_PULSAR STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_PULSAR=ON but libpulsar not found "
                            "(Debian: apache-pulsar-client .deb; macOS: brew install libpulsar)")
    endif()
    message(STATUS "clink::pulsar - libpulsar not found; target not defined")
    return()
endif()

add_library(clink_pulsar STATIC
    src/pulsar_source.cpp
    src/pulsar_sink.cpp
    src/register_factories.cpp
)
add_library(clink::pulsar ALIAS clink_pulsar)

target_include_directories(clink_pulsar PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_pulsar PUBLIC cxx_std_23)
target_compile_definitions(clink_pulsar PUBLIC CLINK_HAS_PULSAR)
target_link_libraries(clink_pulsar
    PUBLIC  clink::core
    PRIVATE _clink_pulsar_external
)
clink_set_warnings(clink_pulsar)

install(TARGETS _clink_pulsar_external EXPORT ${CLINK_EXPORT_NAME})
clink_install_impl(
    TARGET clink_pulsar
    NAME   pulsar)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::pulsar - enabled")
