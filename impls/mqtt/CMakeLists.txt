# clink::mqtt - MQTT connectors (mqtt_source via SUBSCRIBE, mqtt_sink via PUBLISH)
# built against libmosquitto. Dep-gated: builds only when libmosquitto is found.
# TLS is built into libmosquitto (OpenSSL), so unlike redis there is no separate
# SSL library to discover - tls=true works whenever the found libmosquitto was
# compiled with TLS support (else it fails at connect with a clear error).

if(CLINK_WITH_MQTT STREQUAL "OFF")
    message(STATUS "clink::mqtt - CLINK_WITH_MQTT=OFF; target not defined")
    return()
endif()

# Discover libmosquitto three ways, most-portable first:
#   1. pkg-config imported target (apt libmosquitto-dev ships libmosquitto.pc)
#   2. mosquitto's own CONFIG package (rare)
#   3. a manual header/lib probe (Homebrew, or a bare Linux install with no
#      pkg-config tool - which is this dev host's case)
find_package(PkgConfig QUIET)
set(_mqtt_target "")
if(PkgConfig_FOUND)
    pkg_check_modules(LIBMOSQUITTO QUIET IMPORTED_TARGET libmosquitto)
    if(LIBMOSQUITTO_FOUND)
        set(_mqtt_target PkgConfig::LIBMOSQUITTO)
    endif()
endif()
if(NOT _mqtt_target)
    find_package(mosquitto CONFIG QUIET)
    if(mosquitto_FOUND AND TARGET mosquitto::libmosquitto)
        set(_mqtt_target mosquitto::libmosquitto)
    endif()
endif()
if(NOT _mqtt_target)
    find_path(_mqtt_inc NAMES mosquitto.h
        HINTS /opt/homebrew/opt/mosquitto/include /opt/homebrew/include
              /usr/local/include /usr/include)
    find_library(_mqtt_lib NAMES mosquitto
        HINTS /opt/homebrew/opt/mosquitto/lib /opt/homebrew/lib
              /usr/local/lib /usr/lib)
    if(_mqtt_inc AND _mqtt_lib)
        add_library(clink_mosquitto_external INTERFACE)
        target_include_directories(clink_mosquitto_external INTERFACE ${_mqtt_inc})
        target_link_libraries(clink_mosquitto_external INTERFACE ${_mqtt_lib})
        set(_mqtt_target clink_mosquitto_external)
    endif()
endif()
if(NOT _mqtt_target)
    if(CLINK_WITH_MQTT STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_MQTT=ON but libmosquitto was not found")
    endif()
    message(STATUS "clink::mqtt - libmosquitto not found; target not defined")
    return()
endif()

# mosquitto 2.1's umbrella <mosquitto.h> pulls libcommon.h -> libcommon_cjson.h, which
# #includes <cjson/cJSON.h>. On Homebrew, cJSON is a SEPARATE keg (/opt/homebrew/include)
# from the mosquitto keg, and clink no longer links Homebrew Arrow (which used to add
# /opt/homebrew/include to the global -isystem path as a side-effect), so discover cJSON
# explicitly and add it to the mqtt include path. Best-effort: older mosquitto (e.g.
# Debian's 2.0) does not split out cjson, so a miss is not fatal. Only the HEADER is
# needed (type decls); libmosquitto links cjson itself, so no extra link dependency.
find_path(_mqtt_cjson_inc NAMES cjson/cJSON.h
    HINTS /opt/homebrew/include /usr/local/include /usr/include)

add_library(clink_mqtt STATIC
    src/register_factories.cpp
)
add_library(clink::mqtt ALIAS clink_mqtt)

target_include_directories(clink_mqtt PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
# PUBLIC so the mqtt tests + clink_node (which transitively include <mosquitto.h>) also
# resolve the cJSON header the mosquitto umbrella drags in. SYSTEM (= -isystem, low
# priority) so adding the Homebrew include dir cannot shadow pinned headers (e.g. the
# FetchContent spdlog); BUILD_INTERFACE only - a host path, not the installed contract.
if(_mqtt_cjson_inc)
    target_include_directories(clink_mqtt SYSTEM PUBLIC "$<BUILD_INTERFACE:${_mqtt_cjson_inc}>")
endif()
target_compile_features(clink_mqtt PUBLIC cxx_std_23)
target_compile_definitions(clink_mqtt PUBLIC CLINK_HAS_MQTT)
target_link_libraries(clink_mqtt
    PUBLIC  clink::core
    PRIVATE ${_mqtt_target}
)
clink_set_warnings(clink_mqtt)

# The manual-probe INTERFACE wrapper is a local target and must be exported with
# the impl so an installed clinkConfig can resolve it; the package arms instead
# emit a find_dependency line.
if(TARGET clink_mosquitto_external)
    install(TARGETS clink_mosquitto_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(TARGET clink_mqtt NAME mqtt)
else()
    clink_install_impl(
        TARGET   clink_mqtt
        NAME     mqtt
        FIND_DEP "find_dependency(mosquitto CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::mqtt - enabled")
