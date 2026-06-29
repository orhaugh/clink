# clink::rabbitmq - RabbitMQ / AMQP 0-9-1 source + sink via librabbitmq (rabbitmq-c).
# librabbitmq is a transport-layer client (like librdkafka / libpq): distro-packaged on
# Debian (librabbitmq-dev) and Homebrew (rabbitmq-c) rather than pinned from source.

if(CLINK_WITH_RABBITMQ STREQUAL "OFF")
    message(STATUS "clink::rabbitmq - CLINK_WITH_RABBITMQ=OFF; target not defined")
    return()
endif()

set(_rmq_found FALSE)
unset(_rmq_inc CACHE)
unset(_rmq_lib CACHE)

# Discovery: pkg-config (librabbitmq.pc ships on both Debian + Homebrew) then a manual
# probe. Both paths funnel into ONE INTERFACE target so the install/export is uniform.
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(RABBITMQ QUIET librabbitmq)
    if(RABBITMQ_FOUND)
        set(_rmq_found TRUE)
        add_library(_clink_rabbitmq_external INTERFACE)
        target_include_directories(_clink_rabbitmq_external INTERFACE ${RABBITMQ_INCLUDE_DIRS})
        target_link_libraries(_clink_rabbitmq_external INTERFACE ${RABBITMQ_LINK_LIBRARIES})
    endif()
endif()
if(NOT _rmq_found)
    find_path(_rmq_inc NAMES rabbitmq-c/amqp.h amqp.h
        HINTS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(_rmq_lib NAMES rabbitmq
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    if(_rmq_inc AND _rmq_lib)
        set(_rmq_found TRUE)
        add_library(_clink_rabbitmq_external INTERFACE)
        target_include_directories(_clink_rabbitmq_external INTERFACE ${_rmq_inc})
        target_link_libraries(_clink_rabbitmq_external INTERFACE ${_rmq_lib})
    endif()
endif()

if(NOT _rmq_found)
    if(CLINK_WITH_RABBITMQ STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_RABBITMQ=ON but librabbitmq (rabbitmq-c) not found "
                            "(Debian: librabbitmq-dev; macOS: brew install rabbitmq-c)")
    endif()
    message(STATUS "clink::rabbitmq - librabbitmq not found; target not defined")
    return()
endif()

add_library(clink_rabbitmq STATIC
    src/rabbitmq_source.cpp
    src/rabbitmq_sink.cpp
    src/register_factories.cpp
)
add_library(clink::rabbitmq ALIAS clink_rabbitmq)

target_include_directories(clink_rabbitmq PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_rabbitmq PUBLIC cxx_std_23)
target_compile_definitions(clink_rabbitmq PUBLIC CLINK_HAS_RABBITMQ)
target_link_libraries(clink_rabbitmq
    PUBLIC  clink::core
    PRIVATE _clink_rabbitmq_external
)
clink_set_warnings(clink_rabbitmq)

# Static lib with a PRIVATE dep: the INTERFACE target must be in the export set so the
# generated config resolves, matching the kafka manual-probe install path.
install(TARGETS _clink_rabbitmq_external EXPORT ${CLINK_EXPORT_NAME})
clink_install_impl(
    TARGET clink_rabbitmq
    NAME   rabbitmq)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::rabbitmq - enabled")
