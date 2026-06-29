# clink::cassandra - Cassandra / ScyllaDB sink via the DataStax C/C++ driver (cassandra.h).
# A transport-layer client: Homebrew (cassandra-cpp-driver) on the host, built from source in the
# Debian image (see scripts/setup-build-env.sh; needs libuv). We use the C API (cassandra.h),
# which is C-linkage and ABI-stable. No pkg-config ships, so probe.

if(CLINK_WITH_CASSANDRA STREQUAL "OFF")
    message(STATUS "clink::cassandra - CLINK_WITH_CASSANDRA=OFF; target not defined")
    return()
endif()

set(_cass_found FALSE)
unset(_cass_inc CACHE)
unset(_cass_lib CACHE)
find_path(_cass_inc NAMES cassandra.h
    HINTS /opt/homebrew/include /usr/local/include /usr/include)
find_library(_cass_lib NAMES cassandra
    HINTS /opt/homebrew/lib /usr/local/lib /usr/lib /usr/local/lib64 /usr/lib64)
if(_cass_inc AND _cass_lib)
    set(_cass_found TRUE)
    add_library(_clink_cassandra_external INTERFACE)
    target_include_directories(_clink_cassandra_external INTERFACE ${_cass_inc})
    target_link_libraries(_clink_cassandra_external INTERFACE ${_cass_lib})
endif()

if(NOT _cass_found)
    if(CLINK_WITH_CASSANDRA STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_CASSANDRA=ON but the DataStax cpp-driver not found "
                            "(Debian: built from source; macOS: brew install cassandra-cpp-driver)")
    endif()
    message(STATUS "clink::cassandra - cpp-driver not found; target not defined")
    return()
endif()

add_library(clink_cassandra STATIC
    src/cassandra_sink.cpp
    src/register_factories.cpp
)
add_library(clink::cassandra ALIAS clink_cassandra)

target_include_directories(clink_cassandra PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_cassandra PUBLIC cxx_std_23)
target_compile_definitions(clink_cassandra PUBLIC CLINK_HAS_CASSANDRA)
target_link_libraries(clink_cassandra
    PUBLIC  clink::core
    PRIVATE _clink_cassandra_external
)
clink_set_warnings(clink_cassandra)

# Re-export the driver lib so the live test can create the keyspace/table + read back via the C
# API directly (the sink itself does not provision schema).
set(CLINK_CASSANDRA_LIBS_EXPORT _clink_cassandra_external CACHE INTERNAL "cassandra libs for tests")

install(TARGETS _clink_cassandra_external EXPORT ${CLINK_EXPORT_NAME})
clink_install_impl(
    TARGET clink_cassandra
    NAME   cassandra)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::cassandra - enabled")
