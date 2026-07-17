# clink::kafka - Kafka source/sink connectors built against librdkafka.
#
# Built only when librdkafka is available. Three-tier discovery (CMake
# config, pkg-config, manual probe) preserved from the legacy root
# CMakeLists so Homebrew, vcpkg, Confluent, and source builds keep
# working without the user needing to set CMAKE_PREFIX_PATH.
#
# DRAFT: source files referenced below do not yet exist. Migration plan:
#   src/connectors/kafka_source.cpp        -> impls/kafka/src/kafka_source.cpp
#   src/connectors/kafka_sink.cpp          -> impls/kafka/src/kafka_sink.cpp
#   include/clink/connectors/kafka_*.hpp -> impls/kafka/include/clink/connectors/
# Plus extract the StringKafka{Source,Sink} adapters and the
# kafka_text_{source,sink} factory registrations from
# src/cluster/built_in_factories.cpp into
# impls/kafka/src/{string_adapters,register_factories}.cpp.

if(CLINK_WITH_KAFKA STREQUAL "OFF")
    message(STATUS "clink::kafka - CLINK_WITH_KAFKA=OFF; target not defined")
    return()
endif()

set(_kafka_found FALSE)
set(_kafka_libs)
unset(_kafka_inc CACHE)

find_package(RdKafka CONFIG QUIET)
if(RdKafka_FOUND)
    set(_kafka_found TRUE)
    set(_kafka_libs RdKafka::rdkafka++ RdKafka::rdkafka)
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(RDKAFKA QUIET IMPORTED_TARGET rdkafka rdkafka++)
        if(RDKAFKA_FOUND)
            set(_kafka_found TRUE)
            set(_kafka_libs PkgConfig::RDKAFKA)
        endif()
    endif()
    if(NOT _kafka_found)
        find_path(_kafka_inc NAMES librdkafka/rdkafkacpp.h
            HINTS /opt/homebrew/include /usr/local/include /usr/include)
        find_library(_kafka_c   NAMES rdkafka
            HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
        find_library(_kafka_cpp NAMES rdkafka++
            HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
        if(_kafka_inc AND _kafka_c AND _kafka_cpp)
            set(_kafka_found TRUE)
            add_library(_clink_rdkafka_external INTERFACE)
            target_include_directories(_clink_rdkafka_external INTERFACE ${_kafka_inc})
            target_link_libraries(_clink_rdkafka_external INTERFACE ${_kafka_cpp} ${_kafka_c})
            set(_kafka_libs _clink_rdkafka_external)
        endif()
    endif()
endif()

if(NOT _kafka_found)
    if(CLINK_WITH_KAFKA STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_KAFKA=ON but librdkafka not found")
    endif()
    message(STATUS "clink::kafka - librdkafka not found; target not defined")
    return()
endif()

# rdkafka_mock.h ships with librdkafka >= 1.3. Detect so the test suite
# can opt in to mock-broker tests without hardcoding the version.
find_path(CLINK_RDKAFKA_MOCK_HEADER
    NAMES librdkafka/rdkafka_mock.h
    HINTS /opt/homebrew/include /usr/local/include /usr/include ${_kafka_inc})
if(CLINK_RDKAFKA_MOCK_HEADER)
    # Set in BOTH scopes: PARENT_SCOPE for any upstream consumer, and the
    # local scope so the tests/ subdirectory (added below) inherits it. With
    # only PARENT_SCOPE the value never reached add_subdirectory(tests), so
    # the mock-broker test suite silently compiled to its no-op #else branch.
    set(CLINK_HAS_KAFKA_MOCK TRUE)
    set(CLINK_HAS_KAFKA_MOCK TRUE PARENT_SCOPE)
endif()

add_library(clink_kafka STATIC
    src/kafka_source.cpp
    src/kafka_sink.cpp
    src/pipeline_loader.cpp
    src/register_factories.cpp
)
add_library(clink::kafka ALIAS clink_kafka)

target_include_directories(clink_kafka PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_kafka PUBLIC cxx_std_23)
target_compile_definitions(clink_kafka PUBLIC CLINK_HAS_KAFKA)
target_link_libraries(clink_kafka
    PUBLIC  clink::core
    PRIVATE ${_kafka_libs}
)
clink_set_warnings(clink_kafka)

# Re-export the underlying rdkafka libs (and mock-header presence) as a
# CACHE variable so tests/CMakeLists.txt can link them directly into
# test_kafka.cpp - that test drives the rdkafka_mock broker via the C
# API, which clink_kafka doesn't expose through its public headers.
set(CLINK_KAFKA_LIBS_EXPORT ${_kafka_libs} CACHE INTERNAL "rdkafka libs for tests")

# If the manual-probe path was taken, the INTERFACE wrapper holds the
# absolute paths to libpq etc. that downstream consumers will need. It
# must be in the same export set so clinkTargets.cmake re-creates it on
# the consumer side. Skip the find_dependency line in that case - the
# absolute paths suffice and the consumer needn't have a CMake/pkg-config
# package available for rdkafka.
if(TARGET _clink_rdkafka_external)
    install(TARGETS _clink_rdkafka_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(
        TARGET clink_kafka
        NAME   kafka)
elseif(TARGET PkgConfig::RDKAFKA)
    # Found via the pkg-config tier: the exported target links
    # PkgConfig::RDKAFKA, so the installed config must recreate that exact
    # imported target - a find_dependency(RdKafka CONFIG) would fail on any
    # machine without a CMake config for librdkafka (Debian, Homebrew).
    clink_install_impl(
        TARGET   clink_kafka
        NAME     kafka
        FIND_DEP "find_dependency(PkgConfig)\npkg_check_modules(RDKAFKA REQUIRED IMPORTED_TARGET GLOBAL rdkafka rdkafka++)")
else()
    clink_install_impl(
        TARGET   clink_kafka
        NAME     kafka
        FIND_DEP "find_dependency(RdKafka CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::kafka - enabled")
