# clink::s3 - S3 text source/sink built against the AWS SDK for C++.
#
# DRAFT: source files referenced below do not yet exist. Migration plan:
#   src/connectors/s3_source.cpp        -> impls/s3/src/s3_source.cpp
#   src/connectors/s3_sink.cpp          -> impls/s3/src/s3_sink.cpp
#   include/clink/connectors/s3_*.hpp -> impls/s3/include/clink/connectors/
# Plus extract the s3_text_sink factory registration from
# src/cluster/built_in_factories.cpp into
# impls/s3/src/register_factories.cpp.

if(CLINK_WITH_AWS_S3 STREQUAL "OFF")
    message(STATUS "clink::s3 - CLINK_WITH_AWS_S3=OFF; target not defined")
    return()
endif()

find_package(AWSSDK CONFIG QUIET COMPONENTS s3)
if(NOT AWSSDK_FOUND)
    if(CLINK_WITH_AWS_S3 STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_AWS_S3=ON but AWSSDK not found")
    endif()
    message(STATUS "clink::s3 - AWS SDK not found; target not defined")
    return()
endif()

add_library(clink_s3 STATIC
    src/s3_source.cpp
    src/s3_sink.cpp
    src/register_factories.cpp
)
add_library(clink::s3 ALIAS clink_s3)

target_include_directories(clink_s3 PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_s3 PUBLIC cxx_std_23)
target_compile_definitions(clink_s3 PUBLIC CLINK_HAS_AWS_S3 CLINK_HAS_S3)
target_link_libraries(clink_s3
    PUBLIC  clink::core
    PRIVATE ${AWSSDK_LINK_LIBRARIES}
)
clink_set_warnings(clink_s3)

clink_install_impl(
    TARGET   clink_s3
    NAME     s3
    FIND_DEP "find_dependency(AWSSDK CONFIG COMPONENTS s3)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::s3 - enabled")
