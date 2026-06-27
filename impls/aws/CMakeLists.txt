# clink::aws - AWS-family connectors (Kinesis source+sink, Firehose sink,
# DynamoDB sink) built against the AWS SDK for C++. Separate from clink::s3
# (which predates this and targets only the s3 component).

if(CLINK_WITH_AWS STREQUAL "OFF")
    message(STATUS "clink::aws - CLINK_WITH_AWS=OFF; target not defined")
    return()
endif()

# Find the per-component SDK packages DIRECTLY rather than via the umbrella
# find_package(AWSSDK COMPONENTS ...). The umbrella AWSSDKConfig.cmake returns
# early when AWSSDK_FOUND is already set (the clink::s3 module finds it first for
# the s3 component), so a second umbrella call with different components is a
# no-op: it neither resolves nor creates the kinesis/firehose/dynamodb imported
# targets. The component config packages have no such guard, so they reliably
# create the aws-cpp-sdk-<svc> imported targets (with their transitive deps)
# regardless of order.
find_package(aws-cpp-sdk-core CONFIG QUIET)
find_package(aws-cpp-sdk-kinesis CONFIG QUIET)
find_package(aws-cpp-sdk-firehose CONFIG QUIET)
find_package(aws-cpp-sdk-dynamodb CONFIG QUIET)
if(NOT (TARGET aws-cpp-sdk-core AND TARGET aws-cpp-sdk-kinesis AND
        TARGET aws-cpp-sdk-firehose AND TARGET aws-cpp-sdk-dynamodb))
    if(CLINK_WITH_AWS STREQUAL "ON")
        message(FATAL_ERROR
            "CLINK_WITH_AWS=ON but the AWS SDK kinesis/firehose/dynamodb components were not found")
    endif()
    message(STATUS "clink::aws - AWS SDK kinesis/firehose/dynamodb components not found; "
                   "target not defined")
    return()
endif()

add_library(clink_aws STATIC
    src/register_factories.cpp
)
add_library(clink::aws ALIAS clink_aws)

target_include_directories(clink_aws PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_aws PUBLIC cxx_std_23)
target_compile_definitions(clink_aws PUBLIC CLINK_HAS_AWS)
# Link the specific AWS SDK client imported targets rather than the shared
# ${AWSSDK_LINK_LIBRARIES} variable: the s3 impl's earlier find_package(COMPONENTS
# s3) leaves that global resolved for s3, so a second module reusing it would link
# the wrong client libs. The imported targets carry their own transitive deps.
target_link_libraries(clink_aws
    PUBLIC  clink::core
    PRIVATE aws-cpp-sdk-kinesis aws-cpp-sdk-firehose aws-cpp-sdk-dynamodb aws-cpp-sdk-core
)
clink_set_warnings(clink_aws)

clink_install_impl(
    TARGET   clink_aws
    NAME     aws
    FIND_DEP "find_dependency(aws-cpp-sdk-kinesis CONFIG)\nfind_dependency(aws-cpp-sdk-firehose CONFIG)\nfind_dependency(aws-cpp-sdk-dynamodb CONFIG)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::aws - enabled")
