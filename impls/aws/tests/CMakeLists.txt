# clink_aws_tests - tests for the clink::aws impl (kinesis/firehose/dynamodb)
# in isolation. The test TUs use AWS SDK model types directly (e.g. to inspect a
# mapped AttributeValue), so the target links the SDK client targets itself -
# clink::aws keeps the SDK PRIVATE and does not propagate its headers. The
# imported targets (created by the module's find_package) carry the include dirs
# + transitive deps; we link them directly rather than via the shared
# ${AWSSDK_LINK_LIBRARIES} variable, which the s3 impl's find_package leaves
# resolved for the s3 component set.

add_executable(clink_aws_tests
    register.cpp
    test_factory_registration.cpp
    test_dynamodb_item.cpp
    test_dynamodb_sink.cpp
    test_dynamodb_live.cpp
    test_firehose_sink.cpp
    test_kinesis_sink.cpp
    test_kinesis_sink_live.cpp
    test_kinesis_source.cpp
    test_kinesis_live.cpp
    # Finalises Arrow/AWS S3 before static destruction (joins the AWS CRT threads
    # off the static-destruction path); see the file.
    ${CMAKE_SOURCE_DIR}/tests/test_helpers/s3_finalizing_gtest_main.cpp
)
target_link_libraries(clink_aws_tests PRIVATE
    clink::core
    clink::aws
    GTest::gtest
    aws-cpp-sdk-kinesis aws-cpp-sdk-firehose aws-cpp-sdk-dynamodb aws-cpp-sdk-core
)
target_compile_definitions(clink_aws_tests PRIVATE CLINK_HAS_AWS)
# The Kinesis source's rescale cases restore through rocksdb:// too, to show the
# source refusing a restore that lacks rows it needs. impls/rocksdb is configured
# after this directory, so the test is made at generate time, not with if(TARGET).
target_link_libraries(clink_aws_tests PRIVATE
    $<$<TARGET_EXISTS:clink_rocksdb>:clink_rocksdb>)
target_compile_definitions(clink_aws_tests PRIVATE
    $<$<TARGET_EXISTS:clink_rocksdb>:CLINK_AWS_TESTS_HAVE_ROCKSDB>)
# The Firehose emulator cases read the delivery stream's S3 destination back
# with the SDK's own S3 client; without that component they are not built, and
# scripts/firehose-live.sh fails on the missing suite.
find_package(aws-cpp-sdk-s3 CONFIG QUIET)
if(TARGET aws-cpp-sdk-s3)
    target_sources(clink_aws_tests PRIVATE test_firehose_live.cpp)
    target_link_libraries(clink_aws_tests PRIVATE aws-cpp-sdk-s3)
endif()
# The Kinesis sink's SDK-path cases run the SDK's own client against an
# in-process stand-in for PutRecords built on the vendored cpp-httplib; without
# it they are not built, and scripts/kinesis-live.sh fails on the missing suite.
if(TARGET httplib::httplib)
    target_sources(clink_aws_tests PRIVATE test_kinesis_sink_sdk.cpp)
    target_link_libraries(clink_aws_tests PRIVATE $<BUILD_INTERFACE:httplib::httplib>)
endif()
clink_set_warnings(clink_aws_tests)
gtest_discover_tests(clink_aws_tests
    DISCOVERY_MODE PRE_TEST
    # 60s rather than 30s: the live (endpoint-gated) tests exercise the AWS SDK
    # and even the unit tests link it; matches the clink::s3 test timeout.
    PROPERTIES TIMEOUT 60 LABELS aws
)
