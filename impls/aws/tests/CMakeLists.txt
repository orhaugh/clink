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
    test_kinesis_firehose_sink.cpp
)
target_link_libraries(clink_aws_tests PRIVATE
    clink::core
    clink::aws
    GTest::gtest_main
    aws-cpp-sdk-kinesis aws-cpp-sdk-firehose aws-cpp-sdk-dynamodb aws-cpp-sdk-core
)
target_compile_definitions(clink_aws_tests PRIVATE CLINK_HAS_AWS)
clink_set_warnings(clink_aws_tests)
gtest_discover_tests(clink_aws_tests
    DISCOVERY_MODE PRE_TEST
    # 60s rather than 30s: the live (endpoint-gated) tests exercise the AWS SDK
    # and even the unit tests link it; matches the clink::s3 test timeout.
    PROPERTIES TIMEOUT 60 LABELS aws
)
