# clink_iceberg_tests - tests for the clink::iceberg impl in isolation. The round-trip
# test drives make_iceberg_row_sink (the driver-free factory), so the test TU does NOT
# include any iceberg-cpp headers; the iceberg-cpp static libs come in transitively as
# clink::iceberg's (static-lib) PRIVATE link deps.

add_executable(clink_iceberg_tests
    register.cpp
    test_factory_registration.cpp
    test_iceberg_sink.cpp
    test_iceberg_source.cpp
    # Finalises Arrow/AWS S3 before static destruction (joins the AWS CRT threads
    # off the static-destruction path); see the file.
    ${CMAKE_SOURCE_DIR}/tests/test_helpers/s3_finalizing_gtest_main.cpp
)
target_link_libraries(clink_iceberg_tests PRIVATE
    clink::core
    clink::iceberg
    GTest::gtest
)
target_compile_definitions(clink_iceberg_tests PRIVATE CLINK_HAS_ICEBERG)
clink_set_warnings(clink_iceberg_tests)
gtest_discover_tests(clink_iceberg_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 120 LABELS iceberg
)
