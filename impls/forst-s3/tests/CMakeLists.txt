# clink_forst_s3_tests - tests for the forst_s3 remote-state schemes.
# Links clink::core + clink::forst_s3 (which brings forst + s3).

add_executable(clink_forst_s3_tests
    register.cpp
    test_s3_forst_scheme.cpp
    # Finalises Arrow/AWS S3 before static destruction (joins the AWS CRT threads
    # off the static-destruction path); see the file.
    ${CMAKE_SOURCE_DIR}/tests/test_helpers/s3_finalizing_gtest_main.cpp
)
target_link_libraries(clink_forst_s3_tests PRIVATE
    clink::core
    clink::forst_s3
    GTest::gtest
)
clink_set_warnings(clink_forst_s3_tests)
gtest_discover_tests(clink_forst_s3_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the live S3 tests exercise the AWS SDK; the dead-endpoint
    # construction path stays well under it.
    PROPERTIES TIMEOUT 60 LABELS forst_s3
)
