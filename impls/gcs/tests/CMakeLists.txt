# clink_gcs_tests - tests for the clink::gcs impl in isolation.
# Links clink::core + clink::gcs only (GcsFileSystem rides clink::core's Arrow).

add_executable(clink_gcs_tests
    register.cpp
    test_factory_registration.cpp
    test_parquet_gcs.cpp
    test_parquet_gcs_live.cpp
)
target_link_libraries(clink_gcs_tests PRIVATE
    clink::core
    clink::gcs
    GTest::gtest_main
)
target_compile_definitions(clink_gcs_tests PRIVATE CLINK_HAS_GCS)
clink_set_warnings(clink_gcs_tests)
gtest_discover_tests(clink_gcs_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint tests cap GCS retries (retry_limit_seconds); the live test is gated.
    PROPERTIES TIMEOUT 60 LABELS gcs
)
