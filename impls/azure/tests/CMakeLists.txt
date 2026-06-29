# clink_azure_tests - tests for the clink::azure impl in isolation.
# Links clink::core + clink::azure only (AzureFileSystem rides clink::core's Arrow).

add_executable(clink_azure_tests
    register.cpp
    test_factory_registration.cpp
    test_parquet_azure.cpp
    test_parquet_azure_live.cpp
)
target_link_libraries(clink_azure_tests PRIVATE
    clink::core
    clink::azure
    GTest::gtest_main
)
target_compile_definitions(clink_azure_tests PRIVATE CLINK_HAS_AZURE)
clink_set_warnings(clink_azure_tests)
gtest_discover_tests(clink_azure_tests
    DISCOVERY_MODE PRE_TEST
    # 90s: the dead-endpoint tests wait out azure-sdk's default retry/backoff (no retry-cap
    # option on AzureOptions, unlike GcsOptions); the live test is env-gated.
    PROPERTIES TIMEOUT 90 LABELS azure
)
