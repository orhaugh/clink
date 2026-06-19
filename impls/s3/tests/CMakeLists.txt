# clink_s3_tests - tests for the clink::s3 impl in isolation.
# Links clink::core + clink::s3 only.

add_executable(clink_s3_tests
    register.cpp
    test_factory_registration.cpp
    test_s3_connector.cpp
    test_builder.cpp
    test_parquet_s3.cpp
    test_typed_sink.cpp
    test_s3_materialization_store.cpp
    test_s3_snapshot_store.cpp
    test_local_object_cache.cpp
    test_cas_manifest.cpp
    test_s3_cas_snapshot_store.cpp
    test_s3_remote_pool.cpp
    test_state_parquet_export_s3.cpp
)
target_link_libraries(clink_s3_tests PRIVATE
    clink::core
    clink::s3
    GTest::gtest_main
)
target_compile_definitions(clink_s3_tests PRIVATE CLINK_HAS_AWS_S3)
clink_set_warnings(clink_s3_tests)
gtest_discover_tests(clink_s3_tests
    DISCOVERY_MODE PRE_TEST
    # 60s rather than the 30s default: S3 tests exercise the AWS SDK
    # against unreachable endpoints, and even with our tightened retry
    # strategy the Linux SDK path can occasionally run a couple of
    # quick retries that push past 30s under TSan instrumentation.
    PROPERTIES TIMEOUT 60 LABELS s3
)
