# clink_webhdfs_tests - tests for the clink::webhdfs impl in isolation. Links clink::core +
# clink::webhdfs; the round-trip test spins an in-process httplib server that emulates the WebHDFS
# CREATE/OPEN 307 redirect protocol, so it links the vendored httplib include too.

add_executable(clink_webhdfs_tests
    register.cpp
    test_factory_registration.cpp
    test_webhdfs_parquet.cpp
    test_webhdfs_parquet_live.cpp
)
target_link_libraries(clink_webhdfs_tests PRIVATE
    clink::core
    clink::webhdfs
    GTest::gtest_main
    $<BUILD_INTERFACE:httplib::httplib>
)
target_compile_definitions(clink_webhdfs_tests PRIVATE CLINK_HAS_WEBHDFS)
clink_set_warnings(clink_webhdfs_tests)
gtest_discover_tests(clink_webhdfs_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint tests wait out the HTTP connect timeout; the live test is env-gated.
    PROPERTIES TIMEOUT 60 LABELS webhdfs
)
