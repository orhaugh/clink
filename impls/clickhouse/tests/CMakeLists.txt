# clink_clickhouse_tests - tests for the clink::clickhouse impl in
# isolation. Links clink::core + clink::clickhouse only.

add_executable(clink_clickhouse_tests
    register.cpp
    test_factory_registration.cpp
    test_clickhouse_sink.cpp
    test_clickhouse_source.cpp
    test_builder.cpp
    test_typed_sink.cpp
)
target_link_libraries(clink_clickhouse_tests PRIVATE
    clink::core
    clink::clickhouse
    GTest::gtest_main
)
target_compile_definitions(clink_clickhouse_tests PRIVATE CLINK_HAS_CLICKHOUSE)
clink_set_warnings(clink_clickhouse_tests)
gtest_discover_tests(clink_clickhouse_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS clickhouse
)
