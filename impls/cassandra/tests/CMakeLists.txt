# clink_cassandra_tests - tests for the clink::cassandra impl in isolation.
# The live test drives the driver directly (schema setup + read-back), so it links the driver lib
# (re-exported as CLINK_CASSANDRA_LIBS_EXPORT) for cassandra.h.

add_executable(clink_cassandra_tests
    register.cpp
    test_factory_registration.cpp
    test_cassandra.cpp
    test_cassandra_live.cpp
    test_cassandra_upsert_sink.cpp
)
target_link_libraries(clink_cassandra_tests PRIVATE
    clink::core
    clink::cassandra
    ${CLINK_CASSANDRA_LIBS_EXPORT}
    GTest::gtest_main
)
target_compile_definitions(clink_cassandra_tests PRIVATE CLINK_HAS_CASSANDRA)
clink_set_warnings(clink_cassandra_tests)
gtest_discover_tests(clink_cassandra_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint test bounds its connect attempt; the live test is env-gated.
    PROPERTIES TIMEOUT 60 LABELS cassandra
)
