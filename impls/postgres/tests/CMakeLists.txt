# clink_postgres_tests - tests for the clink::postgres impl in
# isolation. Links clink::core + clink::postgres only.

add_executable(clink_postgres_tests
    register.cpp
    test_factory_registration.cpp
    test_postgres_decoder.cpp
    test_postgres_sink.cpp
    test_postgres_source_replay.cpp
    test_typed_cdc_source.cpp
    test_postgres_cdc_decode.cpp
    test_postgres_cdc_live.cpp
)
target_link_libraries(clink_postgres_tests PRIVATE
    clink::core
    clink::postgres
    GTest::gtest_main
    PostgreSQL::PostgreSQL  # the live CDC test runs raw setup/DML SQL via libpq
)
target_compile_definitions(clink_postgres_tests PRIVATE CLINK_HAS_POSTGRES)
clink_set_warnings(clink_postgres_tests)
gtest_discover_tests(clink_postgres_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS postgres
)
