# clink_avro_tests - tests for the clink::avro impl in isolation.
# Links clink::core + clink::avro only.

add_executable(clink_avro_tests
    register.cpp
    test_codec.cpp
)
target_link_libraries(clink_avro_tests PRIVATE
    clink::core
    clink::avro
    GTest::gtest_main
)
target_compile_definitions(clink_avro_tests PRIVATE CLINK_HAS_AVRO)
clink_set_warnings(clink_avro_tests)
gtest_discover_tests(clink_avro_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS avro
)
