# clink_kafka_tests - tests for the clink::kafka impl in isolation.
# Links clink::core + clink::kafka (and the rdkafka libs directly,
# since test_kafka.cpp drives the rdkafka_mock broker through the C
# API). Other impls are NOT on this target's link line - if a Kafka
# test accidentally pulls in a Postgres header, the build breaks here.

add_executable(clink_kafka_tests
    register.cpp
    test_factory_registration.cpp
    test_kafka.cpp
    test_kafka_security.cpp
    test_pipeline_loader.cpp
    test_pipeline_config.cpp
    test_typed_helpers.cpp
)
target_link_libraries(clink_kafka_tests PRIVATE
    clink::core
    clink::kafka
    ${CLINK_KAFKA_LIBS_EXPORT}
    GTest::gtest_main
)
target_compile_definitions(clink_kafka_tests PRIVATE CLINK_HAS_KAFKA)
if(CLINK_HAS_KAFKA_MOCK)
    target_compile_definitions(clink_kafka_tests PRIVATE CLINK_HAS_KAFKA_MOCK)
endif()
clink_set_warnings(clink_kafka_tests)
gtest_discover_tests(clink_kafka_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS kafka
)
