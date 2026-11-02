# clink_kafka_tests - tests for the clink::kafka impl in isolation.
# Links clink::core + clink::kafka (and the rdkafka libs directly,
# since test_kafka.cpp drives the rdkafka_mock broker through the C
# API). Other impls are NOT on this target's link line - if a Kafka
# test accidentally pulls in a Postgres header, the build breaks here.

add_executable(clink_kafka_tests
    register.cpp
    test_factory_registration.cpp
    test_kafka.cpp
    test_txn_resume.cpp
    test_txn_resume_live.cpp
    test_end_of_input_commit_live.cpp
    test_kafka_security.cpp
    test_pipeline_loader.cpp
    test_pipeline_config.cpp
    test_typed_helpers.cpp
    test_registry_formats.cpp
    test_registry_formats_live.cpp
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
# The registry-format round trip drives the string-channel factories against
# the in-process registry double from impls/schema_registry/tests.
if(TARGET clink::schema_registry)
    target_link_libraries(clink_kafka_tests PRIVATE clink::schema_registry)
    target_include_directories(clink_kafka_tests PRIVATE ${CMAKE_SOURCE_DIR}/impls/schema_registry/tests)
    if(TARGET clink::avro)
        target_compile_definitions(clink_kafka_tests PRIVATE CLINK_SCHEMA_REGISTRY_HAS_AVRO)
    endif()
    if(CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF)
        target_compile_definitions(clink_kafka_tests PRIVATE CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF)
    endif()
endif()
clink_set_warnings(clink_kafka_tests)
# The live suites start a Redpanda container each (tests/integration/
# docker_kafka.hpp) and skip without Docker. Under ctest --parallel several
# started at once and ran past the 30 s the other cases get, so they hold one
# lock and the time a container start takes. scripts/docker-suites-live.sh
# runs them from the binary, outside ctest.
set(_kafka_live_suites
    "TxnResumeLive.*:TxnResumeSaslLive.*:KafkaTxnEndOfInputLive.*:KafkaRegistryFormatsLive.*")
gtest_discover_tests(clink_kafka_tests
    DISCOVERY_MODE PRE_TEST
    TEST_FILTER "-${_kafka_live_suites}"
    PROPERTIES TIMEOUT 30 LABELS kafka
)
gtest_discover_tests(clink_kafka_tests
    DISCOVERY_MODE PRE_TEST
    TEST_FILTER "${_kafka_live_suites}"
    TEST_LIST clink_kafka_live_tests_list
    PROPERTIES TIMEOUT 180 LABELS kafka RESOURCE_LOCK clink_kafka_docker_broker
)

# docker_kafka.hpp (the shared Redpanda fixture) lives under tests/integration/.
target_include_directories(clink_kafka_tests PRIVATE ${CMAKE_SOURCE_DIR})
