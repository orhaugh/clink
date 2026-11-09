# clink_rabbitmq_tests - tests for the clink::rabbitmq impl in isolation.
# Links clink::core + clink::rabbitmq only.

add_executable(clink_rabbitmq_tests
    register.cpp
    test_factory_registration.cpp
    test_rabbitmq.cpp
    test_rabbitmq_live.cpp
    test_rabbitmq_sink_contract_live.cpp
    test_rabbitmq_sink_failures.cpp
)
# The sink's live contract suite reads what the broker holds through an AMQP
# client of its own (librabbitmq, which clink::rabbitmq keeps PRIVATE), and
# takes the TCP fault proxy from tests/integration/.
target_link_libraries(clink_rabbitmq_tests PRIVATE
    clink::core
    clink::rabbitmq
    _clink_rabbitmq_external
    GTest::gtest_main
)
target_include_directories(clink_rabbitmq_tests PRIVATE ${CMAKE_SOURCE_DIR})
target_compile_definitions(clink_rabbitmq_tests PRIVATE CLINK_HAS_RABBITMQ)
clink_set_warnings(clink_rabbitmq_tests)
gtest_discover_tests(clink_rabbitmq_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint tests touch the AMQP socket layer; the live test is env-gated.
    PROPERTIES TIMEOUT 60 LABELS rabbitmq
)
