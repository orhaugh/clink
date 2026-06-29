# clink_rabbitmq_tests - tests for the clink::rabbitmq impl in isolation.
# Links clink::core + clink::rabbitmq only.

add_executable(clink_rabbitmq_tests
    register.cpp
    test_factory_registration.cpp
    test_rabbitmq.cpp
    test_rabbitmq_live.cpp
)
target_link_libraries(clink_rabbitmq_tests PRIVATE
    clink::core
    clink::rabbitmq
    GTest::gtest_main
)
target_compile_definitions(clink_rabbitmq_tests PRIVATE CLINK_HAS_RABBITMQ)
clink_set_warnings(clink_rabbitmq_tests)
gtest_discover_tests(clink_rabbitmq_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint tests touch the AMQP socket layer; the live test is env-gated.
    PROPERTIES TIMEOUT 60 LABELS rabbitmq
)
