# clink_nats_tests - tests for the clink::nats impl in isolation.
# Links clink::core + clink::nats; the live test drives js_AddStream directly, so it also links
# the nats lib (re-exported as CLINK_NATS_LIBS_EXPORT) for nats/nats.h.

add_executable(clink_nats_tests
    register.cpp
    test_factory_registration.cpp
    test_nats.cpp
    test_nats_live.cpp
    test_nats_sink_contract_live.cpp
    test_nats_sink_failures.cpp
)
target_link_libraries(clink_nats_tests PRIVATE
    clink::core
    clink::nats
    ${CLINK_NATS_LIBS_EXPORT}
    GTest::gtest_main
)
# The sink's live contract suite takes the TCP fault proxy from tests/integration/.
target_include_directories(clink_nats_tests PRIVATE ${CMAKE_SOURCE_DIR})
target_compile_definitions(clink_nats_tests PRIVATE CLINK_HAS_NATS)
clink_set_warnings(clink_nats_tests)
gtest_discover_tests(clink_nats_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint tests touch the nats.c socket layer; the live test is env-gated.
    PROPERTIES TIMEOUT 60 LABELS nats
)
