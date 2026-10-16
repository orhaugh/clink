# clink_pulsar_tests - tests for the clink::pulsar impl in isolation.
# Links clink::core + clink::pulsar only (the live test drives the connectors, not libpulsar
# directly, since the broker auto-creates topics).

add_executable(clink_pulsar_tests
    register.cpp
    test_factory_registration.cpp
    test_pulsar.cpp
    test_pulsar_live.cpp
)
target_link_libraries(clink_pulsar_tests PRIVATE
    clink::core
    clink::pulsar
    GTest::gtest_main
)
# The broker-certificate tests connect to a local TLS listener
# (test_helpers/tls_handshake_server.hpp), which needs OpenSSL; libpulsar does
# not expose the one it carries.
find_package(OpenSSL QUIET)
if(OpenSSL_FOUND)
    target_sources(clink_pulsar_tests PRIVATE test_pulsar_tls.cpp)
    target_link_libraries(clink_pulsar_tests PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_include_directories(clink_pulsar_tests PRIVATE ${CMAKE_SOURCE_DIR}/tests)
endif()
target_compile_definitions(clink_pulsar_tests PRIVATE CLINK_HAS_PULSAR)
clink_set_warnings(clink_pulsar_tests)
gtest_discover_tests(clink_pulsar_tests
    DISCOVERY_MODE PRE_TEST
    # 60s: the dead-endpoint tests bound their connect retries via a short operation timeout;
    # the live test is env-gated.
    PROPERTIES TIMEOUT 60 LABELS pulsar
)
