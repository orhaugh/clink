# clink_tls_tests - tests for the clink::tls impl in isolation.
# Links clink::core + clink::tls only.

add_executable(clink_tls_tests
    register.cpp
    test_tls_socket.cpp
)
target_link_libraries(clink_tls_tests PRIVATE
    clink::core
    clink::tls
    GTest::gtest_main
)
target_compile_definitions(clink_tls_tests PRIVATE CLINK_HAS_OPENSSL)
clink_set_warnings(clink_tls_tests)
gtest_discover_tests(clink_tls_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS tls
)
