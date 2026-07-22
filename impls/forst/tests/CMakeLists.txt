# clink_forst_tests - tests for the clink::forst impl in isolation.
# Links clink::core + clink::forst only.

add_executable(clink_forst_tests
    register.cpp
    test_forst_backend.cpp
)
target_link_libraries(clink_forst_tests PRIVATE
    clink::core
    clink::forst
    GTest::gtest_main
)
clink_set_warnings(clink_forst_tests)
gtest_discover_tests(clink_forst_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS forst
)
