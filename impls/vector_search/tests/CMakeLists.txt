# clink_vector_search_tests - distance kernels + KNN index in isolation.
# Links clink::core + clink::vector_search (the impl carries the SimSIMD/usearch
# includes privately, so the test needs only the public headers).

add_executable(clink_vector_search_tests
    test_distance_kernels.cpp
)
target_link_libraries(clink_vector_search_tests PRIVATE
    clink::core
    clink::vector_search
    GTest::gtest_main
)
clink_set_warnings(clink_vector_search_tests)
gtest_discover_tests(clink_vector_search_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS vector_search
)
