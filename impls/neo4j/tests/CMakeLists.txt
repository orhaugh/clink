# clink_neo4j_tests - the Neo4j sink in isolation. The unit tests drive the sink
# against an in-process httplib server standing in for the Query API, so they
# link the vendored httplib too. The live tests (test_neo4j_live.cpp) run only
# when CLINK_NEO4J_TEST_ENDPOINT names a server; scripts/neo4j-live.sh starts the
# pinned one and fails on any skip.

add_executable(clink_neo4j_tests
    register.cpp
    test_neo4j_factory.cpp
    test_neo4j_options.cpp
    test_neo4j_types.cpp
    test_neo4j_reply.cpp
    test_neo4j_sink.cpp
    test_neo4j_live.cpp
)
target_link_libraries(clink_neo4j_tests PRIVATE
    clink::core
    clink::neo4j
    GTest::gtest_main
    $<BUILD_INTERFACE:httplib::httplib>
)
target_include_directories(clink_neo4j_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
    # tcp_fault_proxy.hpp lives under tests/integration/.
    ${CMAKE_SOURCE_DIR}
)
target_compile_definitions(clink_neo4j_tests PRIVATE CLINK_HAS_NEO4J)
clink_set_warnings(clink_neo4j_tests)
gtest_discover_tests(clink_neo4j_tests
    DISCOVERY_MODE PRE_TEST
    # 120s: the live outage and cancellation cases wait on real timeouts.
    PROPERTIES TIMEOUT 120 LABELS neo4j
)
