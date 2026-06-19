# Integration test for the etcd-backed HaCoordinator. Self-skips
# when etcd isn't reachable, so the suite stays green on machines
# without an etcd in the docker-compose stack.

add_executable(clink_etcd_tests
    test_etcd_ha_coordinator.cpp
)
target_link_libraries(clink_etcd_tests PRIVATE
    clink::core
    clink::etcd
    GTest::gtest_main
)
clink_set_warnings(clink_etcd_tests)

gtest_discover_tests(clink_etcd_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS etcd
)
