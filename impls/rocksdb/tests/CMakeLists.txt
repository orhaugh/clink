# clink_rocksdb_tests - tests for the clink::rocksdb impl in
# isolation. Links clink::core + clink::rocksdb only.

add_executable(clink_rocksdb_tests
    register.cpp
    test_rocksdb_backend.cpp
    test_savepoint_relocation.cpp
    test_changelog_rocksdb_store.cpp
    test_rocksdb_snapshot_store.cpp
)
target_link_libraries(clink_rocksdb_tests PRIVATE
    clink::core
    clink::rocksdb
    GTest::gtest_main
)
target_compile_definitions(clink_rocksdb_tests PRIVATE CLINK_HAS_ROCKSDB)
clink_set_warnings(clink_rocksdb_tests)
gtest_discover_tests(clink_rocksdb_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS rocksdb
)
