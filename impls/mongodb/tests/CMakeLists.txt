# clink_mongodb_tests - tests for the clink::mongodb impl in isolation. The live
# test TU includes the driver headers (mongo_client.hpp), so the target links the
# resolved mongocxx target directly (clink::mongodb keeps it PRIVATE and does not
# propagate its headers). ${_mongo_target} is set by the parent (module) CMakeLists
# and inherited into this subdirectory scope.

add_executable(clink_mongodb_tests
    register.cpp
    test_factory_registration.cpp
    test_mongo_event.cpp
    test_mongo_live.cpp
)
target_link_libraries(clink_mongodb_tests PRIVATE
    clink::core
    clink::mongodb
    GTest::gtest_main
    ${_mongo_target}
)
target_compile_definitions(clink_mongodb_tests PRIVATE CLINK_HAS_MONGODB)
clink_set_warnings(clink_mongodb_tests)
gtest_discover_tests(clink_mongodb_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS mongodb
)
