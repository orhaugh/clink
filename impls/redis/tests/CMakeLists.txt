# clink_redis_tests - tests for the clink::redis impl in isolation. The test TUs
# include the source/sink headers (which include <hiredis/hiredis.h>), so the
# target links the resolved hiredis target directly (clink::redis keeps it
# PRIVATE and does not propagate its headers). ${_redis_target} is set by the
# parent (module) CMakeLists and inherited into this subdirectory scope.

add_executable(clink_redis_tests
    register.cpp
    test_factory_registration.cpp
    test_redis_sink_logic.cpp
    test_redis_source_logic.cpp
    test_redis_live.cpp
)
target_link_libraries(clink_redis_tests PRIVATE
    clink::core
    clink::redis
    GTest::gtest_main
    ${_redis_target}
)
target_compile_definitions(clink_redis_tests PRIVATE CLINK_HAS_REDIS)
clink_set_warnings(clink_redis_tests)
gtest_discover_tests(clink_redis_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS redis
)
