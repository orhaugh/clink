# clink_mysql_tests - tests for the clink::mysql impl in isolation. The test TUs
# include the source/sink headers (which include <mariadb/mysql.h>), so the target
# links the resolved mariadb target directly (clink::mysql keeps it PRIVATE and
# does not propagate its headers). ${_mysql_target} is set by the parent (module)
# CMakeLists and inherited into this subdirectory scope.

add_executable(clink_mysql_tests
    register.cpp
    test_factory_registration.cpp
    test_mysql_sink_logic.cpp
    test_mysql_source_logic.cpp
    test_mysql_cdc_decode.cpp
    test_mysql_snapshot.cpp
    test_mysql_live.cpp
    test_mysql_upsert_sink.cpp
    test_mysql_cdc_live.cpp
)
target_link_libraries(clink_mysql_tests PRIVATE
    clink::core
    clink::mysql
    GTest::gtest_main
    ${_mysql_target}
)
target_compile_definitions(clink_mysql_tests PRIVATE CLINK_HAS_MYSQL)
clink_set_warnings(clink_mysql_tests)
gtest_discover_tests(clink_mysql_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS mysql
)
