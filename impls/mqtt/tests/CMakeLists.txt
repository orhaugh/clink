# clink_mqtt_tests - tests for the clink::mqtt impl in isolation. The test TUs
# include the source/sink/client headers (which include <mosquitto.h>), so the
# target links the resolved libmosquitto target directly (clink::mqtt keeps it
# PRIVATE and does not propagate its headers). ${_mqtt_target} is set by the
# parent (module) CMakeLists and inherited into this subdirectory scope.

add_executable(clink_mqtt_tests
    register.cpp
    test_factory_registration.cpp
    test_mqtt_sink_logic.cpp
    test_mqtt_source_logic.cpp
    test_mqtt_live.cpp
)
target_link_libraries(clink_mqtt_tests PRIVATE
    clink::core
    clink::mqtt
    GTest::gtest_main
    ${_mqtt_target}
)
target_compile_definitions(clink_mqtt_tests PRIVATE CLINK_HAS_MQTT)
clink_set_warnings(clink_mqtt_tests)
gtest_discover_tests(clink_mqtt_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS mqtt
)
