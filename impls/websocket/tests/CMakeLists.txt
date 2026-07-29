# clink_websocket_tests - the protocol layer against RFC 6455's own worked
# examples, the source against an in-process WebSocket server (raw sockets,
# CI-runnable, no external endpoint), and the factory registration. The live
# test is env-gated on CLINK_WS_LIVE_URL and skips by default.

add_executable(clink_websocket_tests
    register.cpp
    test_factory_registration.cpp
    test_ws_protocol.cpp
    test_websocket_source.cpp
    test_websocket_live.cpp
)
target_link_libraries(clink_websocket_tests PRIVATE
    clink::core
    clink::websocket
    GTest::gtest_main
)
target_compile_definitions(clink_websocket_tests PRIVATE CLINK_HAS_WEBSOCKET)
clink_set_warnings(clink_websocket_tests)
gtest_discover_tests(clink_websocket_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS websocket
)
