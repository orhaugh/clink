# clink_http_connector_tests - tests for the clink::http_connector impl in
# isolation. Links clink::core + clink::http_connector; the test also spins a
# local httplib server stub, so it links the vendored httplib include too.

add_executable(clink_http_connector_tests
    register.cpp
    test_factory_registration.cpp
    test_http_sink.cpp
    test_elasticsearch_sink.cpp
    test_splunk_sink.cpp
    test_prometheus_sink.cpp
    test_http_poll_source.cpp
)
target_link_libraries(clink_http_connector_tests PRIVATE
    clink::core
    clink::http_connector
    GTest::gtest_main
    $<BUILD_INTERFACE:httplib::httplib>
)
target_compile_definitions(clink_http_connector_tests PRIVATE CLINK_HAS_HTTP_CONNECTOR)
clink_set_warnings(clink_http_connector_tests)
gtest_discover_tests(clink_http_connector_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS http_connector
)
