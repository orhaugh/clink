# clink_clickhouse_tests - tests for the clink::clickhouse impl in
# isolation. Links clink::core + clink::clickhouse only.

add_executable(clink_clickhouse_tests
    register.cpp
    test_factory_registration.cpp
    test_clickhouse_sink.cpp
    test_clickhouse_source.cpp
    test_builder.cpp
    test_typed_sink.cpp
    test_clickhouse_pins_live.cpp
)
target_link_libraries(clink_clickhouse_tests PRIVATE
    clink::core
    clink::clickhouse
    GTest::gtest_main
)
target_compile_definitions(clink_clickhouse_tests PRIVATE CLINK_HAS_CLICKHOUSE)
# The native sink's headers live beside its sources (src/native/), outside the
# installed include tree.
target_include_directories(clink_clickhouse_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
clink_set_warnings(clink_clickhouse_tests)

# The native sink's tests, and the fake server they drive it against. The fake
# is a library of its own so that the SQL-linked tests at the root can use it
# too.
if(_ch_native)
    add_library(clink_clickhouse_testing STATIC fake_transport.cpp)
    target_include_directories(clink_clickhouse_testing PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/../src)
    target_link_libraries(clink_clickhouse_testing PUBLIC clink::core clink::clickhouse
        PRIVATE ${_ch_libs})
    target_compile_definitions(clink_clickhouse_testing PUBLIC CLINK_CLICKHOUSE_NATIVE=1)
    clink_set_warnings(clink_clickhouse_testing)

    target_sources(clink_clickhouse_tests PRIVATE
        test_native_arrow_to_block.cpp
        test_native_client_options.cpp
        test_native_column_plan.cpp
        test_native_error_class.cpp
        test_native_errors.cpp
        test_native_fake_transport.cpp
        test_native_options.cpp
        test_native_retry.cpp
        test_native_row_arrow.cpp
        test_native_sink.cpp
        test_native_socket_factory.cpp
        test_native_statements.cpp
        test_native_target_table.cpp
        test_native_types.cpp
        test_native_writer.cpp
    )
    target_link_libraries(clink_clickhouse_tests PRIVATE clink_clickhouse_testing ${_ch_libs})
    if(_ch_native_tls)
        target_compile_definitions(clink_clickhouse_tests PRIVATE CLINK_CLICKHOUSE_NATIVE_TLS=1)
    endif()
endif()
gtest_discover_tests(clink_clickhouse_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 120 LABELS clickhouse
)
