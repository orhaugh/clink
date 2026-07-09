add_executable(clink_wasm_tests
    test_wasm_udf.cpp
)
target_link_libraries(clink_wasm_tests PRIVATE
    clink::wasm
    clink::core
    GTest::gtest_main
)
# SQL end-to-end (CREATE FUNCTION ... LANGUAGE wasm through the embedded
# engine) needs the SQL frontend; included only when it is built. Gated on
# the option (not the target): impls/ configures before clink::embed is
# defined, and linking a later-defined target resolves at generate time.
if(CLINK_BUILD_SQL)
    target_sources(clink_wasm_tests PRIVATE test_wasm_sql_e2e.cpp)
    target_link_libraries(clink_wasm_tests PRIVATE clink::embed)
endif()
# The tests compile WAT fixtures in-process via wat_to_wasm, which the impl
# wraps; no wasmtime headers are needed here.
clink_set_warnings(clink_wasm_tests)
gtest_discover_tests(clink_wasm_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 30 LABELS wasm
)
