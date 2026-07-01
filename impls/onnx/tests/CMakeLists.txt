# clink_onnx_tests - the ONNX Runtime model provider against a tiny in-tree model.
# Links clink::core + clink::onnx (the impl carries the ORT headers/runtime privately,
# so the test needs only the public provider header + the fixture model bytes).

add_executable(clink_onnx_tests
    test_onnx_model_provider.cpp
)
target_link_libraries(clink_onnx_tests PRIVATE
    clink::core
    clink::onnx
    GTest::gtest_main
)
clink_set_warnings(clink_onnx_tests)
gtest_discover_tests(clink_onnx_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS onnx
)
