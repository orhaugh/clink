add_executable(clink_schema_registry_tests
    register.cpp
    test_wire_format.cpp
    test_client.cpp
    test_schema_derivation.cpp
    test_json_schema_format.cpp
    test_avro_format.cpp
    test_protobuf_format.cpp
)
target_link_libraries(clink_schema_registry_tests PRIVATE
    clink::core
    clink::schema_registry
    GTest::gtest_main
)
target_compile_definitions(clink_schema_registry_tests PRIVATE CLINK_HAS_SCHEMA_REGISTRY)
if(TARGET clink::avro)
    # The Avro tests encode reference payloads with avro-cpp itself, so the
    # decoder is checked against the reference implementation, not against
    # our own encoder.
    target_link_libraries(clink_schema_registry_tests PRIVATE clink::avro)
    target_compile_definitions(clink_schema_registry_tests PRIVATE CLINK_SCHEMA_REGISTRY_HAS_AVRO)
endif()
if(_sr_protobuf)
    target_link_libraries(clink_schema_registry_tests PRIVATE protobuf::libprotobuf protobuf::libprotoc)
    target_compile_definitions(clink_schema_registry_tests PRIVATE CLINK_SCHEMA_REGISTRY_HAS_PROTOBUF)
endif()
clink_set_warnings(clink_schema_registry_tests)
gtest_discover_tests(clink_schema_registry_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 60 LABELS schema_registry
)
