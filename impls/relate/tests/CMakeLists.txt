# clink_relate_tests - the Relate sink in isolation. Relate is embedded, so the
# cases write real stores under the temporary directory and read them back
# through the C library: there is no server to stand in for and nothing to
# skip. clink_relate_store_holder is the other process the cases on a store's
# one writing process spawn, hold the store with, and kill.

add_executable(clink_relate_store_holder store_holder.cpp)
target_link_libraries(clink_relate_store_holder PRIVATE ${CLINK_RELATE_LIBS_EXPORT})
clink_set_warnings(clink_relate_store_holder)

add_executable(clink_relate_tests
    register.cpp
    test_relate_factory.cpp
    test_relate_options.cpp
    test_relate_processes.cpp
    test_relate_sink.cpp
    test_relate_types.cpp
)
add_dependencies(clink_relate_tests clink_relate_store_holder)
target_link_libraries(clink_relate_tests PRIVATE
    clink::core
    clink::relate
    ${CLINK_RELATE_LIBS_EXPORT}
    GTest::gtest_main
)
target_include_directories(clink_relate_tests PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../src
)
target_compile_definitions(clink_relate_tests PRIVATE
    CLINK_HAS_RELATE
    CLINK_RELATE_STORE_HOLDER="$<TARGET_FILE:clink_relate_store_holder>"
)
clink_set_warnings(clink_relate_tests)
gtest_discover_tests(clink_relate_tests
    DISCOVERY_MODE PRE_TEST
    PROPERTIES TIMEOUT 120 LABELS relate
)
