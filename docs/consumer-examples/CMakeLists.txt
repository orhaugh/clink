cmake_minimum_required(VERSION 3.24)
project(clink-consumer-examples LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# This is the entire integration point. If you installed clink under a
# non-standard prefix, configure with:
#
#   cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/clink
#
# COMPONENTS names exactly what these examples link, so configuring only
# resolves the dependencies those components need - a machine without,
# say, the Avro or MongoDB client libraries still builds every example.
# Request more components (kafka, postgres, ...) as you link them.
find_package(clink REQUIRED COMPONENTS core test_support)

message(STATUS "clink ${clink_VERSION}: impls = ${clink_AVAILABLE_IMPLS}")

# Core-only examples - these need nothing beyond clink::core.
set(_core_examples
    01_hello_pipeline
    02_event_time_tumbling
    03_sliding_window_aggregate
    04_keyed_process_state
    05_interval_join
    06_file_io
    07_parquet_io)

foreach(_ex IN LISTS _core_examples)
    add_executable(${_ex} ${_ex}.cpp)
    target_link_libraries(${_ex} PRIVATE clink::core)
endforeach()

# 09 links the public testing framework (clink::test_support) as well as
# clink::core, and is a self-checking test: it returns non-zero on failure, so
# it registers with CTest. This is the consumer-facing proof that the exported
# test framework works from a plain find_package(clink) project.
enable_testing()
add_executable(09_testing_framework 09_testing_framework.cpp)
target_link_libraries(09_testing_framework PRIVATE clink::core clink::test_support)
add_test(NAME 09_testing_framework COMMAND 09_testing_framework)

# 08 is a plugin .so (clink_submit_job loads it). It must be linked as
# MODULE, not an executable. Built only when CLINK_BUILD_PLUGIN_EXAMPLE
# is set, since most consumers don't need the cluster-submit flow.
option(CLINK_BUILD_PLUGIN_EXAMPLE "Build the CLINK_REGISTER_JOB plugin example" OFF)
if(CLINK_BUILD_PLUGIN_EXAMPLE)
    add_library(08_cluster_job_plugin MODULE 08_cluster_job_plugin.cpp)
    target_link_libraries(08_cluster_job_plugin PRIVATE clink::core)
    set_target_properties(08_cluster_job_plugin PROPERTIES
        PREFIX ""              # produce 08_cluster_job_plugin.so, not lib08...so
        SUFFIX ".so")
endif()
