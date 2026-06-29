# clink::azure - Parquet over Azure Blob Storage via Arrow's AzureFileSystem.
# No separate client dep: Arrow (built with ARROW_AZURE=ON, scripts/build-arrow.sh) statically
# bundles azure-sdk-for-cpp into libarrow, which clink::core already links. So this module only
# needs the Arrow that clink::core provides - the gate is whether that Arrow was compiled with
# Azure support (the ARROW_AZURE variable set by find_package(Arrow)).

if(CLINK_WITH_AZURE STREQUAL "OFF")
    message(STATUS "clink::azure - CLINK_WITH_AZURE=OFF; target not defined")
    return()
endif()

if(NOT ARROW_AZURE)
    if(CLINK_WITH_AZURE STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_AZURE=ON but the pinned Arrow was built without ARROW_AZURE "
                            "(set -DARROW_AZURE=ON in scripts/build-arrow.sh and rebuild)")
    endif()
    message(STATUS "clink::azure - Arrow built without Azure support; target not defined")
    return()
endif()

add_library(clink_azure STATIC
    src/register_factories.cpp
)
add_library(clink::azure ALIAS clink_azure)

target_include_directories(clink_azure PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_azure PUBLIC cxx_std_23)
target_compile_definitions(clink_azure PUBLIC CLINK_HAS_AZURE)
target_link_libraries(clink_azure PUBLIC clink::core)  # AzureFileSystem rides clink::core's Arrow
clink_set_warnings(clink_azure)

clink_install_impl(
    TARGET clink_azure
    NAME   azure)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::azure - enabled")
