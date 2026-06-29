# clink::gcs - Parquet over Google Cloud Storage via Arrow's GcsFileSystem.
# No separate client dep: Arrow (built with ARROW_GCS=ON, scripts/build-arrow.sh) statically
# bundles google-cloud-cpp's storage client into libarrow, which clink::core already links. So
# this module only needs the Arrow that clink::core provides - the gate is whether that Arrow
# was compiled with GCS support (the ARROW_GCS variable set by find_package(Arrow)).

if(CLINK_WITH_GCS STREQUAL "OFF")
    message(STATUS "clink::gcs - CLINK_WITH_GCS=OFF; target not defined")
    return()
endif()

if(NOT ARROW_GCS)
    if(CLINK_WITH_GCS STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_GCS=ON but the pinned Arrow was built without ARROW_GCS "
                            "(set -DARROW_GCS=ON in scripts/build-arrow.sh and rebuild)")
    endif()
    message(STATUS "clink::gcs - Arrow built without GCS support; target not defined")
    return()
endif()

add_library(clink_gcs STATIC
    src/register_factories.cpp
)
add_library(clink::gcs ALIAS clink_gcs)

target_include_directories(clink_gcs PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_gcs PUBLIC cxx_std_23)
target_compile_definitions(clink_gcs PUBLIC CLINK_HAS_GCS)
target_link_libraries(clink_gcs PUBLIC clink::core)  # GcsFileSystem rides clink::core's Arrow
clink_set_warnings(clink_gcs)

clink_install_impl(
    TARGET clink_gcs
    NAME   gcs)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::gcs - enabled")
