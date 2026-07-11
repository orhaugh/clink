# clink::vector_search - the vector_search_row KNN operator.
#
# SimSIMD (header-only SIMD distance kernels, its own runtime AVX2/AVX-512/NEON
# dispatch) and usearch (header-only HNSW index) are fetched header-only and linked
# PRIVATE to this impl's TUs, so neither reaches a public clink header or the plugin
# ABI. Both are optional: when a fetch is unavailable the impl still builds with the
# scalar distance fallback + exact flat index (CLINK_VECTOR_SIMSIMD / CLINK_VECTOR_HNSW
# gate the accelerated paths). Unlike the connector impls there is no system package to
# discover, so CLINK_WITH_VECTOR_SEARCH=AUTO behaves as ON.

if(CLINK_WITH_VECTOR_SEARCH STREQUAL "OFF")
    message(STATUS "clink::vector_search - CLINK_WITH_VECTOR_SEARCH=OFF; target not defined")
    return()
endif()

include(FetchContent)

# v6.5.x keeps the stable `simsimd/simsimd.h` umbrella header + simsimd_{dot,cos,l2sq}_f32
# C API (v7 restructured the headers under numkong/ with a different surface).
set(CLINK_SIMSIMD_TAG "v6.5.16" CACHE STRING "SimSIMD version (header-only SIMD distance kernels)")
set(CLINK_USEARCH_TAG "v2.25.3" CACHE STRING "usearch version (header-only HNSW index)")

set(_vs_simd OFF)
set(_vs_hnsw OFF)

FetchContent_Declare(simsimd
    GIT_REPOSITORY https://github.com/ashvardanian/SimSIMD.git
    GIT_TAG ${CLINK_SIMSIMD_TAG}
    GIT_SHALLOW TRUE
    # Never re-run the git update step on reconfigure: a shallow update can
    # drift the checkout off the pinned tag (observed: v6.5.16 -> v7.7.0,
    # which renames the headers and breaks the build).
    UPDATE_DISCONNECTED TRUE
    SYSTEM)
FetchContent_GetProperties(simsimd)
if(NOT simsimd_POPULATED)
    FetchContent_Populate(simsimd)
endif()
if(EXISTS "${simsimd_SOURCE_DIR}/include")
    set(_vs_simd ON)
endif()

FetchContent_Declare(usearch
    GIT_REPOSITORY https://github.com/unum-cloud/usearch.git
    GIT_TAG ${CLINK_USEARCH_TAG}
    GIT_SHALLOW TRUE
    # See simsimd above: no update step on reconfigure.
    UPDATE_DISCONNECTED TRUE
    SYSTEM)
FetchContent_GetProperties(usearch)
if(NOT usearch_POPULATED)
    FetchContent_Populate(usearch)
endif()
if(EXISTS "${usearch_SOURCE_DIR}/include")
    set(_vs_hnsw ON)
endif()

add_library(clink_vector_search STATIC
    src/distance_kernels.cpp
    src/knn_index.cpp
    src/vector_search_operator.cpp
    src/register_factories.cpp
)
add_library(clink::vector_search ALIAS clink_vector_search)

target_include_directories(clink_vector_search PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_vector_search PUBLIC cxx_std_23)
target_compile_definitions(clink_vector_search PUBLIC CLINK_HAS_VECTOR_SEARCH)

# SimSIMD / usearch includes are PRIVATE (impl TUs only) -> no ABI impact. SYSTEM so
# their headers do not trip clink's -Werror.
if(_vs_simd)
    target_include_directories(clink_vector_search SYSTEM PRIVATE
        $<BUILD_INTERFACE:${simsimd_SOURCE_DIR}/include>)
    target_compile_definitions(clink_vector_search PRIVATE CLINK_VECTOR_SIMSIMD=1)
endif()
if(_vs_hnsw)
    target_include_directories(clink_vector_search SYSTEM PRIVATE
        $<BUILD_INTERFACE:${usearch_SOURCE_DIR}/include>)
    # usearch has its own SimSIMD path; keep it off so usearch is self-contained.
    target_compile_definitions(clink_vector_search PRIVATE CLINK_VECTOR_HNSW=1 USEARCH_USE_SIMSIMD=0)
endif()

target_link_libraries(clink_vector_search PUBLIC clink::core)
clink_set_warnings(clink_vector_search)

clink_install_impl(
    TARGET clink_vector_search
    NAME   vector_search)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::vector_search - enabled (SimSIMD=${_vs_simd}, usearch/HNSW=${_vs_hnsw})")
