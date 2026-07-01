# clink::onnx - the local ONNX Runtime model-inference provider (provider='onnx').
#
# ONNX Runtime is a heavy, optional dependency, so this impl is opt-in:
#   OFF  (default): not built. VECTOR_SEARCH and the HTTP ML_PREDICT provider still ship.
#   AUTO : built ONLY if an ONNX Runtime is discoverable locally (system / Homebrew /
#          user prefix). It does NOT download - so an offline configure stays green.
#   ON   : built; a locally-discoverable runtime is used, otherwise the pinned prebuilt
#          runtime is fetched for this platform. FATAL if neither is possible.
#
# The ORT headers are included SYSTEM PRIVATE (impl TUs only) and the runtime is linked
# PRIVATE, so ONNX Runtime never reaches a public clink header or the plugin ABI.

if(CLINK_WITH_ONNX STREQUAL "OFF")
    message(STATUS "clink::onnx - CLINK_WITH_ONNX=OFF; target not defined")
    return()
endif()

set(CLINK_ONNXRUNTIME_VERSION "1.20.1"
    CACHE STRING "ONNX Runtime version for the pinned prebuilt fetch (CLINK_WITH_ONNX=ON)")

# 1. Prefer a locally-installed ONNX Runtime.
find_path(ONNX_INCLUDE_DIR onnxruntime_cxx_api.h PATH_SUFFIXES onnxruntime include/onnxruntime)
find_library(ONNX_LIBRARY NAMES onnxruntime)
set(_onnx_lib_dir "")

if(ONNX_INCLUDE_DIR AND ONNX_LIBRARY)
    get_filename_component(_onnx_lib_dir "${ONNX_LIBRARY}" DIRECTORY)
    message(STATUS "clink::onnx - using local ONNX Runtime (${ONNX_LIBRARY})")
else()
    if(CLINK_WITH_ONNX STREQUAL "AUTO")
        message(STATUS "clink::onnx - ONNX Runtime not found locally and CLINK_WITH_ONNX=AUTO "
                       "does not download; target not defined "
                       "(set -DCLINK_WITH_ONNX=ON to fetch the pinned prebuilt runtime)")
        return()
    endif()

    # 2. CLINK_WITH_ONNX=ON: fetch the pinned prebuilt runtime for this platform.
    set(_ort_ver "${CLINK_ONNXRUNTIME_VERSION}")
    set(_ort_pkg "")
    set(_ort_hash "")
    if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
            set(_ort_pkg "onnxruntime-osx-arm64-${_ort_ver}")
            set(_ort_hash "b678fc3c2354c771fea4fba420edeccfba205140088334df801e7fc40e83a57a")
        else()
            set(_ort_pkg "onnxruntime-osx-x86_64-${_ort_ver}")
            set(_ort_hash "0f73006813af2a1a5d1723ed7dfb694fc629d15037124081bb61b7bf7d99fc78")
        endif()
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
            set(_ort_pkg "onnxruntime-linux-aarch64-${_ort_ver}")
            set(_ort_hash "ae4fedbdc8c18d688c01306b4b50c63de3445cdf2dbd720e01a2fa3810b8106a")
        else()
            set(_ort_pkg "onnxruntime-linux-x64-${_ort_ver}")
            set(_ort_hash "67db4dc1561f1e3fd42e619575c82c601ef89849afc7ea85a003abbac1a1a105")
        endif()
    endif()
    if(NOT _ort_pkg)
        message(FATAL_ERROR "CLINK_WITH_ONNX=ON but no prebuilt ONNX Runtime is pinned for "
                            "${CMAKE_SYSTEM_NAME}/${CMAKE_SYSTEM_PROCESSOR}; install ONNX Runtime "
                            "locally instead.")
    endif()

    include(FetchContent)
    FetchContent_Declare(onnxruntime
        URL "https://github.com/microsoft/onnxruntime/releases/download/v${_ort_ver}/${_ort_pkg}.tgz"
        URL_HASH SHA256=${_ort_hash})
    FetchContent_GetProperties(onnxruntime)
    if(NOT onnxruntime_POPULATED)
        FetchContent_Populate(onnxruntime)
    endif()

    # ExternalProject strips a single leading directory, so the runtime usually lands
    # directly in SOURCE_DIR; fall back to a globbed subdir if not.
    set(_ort_root "${onnxruntime_SOURCE_DIR}")
    if(NOT EXISTS "${_ort_root}/include/onnxruntime_cxx_api.h")
        file(GLOB _ort_subs LIST_DIRECTORIES true "${onnxruntime_SOURCE_DIR}/onnxruntime-*")
        foreach(_d ${_ort_subs})
            if(EXISTS "${_d}/include/onnxruntime_cxx_api.h")
                set(_ort_root "${_d}")
            endif()
        endforeach()
    endif()
    set(ONNX_INCLUDE_DIR "${_ort_root}/include")
    unset(ONNX_LIBRARY CACHE)
    find_library(ONNX_LIBRARY NAMES onnxruntime PATHS "${_ort_root}/lib" NO_DEFAULT_PATH)
    set(_onnx_lib_dir "${_ort_root}/lib")
    if(NOT ONNX_LIBRARY)
        message(FATAL_ERROR "clink::onnx - fetched ${_ort_pkg} but libonnxruntime was not found "
                            "under ${_ort_root}/lib")
    endif()
    message(STATUS "clink::onnx - using fetched ONNX Runtime ${_ort_ver} (${ONNX_LIBRARY})")
endif()

add_library(clink_onnx STATIC
    src/onnx_model_provider.cpp
    src/register_factories.cpp
)
add_library(clink::onnx ALIAS clink_onnx)

target_include_directories(clink_onnx PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_onnx PUBLIC cxx_std_23)
target_compile_definitions(clink_onnx PUBLIC CLINK_HAS_ONNX)

# ORT headers SYSTEM (keep them out of -Werror) and PRIVATE (impl TUs only) -> no ABI
# impact; the runtime is linked PRIVATE.
target_include_directories(clink_onnx SYSTEM PRIVATE ${ONNX_INCLUDE_DIR})
target_link_libraries(clink_onnx
    PUBLIC  clink::core
    PRIVATE ${ONNX_LIBRARY})

# The prebuilt runtime's install name is @rpath/libonnxruntime.*, so any executable that
# links this impl needs an rpath to the runtime dir. Propagate it as an INTERFACE link
# option so consumers (clink_node, the tests) resolve libonnxruntime at load time.
if(_onnx_lib_dir)
    if(APPLE)
        target_link_options(clink_onnx INTERFACE "-Wl,-rpath,${_onnx_lib_dir}")
    else()
        target_link_options(clink_onnx INTERFACE "-Wl,-rpath,${_onnx_lib_dir}" "-Wl,--disable-new-dtags")
    endif()
endif()

clink_set_warnings(clink_onnx)

clink_install_impl(
    TARGET clink_onnx
    NAME   onnx)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::onnx - enabled")
