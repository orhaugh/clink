# clink::wasm - sandboxed WebAssembly scalar UDFs (CREATE FUNCTION ... LANGUAGE wasm).
#
# wasmtime is a heavy, optional dependency, so this impl is opt-in (the ONNX pattern):
#   OFF  (default): not built. CREATE FUNCTION ... LANGUAGE wasm errors with a clear
#          "no loader for language" message.
#   AUTO : built ONLY if the wasmtime C API is discoverable locally (system / user
#          prefix). It does NOT download - an offline configure stays green.
#   ON   : built; a locally-discoverable C API is used, otherwise the pinned prebuilt
#          c-api release is fetched for this platform. FATAL if neither is possible.
#
# The wasmtime headers are included SYSTEM PRIVATE (impl TUs only) and the library is
# linked PRIVATE, so wasmtime never reaches a public clink header or the plugin ABI.

if(CLINK_WITH_WASM STREQUAL "OFF")
    message(STATUS "clink::wasm - CLINK_WITH_WASM=OFF; target not defined")
    return()
endif()

set(CLINK_WASMTIME_VERSION "46.0.1"
    CACHE STRING "wasmtime version for the pinned prebuilt c-api fetch (CLINK_WITH_WASM=ON)")

# 1. Prefer a locally-installed wasmtime C API.
find_path(WASMTIME_INCLUDE_DIR wasmtime.h PATH_SUFFIXES wasmtime)
find_library(WASMTIME_LIBRARY NAMES wasmtime)
set(_wasmtime_lib_dir "")

if(WASMTIME_INCLUDE_DIR AND WASMTIME_LIBRARY)
    get_filename_component(_wasmtime_lib_dir "${WASMTIME_LIBRARY}" DIRECTORY)
    message(STATUS "clink::wasm - using local wasmtime C API (${WASMTIME_LIBRARY})")
else()
    if(CLINK_WITH_WASM STREQUAL "AUTO")
        message(STATUS "clink::wasm - wasmtime C API not found locally and CLINK_WITH_WASM=AUTO "
                       "does not download; target not defined "
                       "(set -DCLINK_WITH_WASM=ON to fetch the pinned prebuilt c-api)")
        return()
    endif()

    # 2. CLINK_WITH_WASM=ON: fetch the pinned prebuilt c-api for this platform.
    set(_wt_ver "${CLINK_WASMTIME_VERSION}")
    set(_wt_pkg "")
    set(_wt_hash "")
    if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
            set(_wt_pkg "wasmtime-v${_wt_ver}-aarch64-macos-c-api")
            set(_wt_hash "f2a33c780715cbe740ca888ed3ee38a197d970800ca1b117b338db3aed819333")
        endif()
    elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
            set(_wt_pkg "wasmtime-v${_wt_ver}-aarch64-linux-c-api")
            set(_wt_hash "368406db8027c361e12ad838fb53f049b992985806bfb1256abba27112dff5ae")
        else()
            set(_wt_pkg "wasmtime-v${_wt_ver}-x86_64-linux-c-api")
            set(_wt_hash "4b7e7acf08467de6147f11c8fb71c4db7623035064c096976aa969d54171fed4")
        endif()
    endif()
    if(NOT _wt_pkg)
        message(FATAL_ERROR "CLINK_WITH_WASM=ON but no prebuilt wasmtime c-api is pinned for "
                            "${CMAKE_SYSTEM_NAME}/${CMAKE_SYSTEM_PROCESSOR}; install the wasmtime "
                            "C API locally instead.")
    endif()

    include(FetchContent)
    FetchContent_Declare(wasmtime_capi
        URL "https://github.com/bytecodealliance/wasmtime/releases/download/v${_wt_ver}/${_wt_pkg}.tar.xz"
        URL_HASH SHA256=${_wt_hash})
    FetchContent_GetProperties(wasmtime_capi)
    if(NOT wasmtime_capi_POPULATED)
        FetchContent_Populate(wasmtime_capi)
    endif()

    set(_wt_root "${wasmtime_capi_SOURCE_DIR}")
    if(NOT EXISTS "${_wt_root}/include/wasmtime.h")
        file(GLOB _wt_subs LIST_DIRECTORIES true "${wasmtime_capi_SOURCE_DIR}/wasmtime-*")
        foreach(_d ${_wt_subs})
            if(EXISTS "${_d}/include/wasmtime.h")
                set(_wt_root "${_d}")
            endif()
        endforeach()
    endif()
    set(WASMTIME_INCLUDE_DIR "${_wt_root}/include")
    unset(WASMTIME_LIBRARY CACHE)
    find_library(WASMTIME_LIBRARY NAMES wasmtime PATHS "${_wt_root}/lib" NO_DEFAULT_PATH)
    set(_wasmtime_lib_dir "${_wt_root}/lib")
    if(NOT WASMTIME_LIBRARY)
        message(FATAL_ERROR "clink::wasm - fetched ${_wt_pkg} but libwasmtime was not found "
                            "under ${_wt_root}/lib")
    endif()
    message(STATUS "clink::wasm - using fetched wasmtime ${_wt_ver} (${WASMTIME_LIBRARY})")
endif()

add_library(clink_wasm STATIC
    src/wasm_udf.cpp
)
add_library(clink::wasm ALIAS clink_wasm)
set_target_properties(clink_wasm PROPERTIES EXPORT_NAME wasm)
target_include_directories(clink_wasm PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_include_directories(clink_wasm SYSTEM PRIVATE ${WASMTIME_INCLUDE_DIR})
target_compile_features(clink_wasm PUBLIC cxx_std_23)
target_link_libraries(clink_wasm
    PUBLIC clink::core
    PRIVATE ${WASMTIME_LIBRARY}
)
clink_set_warnings(clink_wasm)
message(STATUS "clink::wasm - enabled (WebAssembly scalar UDFs)")

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()
