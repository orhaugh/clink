# clink::relate - a sink that writes rows into a Relate store opened in the
# engine's own process: each batch is one transaction of the table's statement,
# with the rows as the $rows parameter. Relate (github.com/relatedb/relate) is
# an embedded graph database, so there is no server and no network; its C
# library (relate.h and librelate_ffi.a, the typed C ABI v2) is linked into this
# module. scripts/fetch-relate.sh installs the version scripts/versions.env pins
# under the dependency prefix; RELATE_FFI_ROOT names another copy.

if(CLINK_WITH_RELATE STREQUAL "OFF")
    message(STATUS "clink::relate - CLINK_WITH_RELATE=OFF; target not defined")
    return()
endif()

set(_relate_roots "")
if(RELATE_FFI_ROOT)
    list(APPEND _relate_roots "${RELATE_FFI_ROOT}")
endif()
if(DEFINED ENV{RELATE_FFI_ROOT})
    list(APPEND _relate_roots "$ENV{RELATE_FFI_ROOT}")
endif()
if(_clink_deps_prefix)
    list(APPEND _relate_roots "${_clink_deps_prefix}/relate-ffi")
endif()

unset(_relate_inc CACHE)
unset(_relate_lib CACHE)
find_path(_relate_inc NAMES relate.h
    HINTS ${_relate_roots} PATH_SUFFIXES include NO_DEFAULT_PATH)
find_library(_relate_lib NAMES librelate_ffi.a
    HINTS ${_relate_roots} PATH_SUFFIXES lib NO_DEFAULT_PATH)

if(NOT (_relate_inc AND _relate_lib))
    if(CLINK_WITH_RELATE STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_RELATE=ON but Relate's C library was not found "
                            "(relate.h and librelate_ffi.a). Run scripts/fetch-relate.sh, "
                            "or point RELATE_FFI_ROOT at a directory holding include/ and lib/")
    endif()
    message(STATUS "clink::relate - Relate's C library not found "
                   "(scripts/fetch-relate.sh installs it); target not defined")
    return()
endif()

# The static library carries Rust's runtime and needs only what the platform
# already has.
add_library(_clink_relate_external INTERFACE)
target_include_directories(_clink_relate_external INTERFACE ${_relate_inc})
target_link_libraries(_clink_relate_external INTERFACE ${_relate_lib})
if(APPLE)
    target_link_libraries(_clink_relate_external INTERFACE "-framework CoreFoundation" iconv)
else()
    find_package(Threads REQUIRED)
    target_link_libraries(_clink_relate_external INTERFACE Threads::Threads ${CMAKE_DL_LIBS} m)
endif()

add_library(clink_relate STATIC
    src/errors.cpp
    src/options.cpp
    src/register_factories.cpp
    src/relate_sink.cpp
    src/store.cpp
    src/types.cpp
)
add_library(clink::relate ALIAS clink_relate)

target_include_directories(clink_relate
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_features(clink_relate PUBLIC cxx_std_23)
target_compile_definitions(clink_relate PUBLIC CLINK_HAS_RELATE)
target_link_libraries(clink_relate
    PUBLIC  clink::core
    PRIVATE _clink_relate_external
)
clink_set_warnings(clink_relate)

# The tests read a store back through the C library directly.
set(CLINK_RELATE_LIBS_EXPORT _clink_relate_external CACHE INTERNAL "Relate's C library, for tests")

install(TARGETS _clink_relate_external EXPORT ${CLINK_EXPORT_NAME})
clink_install_impl(
    TARGET clink_relate
    NAME   relate)

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::relate - enabled (${_relate_lib})")
