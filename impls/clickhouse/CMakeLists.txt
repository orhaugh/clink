# clink::clickhouse - ClickHouse insert sink (TSV / JSONEachRow).
#
# DRAFT: source files referenced below do not yet exist. Migration plan:
#   src/connectors/clickhouse_sink.cpp       -> impls/clickhouse/src/clickhouse_sink.cpp
#   include/clink/connectors/clickhouse_sink.hpp ->
#       impls/clickhouse/include/clink/connectors/clickhouse_sink.hpp
# Plus extract the clickhouse_sink factory registration from
# src/cluster/built_in_factories.cpp into
# impls/clickhouse/src/register_factories.cpp.

if(CLINK_WITH_CLICKHOUSE STREQUAL "OFF")
    message(STATUS "clink::clickhouse - CLINK_WITH_CLICKHOUSE=OFF; target not defined")
    return()
endif()

set(_ch_found FALSE)
set(_ch_libs)
set(_ch_include_dir)

# The shared libraries a static client archive needs, as raw paths placed after
# it. Other targets already link OpenSSL and abseil through their imported
# targets, and CMake puts a de-duplicated shared library at its first position on
# the link line, ahead of the archive; GNU ld (with --as-needed, Debian's default)
# then drops it as unneeded and leaves the archive's TLS and int128 references
# undefined. A path is a distinct link item, so it lands after the archive.
function(_clink_clickhouse_shared_deps out)
    set(_deps)
    if(OPENSSL_SSL_LIBRARY)
        list(APPEND _deps "${OPENSSL_SSL_LIBRARY}")
    endif()
    if(OPENSSL_CRYPTO_LIBRARY)
        list(APPEND _deps "${OPENSSL_CRYPTO_LIBRARY}")
    endif()
    if(TARGET absl::int128)
        list(APPEND _deps "$<BUILD_INTERFACE:$<TARGET_LINKER_FILE:absl::int128>>")
    endif()
    set(${out} "${_deps}" PARENT_SCOPE)
endfunction()

# The pinned client first: scripts/build-clickhouse-cpp.sh builds the version
# scripts/versions.env names, static, in Release, with TLS, into
# <CLINK_DEPS_PREFIX>/clickhouse-cpp (the image installs it into /usr/local, where
# the search below finds it). Homebrew's or the system's copy is the fallback, and
# the configure log says which one was taken and its version.
set(_ch_pinned_dir)
if(_clink_deps_prefix AND EXISTS "${_clink_deps_prefix}/clickhouse-cpp/include/clickhouse/client.h")
    set(_ch_pinned_dir "${_clink_deps_prefix}/clickhouse-cpp")
endif()
if(_ch_pinned_dir)
    find_library(_ch_pinned_lib NAMES clickhouse-cpp-lib
        PATHS "${_ch_pinned_dir}/lib" NO_DEFAULT_PATH)
    find_library(_ch_pinned_cityhash NAMES cityhash
        PATHS "${_ch_pinned_dir}/lib" NO_DEFAULT_PATH)
    find_library(_ch_pinned_lz4 NAMES lz4 HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    find_library(_ch_pinned_zstd NAMES zstd HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    find_package(absl CONFIG QUIET)
    find_package(OpenSSL QUIET)
    if(_ch_pinned_lib AND _ch_pinned_cityhash AND _ch_pinned_lz4 AND _ch_pinned_zstd
       AND TARGET absl::int128 AND OpenSSL_FOUND)
        add_library(_clink_clickhouse_external INTERFACE)
        target_include_directories(_clink_clickhouse_external INTERFACE
            "${_ch_pinned_dir}/include")
        _clink_clickhouse_shared_deps(_ch_after)
        target_link_libraries(_clink_clickhouse_external INTERFACE
            ${_ch_pinned_lib} ${_ch_pinned_cityhash} ${_ch_pinned_lz4} ${_ch_pinned_zstd}
            absl::int128 OpenSSL::SSL OpenSSL::Crypto ${_ch_after})
        set(_ch_libs _clink_clickhouse_external)
        set(_ch_include_dir "${_ch_pinned_dir}/include")
        set(_ch_found TRUE)
    else()
        message(STATUS "clink::clickhouse - the pinned client in ${_ch_pinned_dir} is "
                       "missing a dependency (lz4, zstd, abseil or OpenSSL); "
                       "falling back to the system copy")
    endif()
endif()

if(NOT _ch_found)
    find_package(clickhouse-cpp CONFIG QUIET)
endif()
if(_ch_found)
    # The pinned client, above.
elseif(clickhouse-cpp_FOUND)
    set(_ch_found TRUE)
    set(_ch_libs clickhouse-cpp-lib)
else()
    find_path(_ch_inc NAMES clickhouse/client.h
        HINTS /opt/homebrew/include /usr/local/include /usr/include)
    find_library(_ch_lib NAMES clickhouse-cpp-lib
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    # clickhouse-cpp's Homebrew bottle (and source build) ships
    # libcityhash.a as a sibling static archive - clickhouse-cpp uses
    # cityhash internally but doesn't bundle the symbols, so any
    # downstream linking clickhouse-cpp must also link cityhash.
    find_library(_ch_cityhash NAMES cityhash
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    # clickhouse-cpp's CompressedInput/Output use LZ4 + ZSTD; the
    # archive has unresolved references to those that the final binary
    # must satisfy.
    find_library(_ch_lz4 NAMES lz4
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    find_library(_ch_zstd NAMES zstd
        HINTS /opt/homebrew/lib /usr/local/lib /usr/lib)
    if(_ch_inc AND _ch_lib)
        set(_ch_found TRUE)
        add_library(_clink_clickhouse_external INTERFACE)
        target_include_directories(_clink_clickhouse_external INTERFACE ${_ch_inc})
        target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_lib})
        if(_ch_cityhash)
            target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_cityhash})
        endif()
        if(_ch_lz4)
            target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_lz4})
        endif()
        if(_ch_zstd)
            target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_zstd})
        endif()
        # A client built against the system abseil leaves absl::int128 to the
        # final link.
        find_package(absl CONFIG QUIET)
        if(TARGET absl::int128)
            target_link_libraries(_clink_clickhouse_external INTERFACE absl::int128)
        endif()
        # clickhouse-cpp's client TLS path links against OpenSSL.
        find_package(OpenSSL QUIET)
        if(OpenSSL_FOUND)
            target_link_libraries(_clink_clickhouse_external INTERFACE
                OpenSSL::SSL OpenSSL::Crypto)
        endif()
        _clink_clickhouse_shared_deps(_ch_after)
        target_link_libraries(_clink_clickhouse_external INTERFACE ${_ch_after})
        set(_ch_libs _clink_clickhouse_external)
        set(_ch_include_dir "${_ch_inc}")
    endif()
endif()

if(_ch_found AND _ch_include_dir AND EXISTS "${_ch_include_dir}/clickhouse/version.h")
    file(STRINGS "${_ch_include_dir}/clickhouse/version.h" _ch_version_lines
        REGEX "^#define CLICKHOUSE_CPP_VERSION_(MAJOR|MINOR|PATCH) ")
    set(_ch_version "")
    foreach(_part MAJOR MINOR PATCH)
        foreach(_l IN LISTS _ch_version_lines)
            if(_l MATCHES "CLICKHOUSE_CPP_VERSION_${_part} ([0-9]+)")
                string(APPEND _ch_version "${CMAKE_MATCH_1}.")
            endif()
        endforeach()
    endforeach()
    string(REGEX REPLACE "\\.$" "" _ch_version "${_ch_version}")
    file(STRINGS "${CMAKE_SOURCE_DIR}/scripts/versions.env" _ch_pin_line
        REGEX "^CLICKHOUSE_CPP_VERSION=")
    string(REGEX REPLACE "^CLICKHOUSE_CPP_VERSION=" "" _ch_pinned_version "${_ch_pin_line}")
    if(_ch_pinned_dir AND _ch_include_dir STREQUAL "${_ch_pinned_dir}/include")
        message(STATUS "clink::clickhouse - clickhouse-cpp ${_ch_version} (pinned, Release, TLS)")
    elseif(_ch_version STREQUAL _ch_pinned_version)
        message(STATUS "clink::clickhouse - clickhouse-cpp ${_ch_version} from ${_ch_include_dir}")
    else()
        message(STATUS "clink::clickhouse - clickhouse-cpp ${_ch_version} from ${_ch_include_dir}, "
                       "not the pinned ${_ch_pinned_version}: run scripts/build-clickhouse-cpp.sh")
    endif()
endif()

# The chosen client's headers ahead of every other include path in this
# directory: Homebrew's /opt/homebrew/include (which other dependencies put on
# the path) carries its own clickhouse/ headers, and compiling against those
# while linking the pinned library mixes two versions of the client.
if(_ch_found AND _ch_include_dir)
    include_directories(BEFORE "${_ch_include_dir}")
endif()

if(NOT _ch_found)
    if(CLINK_WITH_CLICKHOUSE STREQUAL "ON")
        message(FATAL_ERROR "CLINK_WITH_CLICKHOUSE=ON but clickhouse-cpp not found")
    endif()
    message(STATUS "clink::clickhouse - clickhouse-cpp not found; target not defined")
    return()
endif()

add_library(clink_clickhouse STATIC
    src/clickhouse_sink.cpp
    src/clickhouse_source.cpp
    src/register_factories.cpp
)
add_library(clink::clickhouse ALIAS clink_clickhouse)

target_include_directories(clink_clickhouse PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(clink_clickhouse PUBLIC cxx_std_23)
target_compile_definitions(clink_clickhouse PUBLIC CLINK_HAS_CLICKHOUSE)
target_link_libraries(clink_clickhouse
    PUBLIC  clink::core
    PRIVATE ${_ch_libs}
)
clink_set_warnings(clink_clickhouse)

if(TARGET _clink_clickhouse_external)
    install(TARGETS _clink_clickhouse_external EXPORT ${CLINK_EXPORT_NAME})
    clink_install_impl(
        TARGET clink_clickhouse
        NAME   clickhouse)
else()
    clink_install_impl(
        TARGET   clink_clickhouse
        NAME     clickhouse
        FIND_DEP "find_dependency(clickhouse-cpp CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

message(STATUS "clink::clickhouse - enabled")
