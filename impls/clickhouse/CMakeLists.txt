# clink::clickhouse - the ClickHouse source, the text insert sink (TSV or
# JSONEachRow rows sent as one INSERT statement) and the native-block insert
# sink (src/native/), which builds only against clickhouse-cpp 2.6.2 or later.

# The SSL factory probe's cached answer (below) belongs to this configure only:
# drop it before any path that returns early or skips the probe, so a reconfigure
# that builds no native sink cannot leave an earlier configure's "linked" behind.
unset(CLINK_CLICKHOUSE_SSL_FACTORY_LINKS CACHE)

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
    if(_ch_bundled)
        list(APPEND _deps "${_ch_pinned_int128}")
    elseif(TARGET absl::int128)
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
set(_ch_bundled FALSE)
if(_clink_deps_prefix AND EXISTS "${_clink_deps_prefix}/clickhouse-cpp/include/clickhouse/client.h")
    set(_ch_pinned_dir "${_clink_deps_prefix}/clickhouse-cpp")
    # The script's bundled-deps mode (the self-contained Linux libclink) stamps its
    # install; the stamp, not whichever files happen to be there, says which layout
    # this is. A pending marker means an install that stopped part way, which is
    # neither layout.
    file(GLOB _ch_pending "${_ch_pinned_dir}/.clink-clickhouse-cpp-pending*")
    if(_ch_pending)
        message(FATAL_ERROR "clink::clickhouse - the client install in ${_ch_pinned_dir} "
                            "did not finish (${_ch_pending}): rerun "
                            "scripts/build-clickhouse-cpp.sh")
    endif()
    file(GLOB _ch_bundled_stamps "${_ch_pinned_dir}/.clink-clickhouse-cpp-*-bundled")
    if(_ch_bundled_stamps)
        set(_ch_bundled TRUE)
    endif()
endif()
if(_ch_pinned_dir AND _ch_bundled)
    # Bundled deps: the client carries its own abseil int128 and cityhash and links
    # no system library. Its lz4 and zstd were deleted after install, so its
    # references resolve against a copy the rest of the link already carries. The
    # link line offers zstd from CLINK_ZSTD_LIBRARY when clink_core found one, and
    # lz4 (and zstd otherwise) from Arrow's bundled-dependencies archive, so a
    # definition always exists; which archive supplies it is whichever comes first
    # on the final link line. In the Linux wheel that is the static librdkafka,
    # ahead of Arrow's archive, as the wheel's link map shows. Every library is a
    # raw path. No
    # absl::int128 target is created here: a later find_package(absl) (protobuf,
    # gRPC) would fail on the name already being taken.
    set(_ch_pinned_int128 "${_ch_pinned_dir}/lib/libabsl_int128.a")
    foreach(_f "${_ch_pinned_int128}" "${_ch_pinned_dir}/include/absl/numeric/int128.h")
        if(NOT EXISTS "${_f}")
            message(FATAL_ERROR "clink::clickhouse - the bundled-deps client in "
                                "${_ch_pinned_dir} is missing ${_f}: rerun "
                                "scripts/build-clickhouse-cpp.sh with CLICKHOUSE_CPP_BUNDLED_DEPS=1")
        endif()
    endforeach()
    find_library(_ch_pinned_lib NAMES clickhouse-cpp-lib
        PATHS "${_ch_pinned_dir}/lib" NO_DEFAULT_PATH)
    find_library(_ch_pinned_cityhash NAMES cityhash
        PATHS "${_ch_pinned_dir}/lib" NO_DEFAULT_PATH)
    set(_ch_arrow_bundled)
    if(TARGET Arrow::arrow_bundled_dependencies)
        get_target_property(_ch_arrow_cfgs Arrow::arrow_bundled_dependencies
            IMPORTED_CONFIGURATIONS)
        foreach(_cfg IN LISTS _ch_arrow_cfgs)
            get_target_property(_ch_loc Arrow::arrow_bundled_dependencies
                IMPORTED_LOCATION_${_cfg})
            if(_ch_loc AND EXISTS "${_ch_loc}" AND NOT _ch_arrow_bundled)
                set(_ch_arrow_bundled "${_ch_loc}")
            endif()
        endforeach()
    endif()
    if(NOT _ch_arrow_bundled)
        set(_ch_loc "${_clink_deps_prefix}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}")
        if(EXISTS "${_ch_loc}")
            set(_ch_arrow_bundled "${_ch_loc}")
        endif()
    endif()
    if(NOT _ch_arrow_bundled)
        message(FATAL_ERROR "clink::clickhouse - the bundled-deps client needs Arrow's "
                            "bundled-dependencies archive on its link line as the lz4 "
                            "provider of last resort, and this Arrow has none "
                            "(a static Arrow built with ARROW_DEPENDENCY_SOURCE=BUNDLED)")
    endif()
    set(_ch_pinned_lz4 "${_ch_arrow_bundled}")
    if(CLINK_ZSTD_LIBRARY AND EXISTS "${CLINK_ZSTD_LIBRARY}")
        set(_ch_pinned_zstd "${CLINK_ZSTD_LIBRARY}")
    else()
        set(_ch_pinned_zstd "${_ch_arrow_bundled}")
    endif()
    find_package(OpenSSL QUIET)
    if(_ch_pinned_lib AND _ch_pinned_cityhash AND OpenSSL_FOUND)
        add_library(_clink_clickhouse_external INTERFACE)
        target_include_directories(_clink_clickhouse_external INTERFACE
            "${_ch_pinned_dir}/include")
        _clink_clickhouse_shared_deps(_ch_after)
        target_link_libraries(_clink_clickhouse_external INTERFACE
            ${_ch_pinned_lib} ${_ch_pinned_cityhash} ${_ch_pinned_lz4} ${_ch_pinned_zstd}
            ${_ch_pinned_int128} OpenSSL::SSL OpenSSL::Crypto ${_ch_after})
        set(_ch_libs _clink_clickhouse_external)
        set(_ch_include_dir "${_ch_pinned_dir}/include")
        set(_ch_check_libs ${_ch_pinned_lib} ${_ch_pinned_cityhash} ${_ch_pinned_lz4}
            ${_ch_pinned_zstd} ${_ch_pinned_int128} OpenSSL::SSL OpenSSL::Crypto)
        set(_ch_found TRUE)
    else()
        message(FATAL_ERROR "clink::clickhouse - the bundled-deps client in ${_ch_pinned_dir} "
                            "is missing its archives or no OpenSSL was found (the wheel "
                            "passes OPENSSL_ROOT_DIR=<prefix>/openssl-static)")
    endif()
elseif(_ch_pinned_dir)
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
        set(_ch_check_libs ${_ch_pinned_lib} ${_ch_pinned_cityhash} ${_ch_pinned_lz4}
            ${_ch_pinned_zstd} absl::int128 OpenSSL::SSL OpenSSL::Crypto)
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
    set(_ch_check_libs clickhouse-cpp-lib)
    # The package's target carries the include directory; version.h is read
    # from it below like on the other paths.
    get_target_property(_ch_config_inc clickhouse-cpp-lib INTERFACE_INCLUDE_DIRECTORIES)
    if(_ch_config_inc)
        list(GET _ch_config_inc 0 _ch_include_dir)
    endif()
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
        set(_ch_check_libs ${_ch_lib} ${_ch_cityhash} ${_ch_lz4} ${_ch_zstd})
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
            list(APPEND _ch_check_libs absl::int128)
        endif()
        # clickhouse-cpp's client TLS path links against OpenSSL.
        find_package(OpenSSL QUIET)
        if(OpenSSL_FOUND)
            target_link_libraries(_clink_clickhouse_external INTERFACE
                OpenSSL::SSL OpenSSL::Crypto)
            list(APPEND _ch_check_libs OpenSSL::SSL OpenSSL::Crypto)
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
        if(_ch_bundled)
            message(STATUS "clink::clickhouse - clickhouse-cpp ${_ch_version} (pinned, Release, TLS, bundled deps)")
        else()
            message(STATUS "clink::clickhouse - clickhouse-cpp ${_ch_version} (pinned, Release, TLS)")
        endif()
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

# The native sink needs clickhouse-cpp 2.6.2 or later: it relies on that
# release's client behaviour (rethrown EndInsert errors, the socket factory
# seam) and on its layout for the vendored TLS header. A version that cannot be
# read counts as older. Below it, only the registration builds, and its factory
# refuses with clickhouse.native_unavailable.
set(_ch_native FALSE)
if(_ch_version AND NOT _ch_version VERSION_LESS "2.6.2")
    set(_ch_native TRUE)
endif()

# TLS on the native sink needs the client's SSLSocketFactory, which only a client
# built with OpenSSL carries. Finding OpenSSL on this machine is not evidence of
# that, so compile and link a program that constructs one against the chosen
# client. Re-run on every configure: the answer belongs to the client found now.
set(_ch_native_tls FALSE)
if(_ch_native)
    include(CheckCXXSourceCompiles)
    set(_ch_saved_includes "${CMAKE_REQUIRED_INCLUDES}")
    set(_ch_saved_libraries "${CMAKE_REQUIRED_LIBRARIES}")
    set(_ch_saved_flags "${CMAKE_REQUIRED_FLAGS}")
    set(CMAKE_REQUIRED_INCLUDES "${_ch_include_dir}")
    set(CMAKE_REQUIRED_LIBRARIES ${_ch_check_libs})
    set(CMAKE_REQUIRED_FLAGS "-std=c++20")
    check_cxx_source_compiles("
#include \"${CMAKE_CURRENT_SOURCE_DIR}/src/native/vendor/clickhouse/base/sslsocket.h\"
#include <clickhouse/client.h>
int main() {
    clickhouse::ClientOptions opts;
    opts.SetSSLOptions(clickhouse::ClientOptions::SSLOptions{});
    clickhouse::SSLSocketFactory factory(opts);
    return 0;
}" CLINK_CLICKHOUSE_SSL_FACTORY_LINKS)
    set(CMAKE_REQUIRED_INCLUDES "${_ch_saved_includes}")
    set(CMAKE_REQUIRED_LIBRARIES "${_ch_saved_libraries}")
    set(CMAKE_REQUIRED_FLAGS "${_ch_saved_flags}")
    if(CLINK_CLICKHOUSE_SSL_FACTORY_LINKS)
        set(_ch_native_tls TRUE)
    endif()
endif()

set(_ch_sources
    src/clickhouse_sink.cpp
    src/clickhouse_source.cpp
    src/register_factories.cpp
    # Quoting and tokens, shared with the HTTP sink; no client include.
    src/native/sql_text.cpp
    src/native/errors.cpp
    src/native/register_native.cpp
    # The typed helper's non-template half (clink/clickhouse/native_sink.hpp),
    # which refuses clickhouse.native_unavailable without the native sink.
    src/native/typed_entry.cpp
)
if(_ch_native)
    list(APPEND _ch_sources
        src/native/arrow_to_block.cpp
        src/native/clickhouse_transport.cpp
        src/native/column_plan.cpp
        src/native/default_ca.cpp
        src/native/error_class.cpp
        src/native/insert_transport.cpp
        src/native/intake.cpp
        src/native/native_sink.cpp
        src/native/native_sink_core.cpp
        src/native/retry.cpp
        src/native/row_arrow.cpp
        src/native/sink_options.cpp
        src/native/socket_factory.cpp
        src/native/statements.cpp
        src/native/target_table.cpp
        src/native/types.cpp
        src/native/writer.cpp
    )
endif()

add_library(clink_clickhouse STATIC ${_ch_sources})
add_library(clink::clickhouse ALIAS clink_clickhouse)
target_include_directories(clink_clickhouse PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
if(_ch_native)
    target_compile_definitions(clink_clickhouse PRIVATE CLINK_CLICKHOUSE_NATIVE=1)
endif()
if(_ch_native_tls)
    target_compile_definitions(clink_clickhouse PRIVATE CLINK_CLICKHOUSE_NATIVE_TLS=1)
endif()

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
    # The exported client target names imported targets on its interface, which
    # a downstream find_package(clink COMPONENTS clickhouse) must resolve before
    # it can link clink::clickhouse. Only the ones it really links: the bundled
    # client carries abseil by path, and a client found without OpenSSL links
    # none.
    get_target_property(_ch_iface _clink_clickhouse_external INTERFACE_LINK_LIBRARIES)
    set(_ch_find_deps)
    if("OpenSSL::SSL" IN_LIST _ch_iface OR "OpenSSL::Crypto" IN_LIST _ch_iface)
        list(APPEND _ch_find_deps "find_dependency(OpenSSL)")
    endif()
    if("absl::int128" IN_LIST _ch_iface)
        list(APPEND _ch_find_deps "find_dependency(absl CONFIG)")
    endif()
    clink_install_impl(
        TARGET   clink_clickhouse
        NAME     clickhouse
        FIND_DEP ${_ch_find_deps})
else()
    clink_install_impl(
        TARGET   clink_clickhouse
        NAME     clickhouse
        FIND_DEP "find_dependency(clickhouse-cpp CONFIG)")
endif()

if(CLINK_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(_ch_native AND _ch_native_tls)
    message(STATUS "clink::clickhouse - enabled (native sink with TLS)")
elseif(_ch_native)
    message(STATUS "clink::clickhouse - enabled (native sink without TLS: the client has no "
                   "SSLSocketFactory)")
elseif(_ch_version)
    message(STATUS "clink::clickhouse - enabled (native sink unavailable: clickhouse-cpp "
                   "${_ch_version} is older than 2.6.2)")
else()
    message(STATUS "clink::clickhouse - enabled (native sink unavailable: the clickhouse-cpp "
                   "version could not be read)")
endif()
