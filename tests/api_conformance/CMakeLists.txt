# Stable public API gates (design record 011).
#
# Two compile-only object libraries, both in ALL so the ordinary test build
# is the check:
#
#   clink_api_headers_check   one translation unit per Stable header (read from
#                             scripts/public-api-surface.txt), each including
#                             only that header - a Stable header must compile
#                             on its own or it is not a usable contract.
#   clink_api_conformance     the frozen enumeration of promised members
#                             (stable_*.cpp here; see README.md for the rule).
#
# Neither produces a runnable test: a failed compile IS the finding.

enable_language(C)

set(_clink_api_manifest "${CMAKE_SOURCE_DIR}/scripts/public-api-surface.txt")
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${_clink_api_manifest}")

# Write `content` to `path` only when it differs, so a reconfigure does not
# touch the timestamp of every generated TU and force a full recompile.
function(_clink_write_if_changed path content)
    if(EXISTS "${path}")
        file(READ "${path}" _current)
        if(_current STREQUAL content)
            return()
        endif()
    endif()
    file(WRITE "${path}" "${content}")
endfunction()

file(STRINGS "${_clink_api_manifest}" _clink_api_lines)
set(_clink_api_section "")
set(_clink_api_stable_core "")   # include/clink/... headers
set(_clink_api_stable_impl "")   # impls/<name>/include/clink/... headers
foreach(_line IN LISTS _clink_api_lines)
    if(_line MATCHES "^\\[(.*)\\]$")
        set(_clink_api_section "${CMAKE_MATCH_1}")
    elseif(_clink_api_section STREQUAL "stable" AND _line MATCHES "^include/(clink/.*)$")
        list(APPEND _clink_api_stable_core "${CMAKE_MATCH_1}")
    elseif(_clink_api_section STREQUAL "stable" AND _line MATCHES "^impls/([^/]+)/include/(clink/.*)$")
        list(APPEND _clink_api_stable_impl "${CMAKE_MATCH_1}|${CMAKE_MATCH_2}")
    endif()
endforeach()
if(NOT _clink_api_stable_core)
    message(FATAL_ERROR
        "api_conformance: no [stable] headers read from ${_clink_api_manifest}; "
        "regenerate it with scripts/gen-public-api-surface.py")
endif()

set(_clink_api_gen_dir "${CMAKE_CURRENT_BINARY_DIR}/headers")
file(MAKE_DIRECTORY "${_clink_api_gen_dir}")
set(_clink_api_check_sources "")
set(_clink_api_check_links clink::core clink::test_support GTest::gtest)

foreach(_hdr IN LISTS _clink_api_stable_core)
    # embed/embedded_engine.hpp needs the SQL frontend (clink::embed); skip it
    # on a -DCLINK_BUILD_SQL=OFF build rather than fail the whole check.
    if(_hdr MATCHES "^clink/embed/embedded_engine\\.hpp$")
        if(NOT TARGET clink::embed)
            continue()
        endif()
        list(APPEND _clink_api_check_links clink::embed)
    endif()
    string(REGEX REPLACE "^clink/" "" _stem "${_hdr}")
    string(REGEX REPLACE "[/.]" "_" _stem "${_stem}")
    if(_hdr MATCHES "\\.h$")
        # The C ABI header must compile as C99 as well as C++.
        set(_c_tu "${_clink_api_gen_dir}/${_stem}_as_c.c")
        _clink_write_if_changed("${_c_tu}" "#include \"${_hdr}\"\n")
        list(APPEND _clink_api_check_sources "${_c_tu}")
    endif()
    set(_tu "${_clink_api_gen_dir}/${_stem}.cpp")
    _clink_write_if_changed("${_tu}" "#include \"${_hdr}\"\n")
    list(APPEND _clink_api_check_sources "${_tu}")
endforeach()

# Per-connector Stable headers (builders, install entry points) compile only
# when that impl was built: the header needs the impl's include directory and
# often its SDK headers.
foreach(_entry IN LISTS _clink_api_stable_impl)
    string(REPLACE "|" ";" _parts "${_entry}")
    list(GET _parts 0 _impl)
    list(GET _parts 1 _hdr)
    if(NOT TARGET clink::${_impl})
        continue()
    endif()
    list(APPEND _clink_api_check_links clink::${_impl})
    string(REGEX REPLACE "^clink/" "" _stem "${_hdr}")
    string(REGEX REPLACE "[/.]" "_" _stem "${_stem}")
    set(_tu "${_clink_api_gen_dir}/impl_${_impl}_${_stem}.cpp")
    _clink_write_if_changed("${_tu}" "#include \"${_hdr}\"\n")
    list(APPEND _clink_api_check_sources "${_tu}")
endforeach()
list(REMOVE_DUPLICATES _clink_api_check_links)

add_library(clink_api_headers_check OBJECT ${_clink_api_check_sources})
target_link_libraries(clink_api_headers_check PRIVATE ${_clink_api_check_links})
clink_set_warnings(clink_api_headers_check)
list(LENGTH _clink_api_check_sources _clink_api_check_count)
message(STATUS "api_conformance: ${_clink_api_check_count} Stable-header standalone TUs")

# --- the frozen member enumeration -------------------------------------------

add_library(clink_api_conformance OBJECT
    stable_core_types.cpp
    stable_operator_hooks.cpp
    stable_operator_library.cpp
    stable_dag_and_runtime.cpp
    stable_state_and_types.cpp
    stable_connectors.cpp
    stable_pipeline_api.cpp
    stable_job_registration.cpp
    stable_plugin_registration.cpp
    stable_testing_framework.cpp
)
target_link_libraries(clink_api_conformance PRIVATE clink::core clink::test_support GTest::gtest)
clink_set_warnings(clink_api_conformance)

if(TARGET clink::embed)
    add_library(clink_api_conformance_embed OBJECT stable_embedded_engine.cpp)
    target_link_libraries(clink_api_conformance_embed PRIVATE clink::embed)
    clink_set_warnings(clink_api_conformance_embed)
endif()
