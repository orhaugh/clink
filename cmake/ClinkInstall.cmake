# ClinkInstall.cmake - install-rule helpers shared by the top-level and
# per-impl CMakeLists.
#
# Usage from impls/<x>/CMakeLists.txt, after the clink::<x> target is
# defined:
#
#   clink_install_impl(
#       TARGET   clink_kafka
#       NAME     kafka
#       FIND_DEP "find_dependency(RdKafka CONFIG)")
#
# - TARGET    : the underlying static-library target name (clink_<x>).
# - NAME      : short impl name registered in clink_AVAILABLE_IMPLS.
# - FIND_DEP  : verbatim find_dependency() line(s) to emit in
#               clinkConfig.cmake so a downstream find_package(clink) call
#               can reach this impl's transitive deps. Omit if the impl
#               vendors its dep (e.g. rocksdb is FetchContent'd and
#               statically linked, so no find_dependency is needed).
#
# After all impls have called clink_install_impl, the top-level CMakeLists
# calls clink_install_finalize() which installs the EXPORT set and writes
# clinkConfig.cmake + clinkConfigVersion.cmake.

include_guard(GLOBAL)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CLINK_EXPORT_NAME       "clinkTargets")
set(CLINK_INSTALL_CMAKE_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/clink")

# Global property accumulator - concatenated into clinkConfig.cmake.
define_property(GLOBAL PROPERTY CLINK_FIND_DEPS_BODY
    BRIEF_DOCS "Aggregated find_dependency() lines for clinkConfig.cmake"
    FULL_DOCS  "Each impl that needs an externally-resolved transitive dep "
               "appends its find_dependency(...) call here. The top-level "
               "install step substitutes this into clinkConfig.cmake.in.")

define_property(GLOBAL PROPERTY CLINK_AVAILABLE_IMPLS
    BRIEF_DOCS "Short names of every clink impl that was built+installed"
    FULL_DOCS  "Populated by clink_install_impl; surfaced in clinkConfig "
               "as clink_AVAILABLE_IMPLS for downstream gating.")

function(clink_install_impl)
    set(_opts)
    set(_one_value TARGET NAME)
    set(_multi_value FIND_DEP)
    cmake_parse_arguments(CIA "${_opts}" "${_one_value}" "${_multi_value}" ${ARGN})

    if(NOT CIA_TARGET OR NOT CIA_NAME)
        message(FATAL_ERROR "clink_install_impl: TARGET and NAME are required")
    endif()
    if(NOT TARGET ${CIA_TARGET})
        message(FATAL_ERROR "clink_install_impl: target '${CIA_TARGET}' does not exist")
    endif()

    # Make the imported target show up as `clink::<NAME>` (matching the
    # in-tree ALIAS) instead of the default `clink::<TARGET>`.
    set_target_properties(${CIA_TARGET} PROPERTIES EXPORT_NAME ${CIA_NAME})

    install(TARGETS ${CIA_TARGET}
        EXPORT ${CLINK_EXPORT_NAME}
        ARCHIVE     DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY     DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME     DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/include")
        install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include/"
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
            FILES_MATCHING
                PATTERN "*.hpp"
                PATTERN "*.h"
                PATTERN "*.inl")
    endif()

    set_property(GLOBAL APPEND PROPERTY CLINK_AVAILABLE_IMPLS "${CIA_NAME}")

    if(CIA_FIND_DEP)
        foreach(_line IN LISTS CIA_FIND_DEP)
            set_property(GLOBAL APPEND_STRING PROPERTY CLINK_FIND_DEPS_BODY
                "${_line}\n")
        endforeach()
    endif()
endfunction()

# Called once from the top-level CMakeLists after all impls have been
# added. Writes clinkTargets.cmake, clinkConfig.cmake, and
# clinkConfigVersion.cmake, all under <prefix>/lib/cmake/clink/.
function(clink_install_finalize)
    install(EXPORT ${CLINK_EXPORT_NAME}
        FILE        clinkTargets.cmake
        NAMESPACE   clink::
        DESTINATION ${CLINK_INSTALL_CMAKE_DIR})

    get_property(_deps_body GLOBAL PROPERTY CLINK_FIND_DEPS_BODY)
    if(NOT _deps_body)
        set(_deps_body "# (no optional impls were built)")
    endif()
    set(CLINK_FIND_DEPS_BODY "${_deps_body}")

    get_property(_impls GLOBAL PROPERTY CLINK_AVAILABLE_IMPLS)
    list(JOIN _impls ";" CLINK_AVAILABLE_IMPLS)

    configure_package_config_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/clinkConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake/clinkConfig.cmake"
        INSTALL_DESTINATION ${CLINK_INSTALL_CMAKE_DIR})

    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/cmake/clinkConfigVersion.cmake"
        VERSION       ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion)

    install(FILES
            "${CMAKE_CURRENT_BINARY_DIR}/cmake/clinkConfig.cmake"
            "${CMAKE_CURRENT_BINARY_DIR}/cmake/clinkConfigVersion.cmake"
        DESTINATION ${CLINK_INSTALL_CMAKE_DIR})
endfunction()
