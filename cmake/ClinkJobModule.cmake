# clink_add_job_module: the one supported way to build a clink job or plugin
# module, in-tree and out-of-tree.
#
#   clink_add_job_module(<target>
#       SOURCES <src>...
#       [OUTPUT_NAME <name>]      # default: <target>
#       [SUFFIX <suffix>]         # e.g. ".so" to override the platform default
#       [LINK <lib>...]           # extra libraries, linked after clink::core
#       [DEFINES <def>...]        # PRIVATE compile definitions
#       [HIDDEN_VISIBILITY])      # experimental - see below
#
# What it standardises, so seventeen hand-rolled copies cannot drift:
#   - MODULE library linking clink::core PRIVATE (the module resolves the
#     engine's inline templates against its own statically linked copy; on
#     the cluster side dlopen resolves the handshake symbols only)
#   - PREFIX "" (the loader and the docs refer to <name>.so, not lib<name>.so)
#   - split-debug + strip on ELF (see ClinkPluginDebugSplit.cmake: ~20x
#     smaller submits, symbolisation kept via .gnu_debuglink)
#   - the repo's warning set when built in-tree (no-op for consumers)
#
# HIDDEN_VISIBILITY is EXPERIMENTAL and off by default, deliberately: typed
# exceptions cross the dlopen boundary today (the worker catches
# clink::state::CheckpointIntegrityError and plugin::detail::TransportOnlyFailure
# thrown from plugin-compiled code), and hiding typeinfo puts cross-module
# catch-by-type at the mercy of the standard library's RTTI comparison
# strategy. The handshake symbols themselves stay exported either way via
# CLINK_PLUGIN_EXPORT on the declaration macros. Do not turn this on for a
# module whose operators throw typed clink exceptions.

include_guard(GLOBAL)

include("${CMAKE_CURRENT_LIST_DIR}/ClinkPluginDebugSplit.cmake")

function(clink_add_job_module target)
    cmake_parse_arguments(PARSE_ARGV 1 ARG
        "HIDDEN_VISIBILITY" "OUTPUT_NAME;SUFFIX" "SOURCES;LINK;DEFINES")
    if(NOT ARG_SOURCES)
        message(FATAL_ERROR "clink_add_job_module(${target}): SOURCES is required")
    endif()
    if(ARG_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR
            "clink_add_job_module(${target}): unknown arguments '${ARG_UNPARSED_ARGUMENTS}'")
    endif()
    if(NOT ARG_OUTPUT_NAME)
        set(ARG_OUTPUT_NAME "${target}")
    endif()

    add_library(${target} MODULE ${ARG_SOURCES})
    target_link_libraries(${target} PRIVATE clink::core ${ARG_LINK})
    if(ARG_DEFINES)
        target_compile_definitions(${target} PRIVATE ${ARG_DEFINES})
    endif()
    set_target_properties(${target} PROPERTIES
        PREFIX ""
        OUTPUT_NAME "${ARG_OUTPUT_NAME}"
    )
    if(ARG_SUFFIX)
        set_target_properties(${target} PROPERTIES SUFFIX "${ARG_SUFFIX}")
    endif()
    if(ARG_HIDDEN_VISIBILITY)
        set_target_properties(${target} PROPERTIES
            CXX_VISIBILITY_PRESET hidden
            VISIBILITY_INLINES_HIDDEN ON
        )
    endif()
    # In-tree builds apply the repo warning set; the function does not ship
    # with the installed package, so consumers get their own flags.
    if(COMMAND clink_set_warnings)
        clink_set_warnings(${target})
    endif()
    clink_split_debug_and_strip_module(${target})
endfunction()
