# Split-debug + strip for job-plugin modules (item 30).
#
# A job plugin links the engine statically, and on ELF the resulting module
# was ~104 MB - of which 2.6 MB was code and ~102 MB was DWARF that has no
# business travelling on a submit: every submission shipped it over the
# control connection, the coordinator cached it, and every worker received
# it again on Deploy. Stripping with a debuglink cuts the shipped module to
# ~5 MB (measured on heavy_pipeline_job.so, 2026-08-10) while keeping crash
# symbolisation: the DWARF moves WHOLE into <module>.so.debug beside the
# artefact, and the module carries a .gnu_debuglink naming it, which gdb and
# llvm-symbolizer resolve automatically from the same directory.
#
# What survives stripping, by contract rather than luck: the dynamic symbol
# table - the CLINK_REGISTER_JOB entry points, the restore-compat export the
# pre-deploy gate calls, and the ABI-hash string all live there or in
# .rodata, and --strip-unneeded cannot remove them. What is lost: static
# function names in IN-PROCESS backtraces (backtrace_symbols falls back to
# the nearest dynamic symbol); recover them offline against the .debug file.
#
# ELF only. macOS modules never embed the DWARF (it stays in the .o debug
# map / dSYM), which is exactly why the same source builds ~8 MB there -
# stripping on macOS would only remove local symbols and degrade in-place
# debugging on the platform this repo develops on.
#
# Consumers building their own job plugins against an installed clink get
# the same win from the same two objcopy commands; the recipe is documented
# in docs/internals/distributed-runtime.md.

# Apply the split to one MODULE target. Idempotent via a target property, so
# clink_add_job_module (which calls this per target) and the directory sweep
# below can both run over a target without stacking a second POST_BUILD strip
# - a second --only-keep-debug on an already-stripped module would replace
# the .debug file with an empty one.
function(clink_split_debug_and_strip_module target)
    if(NOT (UNIX AND NOT APPLE))
        return()
    endif()
    get_target_property(_applied ${target} CLINK_DEBUG_SPLIT_APPLIED)
    if(_applied)
        return()
    endif()
    if(NOT CMAKE_OBJCOPY)
        find_program(CMAKE_OBJCOPY NAMES objcopy llvm-objcopy)
    endif()
    if(NOT CMAKE_OBJCOPY)
        # The build still works; the modules just ship fat. Loud (but once),
        # because a toolchain without objcopy silently reverts the 20x
        # submit-size win.
        get_property(_warned GLOBAL PROPERTY CLINK_OBJCOPY_WARNED)
        if(NOT _warned)
            set_property(GLOBAL PROPERTY CLINK_OBJCOPY_WARNED TRUE)
            message(WARNING "clink: no objcopy found - job-plugin modules will not be "
                            "split-debug stripped (item 30)")
        endif()
        return()
    endif()
    set_target_properties(${target} PROPERTIES CLINK_DEBUG_SPLIT_APPLIED TRUE)
    add_custom_command(
        TARGET ${target}
        POST_BUILD
        COMMAND "${CMAKE_OBJCOPY}" --only-keep-debug "$<TARGET_FILE:${target}>"
                "$<TARGET_FILE:${target}>.debug"
        COMMAND "${CMAKE_OBJCOPY}" --strip-unneeded
                "--add-gnu-debuglink=$<TARGET_FILE:${target}>.debug" "$<TARGET_FILE:${target}>"
        COMMENT "split-debug+strip ${target} (item 30)"
        VERBATIM)
endfunction()

# Apply the split to every MODULE library defined in the CALLING directory.
# Directory-scoped so a new job plugin added beside the existing ones is
# covered by construction instead of by remembering a per-target call.
function(clink_split_debug_and_strip_modules_in_dir)
    get_property(_tgts DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach(_t IN LISTS _tgts)
        get_target_property(_type ${_t} TYPE)
        if(_type STREQUAL "MODULE_LIBRARY")
            clink_split_debug_and_strip_module(${_t})
        endif()
    endforeach()
endfunction()
