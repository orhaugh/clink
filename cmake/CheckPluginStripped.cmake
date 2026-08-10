# Fails when a job-plugin module still embeds DWARF (item 30).
#
# The split-debug rule in ClinkPluginDebugSplit.cmake moves the ~102 MB of
# .debug_* sections out of every shipped module; this check pins the
# property so a toolchain change, a new generator, or a dropped POST_BUILD
# rule cannot quietly bring the 104 MB submit back. Two assertions, both
# cheap: the module carries no .debug_info section, and it sits under a
# hard size budget with generous headroom over the ~5 MB measured (code
# growth is legitimate; embedded DWARF jumps the size 20x, far past any
# honest growth).
#
# Usage: cmake -DBINARY=<module.so> -P CheckPluginStripped.cmake

if(NOT DEFINED BINARY OR NOT EXISTS "${BINARY}")
    message(FATAL_ERROR "plugin-stripped check: BINARY not set or missing: '${BINARY}'")
endif()

set(_budget_bytes 26214400)  # 25 MiB: ~5x the measured stripped module

find_program(READELF_TOOL NAMES readelf llvm-readelf)
if(NOT READELF_TOOL)
    # A gate whose tool is missing must fail loudly, not pass vacuously.
    message(FATAL_ERROR "plugin-stripped check: no 'readelf' found on PATH")
endif()

execute_process(
    COMMAND "${READELF_TOOL}" -S --wide "${BINARY}"
    OUTPUT_VARIABLE _sections
    RESULT_VARIABLE _rc
    ERROR_VARIABLE _err)
if(NOT _rc EQUAL 0)
    message(FATAL_ERROR "plugin-stripped check: readelf failed on ${BINARY}: ${_err}")
endif()

string(FIND "${_sections}" ".debug_info" _dwarf_at)
if(NOT _dwarf_at EQUAL -1)
    message(FATAL_ERROR
        "plugin-stripped check: ${BINARY} still embeds DWARF (.debug_info present). "
        "The split-debug POST_BUILD rule (cmake/ClinkPluginDebugSplit.cmake) did not "
        "run - every submit of this module ships ~20x the bytes it needs to (item 30).")
endif()

file(SIZE "${BINARY}" _size)
if(_size GREATER ${_budget_bytes})
    message(FATAL_ERROR
        "plugin-stripped check: ${BINARY} is ${_size} bytes, over the ${_budget_bytes}-byte "
        "budget. Measured stripped size is ~5 MB; being 5x past it means something big "
        "and unintended is riding along.")
endif()

message(STATUS "plugin-stripped check: ${BINARY} carries no DWARF and is ${_size} bytes")
