# Fails when the given binary DEFINES arrow::default_memory_pool - the
# signature of a second, statically linked Arrow runtime inside a binary
# that also loads libarrow as a shared library.
#
# Two Arrow instances in one process each carry their own memory pool
# (bundled mimalloc included), and which instance a call binds to is a
# link-order accident. A buffer allocated by one instance's pool and
# Reallocate'd through the other's loses its contents WITHOUT crashing:
# found as F89 (state snapshots whose op_id column arrived zeroed, so
# restores came up silently empty) and F87 (outbound Arrow IPC frames with
# zeroed heads under Debug). The static Arrow arrived via iceberg-cpp's
# exported config preferring Arrow::arrow_static whenever the consumer
# defines it; impls/iceberg/CMakeLists.txt rewrites that interface to the
# shared Arrow, and this check pins the property so it cannot silently
# regress through some future dependency.
#
# Usage: cmake -DBINARY=<path> -P CheckSingleArrowRuntime.cmake

if(NOT DEFINED BINARY OR NOT EXISTS "${BINARY}")
    message(FATAL_ERROR "single-arrow check: BINARY not set or missing: '${BINARY}'")
endif()

find_program(NM_TOOL NAMES nm llvm-nm)
if(NOT NM_TOOL)
    # A gate whose tool is missing must fail loudly, not pass vacuously.
    message(FATAL_ERROR "single-arrow check: no 'nm' found on PATH; cannot verify ${BINARY}")
endif()

execute_process(
    COMMAND "${NM_TOOL}" "${BINARY}"
    OUTPUT_VARIABLE _nm_out
    RESULT_VARIABLE _nm_rc
    ERROR_VARIABLE _nm_err)
if(NOT _nm_rc EQUAL 0)
    message(FATAL_ERROR "single-arrow check: nm failed on ${BINARY}: ${_nm_err}")
endif()

# Defined (T/t) copy of arrow::default_memory_pool() - mangled
# _ZN5arrow19default_memory_poolEv (macOS adds a leading underscore).
string(REGEX MATCH "[ \t][Tt][ \t]+_?_ZN5arrow19default_memory_poolEv" _defined "${_nm_out}")
if(_defined)
    message(FATAL_ERROR
        "single-arrow check: ${BINARY} DEFINES arrow::default_memory_pool - a second static "
        "Arrow runtime is linked into a binary that also loads libarrow dynamically. Two Arrow "
        "memory pools in one process corrupt buffer handoffs (F87/F89). Find which dependency "
        "dragged libarrow.a in (see impls/iceberg/CMakeLists.txt for the known offender and "
        "the interface rewrite that keeps it out).")
endif()

message(STATUS "single-arrow check: ${BINARY} links exactly one Arrow runtime")
