# Fuzzing: discovery targets and the seed generator.
#
# Two halves, only one of which lives here.
#
#   DISCOVERY (this file, opt-in via CLINK_BUILD_FUZZERS) needs a compiler
#   that ships libFuzzer, and runs for as long as you give it. It cannot be
#   a required CI check - an unbounded search is not a gate.
#
#   REGRESSION (tests/test_fuzz_corpus.cpp, always built) replays every
#   committed corpus input through the same functions under plain gtest. No
#   fuzzing engine, milliseconds, gates on every platform.
#
# The seed generator is built unconditionally, because the regression half
# needs the seeds too.

# ---------------------------------------------------------------------------
# Seed generator - always available.
# ---------------------------------------------------------------------------
add_executable(clink_fuzz_seeds generate_seeds.cpp)
target_link_libraries(clink_fuzz_seeds PRIVATE clink_core)
target_include_directories(clink_fuzz_seeds PRIVATE ${CMAKE_SOURCE_DIR})

# Generate the seed corpus at BUILD time rather than asking a human to
# remember. The regression test asserts the corpus is non-empty, so a
# generator that silently stopped running fails a test instead of quietly
# shrinking what the fuzzer explores.
set(CLINK_FUZZ_CORPUS_DIR "${CMAKE_SOURCE_DIR}/fuzz/corpus")
add_custom_command(
    TARGET clink_fuzz_seeds POST_BUILD
    COMMAND clink_fuzz_seeds "${CLINK_FUZZ_CORPUS_DIR}"
    COMMENT "clink: writing fuzz seed corpus to fuzz/corpus"
    VERBATIM)

# ---------------------------------------------------------------------------
# Discovery targets - opt-in, and REFUSED rather than skipped when the
# toolchain cannot build them.
# ---------------------------------------------------------------------------
if(NOT CLINK_BUILD_FUZZERS)
    return()
endif()

# Probe for libFuzzer rather than inferring it from the compiler id. Apple
# clang reports as Clang and does NOT ship libFuzzer, and a Debian clang
# without libclang-rt-<n>-dev fails at link with a missing
# libclang_rt.fuzzer.a - both would otherwise look like "Clang, fine".
include(CheckCXXSourceCompiles)
set(CMAKE_REQUIRED_FLAGS "-fsanitize=fuzzer")
check_cxx_source_compiles("
#include <cstddef>
#include <cstdint>
extern \"C\" int LLVMFuzzerTestOneInput(const uint8_t*, size_t) { return 0; }
int main() { return 0; }
" CLINK_HAVE_LIBFUZZER)
unset(CMAKE_REQUIRED_FLAGS)

if(NOT CLINK_HAVE_LIBFUZZER)
    # A hard error, not a warning-and-skip. Someone who asked for fuzzers
    # and silently got none is worse off than someone told why: they would
    # believe the fuzzing gate exists.
    message(FATAL_ERROR
        "CLINK_BUILD_FUZZERS=ON but this compiler cannot link libFuzzer.\n"
        "  compiler: ${CMAKE_CXX_COMPILER} (${CMAKE_CXX_COMPILER_ID} "
        "${CMAKE_CXX_COMPILER_VERSION})\n"
        "  Apple clang does not ship libFuzzer; use Homebrew LLVM:\n"
        "    cmake -S . -B build-fuzz -DCLINK_BUILD_FUZZERS=ON \\\n"
        "      -DCMAKE_CXX_COMPILER=$(brew --prefix llvm)/bin/clang++\n"
        "  On Debian/Ubuntu install the matching runtime, e.g.\n"
        "    apt-get install libclang-rt-19-dev")
endif()

# Each target is its own binary: libFuzzer owns main(), and one process
# cannot host two entry points.
set(CLINK_FUZZ_TARGETS
    cluster_frame
    checkpoint_meta
    state_version_map)

# The fault-schedule parser only exists when fault injection is compiled
# in. Listing the target regardless would fail to link rather than simply
# not being offered.
if(CLINK_FAULT_INJECTION_ACTIVE)
    list(APPEND CLINK_FUZZ_TARGETS fault_spec)
endif()

if(CLINK_BUILD_SQL)
    list(APPEND CLINK_FUZZ_TARGETS sql_parse)
endif()

foreach(target ${CLINK_FUZZ_TARGETS})
    add_executable(clink_fuzz_${target} fuzz_${target}.cpp)
    target_include_directories(clink_fuzz_${target} PRIVATE ${CMAKE_SOURCE_DIR})
    # address+undefined alongside fuzzer: a fuzzer without a sanitizer
    # finds only the crashes bad enough to fault, and the interesting ones
    # (a one-byte overread, a signed overflow) are exactly the quiet kind.
    target_compile_options(clink_fuzz_${target}
        PRIVATE -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer -g)
    target_link_options(clink_fuzz_${target}
        PRIVATE -fsanitize=fuzzer,address,undefined)
    target_link_libraries(clink_fuzz_${target} PRIVATE clink_core)
    if(CLINK_BUILD_SQL AND target STREQUAL "sql_parse")
        target_link_libraries(clink_fuzz_${target} PRIVATE clink_sql)
        target_compile_definitions(clink_fuzz_${target} PRIVATE CLINK_FUZZ_WITH_SQL=1)
    endif()
endforeach()

message(STATUS "clink: fuzz targets enabled (${CLINK_FUZZ_TARGETS})")
