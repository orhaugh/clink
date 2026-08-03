// libFuzzer entry point for the checkpoint_meta target.
//
// Deliberately thin: the logic is in fuzz_targets.hpp so the same function
// is replayed by tests/test_fuzz_corpus.cpp under plain gtest, which is
// what makes a reproducer found here into a permanent regression test on
// builds that cannot run a fuzzer.

#include <cstddef>
#include <cstdint>

#include "fuzz/fuzz_targets.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
    clink::fuzzing::fuzz_checkpoint_meta(data, size);
    return 0;
}
