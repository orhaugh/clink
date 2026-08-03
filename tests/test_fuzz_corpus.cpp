// Replay every committed fuzz input through its target.
//
// This is the half of fuzzing that can gate. Discovery needs a clang that
// ships libFuzzer and an unbounded time budget, so it cannot be a required
// check; replay needs neither, runs in milliseconds, and works on every
// platform and compiler the project builds on.
//
// The workflow the split enables:
//
//   1. `scripts/fuzz.sh cluster_frame` finds an input that crashes.
//   2. libFuzzer writes it to `crash-<sha>`.
//   3. The file is committed to `fuzz/corpus/cluster_frame/`.
//   4. From then on it is a permanent regression test that runs in CI on
//      builds that could not run a fuzzer at all.
//
// So a crash found once can never come back quietly, which is the point of
// the brief's "add a regression test for every bug discovered" - a fuzzer
// on its own does not do that, because nobody reruns the exact input.
//
// A corpus entry that has NEVER crashed is still worth keeping: it is
// coverage, and it is what a future fuzzing run starts from instead of
// starting from nothing.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "fuzz/fuzz_targets.hpp"

namespace {

using clink::fuzzing::all_targets;

std::filesystem::path corpus_root() {
#ifdef CLINK_FUZZ_CORPUS_DIR
    return std::filesystem::path{CLINK_FUZZ_CORPUS_DIR};
#else
    return {};
#endif
}

std::vector<std::uint8_t> read_file(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

}  // namespace

TEST(FuzzCorpus, TheCorpusDirectoryIsWhereTheBuildThinksItIs) {
    // Without this the suite below silently passes on zero inputs, which
    // is the failure mode a corpus replay is most prone to: a path that
    // moved, a test that keeps reporting green, and a regression corpus
    // that stopped running months ago.
    const auto root = corpus_root();
    ASSERT_FALSE(root.empty()) << "CLINK_FUZZ_CORPUS_DIR was not defined by the build";
    ASSERT_TRUE(std::filesystem::exists(root)) << root.string() << " does not exist";
    for (const auto& t : all_targets()) {
        EXPECT_TRUE(std::filesystem::exists(root / t.name))
            << "no corpus directory for target '" << t.name
            << "'; a target with nowhere to put a reproducer will lose the next one found";
    }
}

TEST(FuzzCorpus, EverySeedAndReproducerReplaysWithoutCrashing) {
    const auto root = corpus_root();
    ASSERT_FALSE(root.empty());

    std::size_t replayed = 0;
    for (const auto& t : all_targets()) {
        const auto dir = root / t.name;
        if (!std::filesystem::exists(dir)) {
            continue;
        }
        std::error_code ec;
        for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
            if (!entry.is_regular_file()) {
                continue;
            }
            // README / .gitkeep and friends are documentation, not inputs.
            const auto name = entry.path().filename().string();
            if (name.starts_with(".") || name == "README.md") {
                continue;
            }
            const auto bytes = read_file(entry.path());
            // The assertion IS reaching the next line. A crash, an abort,
            // an out-of-bounds read (under ASan/UBSan) or a hang takes the
            // process with it and the failure is unmistakable.
            t.run(bytes.data(), bytes.size());
            ++replayed;
            SCOPED_TRACE(t.name + std::string(" / ") + name);
        }
    }
    // Seeds are generated into the corpus at configure time, so zero
    // inputs means the generation broke rather than that there is nothing
    // to test.
    EXPECT_GT(replayed, 0U) << "no corpus inputs were replayed; the corpus is empty or unreadable";
}

TEST(FuzzCorpus, AnEmptyAndASingleByteInputAreHandledByEveryTarget) {
    // The two inputs every fuzzer produces in its first second, and the
    // two most likely to walk off the front of a buffer. Cheap to assert
    // for all targets rather than hoping each corpus happens to contain
    // them.
    for (const auto& t : all_targets()) {
        SCOPED_TRACE(t.name);
        t.run(nullptr, 0);
        const std::uint8_t one = 0;
        t.run(&one, 1);
        const std::uint8_t high = 0xFF;
        t.run(&high, 1);
    }
    SUCCEED();
}
