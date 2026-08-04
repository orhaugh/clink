// The coordinator's plugin cache: where a submitted module's bytes land.
//
// Two properties, both of which were absent and neither of which anything would
// have noticed. The cache is written from `SubmitJob` frames BEFORE any slot or
// admission check runs, and the control plane has no authentication, so this is
// the most exposed write path in the engine.

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/plugin_cache.hpp"

using namespace clink::cluster;

namespace {

std::filesystem::path scratch_dir(const std::string& tag) {
    return std::filesystem::temp_directory_path() /
           ("clink_plugin_cache_test_" + std::to_string(::getpid()) + "_" + tag);
}

PluginBinary blob_with(const std::string& contents, const std::string& declared_hash) {
    PluginBinary b;
    b.name = "test-plugin";
    b.content_hash = declared_hash;
    b.bytes.reserve(contents.size());
    for (const char c : contents) {
        b.bytes.push_back(static_cast<std::byte>(c));
    }
    return b;
}

std::size_t files_in(const std::filesystem::path& dir) {
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return 0;
    }
    std::size_t n = 0;
    for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
        if (e.is_regular_file()) {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(PluginCache, TheFilenameComesFromTheBytesNotFromWhatThePeerClaimed) {
    // content_hash arrives off the wire and was used verbatim as a path
    // component. A peer could therefore steer the write, and every idempotency
    // decision in the cache keyed on a name the peer chose.
    const auto dir = scratch_dir("derived");
    std::filesystem::remove_all(dir);

    const auto honest = blob_with("module-bytes", /*declared_hash=*/"");
    const auto path = write_plugin_to_cache(honest, dir.string());
    EXPECT_EQ(std::filesystem::path{path}.parent_path(), dir)
        << "the module was written outside the cache directory";

    // The name is 16 lowercase hex chars plus the platform suffix - the shape
    // fnv1a_64_hex produces, and nothing else.
    const auto stem = std::filesystem::path{path}.stem().string();
    EXPECT_EQ(stem.size(), 16u) << "unexpected cache filename stem: " << stem;
    for (const char c : stem) {
        EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
            << "cache filename is not hex: " << stem;
    }

    std::filesystem::remove_all(dir);
}

TEST(PluginCache, APathTraversalInTheDeclaredHashCannotEscapeTheCacheDirectory) {
    // The exposure, stated as a test rather than as a comment. Before the
    // filename was derived from the bytes, this string was concatenated straight
    // into the path.
    const auto dir = scratch_dir("escape");
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    const auto sentinel = dir.parent_path() / "clink_plugin_cache_escaped";
    std::filesystem::remove(sentinel);

    const auto malicious = blob_with("bytes", "../clink_plugin_cache_escaped");
    // Either refused, or written under a derived name - never at the peer's path.
    try {
        const auto path = write_plugin_to_cache(malicious, dir.string());
        EXPECT_EQ(std::filesystem::path{path}.parent_path(), dir)
            << "a peer-supplied hash steered the write to " << path;
    } catch (const std::exception&) {
        // Refusing a blob whose declared hash does not describe its bytes is
        // also correct, and is what happens here.
    }
    EXPECT_FALSE(std::filesystem::exists(sentinel))
        << "a peer-supplied content_hash escaped the cache directory";

    std::filesystem::remove_all(dir);
    std::filesystem::remove(sentinel);
}

TEST(PluginCache, ADeclaredHashThatDoesNotDescribeTheBytesIsRefused) {
    // Not silently corrected. A mismatch means the peer and this coordinator
    // disagree about what was sent, and every reuse decision below keys on that
    // name - so caching the module under a name that means something else would
    // make a later submission of DIFFERENT bytes reuse it.
    const auto dir = scratch_dir("mismatch");
    std::filesystem::remove_all(dir);

    const auto lying = blob_with("real-bytes", "0000000000000000");
    EXPECT_THROW((void)write_plugin_to_cache(lying, dir.string()), std::runtime_error);
    EXPECT_EQ(files_in(dir), 0u) << "a refused blob still left a file behind";

    std::filesystem::remove_all(dir);
}

TEST(PluginCache, IdenticalBytesReuseOneFileRatherThanAccumulating) {
    // The property the cache exists for, and the one the derived name makes true
    // rather than assumed: repeat submissions of the same module cost one file.
    const auto dir = scratch_dir("reuse");
    std::filesystem::remove_all(dir);

    const auto blob = blob_with("same-bytes-every-time", "");
    std::string first;
    for (int i = 0; i < 5; ++i) {
        const auto path = write_plugin_to_cache(blob, dir.string());
        if (i == 0) {
            first = path;
        }
        EXPECT_EQ(path, first) << "the same bytes were cached under two names";
    }
    EXPECT_EQ(files_in(dir), 1u) << "five submissions of identical bytes left " << files_in(dir)
                                 << " files";

    // Different bytes get their own file, or the cache would be collapsing
    // distinct modules together.
    (void)write_plugin_to_cache(blob_with("other-bytes", ""), dir.string());
    EXPECT_EQ(files_in(dir), 2u);

    std::filesystem::remove_all(dir);
}
