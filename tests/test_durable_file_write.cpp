// Tests for the durable-write helper that backs FileBacked / Changelog
// snapshot persistence. fsync itself is not directly observable, so these
// assert the contract that must hold whether or not fsync runs: the bytes
// land atomically under the final path with the exact content, no temp file
// is left behind, and the CLINK_STATE_FSYNC escape hatch (fall back to
// flush+rename) still produces a correct file.

#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/state/durable_file_write.hpp"

namespace {

namespace fs = std::filesystem;
using clink::state::detail::write_fsync_rename;

fs::path scratch(const std::string& tag) {
    const auto p = fs::temp_directory_path() / ("clink_durable_write_" + tag);
    fs::remove_all(p);
    fs::create_directories(p);
    return p;
}

std::vector<std::byte> bytes_of(const std::string& s) {
    std::vector<std::byte> out(s.size());
    for (std::size_t i = 0; i < s.size(); ++i) {
        out[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    }
    return out;
}

std::string read_file(const fs::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

TEST(DurableFileWrite, WritesContentAtomicallyAndRemovesTemp) {
    const auto dir = scratch("atomic");
    const auto final_path = dir / "snap";
    const auto tmp = final_path.string() + ".part";
    const auto payload = bytes_of("hello-durable");

    write_fsync_rename(final_path, tmp, payload.data(), payload.size());

    EXPECT_TRUE(fs::exists(final_path));
    EXPECT_EQ(read_file(final_path), "hello-durable");
    EXPECT_FALSE(fs::exists(tmp)) << "temp file must be renamed away, not left behind";
}

TEST(DurableFileWrite, OverwritesExistingFinalFile) {
    const auto dir = scratch("overwrite");
    const auto final_path = dir / "snap";
    const auto a = bytes_of("first");
    const auto b = bytes_of("second-longer");

    write_fsync_rename(final_path, final_path.string() + ".part.0", a.data(), a.size());
    write_fsync_rename(final_path, final_path.string() + ".part.1", b.data(), b.size());

    EXPECT_EQ(read_file(final_path), "second-longer");
}

TEST(DurableFileWrite, EmptyPayloadProducesEmptyFile) {
    const auto dir = scratch("empty");
    const auto final_path = dir / "snap";
    write_fsync_rename(final_path, final_path.string() + ".part", nullptr, 0);
    EXPECT_TRUE(fs::exists(final_path));
    EXPECT_TRUE(read_file(final_path).empty());
}

TEST(DurableFileWrite, FsyncDisabledStillWritesCorrectFile) {
    const auto dir = scratch("nofsync");
    const auto final_path = dir / "snap";
    const auto payload = bytes_of("no-fsync-path");

    ::setenv("CLINK_STATE_FSYNC", "0", 1);
    write_fsync_rename(final_path, final_path.string() + ".part", payload.data(), payload.size());
    ::unsetenv("CLINK_STATE_FSYNC");

    EXPECT_TRUE(fs::exists(final_path));
    EXPECT_EQ(read_file(final_path), "no-fsync-path");
}

TEST(DurableFileWrite, ThrowsWhenTempDirMissing) {
    const auto dir = scratch("missing");
    const auto final_path = dir / "nope" / "snap";  // parent dir does not exist
    const auto payload = bytes_of("x");
    EXPECT_THROW(write_fsync_rename(
                     final_path, final_path.string() + ".part", payload.data(), payload.size()),
                 std::runtime_error);
}

}  // namespace
