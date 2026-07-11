// Registry-free filesystem_from_uri (clink/connectors/arrow_fs_uri.hpp).
//
// The helper must open local filesystems WITHOUT consulting Arrow's factory
// registry: binaries that carry duplicated Arrow filesystem statics (the
// pinned iceberg-cpp bundles them) make arrow::fs::FileSystemFromUri fail for
// every scheme with "scheme 'file' ... already registered". The end-to-end
// proof against such a binary is the ReplayCli capture-push/fetch test, which
// spawns the real `clink` CLI; the tests here pin the helper's own contract.

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/connectors/arrow_fs_uri.hpp"

namespace {

namespace fs = std::filesystem;

fs::path make_scratch_dir(const std::string& tag) {
    const auto dir = fs::temp_directory_path() / ("clink_arrow_fs_uri_" + tag);
    fs::remove_all(dir);
    fs::create_directories(dir);
    return dir;
}

TEST(ArrowFsUri, FileUriYieldsLocalFilesystemAndPath) {
    const auto dir = make_scratch_dir("file_uri");
    std::string path;
    const auto filesystem = clink::connectors::filesystem_from_uri("file://" + dir.string(), &path);
    ASSERT_NE(filesystem, nullptr);
    EXPECT_EQ(filesystem->type_name(), "local");
    EXPECT_EQ(path, dir.string());
    fs::remove_all(dir);
}

TEST(ArrowFsUri, FileUriPercentDecodesPath) {
    std::string path;
    const auto filesystem =
        clink::connectors::filesystem_from_uri("file:///tmp/with%20space", &path);
    ASSERT_NE(filesystem, nullptr);
    EXPECT_EQ(path, "/tmp/with space");
}

TEST(ArrowFsUri, BarePathResolvesAbsolute) {
    std::string path;
    const auto filesystem = clink::connectors::filesystem_from_uri("some/relative/dir", &path);
    ASSERT_NE(filesystem, nullptr);
    EXPECT_EQ(filesystem->type_name(), "local");
    EXPECT_TRUE(fs::path(path).is_absolute()) << path;
    EXPECT_TRUE(path.ends_with("some/relative/dir")) << path;
}

TEST(ArrowFsUri, RoundTripWriteReadThroughReturnedFilesystem) {
    const auto dir = make_scratch_dir("round_trip");
    std::string prefix;
    const auto filesystem =
        clink::connectors::filesystem_from_uri("file://" + dir.string(), &prefix);
    ASSERT_NE(filesystem, nullptr);

    // Write through the Arrow filesystem at the returned prefix, read back
    // through plain std::filesystem: proves the (filesystem, prefix) pair
    // actually addresses the intended location.
    const auto out = filesystem->OpenOutputStream(prefix + "/hello.txt");
    ASSERT_TRUE(out.ok()) << out.status().ToString();
    ASSERT_TRUE((*out)->Write("payload", 7).ok());
    ASSERT_TRUE((*out)->Close().ok());

    std::ifstream in(dir / "hello.txt");
    std::string content((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "payload");
    fs::remove_all(dir);
}

}  // namespace
