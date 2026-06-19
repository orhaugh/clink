// S3SnapshotStore (DISAGG-2): construction validation + clean failure against
// an unreachable endpoint, plus an opt-in checkpoint-dir round-trip against a
// live MinIO/localstack (gated on CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET,
// mirroring test_s3_materialization_store.cpp). Proves a directory of files
// (the shape of a RocksDB checkpoint) uploads to object storage and downloads
// back byte-identical via the DISAGG-1 SnapshotStore seam.

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/s3/s3_snapshot_store.hpp"

using namespace clink;
using clink::s3::S3SnapshotStore;

namespace {

constexpr const char* kDeadEndpoint = "http://127.0.0.1:1";

std::filesystem::path make_local_dir(const std::string& tag,
                                     const std::map<std::string, std::string>& files) {
    static int n = 0;
    auto dir =
        std::filesystem::temp_directory_path() / ("clink_disagg2_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    std::filesystem::create_directories(dir, ec);
    for (const auto& [name, content] : files) {
        std::ofstream out(dir / name, std::ios::binary | std::ios::trunc);
        out.write(content.data(), static_cast<std::streamsize>(content.size()));
    }
    return dir;
}

std::string read_file(const std::filesystem::path& p) {
    std::ifstream in(p, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

int count_regular_files(const std::filesystem::path& dir) {
    int n = 0;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(dir, ec)) {
        if (e.is_regular_file()) {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(S3SnapshotStore, RejectsEmptyBucket) {
    S3SnapshotStore::Options opts;
    opts.bucket = "";
    EXPECT_THROW(S3SnapshotStore{std::move(opts)}, std::invalid_argument);
}

TEST(S3SnapshotStore, ConstructionDoesNotTouchNetwork) {
    S3SnapshotStore::Options opts;
    opts.bucket = "bucket";
    opts.prefix = "job/0";
    opts.endpoint_override = kDeadEndpoint;
    EXPECT_NO_THROW(S3SnapshotStore{std::move(opts)});
}

TEST(S3SnapshotStore, WriteAgainstDeadEndpointFailsCleanly) {
    const auto local = make_local_dir("dead", {{"CURRENT", "x"}, {"000001.sst", "data"}});
    S3SnapshotStore::Options opts;
    opts.bucket = "bucket";
    opts.prefix = "job/0";
    opts.endpoint_override = kDeadEndpoint;
    opts.region = "us-east-1";
    opts.allow_anonymous = true;
    S3SnapshotStore store(std::move(opts));
    EXPECT_THROW(store.write_checkpoint_dir(local.string(), CheckpointId{1}), std::runtime_error);
}

// Opt-in round-trip against a real object store. Set CLINK_S3_TEST_ENDPOINT +
// CLINK_S3_TEST_BUCKET to run; skipped otherwise.
TEST(S3SnapshotStore, CheckpointDirRoundTripAgainstLiveEndpoint) {
    const char* endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    if (endpoint == nullptr || bucket == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET to run the live "
                        "S3 snapshot-dir round-trip";
    }
    const std::map<std::string, std::string> files{
        {"CURRENT", "MANIFEST-000005\n"},
        {"MANIFEST-000005", std::string(1024, 'm')},
        {"000007.sst", std::string(4096, 'S')},
        {"OPTIONS-000008", "rocksdb-options"},
    };
    const auto local = make_local_dir("src", files);

    S3SnapshotStore::Options opts;
    opts.bucket = bucket;
    opts.prefix = "clink-test/snap";
    opts.endpoint_override = std::string{endpoint};
    opts.region = "us-east-1";
    S3SnapshotStore store(std::move(opts));

    const std::string handle = store.write_checkpoint_dir(local.string(), CheckpointId{77});
    EXPECT_NE(handle.find("cp-77"), std::string::npos);

    // A fresh store (different staging) fetches the dir back byte-identical.
    S3SnapshotStore::Options opts2;
    opts2.bucket = bucket;
    opts2.prefix = "clink-test/snap";
    opts2.endpoint_override = std::string{endpoint};
    opts2.region = "us-east-1";
    S3SnapshotStore store2(std::move(opts2));

    const std::string fetched = store2.fetch_checkpoint_dir(handle);
    ASSERT_EQ(count_regular_files(fetched), static_cast<int>(files.size()));
    for (const auto& [name, content] : files) {
        const auto p = std::filesystem::path{fetched} / name;
        ASSERT_TRUE(std::filesystem::exists(p)) << "missing " << name;
        EXPECT_EQ(read_file(p), content) << "content mismatch for " << name;
    }

    // Delete drops the object dir; a re-fetch yields an empty staging dir.
    store2.delete_checkpoint(local.string(), CheckpointId{77});
    const std::string after = store2.fetch_checkpoint_dir(handle);
    EXPECT_EQ(count_regular_files(after), 0);
}

// DISAGG-5: with a cache, a second fetch of the same checkpoint serves the
// large immutable SST/blob objects from the local cache (zero GetObject) and
// re-downloads only the small mutable / appended-to files (CURRENT, MANIFEST,
// OPTIONS). The whole checkpoint is still reconstructed byte-identically.
TEST(S3SnapshotStore, CacheServesImmutableSstOnSecondFetch) {
    const char* endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    if (endpoint == nullptr || bucket == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    const std::map<std::string, std::string> files{
        {"CURRENT", "MANIFEST-000005\n"},            // mutable: always re-fetched
        {"MANIFEST-000005", std::string(512, 'm')},  // appended-to: not cached
        {"000007.sst", std::string(4096, 'S')},      // immutable, large: cached
        {"000009.blob", std::string(2048, 'B')},     // immutable, large: cached
        {"OPTIONS-000008", "opts"},                  // tiny: not cached
    };
    const auto local = make_local_dir("cache-src", files);

    // Writer store (no cache) uploads the checkpoint.
    S3SnapshotStore::Options wopts;
    wopts.bucket = bucket;
    wopts.prefix = "clink-test/cache";
    wopts.endpoint_override = std::string{endpoint};
    wopts.region = "us-east-1";
    S3SnapshotStore writer(std::move(wopts));
    const std::string handle = writer.write_checkpoint_dir(local.string(), CheckpointId{88});

    // Cached reader: first fetch is all misses; second fetch hits the cache for
    // the .sst + .blob and re-downloads the three small mutable files.
    S3SnapshotStore::Options ropts;
    ropts.bucket = bucket;
    ropts.prefix = "clink-test/cache";
    ropts.endpoint_override = std::string{endpoint};
    ropts.region = "us-east-1";
    ropts.cache_dir = make_local_dir("cache-dir", {});
    ropts.cache_bytes = 1 << 20;
    S3SnapshotStore reader(std::move(ropts));

    const std::string first = reader.fetch_checkpoint_dir(handle);
    EXPECT_EQ(reader.objects_downloaded(), 5u);  // cold cache: all five
    EXPECT_EQ(count_regular_files(first), 5);

    const std::string second = reader.fetch_checkpoint_dir(handle);
    EXPECT_EQ(reader.objects_downloaded(), 8u);  // +3: CURRENT + MANIFEST + OPTIONS
    EXPECT_EQ(count_regular_files(second), 5);   // all five present
    EXPECT_EQ(read_file(std::filesystem::path{second} / "000007.sst"), files.at("000007.sst"));
    EXPECT_EQ(read_file(std::filesystem::path{second} / "000009.blob"), files.at("000009.blob"));
}
