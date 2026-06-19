// S3MaterializationStore (DISAGG-0): construction validation + clean failure
// against an unreachable endpoint, plus an opt-in round-trip against a live
// MinIO/localstack (gated on CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET, so
// the default suite needs no S3 backend - mirrors the dead-endpoint pattern in
// test_parquet_s3.cpp). Proves the materialization payload disaggregates to
// object storage with a handle-only snapshot, the changelog-backend beachhead.

#include <cstdint>
#include <cstdlib>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/s3/s3_materialization_store.hpp"

using namespace clink;
using clink::s3::S3MaterializationStore;

namespace {

constexpr const char* kDeadEndpoint = "http://127.0.0.1:1";

std::vector<std::byte> bytes_of(const std::string& s) {
    std::vector<std::byte> out(s.size());
    for (std::size_t i = 0; i < s.size(); ++i) {
        out[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    }
    return out;
}

std::string str_of(const std::vector<std::byte>& b) {
    std::string out(b.size(), '\0');
    for (std::size_t i = 0; i < b.size(); ++i) {
        out[i] = static_cast<char>(b[i]);
    }
    return out;
}

}  // namespace

TEST(S3MaterializationStore, RejectsEmptyBucket) {
    S3MaterializationStore::Options opts;
    opts.bucket = "";
    EXPECT_THROW(S3MaterializationStore{std::move(opts)}, std::invalid_argument);
}

TEST(S3MaterializationStore, ConstructionDoesNotTouchNetwork) {
    // Construction must not build the filesystem or connect; a dead endpoint
    // is fine until the first write/read.
    S3MaterializationStore::Options opts;
    opts.bucket = "bucket";
    opts.prefix = "job/0/mat";
    opts.endpoint_override = kDeadEndpoint;
    EXPECT_NO_THROW(S3MaterializationStore{std::move(opts)});
}

TEST(S3MaterializationStore, WriteAgainstDeadEndpointFailsCleanly) {
    // The first write builds the S3 filesystem and opens an output stream;
    // against an unreachable endpoint it must throw a runtime_error, not
    // abort or deadlock.
    S3MaterializationStore::Options opts;
    opts.bucket = "bucket";
    opts.prefix = "job/0/mat";
    opts.endpoint_override = kDeadEndpoint;
    opts.region = "us-east-1";
    opts.allow_anonymous = true;
    S3MaterializationStore store(std::move(opts));
    const auto payload = bytes_of("hello-state");
    EXPECT_THROW(store.write(CheckpointId{1}, std::span<const std::byte>{payload}),
                 std::runtime_error);
}

// Opt-in round-trip against a real object store. Set CLINK_S3_TEST_ENDPOINT
// (e.g. http://minio:9000) and CLINK_S3_TEST_BUCKET to run; skipped otherwise.
TEST(S3MaterializationStore, RoundTripAgainstLiveEndpoint) {
    const char* endpoint = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bucket = std::getenv("CLINK_S3_TEST_BUCKET");
    if (endpoint == nullptr || bucket == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET to run the live "
                        "S3 materialization round-trip";
    }
    S3MaterializationStore::Options opts;
    opts.bucket = bucket;
    opts.prefix = "clink-test/mat";
    opts.endpoint_override = std::string{endpoint};
    opts.region = "us-east-1";
    S3MaterializationStore store(std::move(opts));

    const auto payload = bytes_of("the-materialization-payload-bytes");
    const std::string handle = store.write(CheckpointId{42}, std::span<const std::byte>{payload});
    EXPECT_FALSE(handle.empty());

    const auto read_back = store.read(handle);
    EXPECT_EQ(str_of(read_back), "the-materialization-payload-bytes");

    // Empty payload round-trips to an empty read (the changelog backend may
    // materialize an empty inner state).
    const std::string empty_handle = store.write(CheckpointId{43}, std::span<const std::byte>{});
    EXPECT_TRUE(store.read(empty_handle).empty());

    store.erase(handle);
    store.erase(empty_handle);
}
