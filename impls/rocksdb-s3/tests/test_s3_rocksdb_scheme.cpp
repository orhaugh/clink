// DISAGG-4: the remote-state factory schemes (s3+rocksdb://,
// changelog+s3+rocksdb://, changelog+s3://). Non-gated tests prove the schemes
// are registered and construct a working backend without touching the network
// (the local DB opens; the S3 store is lazy). The MinIO-gated tests drive a
// full snapshot -> object storage -> restore through the factory, mirroring
// test_s3_*_store.cpp (set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET).

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/state/state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    if (!v.empty()) {
        std::memcpy(out.data(), v.data(), v.size());
    }
    return out;
}

std::string tmp_local(const std::string& tag) {
    static int n = 0;
    auto p =
        std::filesystem::temp_directory_path() / ("clink_disagg4_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p.string();
}

}  // namespace

// All three schemes are registered and build a working backend without network
// (a dead endpoint is fine: the local DB opens, the S3 store is lazy).
TEST(S3RocksdbSchemes, AllThreeRegisteredAndConstruct) {
    auto& f = StateBackendFactory::default_instance();
    const std::string dead = "&endpoint=http://127.0.0.1:1&region=us-east-1";
    for (const std::string scheme : {"s3+rocksdb", "changelog+s3+rocksdb", "changelog+s3"}) {
        StateBackendSpec spec;
        spec.uri = scheme + "://bucket/job?local=" + tmp_local(scheme) + dead;
        spec.subtask_idx = 0;
        auto built = f.build(spec);
        ASSERT_TRUE(built.backend != nullptr) << "scheme not registered/constructed: " << scheme;
        // A keyed put/get works (the live state is local; S3 is only for
        // checkpoints / materialization payloads).
        built.backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));
        EXPECT_EQ(to_string(*built.backend->get(OperatorId{1}, sv(std::string{"k"}))), "v");
    }
}

// Live: s3+rocksdb checkpoint uploads to object storage and restores into a
// fresh backend (fresh local dir) that pulls the checkpoint back from S3.
TEST(S3RocksdbSchemes, S3RocksdbRoundTripAgainstLiveEndpoint) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string suffix = std::string{"?endpoint="} + ep + "&region=us-east-1";
    const std::string write_uri = std::string{"s3+rocksdb://"} + bk +
                                  "/disagg4-rdb?local=" + tmp_local("rdb-w") + "&endpoint=" + ep +
                                  "&region=us-east-1";

    StateBackendSpec spec1;
    spec1.uri = write_uri;
    spec1.subtask_idx = 0;
    auto built1 = f.build(spec1);
    ASSERT_TRUE(built1.backend != nullptr);
    built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
    built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
    (void)built1.backend->snapshot(CheckpointId{1});  // uploads cp dir to S3

    // Fresh backend, fresh local dir, restoring from the same bucket/prefix.
    StateBackendSpec spec2;
    spec2.uri = std::string{"s3+rocksdb://"} + bk + "/disagg4-rdb?local=" + tmp_local("rdb-r") +
                "&endpoint=" + ep + "&region=us-east-1";
    spec2.subtask_idx = 0;
    spec2.restore_uri = write_uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    ASSERT_TRUE(built2.backend->get(op, sv(std::string{"a"})).has_value());
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"b"}))), "2");
}

// Live: s3+rocksdb with ?cas=1 (DISAGG-6 content-addressed manifest store)
// drives a real RocksDB backend through capture/persist/restore end-to-end, and
// an OLDER checkpoint restores the older state (free time-travel through the
// factory).
TEST(S3RocksdbSchemes, S3RocksdbCasRoundTripAndTimeTravel) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    // Per-process-unique prefix so the object pool starts clean.
    const std::string prefix =
        std::string{"disagg6-cas-"} + std::to_string(static_cast<long>(::getpid()));
    auto uri = [&](const std::string& local_tag) {
        return std::string{"s3+rocksdb://"} + bk + "/" + prefix + "?local=" + tmp_local(local_tag) +
               "&endpoint=" + ep + "&region=us-east-1&cas=1";
    };
    const std::string write_uri = uri("cas-w");

    // cp-1 captures {a,b}; then add c and capture cp-2 {a,b,c}. Consecutive
    // RocksDB checkpoints share most files - the CAS store dedups them.
    StateBackendSpec spec1;
    spec1.uri = write_uri;
    spec1.subtask_idx = 0;
    auto built1 = f.build(spec1);
    ASSERT_TRUE(built1.backend != nullptr);
    built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
    built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
    (void)built1.backend->snapshot(CheckpointId{1});
    built1.backend->put(op, sv(std::string{"c"}), sv(std::string{"3"}));
    (void)built1.backend->snapshot(CheckpointId{2});

    // Restore cp-2 into a fresh backend (fresh local dir): all three keys.
    StateBackendSpec spec2;
    spec2.uri = uri("cas-r2");
    spec2.subtask_idx = 0;
    spec2.restore_uri = write_uri;
    spec2.restore_checkpoint_id = 2;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"b"}))), "2");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"c"}))), "3");

    // Time-travel: restore the OLDER cp-1 -> only {a,b}, c was not yet present.
    StateBackendSpec spec3;
    spec3.uri = uri("cas-r1");
    spec3.subtask_idx = 0;
    spec3.restore_uri = write_uri;
    spec3.restore_checkpoint_id = 1;
    auto built3 = f.build(spec3);
    ASSERT_TRUE(built3.restore_from.has_value());
    built3.backend->restore(*built3.restore_from);
    EXPECT_EQ(to_string(*built3.backend->get(op, sv(std::string{"a"}))), "1");
    EXPECT_EQ(to_string(*built3.backend->get(op, sv(std::string{"b"}))), "2");
    EXPECT_FALSE(built3.backend->get(op, sv(std::string{"c"})).has_value());
}

// Live: changelog+s3 materialization payload goes to object storage (framing
// blob stays local); restore replays it back into a fresh inner.
TEST(S3RocksdbSchemes, ChangelogS3RoundTripAgainstLiveEndpoint) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    // Same local dir for write + restore: the changelog framing blob is local
    // (the disaggregated part is the materialization payload, which is in S3).
    const std::string local = tmp_local("cl");
    const std::string uri = std::string{"changelog+s3://"} + bk + "/disagg4-cl?local=" + local +
                            "&endpoint=" + ep + "&region=us-east-1";

    StateBackendSpec spec1;
    spec1.uri = uri;
    spec1.subtask_idx = 0;
    auto built1 = f.build(spec1);
    ASSERT_TRUE(built1.backend != nullptr);
    // Enough writes to push the changelog over its materialization threshold.
    for (int i = 0; i < 2000; ++i) {
        built1.backend->put(
            op, sv("key-" + std::to_string(i % 50)), sv(std::string("v") + std::to_string(i)));
    }
    (void)built1.backend->snapshot(CheckpointId{1});

    StateBackendSpec spec2;
    spec2.uri = uri;
    spec2.subtask_idx = 0;
    spec2.restore_uri = uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    // The last write for each of the 50 keys survives the round-trip.
    auto v = built2.backend->get(op, sv(std::string{"key-0"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(to_string(*v), "v1950");  // last i with i%50==0 is 1950
}
