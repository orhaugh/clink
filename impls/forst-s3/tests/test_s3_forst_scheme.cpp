// The ForSt remote-state factory schemes (s3+forst://,
// changelog+s3+forst://). Non-gated tests prove the schemes are
// registered and construct a working backend without touching the
// network (the local DB opens; the S3 store is lazy). The MinIO-gated
// tests drive a full snapshot -> object storage -> restore through the
// factory, plus the async-persist split the S3 store enables (set
// CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET).

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
        std::filesystem::temp_directory_path() / ("clink_forst_s3_" + tag + std::to_string(n++));
    std::error_code ec;
    std::filesystem::remove_all(p, ec);
    return p.string();
}

}  // namespace

// Both schemes are registered and build a working backend without network
// (a dead endpoint is fine: the local DB opens, the S3 store is lazy).
TEST(S3ForstSchemes, BothRegisteredAndConstruct) {
    auto& f = StateBackendFactory::default_instance();
    const std::string dead = "&endpoint=http://127.0.0.1:1&region=us-east-1";
    for (const std::string scheme : {"s3+forst", "changelog+s3+forst"}) {
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

// The point of the S3 composition for the async substrate: the remote
// store defers the durable write, so the backend reports
// supports_async_persist() and the runner may split capture (operator
// thread) from persist (snapshot worker). Constructible without network -
// the flag is a property of the store, not a round-trip.
TEST(S3ForstSchemes, S3StoreEnablesAsyncPersistSplit) {
    auto& f = StateBackendFactory::default_instance();
    StateBackendSpec spec;
    spec.uri = "s3+forst://bucket/job?local=" + tmp_local("apflag") +
               "&endpoint=http://127.0.0.1:1&region=us-east-1";
    spec.subtask_idx = 0;
    auto built = f.build(spec);
    ASSERT_TRUE(built.backend != nullptr);
    EXPECT_TRUE(built.backend->supports_async_persist())
        << "an S3-published forst checkpoint must ride the capture/persist split";
}

// Live: s3+forst checkpoint uploads to object storage and restores into a
// fresh backend (fresh local dir) that pulls the checkpoint back from S3.
// Driven through capture()/persist() explicitly - the exact split the
// snapshot worker runs - rather than the synchronous snapshot() wrapper.
TEST(S3ForstSchemes, S3ForstCapturePersistRestoreAgainstLiveEndpoint) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    // Per-process-unique prefix so reruns start clean.
    const std::string prefix =
        std::string{"forst-s3-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string write_uri = std::string{"s3+forst://"} + bk + "/" + prefix +
                                  "?local=" + tmp_local("w") + "&endpoint=" + ep +
                                  "&region=us-east-1";

    StateBackendSpec spec1;
    spec1.uri = write_uri;
    spec1.subtask_idx = 0;
    auto built1 = f.build(spec1);
    ASSERT_TRUE(built1.backend != nullptr);
    ASSERT_TRUE(built1.backend->supports_async_persist());
    built1.backend->put(op, sv(std::string{"a"}), sv(std::string{"1"}));
    built1.backend->put(op, sv(std::string{"b"}), sv(std::string{"2"}));
    // capture on the "operator thread", persist as the worker would.
    auto handle = built1.backend->capture(CheckpointId{1});
    auto snap = built1.backend->persist(std::move(handle));  // uploads cp dir to S3
    EXPECT_EQ(snap.checkpoint_id.value(), 1u);

    // Fresh backend, fresh local dir, restoring from the same bucket/prefix.
    StateBackendSpec spec2;
    spec2.uri = std::string{"s3+forst://"} + bk + "/" + prefix + "?local=" + tmp_local("r") +
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

// Live: s3+forst with ?cas=1 (content-addressed manifest store) drives the
// backend through capture/persist/restore end-to-end, and an OLDER
// checkpoint restores the older state (time-travel through the factory).
TEST(S3ForstSchemes, S3ForstCasRoundTripAndTimeTravel) {
    const char* ep = std::getenv("CLINK_S3_TEST_ENDPOINT");
    const char* bk = std::getenv("CLINK_S3_TEST_BUCKET");
    if (ep == nullptr || bk == nullptr) {
        GTEST_SKIP() << "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET";
    }
    auto& f = StateBackendFactory::default_instance();
    const OperatorId op{7};
    const std::string prefix =
        std::string{"forst-s3-cas-"} + std::to_string(static_cast<long>(::getpid()));
    auto uri = [&](const std::string& local_tag) {
        return std::string{"s3+forst://"} + bk + "/" + prefix + "?local=" + tmp_local(local_tag) +
               "&endpoint=" + ep + "&region=us-east-1&cas=1";
    };
    const std::string write_uri = uri("cas-w");

    // cp-1 captures {a,b}; then add c and capture cp-2 {a,b,c}. Consecutive
    // checkpoints share most files - the CAS store dedups them.
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

// Live: changelog+s3+forst materialization payload goes to object storage
// (framing blob stays local); restore replays it back into a fresh ForSt
// inner.
TEST(S3ForstSchemes, ChangelogS3ForstRoundTripAgainstLiveEndpoint) {
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
    const std::string prefix =
        std::string{"forst-s3-cl-"} + std::to_string(static_cast<long>(::getpid()));
    const std::string uri = std::string{"changelog+s3+forst://"} + bk + "/" + prefix +
                            "?local=" + local + "&endpoint=" + ep + "&region=us-east-1";

    // Scoped: the ForSt inner holds an exclusive LOCK on its working dir
    // while open, so the writer must be torn down (as a process restart
    // would) before the restoring build re-opens the same local dir. The
    // framing blob and the inner's checkpoint dirs persist on disk.
    {
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
    }

    StateBackendSpec spec2;
    spec2.uri = uri;
    spec2.subtask_idx = 0;
    spec2.restore_uri = uri;
    spec2.restore_checkpoint_id = 1;
    auto built2 = f.build(spec2);
    ASSERT_TRUE(built2.backend != nullptr);
    ASSERT_TRUE(built2.restore_from.has_value());
    built2.backend->restore(*built2.restore_from);
    // The last write per key wins (i runs 0..1999 over 50 keys).
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"key-0"}))), "v1950");
    EXPECT_EQ(to_string(*built2.backend->get(op, sv(std::string{"key-49"}))), "v1999");
}
