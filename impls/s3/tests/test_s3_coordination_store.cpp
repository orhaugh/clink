// S3CoordinationStore against a real object store.
//
// LIVE tests: SKIPPED unless CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET
// are set (MinIO / LocalStack; credentials via the AWS env vars). The
// endpoint must support conditional PUTs (If-Match / If-None-Match) - the
// store's whole guarantee rides on them.
//
// The semantic contract lives in test_helpers/coordination_store_contract.hpp
// and runs verbatim against this store; what is S3-specific here is scheme
// dispatch through the registry and the CAS interleave: a fenced write whose
// read went stale must lose its conditional PUT, never land over a fresher
// record. That last case is the mutation check for the If-Match header -
// build the store without it and the test fails with a superseded writer's
// record on top of the leader's.

#include <cstdlib>
#include <ctime>
#include <memory>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cluster/coordination_store.hpp"
#include "clink/s3/install.hpp"
#include "clink/s3/read_all.hpp"
#include "clink/s3/s3_coordination_store.hpp"

#include "test_helpers/coordination_store_contract.hpp"

namespace {

const char* env_or(const char* name, const char* fallback) {
    const char* v = std::getenv(name);
    return v != nullptr ? v : fallback;
}

bool live_configured() {
    return std::getenv("CLINK_S3_TEST_ENDPOINT") != nullptr &&
           std::getenv("CLINK_S3_TEST_BUCKET") != nullptr;
}

std::string endpoint() {
    return env_or("CLINK_S3_TEST_ENDPOINT", "");
}
std::string bucket() {
    return env_or("CLINK_S3_TEST_BUCKET", "");
}
std::string region() {
    return env_or("CLINK_S3_TEST_REGION", "us-east-1");
}

// A prefix fresh for this test AND this run: the bucket outlives the
// process, so records from an earlier invocation must never be visible.
std::string fresh_prefix() {
    static const auto run_stamp =
        std::to_string(::getpid()) + "-" + std::to_string(::time(nullptr));
    return "coordstore-tests/" + run_stamp + "/" +
           ::testing::UnitTest::GetInstance()->current_test_info()->name();
}

struct S3Provider {
    static std::string Unavailable() {
        if (!live_configured()) {
            return "set CLINK_S3_TEST_ENDPOINT + CLINK_S3_TEST_BUCKET (MinIO/LocalStack with "
                   "conditional-PUT support) to run the S3 coordination-store contract";
        }
        return {};
    }

    static std::shared_ptr<clink::cluster::CoordinationStore> Make() {
        clink::s3::ensure_bucket(endpoint(), region(), bucket());
        current_prefix() = fresh_prefix();
        return std::make_shared<clink::s3::S3CoordinationStore>(options(current_prefix()));
    }

    static void Cleanup() {
        auto store = std::make_shared<clink::s3::S3CoordinationStore>(options(current_prefix()));
        for (const auto& key : store->list("")) {
            store->remove(key);
        }
    }

    static clink::s3::S3CoordinationStore::Options options(const std::string& prefix) {
        clink::s3::S3CoordinationStore::Options o;
        o.bucket = bucket();
        o.prefix = prefix;
        o.endpoint = endpoint();
        o.region = region();
        return o;
    }

    static std::string& current_prefix() {
        static std::string p;
        return p;
    }
};

}  // namespace

// The typed-suite macro pastes the suite name into generated identifiers
// and looks its registration namespace up unqualified, so the contract's
// namespace must be visible here.
using namespace clink::test;  // NOLINT(google-build-using-namespace)
INSTANTIATE_TYPED_TEST_SUITE_P(S3, CoordinationStoreContract, S3Provider);

namespace {

struct S3CoordinationStoreLive : ::testing::Test {
    void SetUp() override {
        if (!live_configured()) {
            GTEST_SKIP() << S3Provider::Unavailable();
        }
        clink::s3::ensure_bucket(endpoint(), region(), bucket());
        S3Provider::current_prefix() = fresh_prefix();
        prefix = S3Provider::current_prefix();
    }
    void TearDown() override {
        if (!prefix.empty()) {
            S3Provider::Cleanup();
        }
    }

    std::string prefix;
};

TEST_F(S3CoordinationStoreLive, TheS3SchemeDispatchesThroughTheRegistry) {
    clink::s3::install_coordination_store();  // idempotent
    const auto uri =
        "s3://" + bucket() + "/" + prefix + "?endpoint=" + endpoint() + "&region=" + region();
    auto store = clink::cluster::make_coordination_store(uri);
    store->put("_jobs/1/COMPLETED-1", "job=1\ncheckpoint=1\n");
    EXPECT_EQ(store->get("_jobs/1/COMPLETED-1").value(), "job=1\ncheckpoint=1\n");
    EXPECT_EQ(clink::cluster::make_coordination_store(uri).get(), store.get())
        << "same root must resolve to the same cached instance";
}

// The read-then-write interleave: writer A reads the record, a newer
// leader B lands, A's conditional PUT must refuse. This is the flock
// critical section of the filesystem store expressed as an ETag CAS, and
// it is exactly the window fenced_metadata_cas_write exists to close -
// "stale reads, fresh writes, stale renames over it" must be impossible
// on this substrate too.
TEST_F(S3CoordinationStoreLive, AFencedWriteWhoseReadWentStaleLosesTheCas) {
    auto a = std::make_shared<clink::s3::S3CoordinationStore>(S3Provider::options(prefix));
    auto b = std::make_shared<clink::s3::S3CoordinationStore>(S3Provider::options(prefix));

    ASSERT_TRUE(a->fenced_put(
        "jobs/1/manifest.json", "epoch=5\n", 5, clink::test::contract_epoch_of, "writer-a"));

    // B lands epoch 9 inside A's read-to-write window.
    a->set_test_hook_between_check_and_put([&] {
        ASSERT_TRUE(b->fenced_put(
            "jobs/1/manifest.json", "epoch=9\n", 9, clink::test::contract_epoch_of, "writer-b"));
    });

    // A's epoch 6 passed the fence against the epoch 5 it read, but its
    // ETag no longer matches; the retry re-reads epoch 9 and refuses.
    EXPECT_FALSE(a->fenced_put(
        "jobs/1/manifest.json", "epoch=6\n", 6, clink::test::contract_epoch_of, "writer-a"));
    EXPECT_EQ(a->get("jobs/1/manifest.json").value(), "epoch=9\n")
        << "a superseded writer's record landed on top of the newer leader's";
}

}  // namespace
