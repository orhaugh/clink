// The CoordinationStore contract, as a typed test suite every
// implementation must pass verbatim. The filesystem store instantiates it
// in tests/test_coordination_store.cpp; the S3 store instantiates it in
// impls/s3/tests/test_s3_coordination_store.cpp (gated on a reachable
// bucket). The cases assert semantics only - durability shape, locking and
// substrate details belong to the per-implementation files.
//
// A provider supplies:
//   static std::string Unavailable();   // empty = run; else GTEST_SKIP reason
//   static std::shared_ptr<clink::cluster::CoordinationStore> Make();
//       // a store over a namespace fresh for this test (records from
//       // earlier tests or runs must not be visible)
//   static void Cleanup();              // drop the namespace Make() created

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordination_store.hpp"

namespace clink::test {

// Coordination records spell their fencing epoch as "epoch=<n>" (job
// manifests use "coordinator_epoch"; the extractor is the caller's for
// exactly that reason). The contract fences on this simple form.
inline std::uint64_t contract_epoch_of(const std::string& body) {
    const auto key = std::string("epoch=");
    const auto pos = body.find(key);
    if (pos == std::string::npos) {
        return 0;
    }
    try {
        return std::stoull(body.substr(pos + key.size()));
    } catch (const std::exception&) {
        return 0;
    }
}

template <class Provider>
class CoordinationStoreContract : public ::testing::Test {
protected:
    void SetUp() override {
        const auto reason = Provider::Unavailable();
        if (!reason.empty()) {
            GTEST_SKIP() << reason;
        }
        store = Provider::Make();
    }

    void TearDown() override {
        if (store) {
            store.reset();
            Provider::Cleanup();
        }
    }

    std::shared_ptr<clink::cluster::CoordinationStore> store;
};

TYPED_TEST_SUITE_P(CoordinationStoreContract);

TYPED_TEST_P(CoordinationStoreContract, PutGetExistsRoundTrip) {
    EXPECT_FALSE(this->store->exists("_jobs/7/COMPLETED-4"));
    EXPECT_FALSE(this->store->get("_jobs/7/COMPLETED-4").has_value());
    this->store->put("_jobs/7/COMPLETED-4", "job=7\ncheckpoint=4\n");
    EXPECT_TRUE(this->store->exists("_jobs/7/COMPLETED-4"));
    EXPECT_EQ(this->store->get("_jobs/7/COMPLETED-4").value(), "job=7\ncheckpoint=4\n");
}

TYPED_TEST_P(CoordinationStoreContract, PutIfAbsentCreatesOnceAndPreservesTheFirstBody) {
    EXPECT_TRUE(this->store->put_if_absent("k", "first"));
    EXPECT_FALSE(this->store->put_if_absent("k", "second"));
    EXPECT_EQ(this->store->get("k").value(), "first");
}

TYPED_TEST_P(CoordinationStoreContract, ConcurrentPutIfAbsentHasExactlyOneWinner) {
    std::atomic<int> wins{0};
    std::vector<std::thread> writers;
    writers.reserve(8);
    for (int i = 0; i < 8; ++i) {
        writers.emplace_back([&, i] {
            if (this->store->put_if_absent("contended", "writer-" + std::to_string(i))) {
                wins.fetch_add(1);
            }
        });
    }
    for (auto& t : writers) {
        t.join();
    }
    EXPECT_EQ(wins.load(), 1);
    const auto body = this->store->get("contended");
    ASSERT_TRUE(body.has_value());
    EXPECT_EQ(body->rfind("writer-", 0), 0u);
}

TYPED_TEST_P(CoordinationStoreContract, FencedPutRefusesAStaleEpochAndKeepsTheNewerRecord) {
    EXPECT_TRUE(this->store->fenced_put(
        "jobs/1/manifest.json", "epoch=5\n", 5, contract_epoch_of, "contract"));
    // A superseded writer must not clobber the leader's record.
    EXPECT_FALSE(this->store->fenced_put(
        "jobs/1/manifest.json", "epoch=3\n", 3, contract_epoch_of, "contract"));
    EXPECT_EQ(this->store->get("jobs/1/manifest.json").value(), "epoch=5\n");
    // A newer writer replaces.
    EXPECT_TRUE(this->store->fenced_put(
        "jobs/1/manifest.json", "epoch=9\n", 9, contract_epoch_of, "contract"));
    EXPECT_EQ(this->store->get("jobs/1/manifest.json").value(), "epoch=9\n");
}

TYPED_TEST_P(CoordinationStoreContract, ListIsRecursiveFilesOnlyAndHidesStoreMechanism) {
    this->store->put("_jobs/7/COMPLETED-4", "a");
    this->store->put("_jobs/7/receipts/sub0-4", "wm=100\n");
    this->store->put("_jobs/7/receipts/sub1-4", "wm=100\n");
    (void)this->store->fenced_put(
        "_jobs/7/COMPLETED-5", "epoch=1\n", 1, contract_epoch_of, "contract");
    // A sibling whose name extends the listed prefix must not leak in: the
    // boundary is a path segment, not a string prefix.
    this->store->put("_jobs/70/COMPLETED-1", "b");
    auto keys = this->store->list("_jobs/7");
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys,
              (std::vector<std::string>{"_jobs/7/COMPLETED-4",
                                        "_jobs/7/COMPLETED-5",
                                        "_jobs/7/receipts/sub0-4",
                                        "_jobs/7/receipts/sub1-4"}))
        << "recursive keys, records only, no store-mechanism entries";
    EXPECT_TRUE(this->store->list("_jobs/nothing-here").empty());
}

TYPED_TEST_P(CoordinationStoreContract, RemoveIsIdempotent) {
    this->store->put("r", "x");
    this->store->remove("r");
    EXPECT_FALSE(this->store->exists("r"));
    this->store->remove("r");  // absent: not an error
}

REGISTER_TYPED_TEST_SUITE_P(CoordinationStoreContract,
                            PutGetExistsRoundTrip,
                            PutIfAbsentCreatesOnceAndPreservesTheFirstBody,
                            ConcurrentPutIfAbsentHasExactlyOneWinner,
                            FencedPutRefusesAStaleEpochAndKeepsTheNewerRecord,
                            ListIsRecursiveFilesOnlyAndHidesStoreMechanism,
                            RemoveIsIdempotent);

}  // namespace clink::test
