// The coordination-record store (clink/cluster/coordination_store.hpp):
// the seam every durable control-plane record goes through. The filesystem
// implementation must behave exactly like the helpers it replaced - same
// paths, same bytes, same fencing - and the contract here is written to be
// re-run verbatim against an object-store implementation (W2 of the
// coordination-store plan), so it asserts semantics, never inode details.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordination_store.hpp"
#include "clink/cluster/in_doubt_resolution.hpp"

namespace {

using clink::cluster::make_coordination_store;

std::uint64_t epoch_of(const std::string& body) {
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

struct CoordinationStoreTest : ::testing::Test {
    void SetUp() override {
        root = (std::filesystem::temp_directory_path() /
                ("clink_coordstore_" + std::to_string(::getpid()) + "_" +
                 ::testing::UnitTest::GetInstance()->current_test_info()->name()))
                   .string();
        std::filesystem::remove_all(root);
        std::filesystem::create_directories(root);
        store = make_coordination_store(root);
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(root, ec);
    }

    std::string root;
    std::shared_ptr<clink::cluster::CoordinationStore> store;
};

TEST_F(CoordinationStoreTest, PutGetExistsRoundTrip) {
    EXPECT_FALSE(store->exists("_jobs/7/COMPLETED-4"));
    EXPECT_FALSE(store->get("_jobs/7/COMPLETED-4").has_value());
    store->put("_jobs/7/COMPLETED-4", "job=7\ncheckpoint=4\n");
    EXPECT_TRUE(store->exists("_jobs/7/COMPLETED-4"));
    EXPECT_EQ(store->get("_jobs/7/COMPLETED-4").value(), "job=7\ncheckpoint=4\n");
}

TEST_F(CoordinationStoreTest, PutIfAbsentCreatesOnceAndPreservesTheFirstBody) {
    EXPECT_TRUE(store->put_if_absent("k", "first"));
    EXPECT_FALSE(store->put_if_absent("k", "second"));
    EXPECT_EQ(store->get("k").value(), "first");
}

TEST_F(CoordinationStoreTest, ConcurrentPutIfAbsentHasExactlyOneWinner) {
    std::atomic<int> wins{0};
    std::vector<std::thread> writers;
    writers.reserve(8);
    for (int i = 0; i < 8; ++i) {
        writers.emplace_back([&, i] {
            if (store->put_if_absent("contended", "writer-" + std::to_string(i))) {
                wins.fetch_add(1);
            }
        });
    }
    for (auto& t : writers) {
        t.join();
    }
    EXPECT_EQ(wins.load(), 1);
    const auto body = store->get("contended");
    ASSERT_TRUE(body.has_value());
    EXPECT_EQ(body->rfind("writer-", 0), 0u);
}

TEST_F(CoordinationStoreTest, FencedPutRefusesAStaleEpochAndKeepsTheNewerRecord) {
    EXPECT_TRUE(store->fenced_put("jobs/1/manifest.json", "epoch=5\n", 5, epoch_of, "test"));
    // A superseded writer must not clobber the leader's record.
    EXPECT_FALSE(store->fenced_put("jobs/1/manifest.json", "epoch=3\n", 3, epoch_of, "test"));
    EXPECT_EQ(store->get("jobs/1/manifest.json").value(), "epoch=5\n");
    // A newer writer replaces.
    EXPECT_TRUE(store->fenced_put("jobs/1/manifest.json", "epoch=9\n", 9, epoch_of, "test"));
    EXPECT_EQ(store->get("jobs/1/manifest.json").value(), "epoch=9\n");
}

TEST_F(CoordinationStoreTest, ListIsRecursiveFilesOnlyAndHidesLockFiles) {
    store->put("_jobs/7/COMPLETED-4", "a");
    store->put("_jobs/7/receipts/sub0-4", "wm=100\n");
    store->put("_jobs/7/receipts/sub1-4", "wm=100\n");
    (void)store->fenced_put("_jobs/7/COMPLETED-5", "epoch=1\n", 1, epoch_of, "test");
    auto keys = store->list("_jobs/7");
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys,
              (std::vector<std::string>{"_jobs/7/COMPLETED-4",
                                        "_jobs/7/COMPLETED-5",
                                        "_jobs/7/receipts/sub0-4",
                                        "_jobs/7/receipts/sub1-4"}))
        << "recursive keys, files only, no .wlock mechanism files";
    EXPECT_TRUE(store->list("_jobs/nothing-here").empty());
}

TEST_F(CoordinationStoreTest, RemoveIsIdempotent) {
    store->put("r", "x");
    store->remove("r");
    EXPECT_FALSE(store->exists("r"));
    store->remove("r");  // absent: not an error
}

TEST_F(CoordinationStoreTest, StoresAreCachedPerRootAndUnknownSchemesRefuse) {
    EXPECT_EQ(make_coordination_store(root).get(), store.get())
        << "same root must resolve to the same instance";
    try {
        (void)make_coordination_store("nonsuch://bucket/prefix");
        FAIL() << "an unregistered scheme must throw, never silently coordinate elsewhere";
    } catch (const std::runtime_error& e) {
        EXPECT_NE(std::string(e.what()).find("nonsuch"), std::string::npos)
            << "the refusal must name the scheme: " << e.what();
    }
}

TEST_F(CoordinationStoreTest, RegisteredSchemesDispatchThroughTheRegistry) {
    clink::cluster::register_coordination_store_scheme(
        "coordtest", [&](const std::string&) { return make_coordination_store(root); });
    auto via_scheme = make_coordination_store("coordtest://anything");
    via_scheme->put("through-scheme", "y");
    EXPECT_TRUE(store->exists("through-scheme"));
}

// The golden layout: the store must produce EXACTLY the paths and bytes the
// pre-seam helpers produced, so a deployment migrated onto the seam reads
// its own history and a mixed fleet (plugin-side writes still direct in W1)
// stays coherent.
TEST_F(CoordinationStoreTest, FilesystemLayoutIsByteIdenticalToTheLegacyHelpers) {
    store->put("_jobs/7/COMPLETED-4", "job=7\ncheckpoint=4\ngeneration=1\nsubtasks=0\n");
    store->put("_jobs/7/receipts/sub0-4", "wm=12345\n");

    const auto marker_path = clink::cluster::completed_marker_dir_for(root, 7) / "COMPLETED-4";
    const auto receipt_path = clink::cluster::commit_receipt_dir_for(root, 7) / "sub0-4";
    ASSERT_TRUE(std::filesystem::exists(marker_path))
        << "store key _jobs/7/COMPLETED-4 must land where completed_marker_dir_for points";
    ASSERT_TRUE(std::filesystem::exists(receipt_path));

    std::ifstream in(marker_path);
    std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(body, "job=7\ncheckpoint=4\ngeneration=1\nsubtasks=0\n");
}

}  // namespace
