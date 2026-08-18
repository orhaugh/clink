// The coordination-record store (clink/cluster/coordination_store.hpp).
//
// The semantic contract every implementation must pass lives in
// test_helpers/coordination_store_contract.hpp and is instantiated here
// against the filesystem store (the S3 store instantiates it in
// impls/s3/tests). This file keeps what is filesystem- and registry-
// specific: scheme dispatch, per-root caching, and the golden layout - the
// store must produce EXACTLY the paths and bytes the pre-seam helpers
// produced, so a deployment migrated onto the seam reads its own history.

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cluster/coordination_store.hpp"
#include "clink/cluster/in_doubt_resolution.hpp"

#include "test_helpers/coordination_store_contract.hpp"

namespace {

using clink::cluster::make_coordination_store;

std::filesystem::path test_root() {
    return std::filesystem::temp_directory_path() /
           ("clink_coordstore_" + std::to_string(::getpid()) + "_" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
}

struct FilesystemProvider {
    static std::string Unavailable() { return {}; }

    static std::shared_ptr<clink::cluster::CoordinationStore> Make() {
        const auto root = test_root();
        std::filesystem::remove_all(root);
        std::filesystem::create_directories(root);
        return make_coordination_store(root.string());
    }

    static void Cleanup() {
        std::error_code ec;
        std::filesystem::remove_all(test_root(), ec);
    }
};

}  // namespace

// The typed-suite macro pastes the suite name into generated identifiers
// and looks its registration namespace up unqualified, so the contract's
// namespace must be visible here.
using namespace clink::test;  // NOLINT(google-build-using-namespace)
INSTANTIATE_TYPED_TEST_SUITE_P(Filesystem, CoordinationStoreContract, FilesystemProvider);

namespace {

// -- filesystem- and registry-specific behaviour below --------------------

struct CoordinationStoreTest : ::testing::Test {
    void SetUp() override {
        root = test_root().string();
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

TEST_F(CoordinationStoreTest, ListHidesLockFiles) {
    store->put("_jobs/7/COMPLETED-4", "a");
    (void)store->fenced_put(
        "_jobs/7/COMPLETED-5", "epoch=1\n", 1, clink::test::contract_epoch_of, "test");
    // The fenced write leaves a .wlock beside the record; it is store
    // mechanism, not a record, and must never surface as a key.
    const auto keys = store->list("_jobs/7");
    for (const auto& key : keys) {
        EXPECT_EQ(key.find(".wlock"), std::string::npos) << key;
    }
    EXPECT_EQ(keys.size(), 2u);
}

// The golden layout: byte-identical paths and bodies to the pre-seam
// helpers, so a mixed fleet (plugin-side writes still direct in W1) stays
// coherent.
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
