// A RocksDB-backed checkpoint, kept outside the checkpoint directory in the
// backend's own <subtask>.cp-<id> directories, exported as a checkpoint dataset
// (clink/state_processor/checkpoint_dataset.hpp) through the RocksDB reader a
// RocksDB-linked process registers.

#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <unistd.h>

#include <gtest/gtest.h>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/state_location.hpp"
#include "clink/incident/bundle.hpp"
#include "clink/state/rocksdb_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/checkpoint_dataset.hpp"

namespace fs = std::filesystem;
namespace sp = clink::state_processor;

namespace {

constexpr std::uint64_t kRocksJob = 21;

std::string keyed_key(std::uint8_t kg, const std::string& user_key) {
    return std::string(1, static_cast<char>(kg)) + "counts|" + user_key;
}

class RocksDbDatasetTest : public ::testing::Test {
protected:
    fs::path root;
    fs::path ckpt;
    std::string state_root;

    void SetUp() override {
        if (!clink::RocksDBStateBackend::is_real_implementation()) {
            GTEST_SKIP() << "built without RocksDB";
        }
        root = fs::temp_directory_path() /
               ("clink_rocksdb_dataset_" + std::to_string(::getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name());
        fs::remove_all(root);
        ckpt = root / "ckpt";
        fs::create_directories(ckpt);
        state_root = (root / "rocks" / "_state" / std::to_string(kRocksJob)).string();
        clink::incident::register_directory_state_reader("rocksdb", [](const fs::path& dir) {
            return clink::rocksdb_checkpoint_to_arrow(dir.string());
        });
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(root, ec);
    }

    // Two subtasks' RocksDB checkpoints of checkpoint 1, and its marker.
    void write_checkpoint() const {
        for (std::uint32_t sub = 0; sub < 2; ++sub) {
            fs::create_directories(
                fs::path{clink::state_dir_for(state_root, 1, sub)}.parent_path());
            clink::RocksDBStateBackend b(clink::RocksDBStateBackend::Options{
                .path = clink::state_dir_for(state_root, 1, sub)});
            const std::uint8_t kg = sub == 0 ? 3 : 70;
            for (int i = 0; i < 50; ++i) {
                b.put(clink::OperatorId{42},
                      keyed_key(kg, "k" + std::to_string(sub) + "-" + std::to_string(i)),
                      "v" + std::to_string(i));
            }
            (void)b.snapshot(clink::CheckpointId{1});
        }
        const auto dir = clink::cluster::completed_marker_dir_for(ckpt.string(), kRocksJob);
        fs::create_directories(dir);
        std::ofstream(dir / "COMPLETED-1") << clink::cluster::completed_marker_body(
            kRocksJob, 1, 1, {0, 1}, "rocksdb://" + state_root);
    }

    [[nodiscard]] sp::DatasetOptions options(const std::string& out) const {
        sp::DatasetOptions o;
        o.checkpoint_dir = ckpt.string();
        o.job = kRocksJob;
        o.commit_confirmation = false;
        o.checkpoint = 1;
        o.out = (root / out).string();
        return o;
    }
};

TEST_F(RocksDbDatasetTest, ARocksDbCheckpointOutsideTheCheckpointDirectoryExportsComplete) {
    write_checkpoint();
    const auto r = sp::export_checkpoint_dataset(options("ds"));
    EXPECT_TRUE(r.complete);
    EXPECT_EQ(r.entries, 100U);
    const auto d = sp::open_dataset(root / "ds");
    std::set<std::string> keys;
    sp::for_each_entry(d, [&](const sp::DatasetEntry& e) {
        EXPECT_EQ(e.slot, "counts");
        keys.insert(e.user_key);
        return true;
    });
    EXPECT_EQ(keys.size(), 100U);
    // The bundle holds the checkpoint directories it was read from, under _external.
    EXPECT_TRUE(fs::exists(root / "ds" / "checkpoint" / "_external" / std::to_string(kRocksJob) /
                           "v1" / "0.cp-1" / "CURRENT"));
    // And incident-verify's checks read it the same way.
    const auto b = clink::incident::read_bundle(root / "ds");
    clink::incident::VerifyOptions vo;
    for (const auto& c : clink::incident::verify_core(b, vo)) {
        if (c.id == "state.decode" || c.id == "checkpoint.participants") {
            EXPECT_EQ(c.verdict, clink::incident::Verdict::Passed) << c.id << ": " << c.reason;
        }
    }
}

TEST_F(RocksDbDatasetTest, ADamagedRocksDbCheckpointIsRefused) {
    write_checkpoint();
    // Truncate every table file of subtask 1's checkpoint.
    for (const auto& e : fs::directory_iterator(clink::state_dir_for(state_root, 1, 1) + ".cp-1")) {
        if (e.path().extension() == ".sst") {
            fs::resize_file(e.path(), fs::file_size(e.path()) / 2);
        }
    }
    try {
        (void)sp::export_checkpoint_dataset(options("ds"));
        FAIL() << "a damaged RocksDB checkpoint exported";
    } catch (const sp::DatasetRefused& e) {
        EXPECT_EQ(e.kind, "incomplete") << e.what();
    }
    EXPECT_FALSE(fs::exists(root / "ds"));
}

}  // namespace
