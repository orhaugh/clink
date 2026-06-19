// Tests for the RocksDB-backed materialization store used by
// ChangelogStateBackend in changelog+rocksdb:// mode.
//
// Exercises:
//   * write + read round-trip for a single handle
//   * erase removes the handle
//   * ChangelogStateBackend over (InMemory inner + RocksDb store)
//     produces a small snapshot blob (just the handle) and restores
//     correctly through a fresh store opened at the same path.
//   * StateBackendFactory "changelog+rocksdb://" scheme builds a
//     working backend (relies on clink::rocksdb::install() running
//     via the register.cpp test bootstrap).

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#if __has_include("clink/rocksdb/rocksdb_materialization_store.hpp")
#include "clink/rocksdb/rocksdb_materialization_store.hpp"
#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string to_string(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    for (std::size_t i = 0; i < v.size(); ++i) {
        out[i] = static_cast<char>(v[i]);
    }
    return out;
}

std::filesystem::path tmp_dir(const std::string& tag) {
    auto p = std::filesystem::temp_directory_path() /
             ("clink_rocksdb_mat_" + tag + "_" +
              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(p);
    return p;
}

}  // namespace

TEST(RocksDbMaterializationStore, WriteReadRoundTrip) {
    const auto dir = tmp_dir("rt");
    clink::rocksdb::RocksDbMaterializationStore store(dir);

    std::vector<std::byte> payload(256);
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<std::byte>(i & 0xFF);
    }
    auto handle =
        store.write(CheckpointId{17}, std::span<const std::byte>{payload.data(), payload.size()});
    EXPECT_FALSE(handle.empty());

    auto got = store.read(handle);
    EXPECT_EQ(got, payload);

    std::filesystem::remove_all(dir);
}

TEST(RocksDbMaterializationStore, EraseRemovesHandle) {
    const auto dir = tmp_dir("erase");
    clink::rocksdb::RocksDbMaterializationStore store(dir);

    std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}};
    auto handle =
        store.write(CheckpointId{5}, std::span<const std::byte>{payload.data(), payload.size()});
    EXPECT_FALSE(store.read(handle).empty());

    store.erase(handle);
    EXPECT_THROW(store.read(handle), std::runtime_error);

    std::filesystem::remove_all(dir);
}

TEST(RocksDbMaterializationStore, ChangelogBackendRoundTripsViaStore) {
    const auto dir = tmp_dir("clog_rt");
    Snapshot snap;
    {
        auto store_a = std::make_shared<clink::rocksdb::RocksDbMaterializationStore>(dir);
        ChangelogStateBackend producer(std::make_shared<InMemoryStateBackend>(), store_a);

        for (int i = 0; i < 100; ++i) {
            producer.put(
                OperatorId{2}, sv("k" + std::to_string(i)), sv("payload_" + std::to_string(i)));
        }
        producer.set_materialization_threshold_bytes(64);
        snap = producer.snapshot(CheckpointId{1});

        // Snapshot blob is much smaller than the inner state would be
        // on its own - materialization payload lives in RocksDB.
        EXPECT_LT(snap.bytes.size(), 1000u);
        // producer + store_a go out of scope here, releasing
        // RocksDB's LOCK so the restore-side opener can acquire it.
    }
    {
        auto store_b = std::make_shared<clink::rocksdb::RocksDbMaterializationStore>(dir);
        ChangelogStateBackend restored(std::make_shared<InMemoryStateBackend>(), store_b);
        restored.restore(snap);

        auto v = restored.get(OperatorId{2}, sv(std::string{"k42"}));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(to_string(*v), "payload_42");
    }
    std::filesystem::remove_all(dir);
}

TEST(RocksDbMaterializationStore, FactoryChangelogRocksdbBuildsRocksDbInnerWithFileMat) {
    // changelog+rocksdb:// is the production-grade large-state config:
    // RocksDB inner backend (state can exceed RAM) + file-based
    // materialization store (snapshots stay small as the inner grows).
    const auto dir = tmp_dir("factory");
    StateBackendSpec spec;
    spec.uri = "changelog+rocksdb://" + dir.string();
    spec.subtask_idx = 0;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    const auto desc = built.backend->description();
    EXPECT_NE(desc.find("changelog"), std::string::npos);
    // Inner is RocksDB.
    EXPECT_NE(desc.find("rocksdb state backend at"), std::string::npos) << desc;
    // External store is file-based.
    EXPECT_NE(desc.find("file://"), std::string::npos) << desc;

    built.backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));
    auto v = built.backend->get(OperatorId{1}, sv(std::string{"k"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(to_string(*v), "v");

    // Inner RocksDB directory must exist under <dir>/<subtask>/inner.
    EXPECT_TRUE(std::filesystem::exists(dir / "0" / "inner"));

    // Release the built backend before removing the dir (RocksDB holds
    // a LOCK file under it).
    built.backend.reset();
    std::filesystem::remove_all(dir);
}

TEST(RocksDbMaterializationStore, FactoryChangelogRocksdbRoundTripsThroughRocksDbInner) {
    // Large-state round-trip: 1000 entries through RocksDB inner,
    // snapshot triggers materialization to a file, restore at the same
    // path picks up the state correctly. RocksDB's LOCK requires
    // releasing the first backend before opening the second.
    const auto dir = tmp_dir("rt_rocks_inner");
    Snapshot snap;
    {
        StateBackendSpec spec;
        spec.uri = "changelog+rocksdb://" + dir.string();
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        ASSERT_NE(built.backend, nullptr);
        auto* clog = dynamic_cast<ChangelogStateBackend*>(built.backend.get());
        ASSERT_NE(clog, nullptr);
        clog->set_materialization_threshold_bytes(64);

        for (int i = 0; i < 1000; ++i) {
            built.backend->put(
                OperatorId{2}, sv("k" + std::to_string(i)), sv("payload_" + std::to_string(i)));
        }
        snap = built.backend->snapshot(CheckpointId{7});

        // Snapshot blob stays small - it carries only the handle
        // string pointing at the mat file. The mat file itself, in
        // the RocksDB case, carries the RocksDB checkpoint path
        // (RocksDB snapshots are hard-linked SST directories, not
        // packed byte payloads); the bulk of the materialized state
        // lives in the checkpoint dir on disk, NOT the mat file.
        EXPECT_LT(snap.bytes.size(), 2000u);
        const auto mat_file = dir / "0" / "mat" / "mat-7.bin";
        ASSERT_TRUE(std::filesystem::exists(mat_file));
        EXPECT_GT(std::filesystem::file_size(mat_file), 0u);
        // RocksDB writes its checkpoint into <inner>.cp-<id>; verify
        // it exists and has SST files (proves we exercised the real
        // RocksDB incremental-checkpoint path, not a stub).
        const auto cp_dir = dir / "0" / "inner.cp-7";
        ASSERT_TRUE(std::filesystem::exists(cp_dir));
        bool any_sst = false;
        for (const auto& e : std::filesystem::directory_iterator(cp_dir)) {
            const auto n = e.path().filename().string();
            if (n.size() >= 4 && n.compare(n.size() - 4, 4, ".sst") == 0) {
                any_sst = true;
                break;
            }
        }
        EXPECT_TRUE(any_sst) << "expected at least one SST in the RocksDB checkpoint";

        // Verify a sample key reads back correctly via the inner.
        auto v = built.backend->get(OperatorId{2}, sv(std::string{"k500"}));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(to_string(*v), "payload_500");
    }
    {
        StateBackendSpec spec;
        spec.uri = "changelog+rocksdb://" + dir.string();
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        ASSERT_NE(built.backend, nullptr);
        built.backend->restore(snap);
        auto v = built.backend->get(OperatorId{2}, sv(std::string{"k500"}));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(to_string(*v), "payload_500");
    }
    std::filesystem::remove_all(dir);
}

// Cross-process restore through the FACTORY: build_changelog_rocksdb must
// self-persist the framing blob (scope 1) and wire restore_from to it
// (scope 2), so a fresh process recovers without keeping the in-RAM
// Snapshot. Exercises the deepest path: blob + mat-<id>.bin + inner.cp-<id>.
TEST(RocksDbMaterializationStore, FactoryChangelogRocksdbRestoreSetsRestoreFromAndRecovers) {
    const auto dir = tmp_dir("rt_rocks_restore_factory");
    {
        StateBackendSpec spec;
        spec.uri = "changelog+rocksdb://" + dir.string();
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        ASSERT_NE(built.backend, nullptr);
        EXPECT_FALSE(built.restore_from.has_value());
        auto* clog = dynamic_cast<ChangelogStateBackend*>(built.backend.get());
        ASSERT_NE(clog, nullptr);
        clog->set_materialization_threshold_bytes(64);  // force a materialization
        for (int i = 0; i < 1000; ++i) {
            built.backend->put(
                OperatorId{2}, sv("k" + std::to_string(i)), sv("payload_" + std::to_string(i)));
        }
        (void)built.backend->snapshot(CheckpointId{7});
    }
    // The three artifacts restore depends on must all be on disk.
    EXPECT_TRUE(std::filesystem::exists(dir / "0" / "changelog-7.snap"));
    EXPECT_TRUE(std::filesystem::exists(dir / "0" / "mat" / "mat-7.bin"));
    EXPECT_TRUE(std::filesystem::exists(dir / "0" / "inner.cp-7"));

    StateBackendSpec spec;
    spec.uri = "changelog+rocksdb://" + dir.string();
    spec.subtask_idx = 0;
    spec.restore_uri = "changelog+rocksdb://" + dir.string();
    spec.restore_checkpoint_id = 7;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value()) << "build_changelog_rocksdb must wire restore_from";
    built.backend->restore(*built.restore_from);
    auto v = built.backend->get(OperatorId{2}, sv(std::string{"k500"}));
    ASSERT_TRUE(v.has_value()) << "state lost on restart - the gap this closes";
    EXPECT_EQ(to_string(*v), "payload_500");

    std::filesystem::remove_all(dir);
}

// Scale-up rescale through the factory: a new subtask reads its assigned
// parent's blob, narrows to its key-group slice, and keeps operator state.
// Materialization is forced so the keyed state is rehydrated through the inner
// RocksDB restore (which honours the filter + the op-state exemption).
TEST(RocksDbMaterializationStore, FactoryChangelogRocksdbScaleUpNarrows) {
    const auto root = tmp_dir("rt_rocks_scaleup");
    std::filesystem::create_directories(root);
    const std::string run1 = "changelog+rocksdb://" + (root / "run1").string();
    const std::string run2 = "changelog+rocksdb://" + (root / "run2").string();
    const OperatorId op{9};
    std::string key_lo;
    key_lo.push_back(static_cast<char>(5));  // key group 5
    key_lo += "lo";
    std::string key_hi;
    key_hi.push_back(static_cast<char>(112));  // key group 112
    key_hi += "hi";

    {
        StateBackendSpec spec;
        spec.uri = run1;
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        auto* clog = dynamic_cast<ChangelogStateBackend*>(built.backend.get());
        ASSERT_NE(clog, nullptr);
        clog->set_materialization_threshold_bytes(0);  // force materialization
        built.backend->put(op, sv(key_lo), sv(std::string{"LO"}));
        built.backend->put(op, sv(key_hi), sv(std::string{"HI"}));
        built.backend->put_operator_state(op, sv(std::string{"offsets"}), sv(std::string{"OFF"}));
        (void)built.backend->snapshot(CheckpointId{7});
    }

    StateBackendSpec spec;
    spec.uri = run2;
    spec.subtask_idx = 0;
    spec.restore_uri = run1;
    spec.restore_checkpoint_id = 7;
    spec.restore_from_subtask_idx = 0;   // scale-up: read parent subtask 0
    spec.restore_from_parent_count = 1;  // single parent
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_changelog_rocksdb must wire scale-up restore_from";
    built.backend->restore(*built.restore_from, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});

    EXPECT_TRUE(built.backend->get(op, sv(key_lo)).has_value()) << "in-range keyed kept";
    EXPECT_FALSE(built.backend->get(op, sv(key_hi)).has_value()) << "out-of-range keyed dropped";
    auto off = built.backend->get_operator_state(op, sv(std::string{"offsets"}));
    ASSERT_TRUE(off.has_value()) << "operator state must survive scale-up";
    EXPECT_EQ(to_string(*off), "OFF");

    std::filesystem::remove_all(root);
}

// Scale-down: one new subtask inherits SEVERAL parents. The factory frames all
// parents' blobs; restore() merges their materializations (the inner RocksDB
// folds the parents' checkpoint dirs via combine_snapshots) and replays all
// logs. The deepest path - blob + mat + inner.cp - for every parent.
TEST(RocksDbMaterializationStore, FactoryChangelogRocksdbScaleDownMergesParents) {
    const auto root = tmp_dir("rt_rocks_scaledown");
    std::filesystem::create_directories(root);
    const std::string run1 = "changelog+rocksdb://" + (root / "run1").string();
    const std::string run2 = "changelog+rocksdb://" + (root / "run2").string();
    const OperatorId op{11};
    std::string key_a;
    key_a.push_back(static_cast<char>(10));  // key group 10
    key_a += "a";
    std::string key_b;
    key_b.push_back(static_cast<char>(100));  // key group 100
    key_b += "b";

    auto write_parent = [&](std::uint32_t sub,
                            const std::string& key,
                            const std::string& val,
                            const std::string& off_key,
                            const std::string& off_val) {
        StateBackendSpec spec;
        spec.uri = run1;
        spec.subtask_idx = sub;
        auto built = StateBackendFactory::default_instance().build(spec);
        auto* clog = dynamic_cast<ChangelogStateBackend*>(built.backend.get());
        ASSERT_NE(clog, nullptr);
        clog->set_materialization_threshold_bytes(0);  // force materialization
        built.backend->put(op, sv(key), sv(val));
        built.backend->put_operator_state(op, sv(off_key), sv(off_val));
        (void)built.backend->snapshot(CheckpointId{7});
    };
    write_parent(0, key_a, "A", "off0", "O0");
    write_parent(1, key_b, "B", "off1", "O1");

    StateBackendSpec spec;
    spec.uri = run2;
    spec.subtask_idx = 0;
    spec.restore_uri = run1;
    spec.restore_checkpoint_id = 7;
    spec.restore_from_subtask_idx = 0;   // first parent
    spec.restore_from_parent_count = 2;  // inherit parents 0 and 1
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_changelog_rocksdb must wire scale-down restore_from";
    built.backend->restore(*built.restore_from,
                           KeyGroupRange{KeyGroup{0}, KeyGroup{kNumKeyGroups}});

    auto va = built.backend->get(op, sv(key_a));
    auto vb = built.backend->get(op, sv(key_b));
    ASSERT_TRUE(va.has_value());
    ASSERT_TRUE(vb.has_value());
    EXPECT_EQ(to_string(*va), "A");
    EXPECT_EQ(to_string(*vb), "B");
    auto o0 = built.backend->get_operator_state(op, sv(std::string{"off0"}));
    auto o1 = built.backend->get_operator_state(op, sv(std::string{"off1"}));
    ASSERT_TRUE(o0.has_value());
    ASSERT_TRUE(o1.has_value());
    EXPECT_EQ(to_string(*o0), "O0");
    EXPECT_EQ(to_string(*o1), "O1");

    std::filesystem::remove_all(root);
}

#endif  // __has_include
