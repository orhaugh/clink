// Changelog State Backend tests. Exercises:
//
//   * put/erase/get round-trip via the wrapper
//   * Snapshot includes log entries, restore replays them
//   * Materialization clears the log and reduces snapshot size
//   * Snapshot+restore preserves state across "process restart"
//   * StateBackendFactory "changelog" scheme builds a wrapped backend

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/state/changelog_state_backend.hpp"
#include "clink/state/file_materialization_store.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

using namespace clink;

namespace {

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

std::string str(const StateBackend::Value& v) {
    std::string out(v.size(), '\0');
    for (std::size_t i = 0; i < v.size(); ++i) {
        out[i] = static_cast<char>(v[i]);
    }
    return out;
}

}  // namespace

// In-RAM changelog (no working dir) has no durable write to defer, so it
// stays synchronous - the runner must not route it through the worker.
TEST(ChangelogBackend, InRamDoesNotSupportAsyncPersist) {
    ChangelogStateBackend backend;
    EXPECT_FALSE(backend.supports_async_persist());
}

// Disk-backed changelog opts into the async split: snapshot() = capture()
// (serialise under the log lock) + persist() (write the framing blob), so
// the durable write moves off the operator thread. capture()+persist() must
// produce the same restorable blob as the fused snapshot().
TEST(ChangelogBackend, DiskBackedAsyncCapturePersistRoundTrips) {
    const auto dir = std::filesystem::temp_directory_path() / "clink_changelog_async_rt";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    ChangelogStateBackend backend;
    backend.set_snapshot_dir(dir);
    EXPECT_TRUE(backend.supports_async_persist());

    backend.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"1"}));
    backend.put(OperatorId{1}, sv(std::string{"b"}), sv(std::string{"2"}));

    // Split path: capture on the op thread, persist off-thread. Mutating
    // live state after capture must not change the captured blob.
    auto handle = backend.capture(CheckpointId{5});
    EXPECT_EQ(handle.checkpoint_id.value(), 5u);
    backend.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"MUTATED"}));
    const auto snap = backend.persist(std::move(handle));
    EXPECT_EQ(snap.checkpoint_id.value(), 5u);
    EXPECT_TRUE(std::filesystem::exists(dir / "changelog-5.snap"));

    // Restore the async-written blob into a fresh disk-backed backend: it
    // must reflect state at capture time, not the post-capture mutation.
    ChangelogStateBackend reader;
    reader.set_snapshot_dir(dir);
    reader.restore(snap);
    auto a = reader.get(OperatorId{1}, sv(std::string{"a"}));
    auto b = reader.get(OperatorId{1}, sv(std::string{"b"}));
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(str(*a), "1");
    EXPECT_EQ(str(*b), "2");

    std::filesystem::remove_all(dir);
}

TEST(ChangelogBackend, PutGetEraseRoundTrip) {
    ChangelogStateBackend backend;
    backend.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"hello"}));
    backend.put(OperatorId{1}, sv(std::string{"b"}), sv(std::string{"world"}));
    backend.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"hi"}));  // overwrite

    auto a = backend.get(OperatorId{1}, sv(std::string{"a"}));
    auto b = backend.get(OperatorId{1}, sv(std::string{"b"}));
    auto c = backend.get(OperatorId{1}, sv(std::string{"c"}));
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(str(*a), "hi");
    EXPECT_EQ(str(*b), "world");
    EXPECT_FALSE(c.has_value());

    backend.erase(OperatorId{1}, sv(std::string{"a"}));
    EXPECT_FALSE(backend.get(OperatorId{1}, sv(std::string{"a"})).has_value());

    // With log compaction (the default), repeat mutations of the
    // same (op, key) coalesce in place. Two distinct keys touched
    // here (a, b) → 2 entries: erase(a) and put(b).
    EXPECT_EQ(backend.log_entries(), 2u);
}

TEST(ChangelogBackend, SnapshotIncludesLogEntriesAndRestoreReplaysThem) {
    ChangelogStateBackend backend;
    backend.put(OperatorId{5}, sv(std::string{"k1"}), sv(std::string{"v1"}));
    backend.put(OperatorId{5}, sv(std::string{"k2"}), sv(std::string{"v2"}));
    backend.erase(OperatorId{5}, sv(std::string{"k1"}));

    // With compaction: put(k1) is replaced by erase(k1); put(k2)
    // stands. Final log: 2 entries (erase k1, put k2). The replayed
    // semantics - k1 missing, k2 = "v2" - match what the test
    // asserts below after restore.
    EXPECT_EQ(backend.log_entries(), 2u);

    auto snap = backend.snapshot(CheckpointId{1});
    EXPECT_FALSE(snap.bytes.empty());

    // Restore into a fresh backend - must end up with only k2 = "v2".
    ChangelogStateBackend fresh;
    fresh.restore(snap);

    EXPECT_FALSE(fresh.get(OperatorId{5}, sv(std::string{"k1"})).has_value());
    auto v2 = fresh.get(OperatorId{5}, sv(std::string{"k2"}));
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(str(*v2), "v2");
}

TEST(ChangelogBackend, ManualMaterializationClearsLog) {
    ChangelogStateBackend backend;
    for (int i = 0; i < 100; ++i) {
        backend.put(OperatorId{1}, sv("k" + std::to_string(i)), sv(std::string{"v"}));
    }
    EXPECT_EQ(backend.log_entries(), 100u);

    backend.materialize_now();
    EXPECT_EQ(backend.log_entries(), 0u);

    // After materialization, snapshot should still contain everything
    // (it's in the materialization, not the log).
    auto snap = backend.snapshot(CheckpointId{1});

    ChangelogStateBackend fresh;
    fresh.restore(snap);
    auto v = fresh.get(OperatorId{1}, sv(std::string{"k50"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(str(*v), "v");
}

TEST(ChangelogBackend, AutoMaterializationFiresOnSnapshot) {
    ChangelogStateBackend backend;
    backend.set_materialization_threshold_bytes(64);  // tiny - auto-fires fast

    // Put enough entries to exceed the threshold.
    for (int i = 0; i < 50; ++i) {
        backend.put(
            OperatorId{1}, sv("key_" + std::to_string(i)), sv("value_" + std::to_string(i)));
    }
    EXPECT_GE(backend.log_bytes_estimate(), 64u);

    auto snap = backend.snapshot(CheckpointId{1});
    // After snapshot, the auto-materialization should have fired and
    // the log is now empty.
    EXPECT_EQ(backend.log_entries(), 0u);

    // Snapshot still round-trips correctly.
    ChangelogStateBackend fresh;
    fresh.restore(snap);
    auto v = fresh.get(OperatorId{1}, sv(std::string{"key_42"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(str(*v), "value_42");
}

TEST(ChangelogBackend, IncrementalLogIsSmallerThanFullMaterialization) {
    // Demonstrates the architectural property: with materialization
    // not triggered, a snapshot of N mutations after a clean start
    // is roughly proportional to the log entry bytes, not the
    // full-inner-state bytes.
    ChangelogStateBackend backend;
    backend.set_materialization_threshold_bytes(
        std::numeric_limits<std::size_t>::max());  // never auto-materialize

    // Seed the inner state with 1000 entries, then materialize so
    // they live in the materialization snapshot.
    for (int i = 0; i < 1000; ++i) {
        backend.put(OperatorId{1},
                    sv("base_" + std::to_string(i)),
                    sv(std::string(64, 'x')));  // 64-byte values
    }
    backend.materialize_now();

    auto baseline = backend.snapshot(CheckpointId{1});
    const auto baseline_size = baseline.bytes.size();

    // Make a tiny mutation.
    backend.put(OperatorId{1}, sv(std::string{"delta"}), sv(std::string{"new"}));
    auto with_log = backend.snapshot(CheckpointId{2});

    // The snapshot with the log delta should be only a few dozen
    // bytes larger than the baseline (key + value + headers).
    EXPECT_LT(with_log.bytes.size() - baseline_size, 100u)
        << "delta snapshot should be tiny vs baseline";
    EXPECT_GT(with_log.bytes.size(), baseline_size);
}

TEST(ChangelogBackend, LogCompactionReplacesEntriesForSameKey) {
    // 1000 puts to the same key should compress to a single log
    // entry (the latest value). Same after a put-then-erase
    // sequence - the trailing erase wins, in place.
    ChangelogStateBackend backend;
    backend.set_materialization_threshold_bytes(std::numeric_limits<std::size_t>::max());
    for (int i = 0; i < 1000; ++i) {
        backend.put(OperatorId{1}, sv(std::string{"k"}), sv("v" + std::to_string(i)));
    }
    EXPECT_EQ(backend.log_entries(), 1u);

    backend.erase(OperatorId{1}, sv(std::string{"k"}));
    EXPECT_EQ(backend.log_entries(), 1u);

    backend.put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"final"}));
    EXPECT_EQ(backend.log_entries(), 1u);

    // Restore round-trip preserves the compacted state.
    auto snap = backend.snapshot(CheckpointId{1});
    ChangelogStateBackend fresh;
    fresh.restore(snap);
    auto v = fresh.get(OperatorId{1}, sv(std::string{"k"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(str(*v), "final");
}

TEST(ChangelogBackend, LogCompactionCanBeDisabled) {
    // When disabled, every mutation appends a new log entry. Useful
    // for workloads with distinct keys (compaction is pure overhead).
    ChangelogStateBackend backend;
    backend.set_log_compaction_enabled(false);
    backend.set_materialization_threshold_bytes(std::numeric_limits<std::size_t>::max());
    for (int i = 0; i < 5; ++i) {
        backend.put(OperatorId{1}, sv(std::string{"k"}), sv("v" + std::to_string(i)));
    }
    EXPECT_EQ(backend.log_entries(), 5u);
}

TEST(ChangelogBackend, LogCompactionDistinguishesByOperatorId) {
    // Same key under DIFFERENT op_ids must remain distinct after
    // compaction.
    ChangelogStateBackend backend;
    backend.set_materialization_threshold_bytes(std::numeric_limits<std::size_t>::max());
    backend.put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v1"}));
    backend.put(OperatorId{2}, sv(std::string{"k"}), sv(std::string{"v2"}));
    backend.put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v1b"}));
    // Op 1's two puts collapse to one; op 2 stands separately.
    EXPECT_EQ(backend.log_entries(), 2u);

    auto snap = backend.snapshot(CheckpointId{1});
    ChangelogStateBackend fresh;
    fresh.restore(snap);
    auto v1 = fresh.get(OperatorId{1}, sv(std::string{"k"}));
    auto v2 = fresh.get(OperatorId{2}, sv(std::string{"k"}));
    ASSERT_TRUE(v1.has_value());
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(str(*v1), "v1b");
    EXPECT_EQ(str(*v2), "v2");
}

TEST(ChangelogBackend, FactoryRegistersChangelogScheme) {
    StateBackendSpec spec;
    spec.uri = "changelog://";
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);

    // Verify the built backend behaves like a ChangelogStateBackend:
    // put + get round-trip works, and the description string mentions
    // "changelog".
    built.backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));
    auto v = built.backend->get(OperatorId{1}, sv(std::string{"k"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(str(*v), "v");
    EXPECT_NE(built.backend->description().find("changelog"), std::string::npos);
}

// --------------------------------------------------------------------
// External-materialization-store tests (changelog + file store).
// Covers: in-blob-vs-handle snapshot dispatch, materialization
// payload lives on disk, snapshot blob stays small, restore reads
// from the store and reconstructs the inner state.
// --------------------------------------------------------------------

namespace {

std::filesystem::path tmp_subdir(const std::string& tag) {
    auto p = std::filesystem::temp_directory_path() /
             ("clink_changelog_ext_" + tag + "_" +
              std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(p);
    return p;
}

}  // namespace

TEST(ChangelogBackendExternal, FileStoreSnapshotEmbedsHandleNotPayload) {
    const auto store_dir = tmp_subdir("file_handle");
    auto store = std::make_shared<FileMaterializationStore>(store_dir);
    ChangelogStateBackend backend(std::make_shared<InMemoryStateBackend>(), store);

    // Seed enough state that the materialization payload is
    // meaningfully big - then force materialize. The Snapshot blob
    // should be much smaller than the materialization on disk.
    for (int i = 0; i < 200; ++i) {
        backend.put(OperatorId{1}, sv("k" + std::to_string(i)), sv(std::string(128, 'x')));
    }
    // Use a tiny threshold so the snapshot itself triggers
    // materialization (under the snapshot's checkpoint id).
    backend.set_materialization_threshold_bytes(64);

    auto snap = backend.snapshot(CheckpointId{42});

    // Materialization file exists on disk, named by the snapshot's id.
    const auto mat_file = store_dir / "mat-42.bin";
    ASSERT_TRUE(std::filesystem::exists(mat_file));
    const auto on_disk_size = std::filesystem::file_size(mat_file);
    EXPECT_GT(on_disk_size, 1000u);

    // Snapshot blob carries only the handle string (small). Bound at
    // a comfortable upper limit - the actual handle string is the
    // absolute path; in-blob would be on_disk_size + framing bytes.
    EXPECT_LT(snap.bytes.size(), on_disk_size / 2)
        << "snapshot should be much smaller than the materialization on disk";

    std::filesystem::remove_all(store_dir);
}

TEST(ChangelogBackendExternal, RestoreFetchesFromStoreAndReconstructsState) {
    const auto store_dir = tmp_subdir("file_restore");
    auto store = std::make_shared<FileMaterializationStore>(store_dir);
    ChangelogStateBackend producer(std::make_shared<InMemoryStateBackend>(), store);

    for (int i = 0; i < 50; ++i) {
        producer.put(OperatorId{7}, sv("k" + std::to_string(i)), sv("v" + std::to_string(i)));
    }
    producer.materialize_now();
    // Add a few log entries after materialization to exercise
    // handle-fetch + log-replay.
    producer.put(OperatorId{7}, sv(std::string{"delta-a"}), sv(std::string{"A"}));
    producer.erase(OperatorId{7}, sv(std::string{"k0"}));
    auto snap = producer.snapshot(CheckpointId{1});

    // Fresh backend pointed at the SAME store - restore from snap
    // should read the handle, fetch the bytes from disk, and apply
    // the log delta.
    auto store_b = std::make_shared<FileMaterializationStore>(store_dir);
    ChangelogStateBackend restored(std::make_shared<InMemoryStateBackend>(), store_b);
    restored.restore(snap);

    // Verify a few keys round-tripped correctly.
    EXPECT_FALSE(restored.get(OperatorId{7}, sv(std::string{"k0"})).has_value())
        << "k0 was erased post-materialization; should be gone";
    auto v1 = restored.get(OperatorId{7}, sv(std::string{"k1"}));
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(str(*v1), "v1");
    auto va = restored.get(OperatorId{7}, sv(std::string{"delta-a"}));
    ASSERT_TRUE(va.has_value());
    EXPECT_EQ(str(*va), "A");

    std::filesystem::remove_all(store_dir);
}

TEST(ChangelogBackendExternal, InBlobSnapshotIntoExternalBackendThrows) {
    // A snapshot produced by an in-blob backend (no store) cannot be
    // restored by a backend that has a store configured - modes must
    // match. The error message must say so plainly.
    ChangelogStateBackend in_blob;
    in_blob.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"x"}));
    in_blob.materialize_now();
    auto snap = in_blob.snapshot(CheckpointId{1});

    const auto store_dir = tmp_subdir("mode_mismatch_a");
    auto store = std::make_shared<FileMaterializationStore>(store_dir);
    ChangelogStateBackend ext(std::make_shared<InMemoryStateBackend>(), store);
    EXPECT_THROW(ext.restore(snap), std::runtime_error);
    std::filesystem::remove_all(store_dir);
}

TEST(ChangelogBackendExternal, ExternalSnapshotIntoInBlobBackendThrows) {
    const auto store_dir = tmp_subdir("mode_mismatch_b");
    auto store = std::make_shared<FileMaterializationStore>(store_dir);
    ChangelogStateBackend ext(std::make_shared<InMemoryStateBackend>(), store);
    ext.put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"x"}));
    ext.materialize_now();
    auto snap = ext.snapshot(CheckpointId{1});

    ChangelogStateBackend in_blob;
    EXPECT_THROW(in_blob.restore(snap), std::runtime_error);
    std::filesystem::remove_all(store_dir);
}

TEST(ChangelogBackendExternal, FactoryChangelogFileSchemeBuildsExternalBackend) {
    const auto dir = tmp_subdir("factory_file");
    StateBackendSpec spec;
    spec.uri = "changelog+file://" + dir.string();
    spec.subtask_idx = 0;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    EXPECT_NE(built.backend->description().find("changelog"), std::string::npos);
    EXPECT_NE(built.backend->description().find("file://"), std::string::npos);

    built.backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));
    auto v = built.backend->get(OperatorId{1}, sv(std::string{"k"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(str(*v), "v");

    std::filesystem::remove_all(dir);
}

// Rescale restore narrows keyed rows by key group but must keep operator-
// state rows on every subtask (the changelog twin of the in-memory Gap A
// guard, #54). Mirrors InMemoryStateBackend.OperatorStateSurvivesRescale.
TEST(ChangelogBackend, OperatorStateSurvivesRescaleOnEverySubtask) {
    ChangelogStateBackend src;
    const OperatorId op{7};
    std::string keyed95;
    keyed95.push_back(static_cast<char>(95));  // key group 95
    keyed95.append("keyed");
    src.put(op, sv(keyed95), sv(std::string{"KEYED95"}));
    src.put_operator_state(op, sv(std::string{"__src_offsets__"}), sv(std::string{"OFFSETS"}));

    auto snap = src.snapshot(CheckpointId{1});

    for (auto range :
         {KeyGroupRange{KeyGroup{0}, KeyGroup{64}}, KeyGroupRange{KeyGroup{64}, KeyGroup{128}}}) {
        ChangelogStateBackend sub;
        sub.restore(snap, range);
        auto off = sub.get_operator_state(op, sv(std::string{"__src_offsets__"}));
        ASSERT_TRUE(off.has_value())
            << "operator state dropped for range [" << range.first << "," << range.last << ")";
        EXPECT_EQ(str(*off), "OFFSETS");
    }

    // Keyed row still narrowed: present only where group 95 is owned.
    ChangelogStateBackend lower;
    lower.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});
    EXPECT_FALSE(lower.get(op, sv(keyed95)).has_value());
    ChangelogStateBackend upper;
    upper.restore(snap, KeyGroupRange{KeyGroup{64}, KeyGroup{128}});
    EXPECT_TRUE(upper.get(op, sv(keyed95)).has_value());
}

// The headline gap: a changelog+file job restarted at the same parallelism
// must recover its state. snapshot() now self-persists its framing blob and
// build_changelog_file wires restore_from at it. Drive the factory + restore
// directly (as make_subtask_job_config / LocalExecutor would).
TEST(ChangelogBackendExternal, FactoryChangelogFileRestoreRecoversSameParallelism) {
    const auto dir = tmp_subdir("file_restore_factory");
    const std::string uri = "changelog+file://" + dir.string();

    {
        StateBackendSpec spec;
        spec.uri = uri;
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        EXPECT_FALSE(built.restore_from.has_value());
        built.backend->put(OperatorId{1}, sv(std::string{"k1"}), sv(std::string{"v1"}));
        built.backend->put(OperatorId{1}, sv(std::string{"k2"}), sv(std::string{"v2"}));
        (void)built.backend->snapshot(CheckpointId{7});
    }
    // The framing blob is on disk now - the durability the gap lacked.
    EXPECT_TRUE(std::filesystem::exists(dir / "0" / "changelog-7.snap"));

    StateBackendSpec spec;
    spec.uri = uri;
    spec.subtask_idx = 0;
    spec.restore_uri = uri;
    spec.restore_checkpoint_id = 7;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_changelog_file must wire restore_from when a restore checkpoint is set";
    built.backend->restore(*built.restore_from, {});

    auto v1 = built.backend->get(OperatorId{1}, sv(std::string{"k1"}));
    auto v2 = built.backend->get(OperatorId{1}, sv(std::string{"k2"}));
    ASSERT_TRUE(v1.has_value()) << "state lost on restart - the gap this closes";
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(str(*v1), "v1");
    EXPECT_EQ(str(*v2), "v2");

    std::filesystem::remove_all(dir);
}

// Scale-up rescale: a new subtask reads its assigned parent's blob and narrows
// to its key-group slice, keeping operator state. A forced materialization
// routes the keyed state through the inner InMemory restore (not just the log),
// so this also proves the inner restore honours the filter + op-state exemption.
TEST(ChangelogBackendExternal, FactoryChangelogFileScaleUpNarrows) {
    const auto root = tmp_subdir("file_scaleup");
    std::filesystem::create_directories(root);
    const std::string run1 = "changelog+file://" + (root / "run1").string();
    const std::string run2 = "changelog+file://" + (root / "run2").string();
    const OperatorId op{8};
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
        clog->set_materialization_threshold_bytes(0);  // force materialization at snapshot
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
        << "build_changelog_file must wire scale-up restore_from";
    built.backend->restore(*built.restore_from, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});

    EXPECT_TRUE(built.backend->get(op, sv(key_lo)).has_value()) << "in-range keyed kept";
    EXPECT_FALSE(built.backend->get(op, sv(key_hi)).has_value()) << "out-of-range keyed dropped";
    auto off = built.backend->get_operator_state(op, sv(std::string{"offsets"}));
    ASSERT_TRUE(off.has_value()) << "operator state must survive scale-up";
    EXPECT_EQ(str(*off), "OFF");

    std::filesystem::remove_all(root);
}

// Scale-down: one new subtask inherits SEVERAL parents. The factory frames
// all parents' blobs; restore() merges their materializations (the inner
// InMemory folds the IPC streams) and replays all logs. The new subtask ends
// up with the union of the parents' keyed state and operator state.
TEST(ChangelogBackendExternal, FactoryChangelogFileScaleDownMergesParents) {
    const auto root = tmp_subdir("file_scaledown");
    std::filesystem::create_directories(root);
    const std::string run1 = "changelog+file://" + (root / "run1").string();
    const std::string run2 = "changelog+file://" + (root / "run2").string();
    const OperatorId op{10};
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
        << "build_changelog_file must wire scale-down restore_from";
    // 2 -> 1: the new subtask owns the whole key-group space.
    built.backend->restore(*built.restore_from,
                           KeyGroupRange{KeyGroup{0}, KeyGroup{kNumKeyGroups}});

    auto va = built.backend->get(op, sv(key_a));
    auto vb = built.backend->get(op, sv(key_b));
    ASSERT_TRUE(va.has_value());
    ASSERT_TRUE(vb.has_value());
    EXPECT_EQ(str(*va), "A");
    EXPECT_EQ(str(*vb), "B");
    auto o0 = built.backend->get_operator_state(op, sv(std::string{"off0"}));
    auto o1 = built.backend->get_operator_state(op, sv(std::string{"off1"}));
    ASSERT_TRUE(o0.has_value());
    ASSERT_TRUE(o1.has_value());
    EXPECT_EQ(str(*o0), "O0");
    EXPECT_EQ(str(*o1), "O1");

    std::filesystem::remove_all(root);
}

// The in-memory `changelog://` scheme keeps no on-disk artefacts (the
// changelog twin of `memory://`), so it cannot restore across a process
// boundary even when a restore is requested - documented, not a silent bug.
TEST(ChangelogBackend, FactoryInMemoryChangelogDoesNotRestoreAcrossProcess) {
    StateBackendSpec spec;
    spec.uri = "changelog://";
    spec.subtask_idx = 0;
    spec.restore_uri = "changelog://";
    spec.restore_checkpoint_id = 5;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    EXPECT_FALSE(built.restore_from.has_value());
}
