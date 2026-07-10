#include <algorithm>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include <gtest/gtest.h>

// The RocksDB backend is built only when clink::rocksdb is linked.
// On builds without it, this entire file is a no-op - the test body
// would not even link, since the RocksDBStateBackend symbols are
// nowhere in the binary.
#if __has_include("clink/state/rocksdb_state_backend.hpp")
#include "clink/rocksdb/install.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/rocksdb_state_backend.hpp"
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

}  // namespace

TEST(RocksDBStateBackend, StubThrowsWhenNotBuiltWithRocksDB) {
    if (RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built with real RocksDB; stub-throws path not applicable";
    }
    EXPECT_THROW(
        {
            RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = "/tmp/clink_unused",
                                                                     .create_if_missing = true});
            (void)backend;
        },
        std::runtime_error);
}

// Construction-path symmetry: RocksDB stays synchronous. Its checkpoint
// is a hard-link snapshot that already fsyncs internally and is cheap, and
// it is not cleanly splittable (the checkpoint is capture+persist in one
// call and takes the write-buffer lock). The runner must not route it
// through the snapshot worker.
TEST(RocksDBStateBackend, DoesNotSupportAsyncPersist) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_async_flag";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);
    RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (base_dir / "db").string(),
                                                             .create_if_missing = true});
    EXPECT_FALSE(backend.supports_async_persist());
    std::filesystem::remove_all(base_dir);
}

TEST(RocksDBStateBackend, RealBackendPutGetEraseAndSnapshot) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }

    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_test";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    {
        RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (base_dir / "db").string(),
                                                                 .create_if_missing = true});

        OperatorId op{99};
        backend.put(op, sv(std::string{"hello"}), sv(std::string{"world"}));
        backend.put(op, sv(std::string{"alpha"}), sv(std::string{"beta"}));

        auto v = backend.get(op, sv(std::string{"hello"}));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(to_string(*v), "world");

        backend.erase(op, sv(std::string{"hello"}));
        EXPECT_FALSE(backend.get(op, sv(std::string{"hello"})).has_value());
        EXPECT_TRUE(backend.get(op, sv(std::string{"alpha"})).has_value());

        auto snap = backend.snapshot(CheckpointId{1});
        EXPECT_FALSE(snap.bytes.empty());
        EXPECT_EQ(snap.checkpoint_id.value(), 1u);
    }

    std::filesystem::remove_all(base_dir);
}

namespace {

// inode equality across two paths is the "are these the same physical
// file" test on POSIX. rocksdb::Checkpoint hard-links SSTs into each
// checkpoint dir; an SST shared across two checkpoints has identical
// inode numbers in both. We use this as the empirical test that
// incremental SST sharing actually works.
bool same_inode(const std::filesystem::path& a, const std::filesystem::path& b) {
    std::error_code ec;
    return std::filesystem::equivalent(a, b, ec) && !ec;
}

// Force RocksDB to flush its memtable into an SST so the checkpoint
// has something to hard-link. Without an explicit flush, small test
// workloads stay in the memtable and the checkpoint dir contains zero
// SSTs - which would make the sharing test vacuous.
void flush_to_sst(clink::RocksDBStateBackend& backend) {
    // The public surface doesn't expose Flush(), so we trigger it by
    // doing enough writes to spill. A handful is enough at default
    // memtable size for the manual call rocksdb's API exposes -
    // luckily a single snapshot followed by some additional writes
    // typically races into a flush via background compaction. For
    // determinism we lean on RocksDB's auto-flush via repeated puts;
    // tests below add a few thousand keys before snapshotting.
    clink::OperatorId op{0};
    for (int i = 0; i < 4096; ++i) {
        std::string k = "warm_" + std::to_string(i);
        std::string v(64, 'x');
        backend.put(op, k, v);
    }
}

}  // namespace

TEST(RocksDBStateBackend, IncrementalSnapshotsShareSstFiles) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_incremental";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    const auto db_path = base_dir / "db";
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = db_path.string(), .create_if_missing = true});

    flush_to_sst(backend);
    auto snap1 = backend.snapshot(CheckpointId{1});
    auto stats1 = backend.last_snapshot_stats();
    ASSERT_TRUE(stats1.has_value());
    // First snapshot has nothing to share against - expected zero.
    EXPECT_EQ(stats1->shared_sst_count, 0u);

    // Add a few more entries; these go into the memtable and shouldn't
    // produce a brand-new SST yet. Snapshot 2's SSTs should overlap
    // heavily with snapshot 1's.
    clink::OperatorId op{0};
    backend.put(op, sv(std::string{"delta_key"}), sv(std::string{"delta_value"}));
    auto snap2 = backend.snapshot(CheckpointId{2});
    auto stats2 = backend.last_snapshot_stats();
    ASSERT_TRUE(stats2.has_value());
    EXPECT_GT(stats1->total_sst_count, 0u) << "warmup should have produced at least one SST";
    EXPECT_GE(stats2->shared_sst_count, 1u)
        << "incremental snapshot must share at least one SST with the prior one";

    // Empirical check: an SST present in both checkpoints is the same
    // inode on disk, not a duplicated byte copy. Pick the first
    // shared name and stat-compare.
    if (!stats1->sst_files.empty() && !stats2->sst_files.empty()) {
        std::string shared_name;
        std::unordered_set<std::string> s1(stats1->sst_files.begin(), stats1->sst_files.end());
        for (const auto& n : stats2->sst_files) {
            if (s1.count(n) != 0) {
                shared_name = n;
                break;
            }
        }
        ASSERT_FALSE(shared_name.empty());
        const auto p1 = db_path.string() + ".cp-1/" + shared_name;
        const auto p2 = db_path.string() + ".cp-2/" + shared_name;
        EXPECT_TRUE(same_inode(p1, p2))
            << "shared SST should be a hard link, not a duplicate copy: " << shared_name;
    }

    std::filesystem::remove_all(base_dir);
}

TEST(RocksDBStateBackend, RestoreIsNonDestructiveAndCheckpointDirSurvives) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_nondestructive";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    const auto db_path = base_dir / "db";
    clink::OperatorId op{1};
    {
        RocksDBStateBackend backend(
            RocksDBStateBackend::Options{.path = db_path.string(), .create_if_missing = true});
        backend.put(op, sv(std::string{"a"}), sv(std::string{"alpha"}));
        backend.put(op, sv(std::string{"b"}), sv(std::string{"beta"}));
        flush_to_sst(backend);
        (void)backend.snapshot(CheckpointId{7});
    }

    const std::string snap_path = db_path.string() + ".cp-7";
    ASSERT_TRUE(std::filesystem::exists(snap_path));

    // Restore twice from the SAME checkpoint id. With the historic
    // "open the checkpoint dir directly" path the second restore would
    // see a mutated MANIFEST/CURRENT and probably fail or surface stale
    // state. Both restores should land on identical content.
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{7};
    snap.bytes.assign(reinterpret_cast<const std::byte*>(snap_path.data()),
                      reinterpret_cast<const std::byte*>(snap_path.data() + snap_path.size()));
    for (int round = 0; round < 2; ++round) {
        RocksDBStateBackend restored(RocksDBStateBackend::Options{
            .path = (base_dir / ("restore_" + std::to_string(round))).string(),
            .create_if_missing = true});
        restored.restore(snap);
        auto va = restored.get(op, sv(std::string{"a"}));
        auto vb = restored.get(op, sv(std::string{"b"}));
        ASSERT_TRUE(va.has_value()) << "round " << round;
        ASSERT_TRUE(vb.has_value()) << "round " << round;
        EXPECT_EQ(to_string(*va), "alpha");
        EXPECT_EQ(to_string(*vb), "beta");
    }

    // The checkpoint dir is still intact after two restores - the
    // incremental story requires this so subsequent snapshots can
    // still hard-link from it.
    EXPECT_TRUE(std::filesystem::exists(snap_path));

    std::filesystem::remove_all(base_dir);
}

TEST(RocksDBStateBackend, PurgeCheckpointRemovesDirectoryWithoutAffectingWorkingDb) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_purge";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    const auto db_path = base_dir / "db";
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = db_path.string(), .create_if_missing = true});
    clink::OperatorId op{2};
    flush_to_sst(backend);
    (void)backend.snapshot(CheckpointId{42});
    const std::string snap_path = db_path.string() + ".cp-42";
    ASSERT_TRUE(std::filesystem::exists(snap_path));

    backend.purge_checkpoint(CheckpointId{42});
    EXPECT_FALSE(std::filesystem::exists(snap_path));

    // The live working DB's SSTs are still hard-linked from the db
    // path; purging the checkpoint dropped a *separate* link, not
    // the working-DB link. A new get() against the in-memory state
    // confirms the DB still works.
    backend.put(op, sv(std::string{"k"}), sv(std::string{"v"}));
    auto v = backend.get(op, sv(std::string{"k"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(to_string(*v), "v");

    std::filesystem::remove_all(base_dir);
}

// The headline cluster-restore wiring: a job restarted at the same
// parallelism must recover its RocksDB state. build_rocksdb wires
// restore_from to the producing subtask's checkpoint dir; LocalExecutor
// would then call backend->restore(). Here we drive the factory + restore
// directly.
TEST(RocksDBStateBackend, FactoryRestoreRecoversStateSameParallelism) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    clink::rocksdb::install();  // register the rocksdb:// scheme
    auto root = std::filesystem::temp_directory_path() / "clink_rocks_factory_restore";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string run1 = "rocksdb://" + (root / "run1").string();
    const std::string run2 = "rocksdb://" + (root / "run2").string();
    const clink::OperatorId op{4};

    // --- run 1: build via the factory, write state, checkpoint 11. ---
    {
        StateBackendSpec spec;
        spec.uri = run1;
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        ASSERT_NE(built.backend, nullptr);
        EXPECT_FALSE(built.restore_from.has_value());
        built.backend->put(op, sv(std::string{"k1"}), sv(std::string{"v1"}));
        built.backend->put(op, sv(std::string{"k2"}), sv(std::string{"v2"}));
        (void)built.backend->snapshot(CheckpointId{11});
    }

    // --- run 2: fresh working dir, restore from run1's checkpoint 11. ---
    StateBackendSpec spec;
    spec.uri = run2;
    spec.subtask_idx = 0;
    spec.restore_uri = run1;
    spec.restore_checkpoint_id = 11;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_rocksdb must wire restore_from when a restore checkpoint is set";
    built.backend->restore(*built.restore_from);  // LocalExecutor does this at start()

    auto v1 = built.backend->get(op, sv(std::string{"k1"}));
    auto v2 = built.backend->get(op, sv(std::string{"k2"}));
    ASSERT_TRUE(v1.has_value()) << "state lost on restart - the gap this closes";
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(to_string(*v1), "v1");
    EXPECT_EQ(to_string(*v2), "v2");

    std::filesystem::remove_all(root);
}

// The rescale follow-on, at the backend level: a narrowed (rescale) restore
// must drop out-of-range KEYED rows but KEEP operator-state rows (the 0xFF
// prefix carries source offsets - broadcast to every subtask). Before the
// fix the filter deleted operator state too, breaking exactly-once.
TEST(RocksDBStateBackend, RestoreKeyGroupFilterKeepsOperatorState) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_kgfilter";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);
    const clink::OperatorId op{3};
    const std::string key_lo = std::string(1, '\x01') + "lo";  // key group 1
    const std::string key_hi = std::string(1, '\x70') + "hi";  // key group 112
    {
        RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (base_dir / "db").string(),
                                                                 .create_if_missing = true});
        backend.put(op, sv(key_lo), sv(std::string{"low"}));
        backend.put(op, sv(key_hi), sv(std::string{"high"}));
        backend.put_operator_state(op, sv(std::string{"offsets"}), sv(std::string{"OFF"}));
        (void)backend.snapshot(CheckpointId{1});
    }
    const std::string snap_path = (base_dir / "db").string() + ".cp-1";
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{1};
    snap.bytes.assign(reinterpret_cast<const std::byte*>(snap_path.data()),
                      reinterpret_cast<const std::byte*>(snap_path.data() + snap_path.size()));

    RocksDBStateBackend restored(RocksDBStateBackend::Options{
        .path = (base_dir / "restore").string(), .create_if_missing = true});
    // New subtask owns the lower half of the key-group space only.
    restored.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});

    EXPECT_TRUE(restored.get(op, sv(key_lo)).has_value()) << "in-range keyed row must survive";
    EXPECT_FALSE(restored.get(op, sv(key_hi)).has_value())
        << "out-of-range keyed row must be dropped";
    auto off = restored.get_operator_state(op, sv(std::string{"offsets"}));
    ASSERT_TRUE(off.has_value()) << "operator state must survive a narrowed (rescale) restore";
    EXPECT_EQ(to_string(*off), "OFF");

    std::filesystem::remove_all(base_dir);
}

// Scale-up rescale through the factory: a new subtask reads its assigned
// parent's checkpoint and narrows to its key-group slice, keeping operator
// state. build_rocksdb now wires this (no longer rejected).
TEST(RocksDBStateBackend, FactoryScaleUpRestoreReadsParentAndNarrows) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    clink::rocksdb::install();
    auto root = std::filesystem::temp_directory_path() / "clink_rocks_scaleup";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string run1 = "rocksdb://" + (root / "run1").string();
    const std::string run2 = "rocksdb://" + (root / "run2").string();
    const clink::OperatorId op{4};
    const std::string key_lo = std::string(1, '\x01') + "lo";
    const std::string key_hi = std::string(1, '\x70') + "hi";

    {
        StateBackendSpec spec;
        spec.uri = run1;
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        built.backend->put(op, sv(key_lo), sv(std::string{"low"}));
        built.backend->put(op, sv(key_hi), sv(std::string{"high"}));
        built.backend->put_operator_state(op, sv(std::string{"offsets"}), sv(std::string{"OFF"}));
        (void)built.backend->snapshot(CheckpointId{9});
    }

    StateBackendSpec spec;
    spec.uri = run2;
    spec.subtask_idx = 0;
    spec.restore_uri = run1;
    spec.restore_checkpoint_id = 9;
    spec.restore_from_subtask_idx = 0;   // scale-up: read parent subtask 0
    spec.restore_from_parent_count = 1;  // single parent
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_rocksdb must wire scale-up restore_from (no longer rejected)";
    built.backend->restore(*built.restore_from, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});

    EXPECT_TRUE(built.backend->get(op, sv(key_lo)).has_value());
    EXPECT_FALSE(built.backend->get(op, sv(key_hi)).has_value());
    auto off = built.backend->get_operator_state(op, sv(std::string{"offsets"}));
    ASSERT_TRUE(off.has_value()) << "operator state must survive scale-up";
    EXPECT_EQ(to_string(*off), "OFF");

    std::filesystem::remove_all(root);
}

// Scale-down rescale through the factory: one new subtask inherits SEVERAL
// parent subtasks. build_rocksdb encodes all assigned parents; restore()
// re-homes onto the first and iterate-merges the rest, so the new subtask
// ends up with the union of the parents' keyed state AND every parent's
// operator state.
TEST(RocksDBStateBackend, FactoryScaleDownRestoreMergesParents) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    clink::rocksdb::install();
    auto root = std::filesystem::temp_directory_path() / "clink_rocks_scaledown";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string run1 = "rocksdb://" + (root / "run1").string();
    const std::string run2 = "rocksdb://" + (root / "run2").string();
    const clink::OperatorId op{5};
    const std::string key_a = std::string(1, '\x0a') + "a";  // key group 10
    const std::string key_b = std::string(1, '\x64') + "b";  // key group 100

    {
        StateBackendSpec spec;
        spec.uri = run1;
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        built.backend->put(op, sv(key_a), sv(std::string{"A"}));
        built.backend->put_operator_state(op, sv(std::string{"off0"}), sv(std::string{"O0"}));
        (void)built.backend->snapshot(CheckpointId{5});
    }
    {
        StateBackendSpec spec;
        spec.uri = run1;
        spec.subtask_idx = 1;
        auto built = StateBackendFactory::default_instance().build(spec);
        built.backend->put(op, sv(key_b), sv(std::string{"B"}));
        built.backend->put_operator_state(op, sv(std::string{"off1"}), sv(std::string{"O1"}));
        (void)built.backend->snapshot(CheckpointId{5});
    }

    StateBackendSpec spec;
    spec.uri = run2;
    spec.subtask_idx = 0;
    spec.restore_uri = run1;
    spec.restore_checkpoint_id = 5;
    spec.restore_from_subtask_idx = 0;   // first parent
    spec.restore_from_parent_count = 2;  // inherit parents 0 and 1
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_rocksdb must wire scale-down restore_from (no longer rejected)";
    // 2 -> 1: the new subtask owns the whole key-group space.
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

// Backend-level: restore() merges multiple checkpoint dirs (newline-joined in
// snap.bytes) and the kg-filter still narrows the MERGED keyed rows while
// keeping operator state - proves the merge and the filter compose.
TEST(RocksDBStateBackend, RestoreMergesMultipleCheckpointsThenFilters) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto base = std::filesystem::temp_directory_path() / "clink_rocks_merge";
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    const clink::OperatorId op{6};
    const std::string lo = std::string(1, '\x05') + "lo";  // key group 5
    const std::string hi = std::string(1, '\x70') + "hi";  // key group 112
    {
        RocksDBStateBackend a(
            RocksDBStateBackend::Options{.path = (base / "a").string(), .create_if_missing = true});
        a.put(op, sv(lo), sv(std::string{"LO"}));
        a.put_operator_state(op, sv(std::string{"offA"}), sv(std::string{"OA"}));
        (void)a.snapshot(CheckpointId{1});
    }
    {
        RocksDBStateBackend b(
            RocksDBStateBackend::Options{.path = (base / "b").string(), .create_if_missing = true});
        b.put(op, sv(hi), sv(std::string{"HI"}));
        b.put_operator_state(op, sv(std::string{"offB"}), sv(std::string{"OB"}));
        (void)b.snapshot(CheckpointId{1});
    }
    const std::string joined =
        (base / "a").string() + ".cp-1" + "\n" + (base / "b").string() + ".cp-1";
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{1};
    snap.bytes.assign(reinterpret_cast<const std::byte*>(joined.data()),
                      reinterpret_cast<const std::byte*>(joined.data() + joined.size()));

    RocksDBStateBackend merged(RocksDBStateBackend::Options{.path = (base / "merged").string(),
                                                            .create_if_missing = true});
    // New subtask owns only the lower half: hi (kg 112) is filtered out of the
    // merged set; lo (kg 5) kept; BOTH parents' operator state kept.
    merged.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});

    EXPECT_TRUE(merged.get(op, sv(lo)).has_value()) << "in-range keyed from parent A kept";
    EXPECT_FALSE(merged.get(op, sv(hi)).has_value()) << "out-of-range keyed from parent B filtered";
    EXPECT_TRUE(merged.get_operator_state(op, sv(std::string{"offA"})).has_value());
    EXPECT_TRUE(merged.get_operator_state(op, sv(std::string{"offB"})).has_value());

    std::filesystem::remove_all(base);
}

// ----- state-as-data: the Arrow export -----

namespace {

// Collect every (op, key, value) triple a backend reports via scan for
// the given operators, as a sorted flat list (comparison-friendly).
std::vector<std::string> collect_triples(const StateBackend& b,
                                         const std::vector<OperatorId>& ops) {
    std::vector<std::string> out;
    for (const auto op : ops) {
        b.scan(op, [&](std::string_view k, std::string_view v) {
            out.push_back(std::to_string(op.value()) + "\x1e" + std::string(k) + "\x1e" +
                          std::string(v));
        });
    }
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

// The live Arrow export must carry the backend's exact contents in the
// canonical snapshot format: restoring the exported bytes into an
// InMemoryStateBackend (the format's reference reader) reproduces every
// keyed and operator-state entry.
TEST(RocksDBStateBackend, ArrowExportRoundTripsThroughInMemoryRestore) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_arrow_export";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

    const OperatorId op_a{1}, op_b{2};
    const std::string k1 = std::string{"\x05"} + "slot|alpha";
    const std::string k2 = std::string{"\x21"} + "slot|beta";
    const std::string v1 = "value-one", v2 = "value-two";
    backend.put(op_a, sv(k1), sv(v1));
    backend.put(op_a, sv(k2), sv(v2));
    backend.put(op_b, sv(k1), sv(v2));
    // Operator-state row (reserved prefix >= kNumKeyGroups) must export too.
    const std::string op_key = std::string{"\xFF"} + "offsets|p0";
    backend.put(op_b, sv(op_key), sv(v1));

    const auto bytes = backend.export_arrow_snapshot();
    ASSERT_FALSE(bytes.empty());

    InMemoryStateBackend reference;
    reference.restore(Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = bytes});
    EXPECT_EQ(collect_triples(reference, {op_a, op_b}), collect_triples(backend, {op_a, op_b}));
}

// The offline checkpoint-dir converter must produce the same stream as
// the live export taken at the same point (deterministic order: ops
// ascending, keys in RocksDB byte order).
TEST(RocksDBStateBackend, CheckpointDirExportMatchesLiveExport) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_arrow_cpdir";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

    const OperatorId op{7};
    for (int i = 0; i < 100; ++i) {
        const std::string k = std::string{static_cast<char>(i % 64)} + "s|k" + std::to_string(i);
        const std::string v = "v" + std::to_string(i * i);
        backend.put(op, sv(k), sv(v));
    }
    const auto live = backend.export_arrow_snapshot();

    const auto snap = backend.snapshot(CheckpointId{42});
    std::string cp_dir(snap.bytes.size(), '\0');
    std::memcpy(cp_dir.data(), snap.bytes.data(), snap.bytes.size());
    const auto offline = rocksdb_checkpoint_to_arrow(cp_dir);

    EXPECT_EQ(live, offline);
}

// An empty backend exports a VALID zero-row stream (schema + EOS), so
// downstream readers need no special-casing.
TEST(RocksDBStateBackend, EmptyBackendExportsValidStream) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_arrow_empty";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});
    const auto bytes = backend.export_arrow_snapshot();
    ASSERT_FALSE(bytes.empty());  // schema + EOS, not zero bytes
    InMemoryStateBackend reference;
    reference.restore(Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = bytes});
    std::size_t rows = 0;
    reference.scan(OperatorId{7}, [&](std::string_view, std::string_view) { ++rows; });
    EXPECT_EQ(rows, 0u);
}

#endif  // __has_include rocksdb_state_backend.hpp
