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

// ----- schema-evolution version stamps -----

// Version stamps set on the live backend ride every checkpoint (a
// reserved key in the default CF) and are recovered by restore - so the
// pre-deploy compatibility check and migrate-at-restore see real stamps
// on RocksDB, not the previous "no stamps recorded" blank.
TEST(RocksDBStateBackend, StateVersionsSurviveSnapshotAndRestore) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_versions";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

    StateVersionMap versions;
    versions.set(OperatorId{1}, "CounterState", 2);
    versions.set(OperatorId{9}, "WindowAgg", 4);
    backend.set_state_versions(versions);
    EXPECT_EQ(backend.restored_state_versions().pack(), versions.pack());

    backend.put(OperatorId{1}, sv(std::string{"\x05"} + "s|k"), sv(std::string{"v"}));
    const auto snap = backend.snapshot(CheckpointId{11});

    RocksDBStateBackend fresh(
        RocksDBStateBackend::Options{.path = (dir / "db2").string(), .create_if_missing = true});
    EXPECT_TRUE(fresh.restored_state_versions().empty());
    fresh.restore(snap);
    EXPECT_EQ(fresh.restored_state_versions().pack(), versions.pack());
}

// Both Arrow exports embed the stamps in the stream's schema metadata,
// so an exported RocksDB checkpoint restored into the in-memory
// reference reader carries them - and check-savepoint on the exported
// stream sees real versions.
TEST(RocksDBStateBackend, ArrowExportsCarryStateVersions) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_versions_export";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});
    StateVersionMap versions;
    versions.set(OperatorId{7}, "AggBucket", 3);
    backend.set_state_versions(versions);
    backend.put(OperatorId{7}, sv(std::string{"\x05"} + "s|k"), sv(std::string{"v"}));

    // Live export.
    InMemoryStateBackend ref;
    ref.restore(
        Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = backend.export_arrow_snapshot()});
    EXPECT_EQ(ref.restored_state_versions().pack(), versions.pack());

    // Offline checkpoint-dir export.
    const auto snap = backend.snapshot(CheckpointId{21});
    std::string cp_dir(snap.bytes.size(), '\0');
    std::memcpy(cp_dir.data(), snap.bytes.data(), snap.bytes.size());
    InMemoryStateBackend ref2;
    ref2.restore(
        Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = rocksdb_checkpoint_to_arrow(cp_dir)});
    EXPECT_EQ(ref2.restored_state_versions().pack(), versions.pack());
}

// Shape fingerprints ride the sibling reserved key exactly like the
// versions: recorded on the live backend, persisted by every checkpoint,
// recovered by restore into a fresh backend - so the bind-time gate works
// over rocksdb:// restores, not only in-memory ones.
TEST(RocksDBStateBackend, StateFingerprintsSurviveSnapshotAndRestore) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_fingerprints";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

    backend.record_state_fingerprint(OperatorId{1}, "counter_slot", 0xabcdef0123456789ULL);
    backend.record_state_fingerprint(OperatorId{9}, "window_slot", 0x1111222233334444ULL);
    backend.put(OperatorId{1}, sv(std::string{"\x05"} + "s|k"), sv(std::string{"v"}));
    const auto snap = backend.snapshot(CheckpointId{11});

    RocksDBStateBackend fresh(
        RocksDBStateBackend::Options{.path = (dir / "db2").string(), .create_if_missing = true});
    EXPECT_FALSE(fresh.restored_state_fingerprint(OperatorId{1}, "counter_slot").has_value());
    fresh.restore(snap);
    const auto got = fresh.restored_state_fingerprint(OperatorId{1}, "counter_slot");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 0xabcdef0123456789ULL);
    const auto got2 = fresh.restored_state_fingerprint(OperatorId{9}, "window_slot");
    ASSERT_TRUE(got2.has_value());
    EXPECT_EQ(*got2, 0x1111222233334444ULL);
}

// Both Arrow exports embed the fingerprints beside the versions, so an
// exported rocksdb checkpoint restored into the in-memory reference
// reader carries them - the pre-deploy savepoint reader sees real stamps.
TEST(RocksDBStateBackend, ArrowExportsCarryStateFingerprints) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_fingerprints_export";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    RocksDBStateBackend backend(
        RocksDBStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});
    backend.record_state_fingerprint(OperatorId{7}, "agg_slot", 0x5555666677778888ULL);
    backend.put(OperatorId{7}, sv(std::string{"\x05"} + "s|k"), sv(std::string{"v"}));

    // Live export.
    InMemoryStateBackend ref;
    ref.restore(
        Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = backend.export_arrow_snapshot()});
    const auto live = ref.restored_state_fingerprint(OperatorId{7}, "agg_slot");
    ASSERT_TRUE(live.has_value());
    EXPECT_EQ(*live, 0x5555666677778888ULL);

    // Offline checkpoint-dir export.
    const auto snap = backend.snapshot(CheckpointId{21});
    std::string cp_dir(snap.bytes.size(), '\0');
    std::memcpy(cp_dir.data(), snap.bytes.data(), snap.bytes.size());
    InMemoryStateBackend ref2;
    ref2.restore(
        Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = rocksdb_checkpoint_to_arrow(cp_dir)});
    const auto offline = ref2.restored_state_fingerprint(OperatorId{7}, "agg_slot");
    ASSERT_TRUE(offline.has_value());
    EXPECT_EQ(*offline, 0x5555666677778888ULL);
}

// The migrator's whole-operator clear (empty slot) must persist: after a
// clear and a reopen of the same working dir, the stamps stay gone.
TEST(RocksDBStateBackend, ClearFingerprintWholeOpPersistsAcrossReopen) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    auto dir = std::filesystem::temp_directory_path() / "clink_rocks_fp_clear";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    const auto db_path = (dir / "db").string();
    {
        RocksDBStateBackend backend(
            RocksDBStateBackend::Options{.path = db_path, .create_if_missing = true});
        backend.record_state_fingerprint(OperatorId{3}, "slot_a", 0x1ULL);
        backend.record_state_fingerprint(OperatorId{3}, "slot_b", 0x2ULL);
        backend.record_state_fingerprint(OperatorId{4}, "slot_c", 0x3ULL);
        backend.clear_state_fingerprint(OperatorId{3}, "");
    }
    RocksDBStateBackend reopened(
        RocksDBStateBackend::Options{.path = db_path, .create_if_missing = false});
    EXPECT_FALSE(reopened.restored_state_fingerprint(OperatorId{3}, "slot_a").has_value());
    EXPECT_FALSE(reopened.restored_state_fingerprint(OperatorId{3}, "slot_b").has_value());
    const auto kept = reopened.restored_state_fingerprint(OperatorId{4}, "slot_c");
    ASSERT_TRUE(kept.has_value());
    EXPECT_EQ(*kept, 0x3ULL);
}

// --- expiry compaction ------------------------------------------------------

TEST(RocksDBStateBackend, ExpiryFilterDropsEntriesDuringCompaction) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    // The reason the compaction hook exists: RocksDB is already rewriting
    // every live SST during compaction, so an expiry predicate consulted
    // there reclaims TTL'd state for free instead of paying for a scan
    // that competes with the write path.
    //
    // This is the test that makes the hook real rather than an interface
    // with a fake behind it: a genuine forced compaction, a genuine
    // CompactionFilterFactory, and an assertion that the dead entries are
    // gone and the live ones are not.
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_expiry";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);
    {
        RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (base_dir / "db").string(),
                                                                 .create_if_missing = true});
        EXPECT_TRUE(backend.supports_expiry_compaction());

        const OperatorId op{7};
        for (int i = 0; i < 50; ++i) {
            backend.put(op, sv("doomed_" + std::to_string(i)), sv(std::string{"x"}));
            backend.put(op, sv("keep_" + std::to_string(i)), sv(std::string{"x"}));
        }

        backend.set_expiry_filter([](OperatorId, std::string_view key, std::string_view) {
            return key.rfind("doomed_", 0) == 0;
        });
        backend.compact_expired(op);

        std::size_t doomed = 0;
        std::size_t kept = 0;
        backend.scan(op, [&](std::string_view k, std::string_view) {
            if (k.rfind("doomed_", 0) == 0) {
                ++doomed;
            } else if (k.rfind("keep_", 0) == 0) {
                ++kept;
            }
        });
        EXPECT_EQ(doomed, 0U) << "the compaction filter did not drop the expired entries";
        EXPECT_EQ(kept, 50U) << "the compaction filter dropped live entries";
    }
    std::filesystem::remove_all(base_dir);
}

TEST(RocksDBStateBackend, NoExpiryFilterMeansCompactionKeepsEverything) {
    if (!RocksDBStateBackend::is_real_implementation()) {
        GTEST_SKIP() << "Built without RocksDB support";
    }
    // A compaction with no predicate installed must be a plain compaction.
    // The factory returns nullptr in that case; if it returned a filter
    // that defaulted to "drop", a job with no TTL would lose its state the
    // first time RocksDB compacted.
    auto base_dir = std::filesystem::temp_directory_path() / "clink_rocks_no_expiry";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);
    {
        RocksDBStateBackend backend(RocksDBStateBackend::Options{.path = (base_dir / "db").string(),
                                                                 .create_if_missing = true});
        const OperatorId op{8};
        for (int i = 0; i < 20; ++i) {
            backend.put(op, sv("k" + std::to_string(i)), sv(std::string{"v"}));
        }
        backend.compact_expired(op);
        std::size_t n = 0;
        backend.scan(op, [&](std::string_view, std::string_view) { ++n; });
        EXPECT_EQ(n, 20U) << "compaction dropped state with no expiry filter installed";
    }
    std::filesystem::remove_all(base_dir);
}

#endif  // __has_include rocksdb_state_backend.hpp
