// Parity suite for the ForSt state backend: the same behavioural
// contract the bundled RocksDB backend's suite pins, run against
// ForStStateBackend and the forst:// / changelog+forst:// schemes.
// Covers keyed CRUD, incremental hard-link checkpoints, non-destructive
// restore, purge, factory-driven restore (same parallelism, scale-up,
// scale-down), key-group narrowing that preserves operator state, the
// canonical Arrow export (live + offline + empty), and schema-evolution
// version stamps.

#include <algorithm>
#include <atomic>
#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/forst/install.hpp"
#include "clink/operators/keyed_aggregate_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/forst_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"
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

// inode equality across two paths is the "are these the same physical
// file" test on POSIX. Checkpoint creation hard-links SSTs into each
// checkpoint dir; an SST shared across two checkpoints has identical
// inode numbers in both.
bool same_inode(const std::filesystem::path& a, const std::filesystem::path& b) {
    std::error_code ec;
    return std::filesystem::equivalent(a, b, ec) && !ec;
}

// Force the engine to spill its memtable into an SST so the checkpoint
// has something to hard-link. Without enough writes, small test
// workloads stay in the memtable and the checkpoint dir contains zero
// SSTs - which would make the sharing test vacuous.
void flush_to_sst(clink::ForStStateBackend& backend) {
    clink::OperatorId op{0};
    for (int i = 0; i < 4096; ++i) {
        std::string k = "warm_" + std::to_string(i);
        std::string v(64, 'x');
        backend.put(op, k, v);
    }
}

}  // namespace

// Construction-path symmetry: with the default local snapshot store the
// checkpoint is a hard-link snapshot that is cheap and not cleanly
// splittable, so the backend stays synchronous. The runner must not
// route it through the snapshot worker.
TEST(ForStStateBackend, DoesNotSupportAsyncPersistWithLocalStore) {
    auto base_dir = std::filesystem::temp_directory_path() / "clink_forst_async_flag";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = (base_dir / "db").string(), .create_if_missing = true});
    EXPECT_FALSE(backend.supports_async_persist());
    std::filesystem::remove_all(base_dir);
}

TEST(ForStStateBackend, PutGetEraseAndSnapshot) {
    auto base_dir = std::filesystem::temp_directory_path() / "clink_forst_test";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    {
        ForStStateBackend backend(ForStStateBackend::Options{.path = (base_dir / "db").string(),
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

TEST(ForStStateBackend, IncrementalSnapshotsShareSstFiles) {
    auto base_dir = std::filesystem::temp_directory_path() / "clink_forst_incremental";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    const auto db_path = base_dir / "db";
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = db_path.string(), .create_if_missing = true});

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
    // inode on disk, not a duplicated byte copy.
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

TEST(ForStStateBackend, RestoreIsNonDestructiveAndCheckpointDirSurvives) {
    auto base_dir = std::filesystem::temp_directory_path() / "clink_forst_nondestructive";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    const auto db_path = base_dir / "db";
    clink::OperatorId op{1};
    {
        ForStStateBackend backend(
            ForStStateBackend::Options{.path = db_path.string(), .create_if_missing = true});
        backend.put(op, sv(std::string{"a"}), sv(std::string{"alpha"}));
        backend.put(op, sv(std::string{"b"}), sv(std::string{"beta"}));
        flush_to_sst(backend);
        (void)backend.snapshot(CheckpointId{7});
    }

    const std::string snap_path = db_path.string() + ".cp-7";
    ASSERT_TRUE(std::filesystem::exists(snap_path));

    // Restore twice from the SAME checkpoint id; both restores must land
    // on identical content and neither may mutate the checkpoint dir.
    Snapshot snap;
    snap.checkpoint_id = CheckpointId{7};
    snap.bytes.assign(reinterpret_cast<const std::byte*>(snap_path.data()),
                      reinterpret_cast<const std::byte*>(snap_path.data() + snap_path.size()));
    for (int round = 0; round < 2; ++round) {
        ForStStateBackend restored(ForStStateBackend::Options{
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
    // incremental story requires this so subsequent snapshots can still
    // hard-link from it.
    EXPECT_TRUE(std::filesystem::exists(snap_path));

    std::filesystem::remove_all(base_dir);
}

TEST(ForStStateBackend, PurgeCheckpointRemovesDirectoryWithoutAffectingWorkingDb) {
    auto base_dir = std::filesystem::temp_directory_path() / "clink_forst_purge";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);

    const auto db_path = base_dir / "db";
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = db_path.string(), .create_if_missing = true});
    clink::OperatorId op{2};
    flush_to_sst(backend);
    (void)backend.snapshot(CheckpointId{42});
    const std::string snap_path = db_path.string() + ".cp-42";
    ASSERT_TRUE(std::filesystem::exists(snap_path));

    backend.purge_checkpoint(CheckpointId{42});
    EXPECT_FALSE(std::filesystem::exists(snap_path));

    // The live working DB's SSTs are still hard-linked from the db path;
    // purging the checkpoint dropped a separate link, not the working-DB
    // link.
    backend.put(op, sv(std::string{"k"}), sv(std::string{"v"}));
    auto v = backend.get(op, sv(std::string{"k"}));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(to_string(*v), "v");

    std::filesystem::remove_all(base_dir);
}

// The headline cluster-restore wiring: a job restarted at the same
// parallelism must recover its ForSt state. build_forst wires
// restore_from to the producing subtask's checkpoint dir; LocalExecutor
// would then call backend->restore(). Here we drive the factory +
// restore directly.
TEST(ForStStateBackend, FactoryRestoreRecoversStateSameParallelism) {
    clink::forst::install();  // register the forst:// scheme
    auto root = std::filesystem::temp_directory_path() / "clink_forst_factory_restore";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string run1 = "forst://" + (root / "run1").string();
    const std::string run2 = "forst://" + (root / "run2").string();
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
        << "build_forst must wire restore_from when a restore checkpoint is set";
    built.backend->restore(*built.restore_from);  // LocalExecutor does this at start()

    auto v1 = built.backend->get(op, sv(std::string{"k1"}));
    auto v2 = built.backend->get(op, sv(std::string{"k2"}));
    ASSERT_TRUE(v1.has_value()) << "state lost on restart";
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(to_string(*v1), "v1");
    EXPECT_EQ(to_string(*v2), "v2");

    std::filesystem::remove_all(root);
}

// A narrowed (rescale) restore must drop out-of-range KEYED rows but KEEP
// operator-state rows (the reserved prefix carries source offsets -
// broadcast to every subtask).
TEST(ForStStateBackend, RestoreKeyGroupFilterKeepsOperatorState) {
    auto base_dir = std::filesystem::temp_directory_path() / "clink_forst_kgfilter";
    std::filesystem::remove_all(base_dir);
    std::filesystem::create_directories(base_dir);
    const clink::OperatorId op{3};
    const std::string key_lo = std::string(1, '\x01') + "lo";  // key group 1
    const std::string key_hi = std::string(1, '\x70') + "hi";  // key group 112
    {
        ForStStateBackend backend(ForStStateBackend::Options{.path = (base_dir / "db").string(),
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

    ForStStateBackend restored(ForStStateBackend::Options{.path = (base_dir / "restore").string(),
                                                          .create_if_missing = true});
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
// parent's checkpoint and narrows to its key-group slice, keeping
// operator state.
TEST(ForStStateBackend, FactoryScaleUpRestoreReadsParentAndNarrows) {
    clink::forst::install();
    auto root = std::filesystem::temp_directory_path() / "clink_forst_scaleup";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string run1 = "forst://" + (root / "run1").string();
    const std::string run2 = "forst://" + (root / "run2").string();
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
    ASSERT_TRUE(built.restore_from.has_value()) << "build_forst must wire scale-up restore_from";
    built.backend->restore(*built.restore_from, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});

    EXPECT_TRUE(built.backend->get(op, sv(key_lo)).has_value());
    EXPECT_FALSE(built.backend->get(op, sv(key_hi)).has_value());
    auto off = built.backend->get_operator_state(op, sv(std::string{"offsets"}));
    ASSERT_TRUE(off.has_value()) << "operator state must survive scale-up";
    EXPECT_EQ(to_string(*off), "OFF");

    std::filesystem::remove_all(root);
}

// Scale-down rescale through the factory: one new subtask inherits
// SEVERAL parent subtasks. build_forst encodes all assigned parents;
// restore() re-homes onto the first and iterate-merges the rest, so the
// new subtask ends up with the union of the parents' keyed state AND
// every parent's operator state.
TEST(ForStStateBackend, FactoryScaleDownRestoreMergesParents) {
    clink::forst::install();
    auto root = std::filesystem::temp_directory_path() / "clink_forst_scaledown";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string run1 = "forst://" + (root / "run1").string();
    const std::string run2 = "forst://" + (root / "run2").string();
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
    ASSERT_TRUE(built.restore_from.has_value()) << "build_forst must wire scale-down restore_from";
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

// Backend-level: restore() merges multiple checkpoint dirs (newline-
// joined in snap.bytes) and the kg-filter still narrows the MERGED keyed
// rows while keeping operator state - proves the merge and the filter
// compose.
TEST(ForStStateBackend, RestoreMergesMultipleCheckpointsThenFilters) {
    auto base = std::filesystem::temp_directory_path() / "clink_forst_merge";
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    const clink::OperatorId op{6};
    const std::string lo = std::string(1, '\x05') + "lo";  // key group 5
    const std::string hi = std::string(1, '\x70') + "hi";  // key group 112
    {
        ForStStateBackend a(
            ForStStateBackend::Options{.path = (base / "a").string(), .create_if_missing = true});
        a.put(op, sv(lo), sv(std::string{"LO"}));
        a.put_operator_state(op, sv(std::string{"offA"}), sv(std::string{"OA"}));
        (void)a.snapshot(CheckpointId{1});
    }
    {
        ForStStateBackend b(
            ForStStateBackend::Options{.path = (base / "b").string(), .create_if_missing = true});
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

    ForStStateBackend merged(
        ForStStateBackend::Options{.path = (base / "merged").string(), .create_if_missing = true});
    // New subtask owns only the lower half: hi (kg 112) is filtered out
    // of the merged set; lo (kg 5) kept; BOTH parents' operator state kept.
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
TEST(ForStStateBackend, ArrowExportRoundTripsThroughInMemoryRestore) {
    auto dir = std::filesystem::temp_directory_path() / "clink_forst_arrow_export";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

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
// ascending, keys in engine byte order).
TEST(ForStStateBackend, CheckpointDirExportMatchesLiveExport) {
    auto dir = std::filesystem::temp_directory_path() / "clink_forst_arrow_cpdir";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

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
    const auto offline = forst_checkpoint_to_arrow(cp_dir);

    EXPECT_EQ(live, offline);
}

// An empty backend exports a VALID zero-row stream (schema + EOS), so
// downstream readers need no special-casing.
TEST(ForStStateBackend, EmptyBackendExportsValidStream) {
    auto dir = std::filesystem::temp_directory_path() / "clink_forst_arrow_empty";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});
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
// on ForSt state.
TEST(ForStStateBackend, StateVersionsSurviveSnapshotAndRestore) {
    auto dir = std::filesystem::temp_directory_path() / "clink_forst_versions";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});

    StateVersionMap versions;
    versions.set(OperatorId{1}, "CounterState", 2);
    versions.set(OperatorId{9}, "WindowAgg", 4);
    backend.set_state_versions(versions);
    EXPECT_EQ(backend.restored_state_versions().pack(), versions.pack());

    backend.put(OperatorId{1}, sv(std::string{"\x05"} + "s|k"), sv(std::string{"v"}));
    const auto snap = backend.snapshot(CheckpointId{11});

    ForStStateBackend fresh(
        ForStStateBackend::Options{.path = (dir / "db2").string(), .create_if_missing = true});
    EXPECT_TRUE(fresh.restored_state_versions().empty());
    fresh.restore(snap);
    EXPECT_EQ(fresh.restored_state_versions().pack(), versions.pack());
}

// Both Arrow exports embed the stamps in the stream's schema metadata,
// so an exported ForSt checkpoint restored into the in-memory reference
// reader carries them - and check-savepoint on the exported stream sees
// real versions.
TEST(ForStStateBackend, ArrowExportsCarryStateVersions) {
    auto dir = std::filesystem::temp_directory_path() / "clink_forst_versions_export";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    ForStStateBackend backend(
        ForStStateBackend::Options{.path = (dir / "db").string(), .create_if_missing = true});
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
        Snapshot{.checkpoint_id = CheckpointId{0}, .bytes = forst_checkpoint_to_arrow(cp_dir)});
    EXPECT_EQ(ref2.restored_state_versions().pack(), versions.pack());
}

// ----- changelog+forst composition -----

// The changelog+forst:// scheme composes the write-ahead changelog over
// a ForSt inner backend. Round-trip: write, checkpoint, restore in a
// fresh process-equivalent (new factory build), read back.
TEST(ForStStateBackend, ChangelogForstFactoryRoundTrip) {
    clink::forst::install();
    auto root = std::filesystem::temp_directory_path() / "clink_forst_changelog";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);
    const std::string uri = "changelog+forst://" + (root / "state").string();
    const clink::OperatorId op{8};

    {
        StateBackendSpec spec;
        spec.uri = uri;
        spec.subtask_idx = 0;
        auto built = StateBackendFactory::default_instance().build(spec);
        ASSERT_NE(built.backend, nullptr);
        built.backend->put(op, sv(std::string{"ck"}), sv(std::string{"cv"}));
        (void)built.backend->snapshot(CheckpointId{3});
    }

    StateBackendSpec spec;
    spec.uri = uri;
    spec.subtask_idx = 0;
    spec.restore_uri = uri;
    spec.restore_checkpoint_id = 3;
    auto built = StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value())
        << "build_changelog_forst must wire restore_from when a restore checkpoint is set";
    built.backend->restore(*built.restore_from);

    auto v = built.backend->get(op, sv(std::string{"ck"}));
    ASSERT_TRUE(v.has_value()) << "changelog+forst state lost across restart";
    EXPECT_EQ(to_string(*v), "cv");

    std::filesystem::remove_all(root);
}

// ----- deferred reads (Options::defer_reads) -----

namespace {

std::shared_ptr<ForStStateBackend> make_defer_backend(const std::string& tag) {
    auto dir = std::filesystem::temp_directory_path() / ("clink_forst_defer_" + tag);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    ForStStateBackend::Options opts;
    opts.path = (dir / "db").string();
    opts.create_if_missing = true;
    opts.defer_reads = true;
    opts.io_threads = 2;
    return std::make_shared<ForStStateBackend>(std::move(opts));
}

}  // namespace

// The headline contract: a deferred read runs on an IO thread and the
// completion is handed back through the wired resume scheduler, so the
// record resumes on the runner - the exact seam the async execution
// controller drives in production.
TEST(ForStStateBackend, DeferredReadDefersAndResumesThroughController) {
    // Controller before backend: the backend's IO thread may call
    // schedule_resume, so it must quiesce before the controller dies.
    AsyncExecutionController aec;
    auto backend = make_defer_backend("ctrl");
    ASSERT_TRUE(backend->supports_async_get());
    backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));

    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });

    std::optional<std::string> resolved;
    std::thread::id resume_thread;
    const bool accepted = aec.submit("k", [&]() -> async::Task<void> {
        auto v = co_await backend->get_async(OperatorId{1}, sv(std::string{"k"}));
        if (v) {
            resolved = to_string(*v);
        }
        resume_thread = std::this_thread::get_id();
        co_return;
    });
    ASSERT_TRUE(accepted);
    aec.drain();

    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(*resolved, "v");
    EXPECT_EQ(resume_thread, std::this_thread::get_id()) << "must resume on the runner";
    EXPECT_EQ(backend->deferred_reads(), 1u);
    backend->set_async_resume_scheduler({});
}

// Without a wired scheduler the read is a safe inline blocking load: the
// task completes in one synchronous step and nothing is deferred.
TEST(ForStStateBackend, DeferredReadFallsBackInlineWithoutScheduler) {
    auto backend = make_defer_backend("inline");
    backend->put(OperatorId{1}, sv(std::string{"k"}), sv(std::string{"v"}));

    auto t = backend->get_async(OperatorId{1}, sv(std::string{"k"}));
    t.resume();
    ASSERT_TRUE(t.done());
    auto got = t.get();
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(to_string(*got), "v");
    EXPECT_EQ(backend->deferred_reads(), 0u);
}

// A batched read is ONE executor job - a single deferral for N keys,
// results positional, absent keys nullopt, buffered writes visible.
TEST(ForStStateBackend, DeferredGetManyIsOneDeferralForTheBatch) {
    AsyncExecutionController aec;
    auto backend = make_defer_backend("many");
    backend->put(OperatorId{1}, sv(std::string{"a"}), sv(std::string{"1"}));
    backend->put(OperatorId{1}, sv(std::string{"b"}), sv(std::string{"2"}));

    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });

    std::vector<std::optional<StateBackend::Value>> got;
    const bool accepted = aec.submit("batch", [&]() -> async::Task<void> {
        got = co_await backend->get_many_async(OperatorId{1}, {"a", "missing", "b"});
        co_return;
    });
    ASSERT_TRUE(accepted);
    aec.drain();

    ASSERT_EQ(got.size(), 3u);
    ASSERT_TRUE(got[0].has_value());
    EXPECT_EQ(to_string(*got[0]), "1");
    EXPECT_FALSE(got[1].has_value());
    ASSERT_TRUE(got[2].has_value());
    EXPECT_EQ(to_string(*got[2]), "2");
    EXPECT_EQ(backend->deferred_reads(), 1u) << "the whole batch is one deferral";
    backend->set_async_resume_scheduler({});
}

// Scheme surface: forst://<dir>?defer_reads=1 builds a deferring backend;
// the plain URI stays synchronous.
TEST(ForStStateBackend, SchemeDeferReadsParam) {
    clink::forst::install();
    auto root = std::filesystem::temp_directory_path() / "clink_forst_defer_scheme";
    std::filesystem::remove_all(root);
    auto& f = StateBackendFactory::default_instance();

    StateBackendSpec plain;
    plain.uri = "forst://" + (root / "plain").string();
    plain.subtask_idx = 0;
    EXPECT_FALSE(f.build(plain).backend->supports_async_get());

    StateBackendSpec defer;
    defer.uri = "forst://" + (root / "defer").string() + "?defer_reads=1&io_threads=2";
    defer.subtask_idx = 0;
    EXPECT_TRUE(f.build(defer).backend->supports_async_get());

    std::filesystem::remove_all(root);
}

// The payoff, end to end through the REAL runner: a keyed aggregate over a
// deferring forst backend rides the async path - per-record hot state
// lives in the engine, every read is deferred to the IO pool and resumed
// by the controller, and the output is byte-identical to the sync run.
TEST(ForStStateBackend, DeferredReadsCarryHotPathStateThroughRealRunner) {
    using KV = std::pair<std::int64_t, std::int64_t>;
    const std::vector<KV> input = {{1, 10}, {2, 20}, {1, 30}, {2, 5}, {1, 7}};

    auto backend = make_defer_backend("runner");

    std::vector<Record<KV>> records;
    records.reserve(input.size());
    for (const auto& x : input) {
        records.emplace_back(x);
    }
    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(std::move(records));
    auto op = std::make_shared<KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        int64_codec(),
        int64_codec(),
        "sum");
    auto sink = std::make_shared<CollectingSink<KV>>();
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Cross-key emission order is completion order on a genuinely
    // deferring backend (the per-key gate orders only same-key records),
    // so assert the per-key running-sum subsequences, not a global order.
    const auto got = sink->collected();
    auto per_key = [&](std::int64_t k) {
        std::vector<std::int64_t> vals;
        for (const auto& [key, v] : got) {
            if (key == k) {
                vals.push_back(v);
            }
        }
        return vals;
    };
    ASSERT_EQ(got.size(), input.size());
    EXPECT_EQ(per_key(1), (std::vector<std::int64_t>{10, 40, 47}));
    EXPECT_EQ(per_key(2), (std::vector<std::int64_t>{20, 25}));
    EXPECT_EQ(backend->deferred_reads(), input.size())
        << "every record's state read must ride the IO pool";
}
