// Unit tests for StateBackendFactory: scheme dispatch, restore staging,
// and custom-scheme registration so a new backend (e.g. S3) only needs
// to plug a builder into the registry.

#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/fault/fault_injection.hpp"
#include "clink/state/checkpoint_integrity.hpp"
#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/remote_read_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace {

// Publish a checkpoint payload the way the production writer does:
// payload first, then the integrity sidecar that certifies it. A fixture
// that writes only the payload is, by the on-disk contract, an UNPUBLISHED
// checkpoint - the factory is right to refuse it, so the fixtures have to
// produce the real thing.
void publish_snapshot(const std::filesystem::path& path,
                      const std::vector<std::byte>& bytes,
                      std::uint64_t ckpt_id) {
    std::filesystem::create_directories(path.parent_path());
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out.write(reinterpret_cast<const char*>(bytes.data()),
                  static_cast<std::streamsize>(bytes.size()));
    }
    clink::state::write_checkpoint_meta(path, ckpt_id, bytes.data(), bytes.size());
}

std::filesystem::path make_temp_dir(const std::string& label) {
    const auto p = std::filesystem::temp_directory_path() /
                   ("clink_factory_test_" + label + "_" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(p);
    std::filesystem::create_directories(p);
    return p;
}

TEST(StateBackendFactory, EmptyUriYieldsInMemoryBackend) {
    clink::StateBackendSpec spec;  // uri empty
    const auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    EXPECT_FALSE(built.restore_from.has_value());
    auto* mem = dynamic_cast<clink::InMemoryStateBackend*>(built.backend.get());
    EXPECT_NE(mem, nullptr) << "empty uri should select InMemoryStateBackend";
}

TEST(StateBackendFactory, MemorySchemeYieldsInMemoryBackend) {
    clink::StateBackendSpec spec;
    spec.uri = "memory://";
    const auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    auto* mem = dynamic_cast<clink::InMemoryStateBackend*>(built.backend.get());
    EXPECT_NE(mem, nullptr);
}

TEST(StateBackendFactory, BarePathSelectsFileBackend) {
    const auto dir = make_temp_dir("bare_path");
    clink::StateBackendSpec spec;
    spec.uri = dir.string();
    spec.subtask_idx = 2;
    const auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    auto* fb = dynamic_cast<clink::FileBackedStateBackend*>(built.backend.get());
    ASSERT_NE(fb, nullptr) << "bare path should select FileBackedStateBackend";
    EXPECT_EQ(fb->snapshot_dir(), dir / "2");
    std::filesystem::remove_all(dir);
}

// Restoring from another directory (a savepoint, a relocated checkpoint dir)
// loads that state and writes NOTHING into the working directory.
//
// This replaces an assertion that the factory "should stage the snapshot file in
// the working dir". That described the implementation rather than the contract,
// and the implementation was wrong: staging into <base>/<subtask idx> aliases the
// parent files of other subtasks once an operator is resized, because global
// indices shift (see ARescaleRestoreLeavesEveryParentSnapshotUntouched below).
// The old assertion also never checked that the state ARRIVED - its payload was
// the string "sentinel", which is not a loadable snapshot, so a factory that
// copied the file and restored nothing would have passed. This asserts the state
// is loadable and that the working dir stays clean.
TEST(StateBackendFactory, FileSchemeRestoresFromAnotherDirWithoutWritingToTheWorkingDir) {
    const auto restore_root = make_temp_dir("restore_src");
    const auto working_root = make_temp_dir("restore_dst");
    const std::uint32_t subtask = 1;
    const std::uint64_t ckpt_id = 42;
    const clink::OperatorId op{1};

    {
        clink::InMemoryStateBackend seed;
        seed.put(op,
                 clink::StateBackend::KeyView{"restored_key"},
                 clink::StateBackend::ValueView{"restored_value"});
        auto snap = seed.snapshot(clink::CheckpointId{ckpt_id});
        publish_snapshot(restore_root / std::to_string(subtask) /
                             ("checkpoint-" + std::to_string(ckpt_id) + ".snap"),
                         snap.bytes,
                         ckpt_id);
    }

    clink::StateBackendSpec spec;
    spec.uri = "file://" + working_root.string();
    spec.subtask_idx = subtask;
    spec.restore_uri = "file://" + restore_root.string();
    spec.restore_checkpoint_id = ckpt_id;

    auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value());
    EXPECT_EQ(built.restore_from->checkpoint_id.value(), ckpt_id);

    built.backend->restore(*built.restore_from);
    EXPECT_TRUE(built.backend->get(op, clink::StateBackend::KeyView{"restored_key"}).has_value())
        << "the backend did not come up holding the state it was told to restore";

    // No snapshot file may appear in the working dir: this subtask has not taken
    // a checkpoint yet, and a staged copy is what aliases other subtasks' parents.
    std::vector<std::string> staged;
    std::error_code ec;
    for (const auto& e : std::filesystem::recursive_directory_iterator(working_root, ec)) {
        if (e.is_regular_file() && e.path().filename().string().starts_with("checkpoint-")) {
            staged.push_back(std::filesystem::relative(e.path(), working_root).string());
        }
    }
    EXPECT_TRUE(staged.empty()) << "the restore wrote " << staged.size()
                                << " checkpoint file(s) into the working dir, the first being "
                                << (staged.empty() ? std::string{} : staged.front());

    std::filesystem::remove_all(restore_root);
    std::filesystem::remove_all(working_root);
}

TEST(StateBackendFactory, ScaleDownMergesMultipleParentSnapshotFiles) {
    // Synthesise two parent snapshot files holding disjoint key sets,
    // ask the factory to merge them into one working dir, then verify
    // the resulting backend sees both parents' keys after restore.
    const auto restore_root = make_temp_dir("merge_src");
    const auto working_root = make_temp_dir("merge_dst");
    const std::uint64_t ckpt_id = 7;

    // Build two valid InMemoryStateBackend snapshots, each with one
    // key under the same OperatorId. The on-disk format is just the
    // snapshot bytes the in-memory backend produces.
    clink::OperatorId op{1};
    auto write_parent = [&](std::uint32_t parent_idx,
                            const std::string& key,
                            const std::string& value) {
        clink::InMemoryStateBackend backend;
        backend.put(op, clink::StateBackend::KeyView{key}, clink::StateBackend::ValueView{value});
        auto snap = backend.snapshot(clink::CheckpointId{ckpt_id});
        const auto dir = restore_root / std::to_string(parent_idx);
        publish_snapshot(
            dir / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"), snap.bytes, ckpt_id);
    };
    write_parent(0, "key_from_parent_0", "v0");
    write_parent(1, "key_from_parent_1", "v1");

    clink::StateBackendSpec spec;
    spec.uri = "file://" + working_root.string();
    spec.subtask_idx = 0;
    spec.restore_uri = "file://" + restore_root.string();
    spec.restore_checkpoint_id = ckpt_id;
    spec.restore_from_subtask_idx = 0;
    spec.restore_from_parent_count = 2;

    auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value());
    built.backend->restore(*built.restore_from);

    EXPECT_TRUE(
        built.backend->get(op, clink::StateBackend::KeyView{"key_from_parent_0"}).has_value());
    EXPECT_TRUE(
        built.backend->get(op, clink::StateBackend::KeyView{"key_from_parent_1"}).has_value());

    std::filesystem::remove_all(restore_root);
    std::filesystem::remove_all(working_root);
}

// #54 Gap B part 2: on rescale a new subtask inherits KEYED state only from
// its assigned parent, but OPERATOR state (source offsets, broadcast) from
// ALL parents - operator state is broadcast, not partitioned. Scale-up here
// assigns subtask only parent 1, yet it must end up with both parents'
// operator rows (and only parent 1's keyed row).
TEST(StateBackendFactory, RescaleUnionsOperatorStateFromAllParents) {
    const auto restore_root = make_temp_dir("opunion_src");
    const auto working_root = make_temp_dir("opunion_dst");
    const std::uint64_t ckpt_id = 11;
    const clink::OperatorId op{1};

    // Two parents (old parallelism 2). Each has a keyed row and one
    // per-partition operator-state row.
    auto write_parent = [&](std::uint32_t idx,
                            const std::string& keyed,
                            const std::string& off_key,
                            const std::string& off_val) {
        clink::InMemoryStateBackend backend;
        backend.put(op, clink::StateBackend::KeyView{keyed}, clink::StateBackend::ValueView{"K"});
        backend.put_operator_state(
            op, clink::StateBackend::KeyView{off_key}, clink::StateBackend::ValueView{off_val});
        auto snap = backend.snapshot(clink::CheckpointId{ckpt_id});
        const auto dir = restore_root / std::to_string(idx);
        publish_snapshot(
            dir / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"), snap.bytes, ckpt_id);
    };
    write_parent(0, "keyed0", "off:0", "P0OFF");
    write_parent(1, "keyed1", "off:1", "P1OFF");

    // New subtask assigned ONLY parent 1 (scale-up shape: one parent).
    clink::StateBackendSpec spec;
    spec.uri = "file://" + working_root.string();
    spec.subtask_idx = 2;
    spec.restore_uri = "file://" + restore_root.string();
    spec.restore_checkpoint_id = ckpt_id;
    spec.restore_from_subtask_idx = 1;  // marks a rescale; assigned parent = 1
    spec.restore_from_parent_count = 1;

    auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_TRUE(built.restore_from.has_value());
    built.backend->restore(*built.restore_from);  // covers_all: isolate the union check

    // Operator rows from BOTH parents (union).
    auto o0 = built.backend->get_operator_state(op, clink::StateBackend::KeyView{"off:0"});
    auto o1 = built.backend->get_operator_state(op, clink::StateBackend::KeyView{"off:1"});
    ASSERT_TRUE(o0.has_value());
    ASSERT_TRUE(o1.has_value());
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(o0->data()), o0->size()), "P0OFF");
    EXPECT_EQ(std::string(reinterpret_cast<const char*>(o1->data()), o1->size()), "P1OFF");
    // Keyed rows: only the assigned parent's. Parent 0 contributed operator
    // rows only, so its keyed row is absent.
    EXPECT_TRUE(built.backend->get(op, clink::StateBackend::KeyView{"keyed1"}).has_value());
    EXPECT_FALSE(built.backend->get(op, clink::StateBackend::KeyView{"keyed0"}).has_value());

    std::filesystem::remove_all(restore_root);
    std::filesystem::remove_all(working_root);
}

TEST(StateBackendFactory, RestoreUriEmptyLeavesRestoreFromUnset) {
    const auto dir = make_temp_dir("no_restore");
    clink::StateBackendSpec spec;
    spec.uri = dir.string();
    const auto built = clink::StateBackendFactory::default_instance().build(spec);
    EXPECT_FALSE(built.restore_from.has_value());
    std::filesystem::remove_all(dir);
}

TEST(StateBackendFactory, RegisterCustomSchemeRoutesViaBuilder) {
    auto& factory = clink::StateBackendFactory::default_instance();
    bool invoked = false;
    factory.register_scheme("test-custom", [&invoked](const clink::StateBackendSpec&) {
        invoked = true;
        clink::BuiltStateBackend out;
        out.backend = std::make_shared<clink::InMemoryStateBackend>();
        return out;
    });
    ASSERT_TRUE(factory.has_scheme("test-custom"));
    clink::StateBackendSpec spec;
    spec.uri = "test-custom://anything-goes";
    const auto built = factory.build(spec);
    EXPECT_TRUE(invoked);
    EXPECT_NE(built.backend, nullptr);
}

TEST(StateBackendFactory, UnknownSchemeThrows) {
    auto& factory = clink::StateBackendFactory::default_instance();
    clink::StateBackendSpec spec;
    spec.uri = "nonexistent-scheme-xyz://anywhere";
    EXPECT_THROW(factory.build(spec), std::runtime_error);
}

// disagg-local:// builds a deferring backend (RemoteReadBackend over an
// in-memory pool) with NO S3, so the async/disaggregated execution path can
// auto-activate. supports_async_get()==true is the capability the runner and
// the SQL aggregate's backend-aware open() gate on.
TEST(StateBackendFactory, DisaggLocalSchemeYieldsDeferringBackend) {
    clink::StateBackendSpec spec;
    spec.uri = "disagg-local://";
    const auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    EXPECT_TRUE(built.backend->supports_async_get())
        << "disagg-local must report a deferring backend so the async path auto-activates";
    // Process-local, no durable remote tier: nothing to stage on restore.
    EXPECT_FALSE(built.restore_from.has_value());
}

// The URI query params (io_threads, hot_max_bytes) parse without throwing and
// the result stays a deferring backend. Malformed values fall back to defaults.
TEST(StateBackendFactory, DisaggLocalParsesParamsAndStaysDeferring) {
    auto& factory = clink::StateBackendFactory::default_instance();
    for (const char* uri : {"disagg-local://?io_threads=4&hot_max_bytes=4096",
                            "disagg-local://?io_threads=0",
                            "disagg-local://?hot_max_bytes=oops"}) {
        clink::StateBackendSpec spec;
        spec.uri = uri;
        const auto built = factory.build(spec);
        ASSERT_NE(built.backend, nullptr) << uri;
        EXPECT_TRUE(built.backend->supports_async_get()) << uri;
    }
}

// hot_max_bytes defaults to a non-zero heap fraction, so disaggregation-of-
// working-set (LRU eviction to the pool) is ON by default. An explicit value
// overrides it; an explicit 0 forces the unbounded tier; a malformed value keeps
// the default.
TEST(StateBackendFactory, DisaggLocalDefaultsHotBudgetToHeapFraction) {
    auto& factory = clink::StateBackendFactory::default_instance();
    auto budget = [&](const char* uri) -> std::size_t {
        clink::StateBackendSpec spec;
        spec.uri = uri;
        auto built = factory.build(spec);
        auto* rrb = dynamic_cast<clink::RemoteReadBackend*>(built.backend.get());
        EXPECT_NE(rrb, nullptr) << uri;
        return rrb != nullptr ? rrb->hot_max_bytes() : 0;
    };
    EXPECT_GT(budget("disagg-local://"), 0u);  // default = heap fraction (non-zero)
    EXPECT_GE(budget("disagg-local://"), 64ull * 1024 * 1024);       // at least the floor
    EXPECT_EQ(budget("disagg-local://?hot_max_bytes=4096"), 4096u);  // explicit overrides
    EXPECT_EQ(budget("disagg-local://?hot_max_bytes=0"), 0u);        // explicit 0 = unbounded
    EXPECT_GT(budget("disagg-local://?hot_max_bytes=oops"), 0u);     // malformed -> default
}

// The common backends are NOT deferring, so the async path must never
// auto-activate on them - the guarantee that makes auto-on safe for existing
// memory/file jobs (they keep their synchronous, byte-for-byte path).
TEST(StateBackendFactory, CommonSchemesAreNotDeferring) {
    auto& factory = clink::StateBackendFactory::default_instance();
    for (const char* uri : {"memory://", "memory+sharded://", "changelog://"}) {
        clink::StateBackendSpec spec;
        spec.uri = uri;
        const auto built = factory.build(spec);
        ASSERT_NE(built.backend, nullptr) << uri;
        EXPECT_FALSE(built.backend->supports_async_get()) << uri;
    }
}

}  // namespace

// ---------------------------------------------------------------------------
// The restore base and the working base are THE SAME DIRECTORY in production.
//
// Every rescale test above hands the factory a separate `restore_root` and
// `working_root`, and that is why none of them caught what follows. The
// coordinator sets `restore_from_dir = checkpoint_dir` (coordinator.cpp), and
// plugin_impl passes `spec.uri = checkpoint_dir`, so a real subtask's working
// directory sits in the same namespace as the parents it reads:
//
//   working  <base>/<new global subtask idx>/checkpoint-<id>.snap
//   parent   <base>/<old global subtask idx>/checkpoint-<id>.snap
//
// The planner allocates global indices as one contiguous block per operator in
// graph order, so resizing one operator SHIFTS every later operator's block.
// After scaling `counter` from 1 to 4 in a source -> counter -> sink job, the
// indices go 0,1,2 to 0,{1,2,3,4},5 - and the new counter children at indices 2,
// 3 and 4 write their stitched snapshots straight over the directories that
// still hold the OLD sink's state, which the new sink at index 5 has to read.
//
// The index MAPPING is correct (F38 fixed that: each task maps its index within
// its operator through its operator's old block base). What is not is the write
// target: a restore rewrites files that other subtasks are reading as parents.
// The fixtures below use ONE root, as production does.

// The invariant, stated directly: restoring must not modify anybody else's
// snapshot. Asserted by digesting every parent file before and after, which
// covers both collisions - a child overwriting another operator's parent, and a
// child rewriting the very file its siblings still have to read.
TEST(StateBackendFactory, ARescaleRestoreLeavesEveryParentSnapshotUntouched) {
    const auto base = make_temp_dir("shared_base_immutable");
    const std::uint64_t ckpt_id = 20;
    const clink::OperatorId op{1};

    // The pre-rescale shape: source at 0, counter at 1, sink at 2.
    auto write_parent = [&](std::uint32_t idx, const std::string& keyed, const std::string& val) {
        clink::InMemoryStateBackend backend;
        backend.put(op, clink::StateBackend::KeyView{keyed}, clink::StateBackend::ValueView{val});
        auto snap = backend.snapshot(clink::CheckpointId{ckpt_id});
        publish_snapshot(
            base / std::to_string(idx) / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"),
            snap.bytes,
            ckpt_id);
    };
    write_parent(0, "source_offset_row", "SRC");
    write_parent(1, "counter_key", "CNT");
    write_parent(2, "sink_handle_row", "SNK");

    // Fingerprint every snapshot file and its sidecar before the rescale.
    std::map<std::string, std::string> before;
    for (const auto& e : std::filesystem::recursive_directory_iterator(base)) {
        if (!e.is_regular_file()) {
            continue;
        }
        std::ifstream in(e.path(), std::ios::binary);
        const std::string body((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        before[std::filesystem::relative(e.path(), base).string()] = body;
    }
    ASSERT_GE(before.size(), 3u) << "fixtures did not publish the three parents";

    // A new counter child whose global index (2) lands on the OLD SINK's
    // directory, inheriting from the old counter at index 1.
    clink::StateBackendSpec spec;
    spec.uri = "file://" + base.string();
    spec.subtask_idx = 2;
    spec.restore_uri = "file://" + base.string();  // the same base, as in production
    spec.restore_checkpoint_id = ckpt_id;
    spec.restore_from_subtask_idx = 1;
    spec.restore_from_parent_count = 1;
    auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);

    for (const auto& [rel, body] : before) {
        std::ifstream in(base / rel, std::ios::binary);
        const std::string now((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
        EXPECT_EQ(now, body) << "the rescale restore rewrote " << rel
                             << ", which belongs to another subtask. A parent snapshot must be "
                                "immutable while other subtasks are still reading it: whoever "
                                "owns that index next inherits the wrong operator's state, and a "
                                "sibling reading it mid-rewrite sees the payload and the sidecar "
                                "disagree and fails its integrity check.";
    }

    std::filesystem::remove_all(base);
}

// The consequence, in the terms an operator would see it: the sink comes up
// holding a counter's state.
TEST(StateBackendFactory, TheSinkStillRestoresItsOwnStateAfterAnOperatorBeforeItGrew) {
    const auto base = make_temp_dir("shared_base_sink");
    const std::uint64_t ckpt_id = 20;
    const clink::OperatorId op{1};

    auto write_parent = [&](std::uint32_t idx, const std::string& keyed) {
        clink::InMemoryStateBackend backend;
        backend.put(op, clink::StateBackend::KeyView{keyed}, clink::StateBackend::ValueView{"V"});
        auto snap = backend.snapshot(clink::CheckpointId{ckpt_id});
        publish_snapshot(
            base / std::to_string(idx) / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"),
            snap.bytes,
            ckpt_id);
    };
    write_parent(0, "source_offset_row");
    write_parent(1, "counter_key");
    write_parent(2, "sink_handle_row");

    const std::string base_uri = "file://" + base.string();
    // The three new counter children that land on indices 2, 3 and 4, all
    // inheriting from the single old counter at 1. Index 2 is the old sink's.
    for (std::uint32_t idx : {2u, 3u, 4u}) {
        clink::StateBackendSpec child;
        child.uri = base_uri;
        child.subtask_idx = idx;
        child.restore_uri = base_uri;
        child.restore_checkpoint_id = ckpt_id;
        child.restore_from_subtask_idx = 1;
        child.restore_from_parent_count = 1;
        (void)clink::StateBackendFactory::default_instance().build(child);
    }

    // Now the new sink at index 5, inheriting the old sink at index 2.
    clink::StateBackendSpec sink;
    sink.uri = base_uri;
    sink.subtask_idx = 5;
    sink.restore_uri = base_uri;
    sink.restore_checkpoint_id = ckpt_id;
    sink.restore_from_subtask_idx = 2;
    sink.restore_from_parent_count = 1;
    auto built = clink::StateBackendFactory::default_instance().build(sink);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value())
        << "the sink restored nothing at all after the operator before it was resized";
    built.backend->restore(*built.restore_from);

    EXPECT_TRUE(built.backend->get(op, clink::StateBackend::KeyView{"sink_handle_row"}).has_value())
        << "the sink did not restore its OWN state. A counter child's new global index landed on "
           "the sink's old directory and overwrote the snapshot the sink had to inherit - for a "
           "2PC sink that is the commit handle, so a staged transaction becomes uncommittable.";
    EXPECT_FALSE(built.backend->get(op, clink::StateBackend::KeyView{"counter_key"}).has_value())
        << "the sink restored the COUNTER's keyed state, which it has no business holding.";

    std::filesystem::remove_all(base);
}

// The state.before_restore fault point must fire on EVERY restore this backend
// performs, whichever source the bytes came from.
//
// A regression test for a real regression. The rescale fix above added an early
// return for caller-supplied bytes, and it went in ABOVE the fault point - so
// arming `state.before_restore` silently stopped killing anything, and
// FaultRecoveryTest.WorkerKilledAtTheStateRestorePointIsRedeployed failed on Linux
// with "the armed worker never reached state.before_restore". The scenario had
// quietly become a test of nothing.
//
// Note precisely what scripts/check-fault-points.sh does and does not buy: it
// proves a declared point has a CALL SITE somewhere, which it still did. It cannot
// prove the call site is REACHABLE. Only arming the point and counting hits does
// that, which is what this test is for. `Observe` counts without perturbing.
TEST(StateBackendFactory, TheBeforeRestoreFaultPointFiresOnBothRestorePaths) {
#ifndef CLINK_FAULT_INJECTION
    GTEST_SKIP() << "built without fault injection";
#else
    const auto dir = make_temp_dir("fault_point_reach");
    const std::uint64_t ckpt_id = 5;
    const clink::OperatorId op{1};

    clink::InMemoryStateBackend seed;
    seed.put(op, clink::StateBackend::KeyView{"k"}, clink::StateBackend::ValueView{"v"});
    const auto snap = seed.snapshot(clink::CheckpointId{ckpt_id});

    // Path 1: bytes on disk, nothing supplied by the caller.
    {
        clink::fault::ScopedFault guard;
        clink::fault::Registry::instance().arm(
            clink::fault::Rule{.point = std::string{clink::fault::points::kStateBeforeRestore},
                               .action = clink::fault::Action::Observe});
        publish_snapshot(
            dir / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"), snap.bytes, ckpt_id);
        clink::FileBackedStateBackend backend(dir);
        backend.restore(clink::Snapshot{clink::CheckpointId{ckpt_id}, {}});
        EXPECT_EQ(
            clink::fault::Registry::instance().hits(clink::fault::points::kStateBeforeRestore), 1u)
            << "the fault point did not fire on the from-disk restore path";
    }

    // Path 2: bytes supplied by the caller, which is how a rescale hands over
    // state stitched from its parents. This is the path that regressed.
    {
        clink::fault::ScopedFault guard;
        clink::fault::Registry::instance().arm(
            clink::fault::Rule{.point = std::string{clink::fault::points::kStateBeforeRestore},
                               .action = clink::fault::Action::Observe});
        clink::FileBackedStateBackend backend(dir / "supplied");
        backend.restore(clink::Snapshot{clink::CheckpointId{ckpt_id}, snap.bytes});
        EXPECT_EQ(
            clink::fault::Registry::instance().hits(clink::fault::points::kStateBeforeRestore), 1u)
            << "the fault point did not fire when the bytes were supplied in memory. An early "
               "return placed above the point makes every fault armed on it inert, and the "
               "integration scenario that arms it becomes a test of nothing.";
    }

    std::filesystem::remove_all(dir);
#endif
}

// Scale-down merge, modelled on the shape the integration test actually runs:
// four parents each owning a disjoint slice of a 12-key space, folded into one
// subtask that owns all of them.
//
// Follow-up 44 reports a counter one too high after exactly this rescale
// (STATE-MISMATCH-key5-at41-got4-want3). The merge is the first thing to rule in or
// out, and it can be ruled on deterministically here rather than by re-running a
// multi-process test that fails about one time in thirty.
TEST(StateBackendFactory, ScaleDownMergeKeepsEveryParentsCountExactly) {
    const auto base = make_temp_dir("scale_down_merge");
    const std::uint64_t ckpt_id = 30;
    const clink::OperatorId op{7};
    constexpr int kKeys = 12;
    constexpr int kParents = 4;

    // Key k belongs to parent k % kParents, mirroring a key-group split. Its value
    // is the count of records seen for that key, which is what the job's operator
    // self-check compares against.
    const auto expected_count = [](int key) { return 3 + key; };

    for (int p = 0; p < kParents; ++p) {
        clink::InMemoryStateBackend backend;
        for (int key = 0; key < kKeys; ++key) {
            if (key % kParents != p) {
                continue;
            }
            backend.put(op,
                        clink::StateBackend::KeyView{"key" + std::to_string(key)},
                        clink::StateBackend::ValueView{std::to_string(expected_count(key))});
        }
        auto snap = backend.snapshot(clink::CheckpointId{ckpt_id});
        publish_snapshot(
            base / std::to_string(p + 1) / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"),
            snap.bytes,
            ckpt_id);
    }

    // One new subtask inheriting all four parents (global indices 1..4).
    clink::StateBackendSpec spec;
    spec.uri = "file://" + base.string();
    spec.subtask_idx = 1;
    spec.restore_uri = "file://" + base.string();
    spec.restore_checkpoint_id = ckpt_id;
    spec.restore_from_subtask_idx = 1;
    spec.restore_from_parent_count = kParents;

    auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value()) << "the scale-down restored nothing at all";
    built.backend->restore(*built.restore_from);

    for (int key = 0; key < kKeys; ++key) {
        const auto got =
            built.backend->get(op, clink::StateBackend::KeyView{"key" + std::to_string(key)});
        ASSERT_TRUE(got.has_value())
            << "key" << key << " (owned by parent " << (key % kParents)
            << ") is absent after the merge - a scale-down dropped a parent's slice";
        const std::string got_str(reinterpret_cast<const char*>(got->data()), got->size());
        EXPECT_EQ(got_str, std::to_string(expected_count(key)))
            << "key" << key
            << " came back with the wrong count after the merge. A value one too "
               "high is the signature reported in follow-up 44.";
    }

    std::filesystem::remove_all(base);
}
