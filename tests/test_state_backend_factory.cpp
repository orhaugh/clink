// Unit tests for StateBackendFactory: scheme dispatch, restore staging,
// and custom-scheme registration so a new backend (e.g. S3) only needs
// to plug a builder into the registry.

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/state_backend_factory.hpp"

namespace {

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

TEST(StateBackendFactory, FileSchemeStagesRestoreFile) {
    // Pre-create a snapshot in the restore dir that the factory must
    // copy into the working dir so the new backend can load it.
    const auto restore_root = make_temp_dir("restore_src");
    const auto working_root = make_temp_dir("restore_dst");
    const std::uint32_t subtask = 1;
    const std::uint64_t ckpt_id = 42;
    const auto restore_subtask = restore_root / std::to_string(subtask);
    std::filesystem::create_directories(restore_subtask);
    const auto snap_file = restore_subtask / ("checkpoint-" + std::to_string(ckpt_id) + ".snap");
    {
        std::ofstream out(snap_file, std::ios::binary);
        out << "sentinel";  // non-empty payload so restore is observable
    }

    clink::StateBackendSpec spec;
    spec.uri = "file://" + working_root.string();
    spec.subtask_idx = subtask;
    spec.restore_uri = "file://" + restore_root.string();
    spec.restore_checkpoint_id = ckpt_id;

    const auto built = clink::StateBackendFactory::default_instance().build(spec);
    ASSERT_NE(built.backend, nullptr);
    ASSERT_TRUE(built.restore_from.has_value());
    EXPECT_EQ(built.restore_from->checkpoint_id.value(), ckpt_id);

    const auto dst_file = working_root / std::to_string(subtask) /
                          ("checkpoint-" + std::to_string(ckpt_id) + ".snap");
    EXPECT_TRUE(std::filesystem::exists(dst_file))
        << "factory should stage the snapshot file in the working dir";

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
        std::filesystem::create_directories(dir);
        std::ofstream out(dir / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"),
                          std::ios::binary);
        out.write(reinterpret_cast<const char*>(snap.bytes.data()),
                  static_cast<std::streamsize>(snap.bytes.size()));
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
        std::filesystem::create_directories(dir);
        std::ofstream out(dir / ("checkpoint-" + std::to_string(ckpt_id) + ".snap"),
                          std::ios::binary);
        out.write(reinterpret_cast<const char*>(snap.bytes.data()),
                  static_cast<std::streamsize>(snap.bytes.size()));
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

}  // namespace
