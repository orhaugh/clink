// Unit tests for the JM-side pre-deploy restore-compatibility gate
// (schema evolution D, part 2). The gate reads a savepoint's stored
// version map from <restore_from_dir>/0/checkpoint-<id>.snap and asks the
// job .so, .so-side, whether it can migrate to its expected versions.
//
// Uses the schema_evo_test_job fixture (expects "counter" v3 with a
// 1->2->3 chain registered, keyed on operator_id_from_uid("counter-op")).
// Writes real savepoints to a temp dir laid out the way the file backend
// does, then drives the gate against them.

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/restore_compat_gate.hpp"
#include "clink/core/types.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/schema_version.hpp"

namespace {

const char* schema_evo_job_path() {
#ifdef CLINK_SCHEMA_EVO_JOB_PATH
    return CLINK_SCHEMA_EVO_JOB_PATH;
#else
    return nullptr;
#endif
}

// Write a savepoint stamped with `versions` to <dir>/0/checkpoint-1.snap
// (the layout the file backend restores from for subtask 0). Returns the
// restore dir.
std::filesystem::path write_savepoint(const clink::StateVersionMap& versions,
                                      const std::string& tag) {
    namespace fs = std::filesystem;
    static std::atomic<std::uint64_t> counter{0};
    auto dir = fs::temp_directory_path() / ("rcg_" + tag + "_" + std::to_string(getpid()) + "_" +
                                            std::to_string(counter.fetch_add(1)));
    fs::create_directories(dir / "0");

    clink::InMemoryStateBackend backend;
    backend.put(clink::operator_id_from_uid("counter-op"), "k", std::string_view{"v"});
    backend.set_state_versions(versions);
    auto snap = backend.snapshot(clink::CheckpointId{1});

    std::ofstream f(dir / "0" / "checkpoint-1.snap", std::ios::binary);
    if (!snap.bytes.empty()) {
        f.write(reinterpret_cast<const char*>(snap.bytes.data()),
                static_cast<std::streamsize>(snap.bytes.size()));
    }
    return dir;
}

}  // namespace

TEST(RestoreCompatGate, EmptyRestoreDirIsNotGated) {
    // No restore configured -> nothing to check -> "" (proceed).
    EXPECT_TRUE(clink::cluster::check_restore_compatibility_via_plugins({}, "", 0).empty());
}

TEST(RestoreCompatGate, CompatibleSavepointPassesGate) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 1);  // -> v3 via chain
    auto dir = write_savepoint(versions, "compat");

    const auto reject =
        clink::cluster::check_restore_compatibility_via_plugins({schema_evo_job_path()},
                                                                dir.string(),
                                                                /*restore_checkpoint_id=*/1);
    EXPECT_TRUE(reject.empty()) << "unexpected reject: " << reject;
    std::filesystem::remove_all(dir);
}

TEST(RestoreCompatGate, IncompatibleSavepointIsRejected) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 5);  // no v5->v3 path
    auto dir = write_savepoint(versions, "incompat");

    const auto reject =
        clink::cluster::check_restore_compatibility_via_plugins({schema_evo_job_path()},
                                                                dir.string(),
                                                                /*restore_checkpoint_id=*/1);
    EXPECT_FALSE(reject.empty());
    EXPECT_NE(reject.find("counter"), std::string::npos) << reject;
    EXPECT_NE(reject.find("incompatible"), std::string::npos) << reject;
    std::filesystem::remove_all(dir);
}

TEST(RestoreCompatGate, UnreadableSavepointIsBestEffortSkip) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // Restore dir points somewhere with no savepoint file -> the gate
    // can't read it -> "" (don't block; C will catch any real problem at
    // TM start).
    const auto reject = clink::cluster::check_restore_compatibility_via_plugins(
        {schema_evo_job_path()}, "/nonexistent/restore/dir", 1);
    EXPECT_TRUE(reject.empty());
}

TEST(RestoreCompatGate, NonJobPluginIsIgnored) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // A .so that doesn't export the check (here: a path that won't load)
    // must not cause a false reject; with an incompatible savepoint and
    // only the real job .so present, the verdict still comes through.
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 5);
    auto dir = write_savepoint(versions, "mixed");

    const auto reject = clink::cluster::check_restore_compatibility_via_plugins(
        {"/nonexistent/connector.so", schema_evo_job_path()}, dir.string(), 1);
    EXPECT_FALSE(reject.empty());
    std::filesystem::remove_all(dir);
}
