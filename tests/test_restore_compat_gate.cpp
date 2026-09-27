// Unit tests for the coordinator-side pre-deploy restore-compatibility gate
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
#include <optional>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/restore_compat_gate.hpp"
#include "clink/core/fields.hpp"
#include "clink/core/types.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/schema_version.hpp"

// The fixture job declares expect_state_shape<EvoCounter>("counter-op",
// "counter_slot") where EvoCounter is {int64 count}. The shape fingerprint
// hashes (field name, wire kind) sequences - not type names - so an
// identically-shaped local type reproduces the declared fingerprint, and a
// differently-shaped one is guaranteed to differ.
struct GateEvoSameShape {
    std::int64_t count{};
};
CLINK_FIELDS(GateEvoSameShape, count);

struct GateEvoOtherShape {
    std::string count;
};
CLINK_FIELDS(GateEvoOtherShape, count);

static_assert(clink::fields_fingerprint_v<GateEvoSameShape> !=
                  clink::fields_fingerprint_v<GateEvoOtherShape>,
              "the two local shapes must differ for the mismatch tests to mean anything");

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
                                      const std::string& tag,
                                      const clink::StateFingerprintMap& fingerprints = {}) {
    namespace fs = std::filesystem;
    static std::atomic<std::uint64_t> counter{0};
    auto dir =
        fs::temp_directory_path() / ("restore_compat_gate_" + tag + "_" + std::to_string(getpid()) +
                                     "_" + std::to_string(counter.fetch_add(1)));
    fs::create_directories(dir / "0");

    clink::InMemoryStateBackend backend;
    backend.put(clink::operator_id_from_uid("counter-op"), "k", std::string_view{"v"});
    backend.set_state_versions(versions);
    backend.set_state_fingerprints(fingerprints);
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

TEST(RestoreCompatGate, MatchingShapeStampPassesGate) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 3);  // already expected
    clink::StateFingerprintMap fps;
    fps.set(clink::operator_id_from_uid("counter-op"),
            "counter_slot",
            clink::fields_fingerprint_v<GateEvoSameShape>);  // == the fixture's EvoCounter
    auto dir = write_savepoint(versions, "fp_match", fps);

    const auto reject = clink::cluster::check_restore_compatibility_via_plugins(
        {schema_evo_job_path()}, dir.string(), 1);
    EXPECT_TRUE(reject.empty()) << reject;
    std::filesystem::remove_all(dir);
}

TEST(RestoreCompatGate, UndeclaredShapeChangeIsRejected) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // Versions already at the expected v3 (no migration intent), but the
    // stored shape differs from the declared one: the bump nobody made.
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 3);
    clink::StateFingerprintMap fps;
    fps.set(clink::operator_id_from_uid("counter-op"),
            "counter_slot",
            clink::fields_fingerprint_v<GateEvoOtherShape>);
    auto dir = write_savepoint(versions, "fp_mismatch", fps);

    const auto reject = clink::cluster::check_restore_compatibility_via_plugins(
        {schema_evo_job_path()}, dir.string(), 1);
    EXPECT_FALSE(reject.empty());
    EXPECT_NE(reject.find("changed shape"), std::string::npos) << reject;
    EXPECT_NE(reject.find("counter_slot"), std::string::npos) << reject;
    std::filesystem::remove_all(dir);
}

TEST(RestoreCompatGate, ShapeChangeWithDeclaredBumpPassesGate) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // Stored shape differs, but stored version 1 vs expected 3 is declared
    // migration intent (the 1->2->3 chain exists in the fixture): the
    // migrator will rewrite the slot and clear the stamp at restore.
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 1);
    clink::StateFingerprintMap fps;
    fps.set(clink::operator_id_from_uid("counter-op"),
            "counter_slot",
            clink::fields_fingerprint_v<GateEvoOtherShape>);
    auto dir = write_savepoint(versions, "fp_bump", fps);

    const auto reject = clink::cluster::check_restore_compatibility_via_plugins(
        {schema_evo_job_path()}, dir.string(), 1);
    EXPECT_TRUE(reject.empty()) << reject;
    std::filesystem::remove_all(dir);
}

TEST(RestoreCompatGate, StamplessSavepointIsNotFingerprintGated) {
    if (schema_evo_job_path() == nullptr) {
        GTEST_SKIP() << "schema_evo_test_job .so not built";
    }
    // No fingerprint stamps at all (an older snapshot / unwired backend):
    // absence gates nothing even though the job declares a shape.
    clink::StateVersionMap versions;
    versions.set(clink::operator_id_from_uid("counter-op"), "counter", 3);
    auto dir = write_savepoint(versions, "fp_absent");

    const auto reject = clink::cluster::check_restore_compatibility_via_plugins(
        {schema_evo_job_path()}, dir.string(), 1);
    EXPECT_TRUE(reject.empty()) << reject;
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
    // worker start).
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

// ---------------------------------------------------------------------------
// The restore LAYOUT gate: a submit-time restore addresses every task's state
// by its job-global subtask index, so the checkpoint's recorded participant set
// must be the plan's. The verdict is pure; the reader is exercised against
// directories laid out the way the coordinator writes its markers.
// ---------------------------------------------------------------------------

namespace {

std::set<std::uint32_t> layout_gate_range(std::uint32_t first, std::uint32_t last) {
    std::set<std::uint32_t> out;
    for (auto i = first; i <= last; ++i) {
        out.insert(i);
    }
    return out;
}

std::filesystem::path layout_gate_temp_dir(const std::string& tag) {
    static std::atomic<std::uint64_t> counter{0};
    auto dir = std::filesystem::temp_directory_path() /
               ("restore_layout_gate_" + tag + "_" + std::to_string(getpid()) + "_" +
                std::to_string(counter.fetch_add(1)));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

// <dir>/_jobs/<job>/COMPLETED-<id>, the key the coordinator writes.
void layout_gate_write_marker(const std::filesystem::path& dir,
                              std::uint64_t job,
                              std::uint64_t checkpoint_id,
                              const std::string& body) {
    const auto job_dir = dir / "_jobs" / std::to_string(job);
    std::filesystem::create_directories(job_dir);
    std::ofstream(job_dir / ("COMPLETED-" + std::to_string(checkpoint_id))) << body;
}

// Byte for byte the COMPLETED-78 a v0.8.0 coordinator wrote for the null-aware
// NOT IN + GROUP BY job at parallelism 2: 16 subtasks, where the same SQL now
// plans 15 because the semi-join runs as a single instance.
constexpr const char* kLayoutGateV080Marker =
    "job=3\ncheckpoint=78\ngeneration=1\nsubtasks=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15\n";

}  // namespace

TEST(RestoreLayoutGate, TheSameLayoutPasses) {
    EXPECT_EQ(clink::cluster::restore_layout_refusal(
                  "/sp", 78, layout_gate_range(0, 15), layout_gate_range(0, 15), {"semijoin_7"}),
              "");
}

// The upgrade shape: one operator the old version ran at parallelism 2 is now a
// single instance, so every later block moves down by one and the last index
// disappears. The refusal has to say which checkpoint, both counts, why that is
// fatal, which operator is the likely cause, and what to do instead.
TEST(RestoreLayoutGate, TheUpgradeShapeIsRefusedWithTheCountsTheCauseAndTheRemedy) {
    const auto reject = clink::cluster::restore_layout_refusal(
        "/sp/ninagg", 78, layout_gate_range(0, 15), layout_gate_range(0, 14), {"semijoin_7"});
    ASSERT_FALSE(reject.empty());
    for (const auto* needle : {"refusing to restore checkpoint 78 from /sp/ninagg",
                               "records 16 participating subtasks (0-15)",
                               "this plan deploys 15 (0-14)",
                               "restored by job-global subtask index",
                               "receive each other's state",
                               "runs semijoin_7 as a single instance",
                               "likely cause after an upgrade",
                               "Resubmit with the layout the checkpoint was taken with",
                               "submit without the savepoint"}) {
        EXPECT_NE(reject.find(needle), std::string::npos)
            << "missing '" << needle << "' in: " << reject;
    }
}

// The same number of subtasks is not the same layout. A hot cutover appends the
// rescaled operator's new block past the old allocation, so its checkpoints
// record a set a fresh plan of the rewritten graph never produces.
TEST(RestoreLayoutGate, ASameSizedSetAtDifferentIndicesIsRefused) {
    const auto reject = clink::cluster::restore_layout_refusal(
        "/ckpt", 12, {0, 3, 4, 5, 6, 7}, layout_gate_range(0, 5), {});
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("records 6 participating subtasks (0, 3-7)"), std::string::npos)
        << reject;
    EXPECT_NE(reject.find("this plan deploys 6 (0-5)"), std::string::npos) << reject;
}

TEST(RestoreLayoutGate, WithNoSingleInstanceOperatorTheUpgradeHintIsLeftOut) {
    const auto reject = clink::cluster::restore_layout_refusal(
        "/ckpt", 4, layout_gate_range(0, 3), layout_gate_range(0, 5), {});
    ASSERT_FALSE(reject.empty());
    EXPECT_NE(reject.find("records 4 participating subtasks"), std::string::npos) << reject;
    EXPECT_NE(reject.find("this plan deploys 6"), std::string::npos) << reject;
    EXPECT_EQ(reject.find("single instance"), std::string::npos)
        << "no operator in this plan is a single instance, so the upgrade hint is noise: "
        << reject;
    EXPECT_NE(reject.find("submit without the savepoint"), std::string::npos) << reject;
}

TEST(RestoreLayoutGate, EverySingleInstanceOperatorIsNamed) {
    const auto reject = clink::cluster::restore_layout_refusal(
        "/sp/sc", 9, layout_gate_range(0, 14), layout_gate_range(0, 13), {"agg_4", "scalarproj_5"});
    EXPECT_NE(reject.find("2 operators as single instances"), std::string::npos) << reject;
    EXPECT_NE(reject.find("(agg_4, scalarproj_5)"), std::string::npos) << reject;
}

// The reader against the real v0.8.0 marker body, among markers for other
// checkpoints whose names share its digits, then the whole gate on top of it.
TEST(RestoreLayoutGate, ReadsTheParticipantSetAVersion080MarkerRecords) {
    const auto dir = layout_gate_temp_dir("v080");
    layout_gate_write_marker(dir, 3, 78, kLayoutGateV080Marker);
    layout_gate_write_marker(dir, 3, 7, "job=3\ncheckpoint=7\ngeneration=1\nsubtasks=0,1\n");
    layout_gate_write_marker(dir, 3, 780, "job=3\ncheckpoint=780\ngeneration=1\nsubtasks=0\n");
    // Commit receipts share the job's directory; they are not markers.
    std::filesystem::create_directories(dir / "_jobs" / "3" / "receipts");

    const auto recorded = clink::cluster::recorded_restore_participants(dir.string(), 78);
    ASSERT_TRUE(recorded.has_value());
    EXPECT_EQ(*recorded, layout_gate_range(0, 15));

    const auto reject = clink::cluster::check_restore_layout(
        dir.string(), 78, layout_gate_range(0, 14), {"semijoin_7"});
    EXPECT_NE(reject.find("records 16 participating subtasks"), std::string::npos) << reject;
    EXPECT_NE(reject.find("semijoin_7"), std::string::npos) << reject;
    EXPECT_EQ(clink::cluster::check_restore_layout(
                  dir.string(), 78, layout_gate_range(0, 15), {"semijoin_7"}),
              "")
        << "the layout the checkpoint was taken with must restore";
    // A file:// URI names the same directory, as it does for the restore.
    EXPECT_EQ(clink::cluster::recorded_restore_participants("file://" + dir.string(), 78),
              recorded);
    std::filesystem::remove_all(dir);
}

// Everything the gate cannot pin to ONE recorded participant set is "cannot
// check", never a refusal: the deploy must then proceed exactly as it did before
// the gate existed.
TEST(RestoreLayoutGate, AnUnidentifiableLayoutIsNotAVerdict) {
    const auto planned = layout_gate_range(0, 2);
    const auto no_verdict = [&](const std::string& dir, std::uint64_t id, const char* why) {
        EXPECT_FALSE(clink::cluster::recorded_restore_participants(dir, id).has_value()) << why;
        EXPECT_EQ(clink::cluster::check_restore_layout(dir, id, planned, {"op"}), "") << why;
    };

    no_verdict("", 5, "no restore directory");
    EXPECT_EQ(clink::cluster::check_restore_layout("/any", 0, planned, {}), "")
        << "checkpoint id 0 restores nothing";
    no_verdict("/nonexistent/restore/layout/gate", 5, "missing directory");
    no_verdict("remote-read://bucket/prefix", 5, "a state backend URI, not a directory");

    const auto dir = layout_gate_temp_dir("unknown");
    no_verdict(dir.string(), 5, "no _jobs directory");
    layout_gate_write_marker(dir, 1, 4, "job=1\ncheckpoint=4\ngeneration=1\nsubtasks=0,1,2,3\n");
    no_verdict(dir.string(), 5, "no marker for this checkpoint");
    layout_gate_write_marker(dir, 1, 5, "job=1\ncheckpoint=5\n");
    no_verdict(dir.string(), 5, "a marker predating the participant set");
    layout_gate_write_marker(dir, 1, 6, "job=1\ncheckpoint=6\ngeneration=1\nsubtasks=\n");
    no_verdict(dir.string(), 6, "an empty participant list");
    layout_gate_write_marker(dir, 1, 7, "job=1\ncheckpoint=7\ngeneration=1\nsubtasks=0,x,2\n");
    no_verdict(dir.string(), 7, "a token that is not an index");

    // Two jobs have written into this root and both recorded checkpoint 8, with
    // different layouts: which one the snapshot files belong to is a guess.
    layout_gate_write_marker(dir, 1, 8, "job=1\ncheckpoint=8\ngeneration=1\nsubtasks=0,1,2,3\n");
    layout_gate_write_marker(dir, 2, 8, "job=2\ncheckpoint=8\ngeneration=1\nsubtasks=0,1\n");
    no_verdict(dir.string(), 8, "markers from two jobs for the same checkpoint");
    std::filesystem::remove_all(dir);
}
