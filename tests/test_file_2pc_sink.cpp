// Unit tests for FileSink2PC.
//
// Exercises the 2PC sink protocol directly against an InMemoryStateBackend,
// without spinning up a cluster. Three flows:
//
//   1. ClampDownsCommitsAtomically: open -> on_data -> on_barrier ->
//      on_commit puts the file under committed/ with no trace in staging/
//      and no leftover state.
//   2. UncommittedBarrierLeavesStagingFile: open -> on_data -> on_barrier
//      WITHOUT on_commit. staging/ has the pre-committed file, state
//      tracks the handle.
//   3. RecoveryCommitsPreStagedFile: pre-populate state with a pending
//      handle pointing at an existing staging file; open() should run
//      recover_pending_() and promote that file to committed/.

#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/record.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

std::filesystem::path mktmpdir(const std::string& tag) {
    static int counter = 0;
    auto dir = std::filesystem::temp_directory_path() /
               ("clink_2pc_" + tag + "_" + std::to_string(++counter));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

std::vector<std::string> read_lines(const std::filesystem::path& p) {
    std::ifstream in(p);
    std::vector<std::string> out;
    std::string line;
    while (std::getline(in, line))
        out.push_back(std::move(line));
    return out;
}

// Build a sink wired to a state backend so we can drive its lifecycle
// from the test. attach_runtime sets the backend pointer the sink reads
// in on_barrier / on_commit / open's recovery scan.
std::shared_ptr<FileSink2PC<std::string>> make_sink(const std::filesystem::path& out_dir,
                                                    RuntimeContext& rctx) {
    auto sink = std::make_shared<FileSink2PC<std::string>>(
        out_dir, string_text_format(), /*subtask_idx=*/0, "file_2pc_sink_string");
    sink->set_id(OperatorId{42});
    sink->attach_runtime(&rctx);
    return sink;
}

Batch<std::string> batch_of(const std::vector<std::string>& xs) {
    Batch<std::string> b;
    for (const auto& s : xs)
        b.emplace(s);
    return b;
}

}  // namespace

TEST(FileSink2PC, CommitGroupDefaultsEmpty) {
    // Phase 30a: every sink has a commit_group accessor; default is empty.
    auto sink = std::make_shared<FileSink2PC<std::string>>(
        std::filesystem::temp_directory_path() / "cg_default",
        string_text_format(),
        /*subtask_idx=*/0,
        "file_2pc_sink_string");
    EXPECT_FALSE(sink->has_commit_group());
    EXPECT_EQ(sink->commit_group(), "");
}

TEST(FileSink2PC, SetCommitGroupIsObservable) {
    auto sink = std::make_shared<FileSink2PC<std::string>>(
        std::filesystem::temp_directory_path() / "cg_set",
        string_text_format(),
        /*subtask_idx=*/0,
        "file_2pc_sink_string");
    sink->set_commit_group("my-group");
    EXPECT_TRUE(sink->has_commit_group());
    EXPECT_EQ(sink->commit_group(), "my-group");
}

// --- Phase 30c: on_abort rollback ---------------------------------

TEST(FileSink2PC, AbortRemovesStagingFileAndClearsState) {
    const auto out = mktmpdir("abort");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);
    auto sink = make_sink(out, rctx);

    sink->open();
    sink->on_data(batch_of({"x", "y", "z"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{11}});
    // Staging file exists, state tracks it.
    EXPECT_TRUE(std::filesystem::exists(out / "staging" / "sub0-11.dat"));
    EXPECT_TRUE(state.get(OperatorId{42}, "_2pc_pending_sub0_11").has_value());

    sink->on_abort(11);

    // Staging file is gone; state cleared.
    EXPECT_FALSE(std::filesystem::exists(out / "staging" / "sub0-11.dat"));
    EXPECT_FALSE(state.get(OperatorId{42}, "_2pc_pending_sub0_11").has_value());
    // committed/ untouched - we never committed.
    EXPECT_FALSE(std::filesystem::exists(out / "committed" / "sub0-11.dat"));
}

TEST(FileSink2PC, AbortIsIdempotent) {
    const auto out = mktmpdir("abort_idem");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);
    auto sink = make_sink(out, rctx);

    sink->open();
    sink->on_data(batch_of({"a"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{3}});
    sink->on_abort(3);
    // Second abort for the same id is a no-op (state already gone).
    EXPECT_NO_THROW(sink->on_abort(3));
    // And an abort for an unknown id is also a no-op.
    EXPECT_NO_THROW(sink->on_abort(999));
}

TEST(FileSink2PC, AbortBeforeCommitMakesCommitNoOp) {
    // After abort, on_commit for the same id should be a no-op (the
    // state key is gone) - matches the existing idempotency
    // guarantee for commit.
    const auto out = mktmpdir("abort_then_commit");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);
    auto sink = make_sink(out, rctx);

    sink->open();
    sink->on_data(batch_of({"k"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{4}});
    sink->on_abort(4);
    EXPECT_NO_THROW(sink->on_commit(4));
    // No file should appear in committed/ as a result of the no-op commit.
    EXPECT_FALSE(std::filesystem::exists(out / "committed" / "sub0-4.dat"));
}

TEST(FileSink2PC, ClampDownsCommitsAtomically) {
    const auto out = mktmpdir("commit");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);
    auto sink = make_sink(out, rctx);

    sink->open();
    sink->on_data(batch_of({"a", "b", "c"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);

    // staging/ now empty; committed/sub0-1.dat has the records.
    EXPECT_FALSE(std::filesystem::exists(out / "staging" / "sub0-1.dat"))
        << "staging file should have been moved to committed/";
    const auto committed = out / "committed" / "sub0-1.dat";
    ASSERT_TRUE(std::filesystem::exists(committed));
    EXPECT_EQ(read_lines(committed), (std::vector<std::string>{"a", "b", "c"}));
    // State key cleared after commit.
    EXPECT_FALSE(state.get(OperatorId{42}, "_2pc_pending_sub0_1").has_value());
}

TEST(FileSink2PC, UncommittedBarrierLeavesStagingFile) {
    const auto out = mktmpdir("staging");
    InMemoryStateBackend state;
    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);
    auto sink = make_sink(out, rctx);

    sink->open();
    sink->on_data(batch_of({"x", "y"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{7}});
    // NO on_commit - simulates JM crash before declaring ckpt 7 complete.

    EXPECT_TRUE(std::filesystem::exists(out / "staging" / "sub0-7.dat"));
    EXPECT_FALSE(std::filesystem::exists(out / "committed" / "sub0-7.dat"));
    EXPECT_TRUE(state.get(OperatorId{42}, "_2pc_pending_sub0_7").has_value());
}

TEST(FileSink2PC, RecoveryCommitsPreStagedFile) {
    const auto out = mktmpdir("recover");
    // Simulate post-crash state: a previous JM had pre-committed
    // checkpoint 5 (staging file exists, state tracks it) but crashed
    // before broadcasting CommitCheckpoint. On restart the sink's
    // open() runs recover_pending_() and promotes the file.
    std::filesystem::create_directories(out / "staging");
    const auto staging_path = out / "staging" / "sub0-5.dat";
    {
        std::ofstream o(staging_path);
        o << "pre-committed-line-1\npre-committed-line-2\n";
    }
    InMemoryStateBackend state;
    state.put(OperatorId{42}, "_2pc_pending_sub0_5", staging_path.string());
    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);

    auto sink = make_sink(out, rctx);
    sink->open();

    EXPECT_FALSE(std::filesystem::exists(staging_path))
        << "staging file should have been renamed to committed/";
    const auto committed = out / "committed" / "sub0-5.dat";
    ASSERT_TRUE(std::filesystem::exists(committed));
    EXPECT_EQ(read_lines(committed),
              (std::vector<std::string>{"pre-committed-line-1", "pre-committed-line-2"}));
    EXPECT_FALSE(state.get(OperatorId{42}, "_2pc_pending_sub0_5").has_value());
}

TEST(FileSink2PC, RecoveryIsNoOpWhenStagingFileAlreadyCommitted) {
    // The previous JM successfully committed (renamed) but crashed
    // BEFORE clearing the state key. On restart the staging file is
    // gone; recover_pending_ should silently clear the stale state
    // entry rather than throw.
    const auto out = mktmpdir("recover_noop");
    std::filesystem::create_directories(out / "staging");
    std::filesystem::create_directories(out / "committed");
    InMemoryStateBackend state;
    state.put(OperatorId{42}, "_2pc_pending_sub0_9", (out / "staging" / "sub0-9.dat").string());
    // The staging file is intentionally absent.

    RuntimeContext rctx(OperatorId{42}, "test_sink", &state, /*metrics=*/nullptr);
    auto sink = make_sink(out, rctx);
    EXPECT_NO_THROW(sink->open());
    EXPECT_FALSE(state.get(OperatorId{42}, "_2pc_pending_sub0_9").has_value())
        << "stale state key should have been cleared during recovery";
}
