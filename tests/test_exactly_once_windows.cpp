// Exactly-once at the SINK, across each crash window in the 2PC protocol.
//
// The multi-process fault-tolerance suite proves a job RECOVERS. That is a
// different and weaker claim than "each record reached the external system
// exactly once", which is what the phrase exactly-once is usually taken to
// mean, and which nothing in this tree asserted.
//
// These tests assert the output MULTISET after a crash at each of the
// windows the two-phase-commit choreography has, using the real
// CommittingSink base and the real file_2pc sink over a real durable state
// backend. A crash is simulated by discarding the sink instance at the
// fault point and constructing a fresh one over the SAME state directory,
// which is exactly what the runtime does after a worker is lost.
//
// The windows, and what each must guarantee:
//
//   before prepare          no external effect at all; the records are
//                           replayed by the source and appear once.
//   after prepare, before   the handle is not yet persisted, so recovery
//   the handle is persisted cannot find it. Staged output must NOT be
//                           visible, and the replay must not double it.
//   after global completion the handle IS persisted. Recovery MUST commit
//   before commit           it, or the records are lost.
//   after the external      the external system has it and the handle is
//   commit, before the      still persisted. Recovery commits AGAIN. Only
//   local acknowledgement   an idempotent commit survives this - it is the
//                           single hardest case in the whole protocol.

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/file_2pc_sink.hpp"
#include "clink/connectors/text_format.hpp"
#include "clink/core/record.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/file_backed_state_backend.hpp"

namespace {

using namespace clink;

std::filesystem::path make_dir(const std::string& tag) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_xo_" + std::to_string(::getpid()) + "_" + tag);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

Batch<std::string> batch_of(const std::vector<std::string>& xs) {
    Batch<std::string> b;
    for (const auto& s : xs) {
        b.emplace(s);
    }
    return b;
}

// Every line under committed/, as a multiset. A multiset, not a set: the
// whole question is whether a record appears MORE than once, so collapsing
// duplicates would erase the property under test.
std::multiset<std::string> committed_lines(const std::filesystem::path& out_dir) {
    std::multiset<std::string> out;
    std::error_code ec;
    const auto committed = out_dir / "committed";
    if (!std::filesystem::exists(committed, ec)) {
        return out;
    }
    for (const auto& e : std::filesystem::recursive_directory_iterator(committed, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file()) {
            continue;
        }
        std::ifstream in(e.path());
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                out.insert(line);
            }
        }
    }
    return out;
}

std::size_t staging_file_count(const std::filesystem::path& out_dir) {
    std::size_t n = 0;
    std::error_code ec;
    const auto staging = out_dir / "staging";
    if (!std::filesystem::exists(staging, ec)) {
        return 0;
    }
    for (const auto& e : std::filesystem::directory_iterator(staging, ec)) {
        if (e.is_regular_file()) {
            ++n;
        }
    }
    return n;
}

// One sink instance over a shared state backend and output dir. Destroying
// it and calling fresh_sink() again is the test's model of a worker crash
// and redeploy: the state directory persists, the sink object does not.
class SinkFixture {
public:
    SinkFixture(std::filesystem::path out_dir, std::filesystem::path state_dir)
        : out_dir_(std::move(out_dir)),
          backend_(std::make_shared<FileBackedStateBackend>(std::move(state_dir))) {}

    std::shared_ptr<FileSink2PC<std::string>> fresh_sink() {
        rctx_ = std::make_unique<RuntimeContext>(
            OperatorId{7}, "xo_sink", backend_.get(), /*metrics=*/nullptr);
        auto sink = std::make_shared<FileSink2PC<std::string>>(
            out_dir_, string_text_format(), /*subtask_idx=*/0, "file_2pc_sink_string");
        sink->set_id(OperatorId{7});
        sink->attach_runtime(rctx_.get());
        return sink;
    }

    StateBackend& backend() { return *backend_; }

private:
    std::filesystem::path out_dir_;
    std::shared_ptr<FileBackedStateBackend> backend_;
    std::unique_ptr<RuntimeContext> rctx_;
};

class ExactlyOnceWindowTest : public ::testing::Test {
protected:
    void SetUp() override { clink::fault::Registry::instance().reset(); }
    void TearDown() override { clink::fault::Registry::instance().reset(); }

    // The records the source would replay for checkpoint 1. Every test
    // asserts the committed output equals exactly this.
    static std::vector<std::string> records() { return {"a", "b", "c"}; }
    static std::multiset<std::string> expected() {
        const auto r = records();
        return {r.begin(), r.end()};
    }
};

// --- baseline: no fault ----------------------------------------------------

TEST_F(ExactlyOnceWindowTest, CleanRunCommitsEachRecordExactlyOnce) {
    const auto out = make_dir("clean_out");
    const auto st = make_dir("clean_state");
    SinkFixture f(out, st);

    auto sink = f.fresh_sink();
    sink->open();
    sink->on_data(batch_of(records()));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);

    EXPECT_EQ(committed_lines(out), expected());
    EXPECT_EQ(staging_file_count(out), 0U) << "a committed checkpoint left a staging file behind";
}

// --- window 1: death before prepare ----------------------------------------

TEST_F(ExactlyOnceWindowTest, DeathBeforePrepareLeavesNoOutputAndReplayCommitsOnce) {
    const auto out = make_dir("beforeprep_out");
    const auto st = make_dir("beforeprep_state");
    SinkFixture f(out, st);

    {
        auto sink = f.fresh_sink();
        sink->open();
        sink->on_data(batch_of(records()));
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kSinkBeforePrepare,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(sink->on_barrier(CheckpointBarrier{CheckpointId{1}}),
                     clink::fault::InjectedFault);
    }
    // Nothing was promised, so nothing may be visible.
    EXPECT_TRUE(committed_lines(out).empty())
        << "output became visible from a checkpoint that never prepared";

    // The source replays the same records into a fresh sink.
    auto sink = f.fresh_sink();
    sink->open();
    sink->on_data(batch_of(records()));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    EXPECT_EQ(committed_lines(out), expected());
}

// --- window 2: death after prepare, before the handle is persisted ---------

TEST_F(ExactlyOnceWindowTest, DeathAfterPrepareBeforePersistDoesNotPublishAndReplayCommitsOnce) {
    const auto out = make_dir("afterprep_out");
    const auto st = make_dir("afterprep_state");
    SinkFixture f(out, st);

    {
        auto sink = f.fresh_sink();
        sink->open();
        sink->on_data(batch_of(records()));
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kSinkAfterPrepare,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(sink->on_barrier(CheckpointBarrier{CheckpointId{1}}),
                     clink::fault::InjectedFault);
    }
    // The staging file exists (prepare ran) but nothing is committed, and
    // no handle was persisted, so recovery cannot and must not promote it.
    EXPECT_TRUE(committed_lines(out).empty())
        << "a prepared-but-unrecorded checkpoint became visible";
    EXPECT_GE(staging_file_count(out), 1U) << "prepare did not actually stage anything, so this "
                                              "test is not exercising the window it claims";

    auto sink = f.fresh_sink();
    sink->open();  // recover_all_() finds no handle: correct, nothing to do
    EXPECT_TRUE(committed_lines(out).empty())
        << "recovery promoted a staging file it had no durable record of";

    sink->on_data(batch_of(records()));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    // Exactly once, despite the orphaned staging file from the first attempt.
    EXPECT_EQ(committed_lines(out), expected());
}

// --- window 3: death after global completion, before commit ----------------

TEST_F(ExactlyOnceWindowTest, DeathBeforeCommitIsRecoveredAndCommitsExactlyOnce) {
    const auto out = make_dir("beforecommit_out");
    const auto st = make_dir("beforecommit_state");
    SinkFixture f(out, st);

    {
        auto sink = f.fresh_sink();
        sink->open();
        sink->on_data(batch_of(records()));
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // handle persisted
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kSinkBeforeCommit,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(sink->on_commit(1), clink::fault::InjectedFault);
    }
    // The checkpoint completed globally, so these records are accounted
    // for. Losing them here would be data loss, not a rewind.
    EXPECT_TRUE(committed_lines(out).empty()) << "commit somehow ran despite the fault";

    auto sink = f.fresh_sink();
    sink->open();  // recover_all_() must find the persisted handle and commit it
    EXPECT_EQ(committed_lines(out), expected())
        << "recovery did not commit a handle left prepared by a globally-completed checkpoint; "
           "those records are lost";
}

// --- window 4: death after the external commit, before local ack -----------

TEST_F(ExactlyOnceWindowTest, DeathAfterExternalCommitRecommitsIdempotently) {
    const auto out = make_dir("afterextcommit_out");
    const auto st = make_dir("afterextcommit_state");
    SinkFixture f(out, st);

    {
        auto sink = f.fresh_sink();
        sink->open();
        sink->on_data(batch_of(records()));
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
        // Fault AFTER commit() has run: the rename has happened, the
        // external system holds the data, but erase_operator_state has not,
        // so the handle survives and recovery will commit it a second time.
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kSinkAfterExternalCommit,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(sink->on_commit(1), clink::fault::InjectedFault);
    }
    // The external commit DID take effect.
    EXPECT_EQ(committed_lines(out), expected()) << "the external commit did not take effect, so "
                                                   "this test is not exercising its window";

    auto sink = f.fresh_sink();
    sink->open();  // recover_all_() commits the same handle AGAIN

    // The whole point: a second commit of an already-committed handle must
    // not duplicate the output. This is the property the CommittingSink
    // contract requires of every connector ("commit MUST be idempotent")
    // and that nothing previously verified.
    EXPECT_EQ(committed_lines(out), expected())
        << "the recovery re-commit duplicated output; commit() is not idempotent, so this sink "
           "cannot provide exactly-once across a crash in this window";
}

// --- a duplicate commit with no crash at all -------------------------------

TEST_F(ExactlyOnceWindowTest, DuplicateCommitOfTheSameCheckpointIsHarmless) {
    const auto out = make_dir("dupcommit_out");
    const auto st = make_dir("dupcommit_state");
    SinkFixture f(out, st);

    auto sink = f.fresh_sink();
    sink->open();
    sink->on_data(batch_of(records()));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_commit(1);
    // A redelivered CommitCheckpoint broadcast - the coordinator retries,
    // or a worker reconnects and is told again.
    sink->on_commit(1);
    sink->on_commit(1);
    EXPECT_EQ(committed_lines(out), expected()) << "a repeated commit broadcast duplicated output";
}

// --- abort ------------------------------------------------------------------

TEST_F(ExactlyOnceWindowTest, AbortedCheckpointPublishesNothingAndReplayCommitsOnce) {
    const auto out = make_dir("abort_out");
    const auto st = make_dir("abort_state");
    SinkFixture f(out, st);

    auto sink = f.fresh_sink();
    sink->open();
    sink->on_data(batch_of(records()));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->on_abort(1);
    EXPECT_TRUE(committed_lines(out).empty()) << "an aborted checkpoint published output";

    // A fresh instance must not resurrect the aborted handle.
    auto sink2 = f.fresh_sink();
    sink2->open();
    EXPECT_TRUE(committed_lines(out).empty())
        << "recovery committed a handle whose checkpoint was aborted";

    sink2->on_data(batch_of(records()));
    sink2->on_barrier(CheckpointBarrier{CheckpointId{2}});
    sink2->on_commit(2);
    EXPECT_EQ(committed_lines(out), expected());
}

// --- several checkpoints, crash in the middle ------------------------------

TEST_F(ExactlyOnceWindowTest, CrashBetweenTwoCheckpointsKeepsBothExactlyOnce) {
    const auto out = make_dir("multi_out");
    const auto st = make_dir("multi_state");
    SinkFixture f(out, st);

    const std::vector<std::string> first{"a", "b"};
    const std::vector<std::string> second{"c", "d"};

    {
        auto sink = f.fresh_sink();
        sink->open();
        sink->on_data(batch_of(first));
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
        sink->on_commit(1);

        sink->on_data(batch_of(second));
        sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
        // Checkpoint 2 completed globally but the commit never ran.
        const clink::fault::ScopedFault guard(
            clink::fault::Rule{.point = clink::fault::points::kSinkBeforeCommit,
                               .action = clink::fault::Action::Throw});
        EXPECT_THROW(sink->on_commit(2), clink::fault::InjectedFault);
    }

    auto sink = f.fresh_sink();
    sink->open();

    std::multiset<std::string> want{"a", "b", "c", "d"};
    EXPECT_EQ(committed_lines(out), want)
        << "after a crash between checkpoints the output is not exactly the union of both";
}

}  // namespace
