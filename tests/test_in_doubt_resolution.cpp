// In-doubt commit resolution at restore-point selection
// (clink/cluster/in_doubt_resolution.hpp): the walk over
// completed-but-unconfirmed checkpoints, driven against real marker files
// and real snapshot files, with a scripted resolver standing in for the
// connector. The conservative stops are the point: every uncertainty must
// leave CONFIRMED where it was, because advancing it past a transaction
// that did not commit converts a bounded replay into silent loss.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/savepoint.hpp"

namespace {

using clink::cluster::completed_marker_dir_for;
using clink::cluster::resolve_in_doubt_commits;
using clink::connectors::InDoubtResolution;
using clink::connectors::TxnResumeRegistry;

constexpr clink::cluster::JobId kJob = 7;

struct ResolutionFixture : ::testing::Test {
    void SetUp() override {
        dir = (std::filesystem::temp_directory_path() /
               ("clink_indoubt_" + std::to_string(::getpid()) + "_" +
                ::testing::UnitTest::GetInstance()->current_test_info()->name()))
                  .string();
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        seen.clear();
        // The scripted resolver: records every handle it is asked about and
        // answers from the script keyed by the handle's "tag" field.
        TxnResumeRegistry::instance().register_resolver(
            "test_resume", [this](const std::string& handle) {
                seen.push_back(handle);
                if (handle.find("\"tag\":\"commit\"") != std::string::npos) {
                    return InDoubtResolution{true, "scripted commit"};
                }
                if (handle.find("\"tag\":\"transport-then-commit\"") != std::string::npos) {
                    // Unreachable broker for two attempts, then reachable:
                    // the shape of broker chaos overlapping a recovery.
                    if (++transport_calls <= 2) {
                        return InDoubtResolution{.committed = false,
                                                 .detail = "scripted transport failure",
                                                 .transport_inconclusive = true};
                    }
                    return InDoubtResolution{true, "scripted commit after transport"};
                }
                if (handle.find("\"tag\":\"transport-always\"") != std::string::npos) {
                    ++transport_calls;
                    return InDoubtResolution{.committed = false,
                                             .detail = "scripted transport failure",
                                             .transport_inconclusive = true};
                }
                return InDoubtResolution{false, "scripted refusal"};
            });
    }
    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    void write_completed_marker(std::uint64_t id,
                                const std::string& subtasks = "0",
                                bool with_participants = true) const {
        const auto job_dir = completed_marker_dir_for(dir, kJob);
        std::filesystem::create_directories(job_dir);
        std::ofstream out(job_dir / ("COMPLETED-" + std::to_string(id)));
        out << "job=" << kJob << "\ncheckpoint=" << id << "\n";
        if (with_participants) {
            out << "generation=1\nsubtasks=" << subtasks << "\n";
        }
    }

    // Stage `handle` as an operator-state row in subtask `sub`'s snapshot
    // for checkpoint `id` - the exact shape the 2PC sink writes.
    void write_snapshot_with_handle(std::uint64_t id,
                                    std::uint32_t sub,
                                    const std::string& handle) const {
        auto sp = clink::state_processor::Savepoint::create();
        const std::string key =
            std::string(clink::connectors::kTxnResumeStateKeyPrefix) + "sub" + std::to_string(sub);
        sp.backend().put_operator_state(
            clink::OperatorId{42},
            clink::StateBackend::KeyView{key.data(), key.size()},
            clink::StateBackend::ValueView{handle.data(), handle.size()});
        const auto sub_dir = std::filesystem::path(clink::state_dir_for(dir, 1, sub));
        std::filesystem::create_directories(sub_dir);
        sp.write_to_file(sub_dir / ("checkpoint-" + std::to_string(id) + ".snap"));
    }

    void write_snapshot_without_handles(std::uint64_t id, std::uint32_t sub) const {
        auto sp = clink::state_processor::Savepoint::create();
        auto ks = sp.keyed_state<std::int64_t, std::int64_t>(
            clink::OperatorId{42}, "counts", clink::int64_codec(), clink::int64_codec());
        ks.put(1, 100);
        const auto sub_dir = std::filesystem::path(clink::state_dir_for(dir, 1, sub));
        std::filesystem::create_directories(sub_dir);
        sp.write_to_file(sub_dir / ("checkpoint-" + std::to_string(id) + ".snap"));
    }

    static std::string handle(const std::string& tag) {
        return "{\"resolver\":\"test_resume\",\"tag\":\"" + tag + "\"}";
    }

    // Drop the durable commit receipt a 2PC sink writes right after its
    // external commit for checkpoint `id` executed.
    void write_receipt(std::uint64_t id, std::uint32_t sub) const {
        const auto rdir = clink::cluster::commit_receipt_dir_for(dir, kJob);
        std::filesystem::create_directories(rdir);
        std::ofstream out(rdir / clink::connectors::commit_receipt_file_name(sub, id));
        out << "wm=12345\n";
    }

    [[nodiscard]] bool confirmed_marker_exists(std::uint64_t id) const {
        return std::filesystem::exists(completed_marker_dir_for(dir, kJob) /
                                       ("CONFIRMED-" + std::to_string(id)));
    }

    std::string dir;
    std::vector<std::string> seen;
    int transport_calls{0};
};

TEST_F(ResolutionFixture, ACommittedHandleAdvancesConfirmedDurably) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("commit"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, /*confirmed=*/3, /*completed=*/4), 4u);
    EXPECT_TRUE(confirmed_marker_exists(4))
        << "the advance must be durable, or the NEXT recovery re-resolves and could disagree";
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_NE(seen[0].find("\"tag\":\"commit\""), std::string::npos);
}

TEST_F(ResolutionFixture, TheWalkCoversEveryUnconfirmedCheckpointInOrder) {
    // Two completed-but-unconfirmed checkpoints: BOTH transactions must
    // commit for the restore point to reach 5 - skipping 4 would lose its
    // interval.
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("commit"));
    write_completed_marker(5);
    write_snapshot_with_handle(5, 0, handle("commit"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5), 5u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    EXPECT_TRUE(confirmed_marker_exists(5));
    EXPECT_EQ(seen.size(), 2u);
}

TEST_F(ResolutionFixture, ARefusedHandleStopsTheWalkAtTheLastSuccess) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("commit"));
    write_completed_marker(5);
    write_snapshot_with_handle(5, 0, handle("refuse"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5), 4u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    EXPECT_FALSE(confirmed_marker_exists(5))
        << "a refused transaction must never be confirmed - that is the false-confirm defect";
}

TEST_F(ResolutionFixture, EveryHandleOfACheckpointMustCommitNotJustOne) {
    // Two subtasks staged handles; one refuses. Confirming on a partial
    // commit would restore past records the refusing sink never published.
    write_completed_marker(4, "0,1");
    write_snapshot_with_handle(4, 0, handle("commit"));
    write_snapshot_with_handle(4, 1, handle("refuse"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
}

// The walk EXECUTES commits handle by handle - EndTxn is the resolution,
// there is no read-only probe - so a transport failure part-way through a
// checkpoint leaves earlier handles committed. Treating that failure as a
// verdict and restoring below the checkpoint would replay the committed
// intervals as duplicates; broker chaos overlapping a recovery reaches
// exactly this. A transport failure is therefore retried in place (the
// broker is coming back; the held restart is already waiting), and
// already-committed handles are never re-resolved across attempts.
TEST_F(ResolutionFixture, ATransportFailureIsRetriedNotTreatedAsAVerdict) {
    write_completed_marker(4, "0,1");
    write_snapshot_with_handle(4, 0, handle("commit"));
    write_snapshot_with_handle(4, 1, handle("transport-then-commit"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 4u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    // sub0's commit executed exactly once: retries must not re-resolve
    // handles that already answered.
    const auto sub0_calls = std::count_if(seen.begin(), seen.end(), [](const std::string& h) {
        return h.find("\"tag\":\"commit\"") != std::string::npos;
    });
    EXPECT_EQ(sub0_calls, 1);
    EXPECT_EQ(transport_calls, 3);  // two failures, then the commit
}

TEST_F(ResolutionFixture, ExhaustedTransportRetriesFallBackWithoutConfirming) {
    write_completed_marker(4, "0,1");
    write_snapshot_with_handle(4, 0, handle("commit"));
    write_snapshot_with_handle(4, 1, handle("transport-always"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
    // The retries genuinely happened before the conservative fallback.
    EXPECT_GE(transport_calls, 3);
}

// A commit receipt on disk is the sink's own durable record that the broker
// acknowledged this subtask's commit. It outlives everything that makes the
// wire path ambiguous - producer fencing under incarnation churn, broker
// restarts, transaction timeouts - so the walk takes it as COMMITTED without
// a wire call. qual01-20260818b: a handle whose commit HAD executed was
// fenced by the successor's init before resolution ran, the wire verdict
// came back not-committed, and the fallback replayed the committed interval.
TEST_F(ResolutionFixture, ACommitReceiptShortCircuitsAnUnreachableResolver) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("transport-always"));
    write_receipt(4, 0);

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 4u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    EXPECT_EQ(transport_calls, 0) << "a receipted handle must never be re-resolved over the wire "
                                     "- the wire can only disagree with a commit that happened";
}

TEST_F(ResolutionFixture, AReceiptForAnotherSubtaskVouchesForNothing) {
    // sub0's handle is unreachable and only sub1 holds a receipt: the walk
    // must fall back. Receipts are per-subtask facts, not per-checkpoint.
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("transport-always"));
    write_receipt(4, 1);

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
}

TEST_F(ResolutionFixture, AMixedWalkConfirmsFromReceiptAndWireTogether) {
    // The partial-commit shape: sub0 committed (receipt written, and its
    // fenced handle would refuse over the wire); sub1 never committed but
    // resolves cleanly. Both proofs together advance CONFIRMED.
    write_completed_marker(4, "0,1");
    write_snapshot_with_handle(4, 0, handle("refuse"));
    write_receipt(4, 0);
    write_snapshot_with_handle(4, 1, handle("commit"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 4u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    ASSERT_EQ(seen.size(), 1u) << "only the unreceipted handle goes to the wire";
    EXPECT_NE(seen[0].find("\"tag\":\"commit\""), std::string::npos);
}

TEST_F(ResolutionFixture, ACheckpointWithNoHandlesStopsConservatively) {
    write_completed_marker(4);
    write_snapshot_without_handles(4, 0);

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
    EXPECT_TRUE(seen.empty());
}

TEST_F(ResolutionFixture, AMissingResolverStopsConservatively) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, "{\"resolver\":\"nobody_registered_this\"}");

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
}

TEST_F(ResolutionFixture, AMarkerWithoutParticipantsStopsConservatively) {
    write_completed_marker(4, "0", /*with_participants=*/false);
    write_snapshot_with_handle(4, 0, handle("commit"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 3u);
    EXPECT_TRUE(seen.empty()) << "no resolver may fire when the participant set is unknown";
}

TEST_F(ResolutionFixture, AnAlreadyConfirmedRangeIsANoOp) {
    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 5, 5), 5u);
    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 6, 5), 6u);
    EXPECT_TRUE(seen.empty());
}

TEST_F(ResolutionFixture, IdsThatNeverCompletedAreSkippedNotFatal) {
    // 4 never completed (no marker - its transaction was aborted with the
    // failed checkpoint); 5 did and commits. The walk reaches 5.
    write_completed_marker(5);
    write_snapshot_with_handle(5, 0, handle("commit"));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5), 5u);
    EXPECT_TRUE(confirmed_marker_exists(5));
}

}  // namespace
