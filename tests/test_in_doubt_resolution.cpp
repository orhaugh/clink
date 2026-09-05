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
                if (handle.find("\"tag\":\"commit-and-cancel\"") != std::string::npos) {
                    // The deadline trips WHILE this probe is in flight: the
                    // resolver answers committed, but the walk must discard
                    // the answer - acting on it is a store effect.
                    if (cancel_to_set != nullptr) {
                        cancel_to_set->store(true, std::memory_order_release);
                    }
                    return InDoubtResolution{true, "scripted commit, cancelled mid-flight"};
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
        write_snapshot_with_handles(id, sub, {{sub, handle}});
    }

    // A snapshot carrying handle rows for arbitrary sink subtasks - the
    // union-restore pollution shape, where a subtask's snapshot re-persists
    // copies of OTHER sinks' staged handles.
    void write_snapshot_with_handles(
        std::uint64_t id,
        std::uint32_t sub,
        const std::vector<std::pair<std::uint32_t, std::string>>& rows) const {
        auto sp = clink::state_processor::Savepoint::create();
        for (const auto& [row_sub, handle] : rows) {
            const std::string key = std::string(clink::connectors::kTxnResumeStateKeyPrefix) +
                                    "sub" + std::to_string(row_sub);
            sp.backend().put_operator_state(
                clink::OperatorId{42},
                clink::StateBackend::KeyView{key.data(), key.size()},
                clink::StateBackend::ValueView{handle.data(), handle.size()});
        }
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

    static std::string handle(const std::string& tag, std::uint64_t ckpt = 4) {
        return "{\"resolver\":\"test_resume\",\"tag\":\"" + tag + "\",\"ckpt\":\"" +
               std::to_string(ckpt) + "\"}";
    }

    // As the 2PC sink stages it since the receipt-materialisation fix: the
    // transaction's watermark horizon rides in the handle so the walk can
    // mint the receipt an ack-window kill prevented.
    static std::string handle_with_wm(const std::string& tag, std::uint64_t ckpt, std::int64_t wm) {
        return "{\"resolver\":\"test_resume\",\"tag\":\"" + tag + "\",\"ckpt\":\"" +
               std::to_string(ckpt) + "\",\"wm\":\"" + std::to_string(wm) + "\"}";
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

    // The .unresolved marker a failed or cancelled walk leaves next to the
    // receipts: body = the staged handle, consumed by the owning sink's
    // pre-fence describe.
    [[nodiscard]] std::filesystem::path unresolved_marker_path(std::uint32_t sub,
                                                               std::uint64_t id) const {
        return clink::cluster::commit_receipt_dir_for(dir, kJob) /
               (clink::connectors::commit_receipt_file_name(sub, id) + ".unresolved");
    }

    [[nodiscard]] bool marker_exists(std::uint32_t sub, std::uint64_t id) const {
        return std::filesystem::exists(unresolved_marker_path(sub, id));
    }

    [[nodiscard]] std::string read_marker(std::uint32_t sub, std::uint64_t id) const {
        std::ifstream in{unresolved_marker_path(sub, id)};
        std::stringstream ss;
        ss << in.rdbuf();
        return ss.str();
    }

    void write_marker(std::uint32_t sub, std::uint64_t id, const std::string& body) const {
        const auto rdir = clink::cluster::commit_receipt_dir_for(dir, kJob);
        std::filesystem::create_directories(rdir);
        std::ofstream out(unresolved_marker_path(sub, id));
        out << body;
    }

    std::string dir;
    std::vector<std::string> seen;
    int transport_calls{0};
    std::atomic<bool>* cancel_to_set{nullptr};
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
    write_snapshot_with_handle(5, 0, handle("commit", 5));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5), 5u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    EXPECT_TRUE(confirmed_marker_exists(5));
    EXPECT_EQ(seen.size(), 2u);
}

TEST_F(ResolutionFixture, ARefusedHandleStopsTheWalkAtTheLastSuccess) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("commit"));
    write_completed_marker(5);
    write_snapshot_with_handle(5, 0, handle("refuse", 5));

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

// Union operator-state restore replicates every sink's staged handle into
// every subtask's live state, and later snapshots re-persist the copies -
// so a subtask's snapshot can carry OTHER sinks' handles from long-dead
// incarnations. qual01-20260818d: the walk saw 64 handles for 4 sinks and
// aborted on a fenced OLD-incarnation copy without ever trying the live
// ones. A handle is authoritative only in its OWN subtask's snapshot.
TEST_F(ResolutionFixture, AStaleUnionCopyFromAnOlderCheckpointIsIgnored) {
    write_completed_marker(4, "0,1");
    // Subtask 0's snapshot: its own live handle (staged AT checkpoint 4)
    // PLUS a stale union-restored copy of subtask 1's handle from an older
    // checkpoint, which the broker would refuse. The stale copy must never
    // reach the wire: only handles staged at the walked checkpoint are
    // authoritative.
    write_snapshot_with_handles(4, 0, {{0, handle("commit")}, {1, handle("refuse", 2)}});
    // Subtask 1's own snapshot carries its live, committable handle.
    write_snapshot_with_handles(4, 1, {{1, handle("commit")}});

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 4u)
        << "the stale union copy poisoned the walk";
    EXPECT_TRUE(confirmed_marker_exists(4));
    EXPECT_EQ(seen.size(), 2u) << "exactly one handle per sink subtask goes to the wire";
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
    write_snapshot_with_handle(5, 0, handle("commit", 5));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5), 5u);
    EXPECT_TRUE(confirmed_marker_exists(5));
}

// -- receipt materialisation (qual01-20260819f) ---------------------------
//
// A commit proven over the wire has, by construction, no receipt on disk:
// the ack window killed the sink between the broker's acknowledgement and
// the receipt write, or the walk itself just executed the commit. The walk
// must write that receipt from the handle's watermark horizon, because if
// resolution later stops on a sibling (the mixed verdict), the restore
// replays this subtask's interval and replay suppression arms ONLY from
// receipts - without one, the committed slice re-publishes as duplicates.

TEST_F(ResolutionFixture, AWireCommittedHandleGetsItsReceiptMaterialised) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle_with_wm("commit", 4, 777));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 4u);
    const auto receipt = clink::cluster::commit_receipt_dir_for(dir, kJob) /
                         clink::connectors::commit_receipt_file_name(0, 4);
    ASSERT_TRUE(std::filesystem::exists(receipt))
        << "the wire-proven commit's receipt was not materialised";
    std::ifstream in(receipt);
    std::string body((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    EXPECT_EQ(body, "wm=777\n") << "the materialised receipt must carry the handle's horizon";
}

// The deterministic pin for the probe-all rule: the REFUSED handle sorts
// FIRST (subtask 0), the provable commit second (subtask 1). A walk that
// stops probing at the first refusal never proves the second handle and
// never materialises its receipt - the order-dependent corner run f hit.
// The checkpoint must still refuse to confirm.
TEST_F(ResolutionFixture, AMixedVerdictStillMaterialisesLaterCommits) {
    write_completed_marker(4, "0,1");
    write_snapshot_with_handle(4, 0, handle("refuse", 4));
    write_snapshot_with_handle(4, 1, handle_with_wm("commit", 4, 4242));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 3u)
        << "a mixed verdict must not confirm the checkpoint";
    EXPECT_FALSE(confirmed_marker_exists(4));
    const auto receipt = clink::cluster::commit_receipt_dir_for(dir, kJob) /
                         clink::connectors::commit_receipt_file_name(1, 4);
    EXPECT_TRUE(std::filesystem::exists(receipt))
        << "the refusal stopped the walk from proving (and receipting) the later commit";
}

// -- the id floor over unmarked snapshot files (qual01-20260819g) ----------
//
// A seconds-lived incarnation dies holding snapshot files for checkpoints
// no marker names. A successor numbering above markers alone reuses those
// ids and its files interleave with the dead incarnation's - the
// mixed-vintage restore. The floor must count every snapshot file.
TEST_F(ResolutionFixture, TheIdFloorCountsUnmarkedSnapshotFiles) {
    write_completed_marker(5);
    write_snapshot_without_handles(9, 0);
    write_snapshot_without_handles(7, 2);
    EXPECT_EQ(clink::cluster::latest_snapshot_id_on_disk(dir), 9u);

    // Non-conforming names and directories are not checkpoints.
    std::filesystem::create_directories(std::filesystem::path(dir) / "vX" / "0");
    std::ofstream(std::filesystem::path(dir) / "v1" / "0" / "checkpoint-.snap").put('x');
    std::ofstream(std::filesystem::path(dir) / "v1" / "0" / "other-12.snap").put('x');
    EXPECT_EQ(clink::cluster::latest_snapshot_id_on_disk(dir), 9u);
    EXPECT_EQ(clink::cluster::latest_snapshot_id_on_disk(""), 0u);
}

// -- cancellation (the rig-night composite's zombie walk) ------------------
//
// The coordinator's watchdog cancels a walk that outruns its deadline. A
// cancelled walk must stop MUTATING - its probes execute commits, its
// store writes steer every later recovery - not merely stop restarting:
// the composite caught a timed-out walk committing transactions and
// writing CONFIRMED for a job the coordinator had already failed.

TEST_F(ResolutionFixture, ACancelledWalkProbesNothingButMarksTheOrphans) {
    write_completed_marker(4);
    const auto h = handle_with_wm("commit", 4, 99);
    write_snapshot_with_handle(4, 0, h);
    std::atomic<bool> cancel{true};

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::seconds{2}, &cancel), 3u);
    EXPECT_TRUE(seen.empty()) << "a cancelled walk fired a wire probe";
    EXPECT_FALSE(confirmed_marker_exists(4));
    EXPECT_FALSE(std::filesystem::exists(clink::cluster::commit_receipt_dir_for(dir, kJob) /
                                         clink::connectors::commit_receipt_file_name(0, 4)));
    // The mandated final act: the handle the walk never settled is left as
    // an .unresolved marker so the owning sink can describe the orphan
    // BEFORE its init fences it - a cancelled walk that writes nothing is
    // what turns a bounded walk into a blind fence.
    EXPECT_EQ(read_marker(0, 4), h);
}

TEST_F(ResolutionFixture, ACancelTrippedMidProbeDiscardsTheInFlightAnswer) {
    write_completed_marker(4);
    const auto h = handle_with_wm("commit-and-cancel", 4, 99);
    write_snapshot_with_handle(4, 0, h);
    std::atomic<bool> cancel{false};
    cancel_to_set = &cancel;

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::seconds{2}, &cancel), 3u)
        << "an answer that arrived after the cancel advanced the restore point";
    EXPECT_EQ(seen.size(), 1u);
    EXPECT_FALSE(confirmed_marker_exists(4))
        << "a cancelled walk published CONFIRMED from an in-flight answer";
    EXPECT_FALSE(std::filesystem::exists(clink::cluster::commit_receipt_dir_for(dir, kJob) /
                                         clink::connectors::commit_receipt_file_name(0, 4)))
        << "a cancelled walk materialised a receipt from an in-flight answer";
    // The discarded answer leaves the handle unresolved - and unresolved
    // means marked: the sink's pre-fence describe recovers the truth the
    // discard threw away.
    EXPECT_EQ(read_marker(0, 4), h);
}

TEST_F(ResolutionFixture, AnExhaustedTransportWalkLeavesMarkersForTheSinks) {
    write_completed_marker(4, "0,1");
    write_snapshot_with_handle(4, 0, handle_with_wm("commit", 4, 777));
    const auto h1 = handle_with_wm("transport-always", 4, 888);
    write_snapshot_with_handle(4, 1, h1);

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 3u);
    // The wire-proven commit got its receipt and needs no marker; the
    // handle the outage kept unknowable is marked for its sink.
    EXPECT_TRUE(std::filesystem::exists(clink::cluster::commit_receipt_dir_for(dir, kJob) /
                                        clink::connectors::commit_receipt_file_name(0, 4)));
    EXPECT_FALSE(marker_exists(0, 4)) << "a committed handle must not be marked unresolved";
    EXPECT_EQ(read_marker(1, 4), h1);
}

// The refusal wall, found by the exactly-once model (formal/ExactlyOnce.tla,
// design record 012) rather than by a rig. A refusal at checkpoint 4 used to
// end the walk with the completed checkpoint 5 above it never looked at.
// Subtask 0's commit for 5 executed without its receipt (an ack-window kill);
// with the restore falling back to 3 and nothing marking the orphan, the
// redeploy fenced it blind and the replay published its interval twice. The
// walk now leaves an .unresolved marker for every unreceipted handle above
// the stop, and the sink's pre-fence describe settles it.
TEST_F(ResolutionFixture, ARefusalMarksTheUnreceiptedHandlesAboveIt) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("refuse", 4));
    write_completed_marker(5, "0,1");
    const auto h0 = handle_with_wm("commit", 5, 501);
    write_snapshot_with_handle(5, 0, h0);
    write_snapshot_with_handle(5, 1, handle_with_wm("commit", 5, 502));
    write_receipt(5, 1);

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5, std::chrono::milliseconds{0}), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
    EXPECT_FALSE(confirmed_marker_exists(5));
    // Nothing above the stop is probed over the wire: a probe EXECUTES a
    // commit, and the job is about to restore below both checkpoints.
    EXPECT_EQ(seen.size(), 1u);
    EXPECT_EQ(read_marker(0, 5), h0);
    EXPECT_FALSE(marker_exists(1, 5)) << "a receipted handle needs no marker";
}

TEST_F(ResolutionFixture, AnExhaustedWalkMarksTheHandlesAboveItToo) {
    write_completed_marker(4);
    const auto h4 = handle_with_wm("transport-always", 4, 400);
    write_snapshot_with_handle(4, 0, h4);
    write_completed_marker(5);
    const auto h5 = handle_with_wm("commit", 5, 500);
    write_snapshot_with_handle(5, 0, h5);

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5, std::chrono::milliseconds{0}), 3u);
    EXPECT_EQ(read_marker(0, 4), h4);
    EXPECT_EQ(read_marker(0, 5), h5);
}

TEST_F(ResolutionFixture, ACheckpointThatNeverCompletedAboveTheStopIsNotMarked) {
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("refuse", 4));
    // 5 has a snapshot with a staged handle but no COMPLETED marker: no commit
    // was ever broadcast for it, so nothing staged there can have executed.
    write_snapshot_with_handle(5, 0, handle_with_wm("commit", 5, 500));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 5, std::chrono::milliseconds{0}), 3u);
    EXPECT_FALSE(marker_exists(0, 5));
}

TEST_F(ResolutionFixture, AResolvedHandleRetiresItsStaleMarker) {
    // An earlier failed episode marked sub0's handle; this episode proves
    // the commit over the wire. The stale marker must go with it - a
    // marker outliving its verdict would be re-judged by a later open
    // against broker state that generations have moved past.
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle_with_wm("commit", 4, 99));
    write_marker(0, 4, "stale-handle-body");

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 4u);
    EXPECT_TRUE(confirmed_marker_exists(4));
    EXPECT_FALSE(marker_exists(0, 4));
}

TEST_F(ResolutionFixture, AFinalRefusalRetiresTheMarker) {
    // A broker that ANSWERS "not committed" is final; the marker from an
    // earlier inconclusive episode must not survive to be re-described.
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("refuse", 4));
    write_marker(0, 4, "stale-handle-body");

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4, std::chrono::milliseconds{0}), 3u);
    EXPECT_FALSE(confirmed_marker_exists(4));
    EXPECT_FALSE(marker_exists(0, 4));
}

TEST_F(ResolutionFixture, AHandleWithoutAHorizonMaterialisesNothing) {
    // Staged by a pre-horizon binary: the commit still confirms, but no
    // receipt can be invented for it - that recovery keeps the documented
    // bounded-replay contract rather than a receipt with a made-up cut.
    write_completed_marker(4);
    write_snapshot_with_handle(4, 0, handle("commit", 4));

    EXPECT_EQ(resolve_in_doubt_commits(dir, kJob, 3, 4), 4u);
    EXPECT_FALSE(std::filesystem::exists(clink::cluster::commit_receipt_dir_for(dir, kJob) /
                                         clink::connectors::commit_receipt_file_name(0, 4)));
}

}  // namespace
