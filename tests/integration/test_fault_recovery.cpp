// Multi-process fault-tolerance scenarios, on the deterministic harness.
//
// These are the scenarios the production-hardening brief requires to be
// gates rather than advisory runs. What makes them gateable is that not
// one of them waits for a duration: every step waits for the CONDITION it
// actually depends on (port accepting, worker registered, COMPLETED-N
// marker on disk, process gone, submitter exited) with a monotonic
// deadline as a failure bound only. The pre-existing multi-process tests
// approximated those conditions with sleep_for, which is what made them
// too flaky to gate.
//
// Where a scenario needs a process to die at an exact point in a protocol
// rather than at an arbitrary moment, the fault-injection framework arms
// the point in the CHILD through CLINK_FAULT_INJECT.

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/state/checkpoint_integrity.hpp"

#include "tests/integration/cluster_harness.hpp"
#include "tests/integration/two_pc_output.hpp"

namespace {

using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ProcOptions;
using clink::itest::ScopedDiagnostics;

std::filesystem::path node_binary() {
#ifdef CLINK_NODE_BINARY
    return std::filesystem::path{CLINK_NODE_BINARY};
#else
    return {};
#endif
}
std::filesystem::path submit_binary() {
#ifdef CLINK_SUBMIT_BINARY
    return std::filesystem::path{CLINK_SUBMIT_BINARY};
#else
    return {};
#endif
}
std::filesystem::path two_phase_commit_job() {
#ifdef CLINK_TWO_PHASE_COMMIT_JOB_PATH
    return std::filesystem::path{CLINK_TWO_PHASE_COMMIT_JOB_PATH};
#else
    return {};
#endif
}

bool binaries_available() {
    return std::filesystem::exists(node_binary()) && std::filesystem::exists(submit_binary()) &&
           std::filesystem::exists(two_phase_commit_job());
}

// The highest COMPLETED-N under the checkpoint tree, or 0.
std::uint64_t latest_completed(const std::filesystem::path& ckpt_root) {
    std::uint64_t latest = 0;
    std::error_code ec;
    for (const auto& e : std::filesystem::recursive_directory_iterator(ckpt_root, ec)) {
        if (ec) {
            break;
        }
        if (!e.is_regular_file()) {
            continue;
        }
        const auto name = e.path().filename().string();
        if (name.rfind("COMPLETED-", 0) != 0) {
            continue;
        }
        try {
            // Explicit cast, not std::max(latest, std::stoull(...)): stoull
            // returns unsigned long long, and std::uint64_t is unsigned long
            // on Linux/gcc but unsigned long long on macOS. The two-arg
            // std::max cannot deduce T from mismatched types, so this builds
            // on macOS and fails on Linux - the exact typedef trap this
            // repository has been bitten by before.
            const auto id = static_cast<std::uint64_t>(std::stoull(name.substr(10)));
            latest = std::max(latest, id);
        } catch (const std::exception&) {
        }
    }
    return latest;
}

// Every scenario runs the same bounded 2PC job: a slow source feeding a
// file_2pc_sink. Bounded so "did it finish" is a real question, slow
// enough that several checkpoints land mid-run, and 2PC so the sink's
// commit protocol is genuinely exercised rather than simulated.
// Records the 2PC job emits, and therefore the exact multiset the committed
// output must contain. Named rather than repeated so the environment the job
// is given and the expectation the test checks cannot drift apart - which
// would make an output-equality assertion silently vacuous.
constexpr int kTotalRecords = 40;

class FaultRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!binaries_available()) {
            GTEST_SKIP() << "cluster binaries or the 2PC job plugin are not built";
        }
        out_dir_ = std::filesystem::temp_directory_path() /
                   ("clink_fr_out_" + std::to_string(::getpid()) + "_" +
                    ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(out_dir_);
        std::filesystem::create_directories(out_dir_);
        ::setenv("CLINK_2PC_OUT_DIR", out_dir_.c_str(), 1);
        ::setenv("CLINK_2PC_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
        ::setenv("CLINK_2PC_TICK_MS", "50", 1);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(out_dir_, ec);
    }

    // Submit the job against `c`. Returns the submitter process, which the
    // caller waits on for the job's terminal state.
    //
    // `state_backend` defaults to the submit tool's own default (in-memory).
    // A scenario that needs the file-backed state path - which is where the
    // state.before_restore fault point lives - must ask for it explicitly:
    // --checkpoint-dir alone does NOT select a durable backend, so an
    // in-memory job never travels that code and a fault armed there would
    // silently never fire.
    static std::unique_ptr<Process> submit(Cluster& c,
                                           int max_restarts,
                                           const std::string& state_backend = {},
                                           std::int64_t checkpoint_interval_ms = 150) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{
            submit_binary().string(),
            "--job=" + two_phase_commit_job().string(),
            "--coordinator-host=127.0.0.1",
            "--coordinator-port=" + std::to_string(c.coordinator_port()),
            "--wait-timeout-s=90",
            "--checkpoint-dir=" + c.checkpoint_dir().string(),
            "--checkpoint-interval-ms=" + std::to_string(checkpoint_interval_ms),
            "--max-restarts-on-worker-loss=" + std::to_string(max_restarts)};
        if (!state_backend.empty()) {
            argv.push_back("--state-backend=" + state_backend);
        }
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    // A 2-worker cluster, both registered. Either worker alone can host
    // the whole job, so killing one always leaves somewhere to redeploy.
    static void bring_up(Cluster& c) {
        ASSERT_TRUE(c.start_coordinator()) << "coordinator did not come up";
        ASSERT_TRUE(c.start_worker(0));
        ASSERT_TRUE(c.start_worker(1));
        ASSERT_TRUE(c.await_workers_registered(2))
            << "both workers must register before the job is submitted";
    }

    static ClusterSpec spec() {
        ClusterSpec s;
        s.node_binary = node_binary();
        s.workers = 2;
        s.slots_per_worker = 4;
        return s;
    }

    std::filesystem::path out_dir_;
};

// --- the harness itself ----------------------------------------------------

TEST_F(FaultRecoveryTest, HarnessBringsUpAClusterAndReapsIt) {
    pid_t coord_pid = -1;
    {
        Cluster c(spec());
        ScopedDiagnostics diag(c);
        bring_up(c);
        coord_pid = c.coordinator().pid();
        EXPECT_GT(coord_pid, 0);
        EXPECT_TRUE(c.coordinator().running());
        EXPECT_TRUE(c.worker(0).running());
        EXPECT_TRUE(c.worker(1).running());
    }
    // The destructor must leave nothing behind: an abandoned coordinator
    // holding a port is how one failing test poisons the next.
    EXPECT_NE(::kill(coord_pid, 0), 0) << "the cluster destructor left a coordinator running";
}

// --- worker death before any checkpoint ------------------------------------

TEST_F(FaultRecoveryTest, WorkerDeathBeforeAnyCheckpointStillCompletes) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    // Keep the first periodic trigger well beyond this bounded job's normal
    // lifetime. The end-of-stream checkpoint still proves that checkpointing
    // works after recovery, while the pre-kill state is deterministically
    // checkpoint-free even on a heavily loaded runner.
    auto sub = submit(c,
                      /*max_restarts=*/2,
                      /*state_backend=*/{},
                      /*checkpoint_interval_ms=*/60'000);
    ASSERT_NE(sub, nullptr);

    // Wait until the job is DEPLOYED before killing.
    //
    // The first version of this test killed immediately and failed with
    // "Coordinator::deploy: send failed for worker-0" - the submission was
    // rejected outright. That is not a recovery bug: max-restarts-on-worker-
    // loss governs a worker lost from a RUNNING job, and a job that never
    // started has no state and no restart semantics. Killing during deploy
    // tests an undefined window, so the test waits for a defined one.
    //
    // The condition is the coordinator creating this job's checkpoint
    // directory, which happens once the job is running and checkpointing -
    // an observable fact, not an elapsed time.
    ASSERT_TRUE(c.await_job_checkpointing()) << "the job never reached a running state";

    // NO checkpoint has completed yet, so recovery has nothing to restore
    // from and must restart the job from the source rather than from state.
    ASSERT_EQ(latest_completed(c.checkpoint_dir()), 0U)
        << "a checkpoint completed before the kill; this is the other scenario";
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(std::chrono::seconds(90));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after an early worker loss";
    EXPECT_EQ(*code, 0) << "the job did not recover from a worker lost before any checkpoint";
}

// --- worker death after a completed checkpoint -----------------------------

TEST_F(FaultRecoveryTest, WorkerDeathAfterCheckpointRecoversAndCompletes) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    // Wait for the CONDITION - a COMPLETED-N marker exists - not for a
    // duration. This is the difference between a gate and a coin flip.
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)))
        << "no checkpoint completed before the kill; the scenario never ran";
    const auto before = latest_completed(c.checkpoint_dir());

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(std::chrono::seconds(90));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the worker crash";
    EXPECT_EQ(*code, 0) << "the job did not recover";
    EXPECT_GT(latest_completed(c.checkpoint_dir()), before)
        << "no checkpoint completed after the restart, so the job never really resumed";
}

// --- multiple consecutive worker failures ----------------------------------

// Two consecutive worker losses, with the first worker back before the second dies.
//
// The gap: a second worker loss arriving while the FIRST loss's restart was still
// draining was not folded into that restart. Its subtasks had been survivors at the
// first loss, so they sat in restart_drain_expected and could never drain - their
// worker was dead - and the submitter failed with "restart drain timed out after
// 30000ms (survivors did not drain)" instead of the coordinator noticing and
// re-planning.
//
// It was written asserting the CORRECT behaviour and left disabled rather than
// weakened to assert the bug, because a test that encodes a defect as correct is
// worse than a red one. It now passes and is enabled: a disabled test that would
// pass is coverage the suite is not getting.
//
// But it is NOT the F12 regression test, and was nearly mislabelled as one. It waits
// for worker 0 to RE-REGISTER before killing worker 1, and that wait is long enough
// that the first restart has already settled - so the second loss does not arrive
// during the first restart's drain, which is the whole of F12. Proved rather than
// assumed: with the folding path hard-disabled, this test still passes.
//
// AJobSurvivesASecondLossDuringTheFirstRestartsDrain below is the one that actually
// gates that window.
TEST_F(FaultRecoveryTest, TwoConsecutiveWorkerFailuresAreSurvived) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)));

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    // Bring the first one back so the second kill has somewhere to land,
    // and wait for it to REGISTER rather than assuming it has.
    ASSERT_TRUE(c.restart_worker(0));
    ASSERT_TRUE(c.await_workers_registered(3)) << "the restarted worker never re-registered";

    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after two worker losses";
    EXPECT_EQ(*code, 0) << "the job did not survive two consecutive worker failures";
}

// F12's regression test - RE-ENABLED 2026-08-10. The recorded re-enable
// condition is met: clink_node installs the F83 fatal-signal backtrace
// handler, so a recurrence of the label-only coordinator death prints a
// stack into the kept artefacts instead of another lottery ticket. And the
// death itself now has a plausible, SINCE-FIXED cause: the label-only
// signal exit matches the F89 family exactly - a second, statically linked
// Arrow runtime corrupting memory only under full-process context, on this
// platform, with isolated runs green - and that link was removed on
// 2026-08-10 (one Arrow runtime per process, gated by
// single_arrow_runtime). If this test goes red in a label again, read the
// artefacts' backtrace first; do not re-disable without one.
//
// The history below is kept because the diagnosis discipline in it is the
// point: two plausible theories were tested against the label output and
// both were WRONG, and the exit code was decoded rather than guessed at.
//
// Previously: DISABLED, with the label interaction PARTIALLY DIAGNOSED
// rather than mysterious (F83).
//
// Re-enabling it captured the evidence its earlier failures never had. The
// submitter's own log says "connection closed by the coordinator" (completed=0),
// and the kept coordinator log TERMINATES mid-tick, right after
//   awaiting_restart (attempt 1/3) drain_expected=0
//   worker lost: worker-0
// with no [coordinator.restart] line and no terminate() message: the COORDINATOR
// DIES BY SIGNAL inside the first-loss restart_job_locked_ call, before its first
// log line, and only under whole-label machine timing - isolated runs pass every
// time, including the F12 mutation checks.
//
// clink_node now installs a fatal-signal backtrace handler (see F83), so the next
// label run that hits this prints the stack into these same kept artefacts. THAT
// is the re-enable condition: a stack, not another lottery. Do not raise timeouts;
// the failure is fast and a longer wait changes nothing.
//
// It passes in isolation and FAILS in the full integration label, even with the heavy
// multi-process tests serialised by RESOURCE_LOCK. Running it red in the gate would be
// worse than not running it, so it is disabled while that is understood.
//
// Two theories were tested against the label output and BOTH are wrong:
//
//   * "contention" - RESOURCE_LOCK serialises these tests and it still fails.
//   * "something accumulates over the ~120 tests before it" - it runs 4th of 126. Only
//     three tests precede it.
//
// What the label actually shows:
//
//     4/126 Test #2694: ...AJobSurvivesASecondLossDuringTheFirstRestartsDrain ***Failed 22.48 sec
//     test_fault_recovery.cpp:392: Expected equality of these values:
//       *code  Which is: 9
//       0
//
// Exit 9 is now DECODED: the submitter returns `result.ok ? 0 : 9` after completion,
// so the job RAN TO COMPLETION and reported errors - no wedge, no timeout, no refusal.
// The submitter prints `errors=<front>` on its stdout, which the harness keeps, so if
// this fails in the label again the artefacts' submit.log names the cause directly.
// Do NOT raise any timeout here - the failure is fast (22s of a 180s allowance), so a
// longer wait cannot help and would only obscure it.
//
// The FIX it covers is verified independently and stands: with the empty-drain kick
// disabled this test fails with "restart drain timed out", and with it enabled it
// passes. That mutation check was run in isolation, where the test is reliable.
//
// To enable it, find what it is picking up from the tests before it. Do not simply
// raise its timeouts - it waits on conditions, not durations, so a longer wait would
// only hide whatever the real interaction is.
//
// A second worker loss that arrives WHILE the first restart is still draining.
//
// This is F12's window, and the one the two tests either side of it miss: the
// "consecutive" case waits for a re-registration first, and the "separated" case
// waits for a whole checkpoint. Both let the first restart finish.
//
// Entered deterministically rather than by racing a sleep against it. The
// coordinator logs "awaiting_restart (attempt" the moment it begins draining for a
// restart, so the second kill waits for THAT line and cannot land early or late. The
// second worker's subtasks were survivors at the first loss, so they sit in
// restart_drain_expected and can never drain - their worker is now dead - and the
// drain never completes, so the job wedges in awaiting_restart until the submitter
// times out.
//
// Observed symptom, which matches F12's original description exactly:
//   [coordinator.watchdog] job_id=1 restart drain timed out; failing job
//
// The cause, from the coordinator's own log rather than from the follow-up's
// description (which blamed draining survivors and was wrong - drain_expected was 0
// at both attempts):
//
//   attempt 1, entered from the WATCHDOG: restart fires, job redeploys.
//   attempt 2, entered from the REGISTER path when the killed worker comes back and
//   its previous session is retired: awaiting_restart set, drain_expected=0, and no
//   [coordinator.restart] line ever follows. Nothing called restart_job_locked_.
//
// The empty-drain kick existed but sat inside the watchdog's lost-worker block, gated
// on a worker having been newly declared lost in THAT tick. A restart entered from any
// other path had nothing to fire it, so the job waited 30s for a drain of zero
// subtasks and was failed with "survivors did not drain".
//
// Fixed by moving the kick to the unconditional per-tick sweep, beside the deadline
// check that already runs every tick for the same reason: the condition is a property
// of the job's state, not of what happened in that tick.
TEST_F(FaultRecoveryTest, AJobSurvivesASecondLossDuringTheFirstRestartsDrain) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)));

    const auto restarts_before = c.count_in_coordinator_log("awaiting_restart (attempt");

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    // The drain is now in progress. Kill the second worker inside that window.
    ASSERT_TRUE(clink::itest::await(
        [&] { return c.count_in_coordinator_log("awaiting_restart (attempt") > restarts_before; },
        std::chrono::seconds(60)))
        << "the coordinator never began a restart drain, so there was no window to kill "
           "the second worker inside";

    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));
    // Give the redeploy somewhere to land - two of three workers are now dead.
    ASSERT_TRUE(c.restart_worker(0));
    ASSERT_TRUE(c.restart_worker(1));

    const auto code = sub->await_exit(std::chrono::seconds(180));
    ASSERT_TRUE(code.has_value())
        << "submitter never exited; the job wedged in awaiting_restart waiting on a drain "
           "from subtasks whose worker had died";
    EXPECT_EQ(*code, 0) << "the job did not survive a second loss during the first restart's "
                           "drain";
}

// The SEQUENTIAL form of the same scenario, which does gate: the second
// loss arrives only after the first restart has fully settled (a new
// checkpoint has completed), so the two restarts never overlap. This is
// the part of "multiple consecutive worker failures" the engine handles
// today, and pinning it keeps that from regressing while F12 is open.
TEST_F(FaultRecoveryTest, TwoSeparatedWorkerFailuresAreSurvived) {
    // Same bounded-source hazard as the budget test below, in its vacuous
    // form. This test expects the job to SURVIVE, so if the job instead
    // completes before the second kill, the second loss lands on a worker with
    // nothing on it, the submitter still exits 0 and the test passes without
    // ever observing the second recovery it is named for. Demonstrated:
    // standing a 5 s sleep in for a slow window between the two kills ends the
    // job early every time, and the precondition below catches it.
    //
    // So the source needs to outlast the window between the first recovery and
    // the second kill - normally well under a second - without outlasting the
    // submitter's own wait, because unlike the budget test this one does need
    // the job to finish (--wait-timeout-s=90). Ten seconds of emission is the
    // balance: it absorbs the 5 s forcing that reliably breaks the 40-record
    // version, and still finishes an order of magnitude inside the wait. The
    // margin is not free and is not pretended to be: measured on one host,
    // 14.9-15.5 s at 40 records against 23.6 s here, so about eight seconds on
    // a test that is one of ~199 in a serial gate of ~890 s.
    ::setenv("CLINK_2PC_TOTAL", "200", 1);

    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)));
    const auto before_first = latest_completed(c.checkpoint_dir());

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    // Settle on a CONDITION - the job has checkpointed again, so the
    // restart is complete - rather than on a guess about how long a
    // restart takes.
    ASSERT_TRUE(
        clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > before_first; },
                            std::chrono::seconds(60)))
        << "the job never checkpointed again after the first worker loss";

    // The second loss has to land on a RUNNING job or this test proves nothing:
    // a job that already finished exits 0 whatever the second kill does. Assert
    // it, so that outcome fails loudly instead of passing as a survival.
    ASSERT_TRUE(sub->running())
        << "the job completed before the second worker loss, so the second recovery this test is "
           "named for never happened and its exit code proves nothing. The source drained inside "
           "the window: raise this test's CLINK_2PC_TOTAL override.";

    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));
    ASSERT_TRUE(c.restart_worker(0));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after two worker losses";
    EXPECT_EQ(*code, 0) << "the job did not survive two separated worker failures";
}

// The restart BUDGET is spent, and the job fails rather than restarting forever.
//
// Follow-up 24. `JobState::restart_attempts` gates every restart
// (`restart_attempts < effective_max_restarts`), and nothing asserted it anywhere -
// it is also the only field of `CompletedJobRecord` no test looks at. If it never
// incremented, a job would restart on every worker loss indefinitely, and every
// existing test would still pass: they all give a budget large enough that they
// never reach the end of it.
//
// So this one gives a budget of exactly ONE and spends it. The first loss must be
// survived (proving the budget is not zero and recovery works) and the second must
// end the job (proving the counter advanced and the gate holds). Asserting only the
// second would pass against a coordinator that never restarts at all.
//
// The losses are SEPARATED by a completed checkpoint, like the test above: two
// overlapping restarts are a different scenario with a known gap (F12), and mixing
// them in would make a failure here ambiguous.
TEST_F(FaultRecoveryTest, AJobFailsOnceItsRestartBudgetIsSpent) {
    // This scenario needs the job to still be RUNNING when the second kill
    // lands, and with the fixture's default source it is not reliably: the 2PC
    // job's source is bounded, so kTotalRecords at a 50 ms tick is about two
    // seconds of emission, and less than that after the first recovery replays
    // from the last checkpoint. Between the two kills this test waits for a
    // redeploy, a fresh checkpoint, a worker respawn and a re-registration.
    // When that outlasts what the source has left - which is what a contended
    // machine does to it - the job simply COMPLETES, the submitter exits 0, and
    // the budget assertion below reads a successful job as an unenforced gate.
    // That is how this test failed in CI (run 33839925755) with the gate it is
    // named for working correctly, and why it failed FASTER than it passes.
    // Standing a 5 s sleep in for the slow machine reproduces that failure
    // exactly, and does not reproduce it once this override is in place.
    //
    // So give this one scenario a source that cannot drain inside it: a minute
    // of emission against a sequence that takes seconds. The rate is unchanged,
    // so this adds no load to the machine whose slowness is the hazard, and the
    // job's own length is nothing this test asserts on - it ends on the
    // submitter exiting, never on a drain. SetUp re-sets both knobs before
    // every test, so the override cannot leak into another one, and the count
    // is deliberately NOT kTotalRecords, which the output-equality tests assert
    // against.
    ::setenv("CLINK_2PC_TOTAL", "1200", 1);

    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/1);
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)))
        << "no checkpoint completed before the first kill; the scenario never ran";
    const auto before_first = latest_completed(c.checkpoint_dir());

    // First loss: inside the budget, so the job must recover.
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    ASSERT_TRUE(
        clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > before_first; },
                            std::chrono::seconds(60)))
        << "the job never checkpointed again after the FIRST worker loss, so the budget was "
           "never actually spent and what follows would prove nothing";

    // Bring worker 0 back BEFORE the second kill, so the job has somewhere to run.
    //
    // Without this the test is vacuous, and it was: with only two workers, killing
    // the second leaves nowhere to redeploy and the job fails for lack of capacity
    // whatever the budget says. Mutating the gate to `true` (budget never runs out)
    // still passed. Restoring capacity makes the BUDGET the only thing that can end
    // the job, which is what this test is named for.
    ASSERT_TRUE(c.restart_worker(0));
    ASSERT_TRUE(c.await_workers_registered(3))
        << "worker 0 never re-registered, so the second kill would leave the cluster empty and "
           "this test would prove nothing about the budget";

    // Everything below assumes the job is still running. Check it, so that if
    // the source ever does drain in here the failure says the scenario never
    // ran instead of accusing the restart-budget gate of a fault it does not
    // have. Polling caches the exit code, so the await at the end still sees it.
    ASSERT_TRUE(sub->running())
        << "the job ended before the second kill, so the restart budget was never exercised: the "
           "source drained during the restart rather than the gate ending the job. Raise this "
           "test's CLINK_2PC_TOTAL override.";

    // Second loss: the budget is gone. The job must END, not restart again - even
    // though a worker is available to run it.
    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value())
        << "the submitter never exited after the budget was spent: the job neither recovered nor "
           "failed, which is the outcome an un-incremented counter produces - it would keep "
           "restarting";
    EXPECT_NE(*code, 0)
        << "the job reported SUCCESS after losing a worker with no restart budget left. Either "
           "the budget is not being enforced or restart_attempts is not advancing.";

    // The coordinator must say it refused, and name the budget. Without this the
    // assertion above also passes for a job that failed for an unrelated reason.
    EXPECT_TRUE(c.coordinator().log_contains("attempt 1/1") ||
                c.coordinator().log_contains("restart budget"))
        << "the coordinator never reported spending the restart budget, so this test did not "
           "observe the gate it is named for";
}

// --- coordinator death -----------------------------------------------------

TEST_F(FaultRecoveryTest, CoordinatorDeathIsReportedNotSilentlyHung) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/0);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)));

    c.coordinator().kill_hard();
    ASSERT_TRUE(clink::itest::await([&] { return !c.coordinator().running(); }));

    // The contract under test is failure TRANSPARENCY, not recovery: a
    // single-coordinator cluster has no standby, so the submitter must
    // terminate with a clear non-zero status rather than block for ever.
    // (HA failover with an elected standby is test_coordinator_ha_failover.)
    const auto code = sub->await_exit(std::chrono::seconds(90));
    ASSERT_TRUE(code.has_value())
        << "the submitter hung after the coordinator died instead of failing";
    EXPECT_NE(*code, 0) << "the submitter reported success despite losing the coordinator";
}

// --- checkpoint integrity across the real cluster --------------------------

TEST_F(FaultRecoveryTest, EveryCheckpointTheClusterPublishesVerifies) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/1);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value());
    ASSERT_EQ(*code, 0);

    // Every payload the cluster wrote must carry a matching sidecar. This
    // is the end-to-end proof that the integrity contract holds through
    // the real distributed write path, not only in the unit tests.
    std::size_t checked = 0;
    std::vector<std::string> bad;
    std::error_code ec;
    for (const auto& e : std::filesystem::recursive_directory_iterator(c.checkpoint_dir(), ec)) {
        if (ec) {
            break;
        }
        const auto name = e.path().filename().string();
        if (!e.is_regular_file() || name.rfind("checkpoint-", 0) != 0 ||
            name.find(".snap") == std::string::npos || name.find(".meta") != std::string::npos) {
            continue;
        }
        ++checked;
        if (const auto v = clink::state::verify_checkpoint(e.path()); !v.ok()) {
            bad.push_back(e.path().string() + ": " + v.detail);
        }
    }
    EXPECT_TRUE(bad.empty()) << [&] {
        std::string s = "checkpoints published by the cluster that do not verify:\n";
        for (const auto& b : bad) {
            s += "  " + b + "\n";
        }
        return s;
    }();
    // A pass with nothing checked would be vacuous.
    EXPECT_GT(checked, 0U) << "the job wrote no checkpoint payloads, so nothing was verified";
}

// --- fault-injected death at an exact protocol point -----------------------

TEST_F(FaultRecoveryTest, WorkerKilledAtTheStateRestorePointIsRedeployed) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    // Worker 1 is armed to die the first time it reaches the state-restore
    // point - i.e. exactly when it is asked to take over recovered work,
    // which is the hardest moment for the coordinator to handle. A
    // wall-clock kill cannot target this.
    ASSERT_TRUE(c.start_worker(1, ProcOptions{.fault = "state.before_restore=exit:70@1"}));
    ASSERT_TRUE(c.await_workers_registered(2));

    // file:// so the worker actually uses FileBackedStateBackend, which is
    // where state.before_restore lives. With the default in-memory backend
    // the point is never reached and the armed fault would never fire.
    auto sub = submit(c, /*max_restarts=*/3, "file:" + (c.root() / "state").string());
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)));

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    // The armed worker dies the moment the coordinator hands it recovered
    // work. What is under test here is that the FAULT LANDS where it was
    // aimed and the coordinator observes the loss - the recovery that
    // follows needs somewhere to run, and providing that is the next
    // assertion. Verifying the fault fired (rather than assuming it) is
    // what stops this from silently becoming a test of nothing if the
    // point is renamed.
    ASSERT_TRUE(
        clink::itest::await([&] { return !c.worker(1).running(); }, std::chrono::seconds(60)))
        << "the armed worker never reached state.before_restore, so the scenario did not run";
    const auto armed_exit = c.worker(1).poll_exit();
    ASSERT_TRUE(armed_exit.has_value());
    EXPECT_EQ(*armed_exit, 70)
        << "the worker exited for a reason other than the injected fault (expected _exit(70))";

    // Worker 0 is the only place left to run, so bring it back and let the
    // coordinator try again.
    ASSERT_TRUE(c.restart_worker(0));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    // F13 - CLOSED by the F12 fix. Two losses in the same restart window used to
    // wedge because the empty-drain kick was gated on that tick's events; with the
    // kick moved to the unconditional per-tick sweep, the second loss folds in and
    // the job completes. This was a GTEST_SKIP for the failure case, which the
    // fixed engine made dead code - so completion now GATES, and a regression in
    // the kick fails here rather than skipping silently.
    EXPECT_EQ(*code, 0) << "the job did not survive a worker lost at the state-restore point - the "
                           "F12/F13 restart-window fold has regressed";
}

// --- exactly-once at the SINK, across a process failure -------------------
//
// This is the claim the whole exercise turns on, and until now nothing
// asserted it. Every fault-recovery test above checks that the job
// COMPLETES and that checkpoints PROGRESS. Neither says anything about the
// output: a job that duplicated every record after a restart, or silently
// dropped the ones in flight, passes all of them.
//
// So these read what an external consumer would actually see and compare it
// to the exact multiset the source promises. The 2PC job emits "record-0"
// through "record-(N-1)", once each, and checkpoints its offset; the sink
// commits by atomic rename from staging/ into committed/. Only committed/
// is visible downstream - a file left in staging/ is a transaction nobody
// ever agreed to - so that is what gets read.
//
// Duplicates and losses are reported SEPARATELY, because they are different
// failures: a duplicate means the recovery replayed work already published
// (an at-least-once leak), a loss means it published nothing for records the
// source had already passed (data loss). A single "mismatch" count would
// hide which.

// The verifier lives in two_pc_output.hpp, shared with the coordinator-HA
// compound failure test so both compare output against the same contract.
using clink::itest::committed_records;
using clink::itest::describe;
using clink::itest::verify_exactly_once;

// The premise: on a clean run with no faults, the output is exactly the
// expected multiset. Without this, a failure in the crash tests below could
// be the job, the sink, or the verifier, and there would be no way to tell.
TEST_F(FaultRecoveryTest, ACleanRunCommitsEveryRecordExactlyOnce) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/0);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(std::chrono::seconds(90));
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0);

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty() && v.missing.empty() && v.unexpected.empty())
        << "a CLEAN run did not commit each record exactly once: " << describe(v);
}

TEST_F(FaultRecoveryTest, EveryRecordIsCommittedExactlyOnceAcrossAWorkerKill) {
    // The headline claim. A worker is SIGKILLed after a checkpoint has
    // completed, the job recovers, and the committed output must still be
    // each record exactly once - no replay of already-published work, no
    // gap where the in-flight records were.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing()) << "the job never started";
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(30)))
        << "no checkpoint completed before the kill, so recovery has nothing to resume from and "
           "this would be testing a different scenario";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the worker kill";
    ASSERT_EQ(*code, 0) << "the job did not recover";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << "records were committed MORE than once across the recovery, so the pipeline is "
           "at-least-once rather than exactly-once: "
        << describe(v);
    EXPECT_TRUE(v.missing.empty()) << "records were LOST across the recovery: " << describe(v);
    EXPECT_TRUE(v.unexpected.empty())
        << "output contains records the source never emitted: " << describe(v);
}

TEST_F(FaultRecoveryTest, ExactlyOnceHoldsWhenTheKillPrecedesAnyCheckpoint) {
    // A genuinely different recovery path, and the one where a naive sink
    // leaks. With no completed checkpoint there is nothing to resume from,
    // so the source replays from zero - and exactly-once then depends
    // entirely on the sink having committed NOTHING, because a commit only
    // follows a checkpoint the coordinator completed. If anything had been
    // published before the kill, the replay would duplicate it.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing()) << "the job never started";
    ASSERT_EQ(latest_completed(c.checkpoint_dir()), 0U)
        << "a checkpoint completed before the kill; that is the other scenario";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0) << "the job did not recover from a kill before any checkpoint";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << "the replay re-published records the sink had already committed, which means it "
           "committed without a completed checkpoint behind it: "
        << describe(v);
    EXPECT_TRUE(v.missing.empty()) << "records were lost: " << describe(v);
    EXPECT_TRUE(v.unexpected.empty()) << describe(v);
}

TEST_F(FaultRecoveryTest, NoUncommittedOutputIsVisibleAfterAKill) {
    // The other half of the sink contract, and one no test asserted: a
    // transaction that was staged but never committed must not be readable.
    // If a crash left staging files that a consumer would pick up, the
    // pipeline publishes work the coordinator never agreed to - which is
    // worse than a duplicate, because no checkpoint ever accounted for it.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing());
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value());

    // Whatever the outcome, nothing in committed/ may be absent from the
    // source's vocabulary, and the committed count can never exceed the
    // total the source ever emitted.
    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.unexpected.empty()) << describe(v);
    EXPECT_LE(v.total_lines, static_cast<std::size_t>(kTotalRecords))
        << "more lines are committed than the source ever emitted: " << describe(v);
}

}  // namespace

// --- item 19: the two fault moments the suite did not cover ----------------

// A worker dies immediately AFTER its sink's external commit - the transaction is
// published, the death lands before anything else happens. The recovery contract
// is idempotent re-commit: the restore point's snapshot holds the sink's handle
// for that checkpoint (staged before capture, F31), recover_all_() re-commits it,
// and an already-committed rename is a no-op. The output must still be exactly
// once - death-after-publish is precisely where a naive recovery double-publishes.
//
// Both workers are armed with the same fault because placement decides which of
// them hosts the sink subtask, and arming only one would make the scenario
// depend on the planner. Whichever worker commits first dies at that instant
// (_exit(71), first hit); the other never reaches a sink point and survives.
// The respawn is UNARMED - restart_worker takes fresh options - so the fault
// fires exactly once per test, deterministically.
TEST_F(FaultRecoveryTest, WorkerKilledAfterExternalCommitStillCommitsExactlyOnce) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0, ProcOptions{.fault = "sink.after_external_commit=exit:71@1"}));
    ASSERT_TRUE(c.start_worker(1, ProcOptions{.fault = "sink.after_external_commit=exit:71@1"}));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/3, "file:" + (c.root() / "state").string());
    ASSERT_NE(sub, nullptr);

    // The armed worker dies at the FIRST external commit, which is the first
    // CommitCheckpoint broadcast. Verify the fault landed where it was aimed
    // rather than assuming it - the exit code is the fault's own.
    std::size_t died = 2;
    ASSERT_TRUE(clink::itest::await(
        [&] {
            for (std::size_t i = 0; i < 2; ++i) {
                if (!c.worker(i).running()) {
                    died = i;
                    return true;
                }
            }
            return false;
        },
        std::chrono::seconds(60)))
        << "no worker reached sink.after_external_commit, so the scenario did not run";
    const auto armed_exit = c.worker(died).poll_exit();
    ASSERT_TRUE(armed_exit.has_value());
    ASSERT_EQ(*armed_exit, 71) << "the worker exited for a reason other than the injected fault";

    // Bring the dead worker back CLEAN and let recovery finish the job.
    ASSERT_TRUE(c.restart_worker(died));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the mid-commit death";
    EXPECT_EQ(*code, 0) << "the job did not survive a death immediately after external commit";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << v.duplicated.size() << " record(s) published MORE THAN ONCE after a death "
        << "immediately following external commit; first: " << v.duplicated.front()
        << ". recover_all_'s re-commit of an already-committed transaction must be "
           "idempotent, and was not.";
    EXPECT_TRUE(v.missing.empty())
        << v.missing.size() << " record(s) never published; first: " << v.missing.front();
}

// The coordinator and a worker die TOGETHER - the compound failure item 19 lists
// as uncovered.
//
// Job resume across a coordinator restart is an HA-DIR feature, and that contract
// was established by this test's own first failure: recover_persisted_jobs()
// returns immediately when ha_dir_ is empty, and its only caller is the
// leadership callback. Without --ha-dir, a restarted coordinator abandons every
// running job - the COMPLETED markers preserve a restore POINT, but nothing
// re-submits the job. The first version of this test assumed checkpoint_dir alone
// sufficed, converged on nothing for 60s, and the respawned coordinator's log
// showed no deploy at all. So the scenario runs on the engine's actual durability
// contract: one coordinator WITH an ha-dir (leadership self-acquired, manifests
// persisted under <ha_dir>/jobs), workers that discover the leader through it.
//
// The judgement is on DISK, not on the submitter: the submitter's connection died
// with the coordinator, so its exit code reflects the connection, not the job.
// What an external consumer sees is committed/, and that must converge to exactly
// the promised multiset once the coordinator returns (same ha-dir, so it wins
// leadership and recovers the persisted job) and the workers are back.
TEST_F(FaultRecoveryTest, CoordinatorAndWorkerDyingTogetherStillCommitsExactlyOnce) {
    auto sp = spec();
    sp.ha = true;  // start_ha_coordinators refuses on a non-HA spec
    Cluster c(sp);
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(1));
    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.start_ha_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/3, "file:" + (c.root() / "state").string());
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)))
        << "no checkpoint completed before the compound failure";

    // Both at once, hard. No ordering between the two kills is assumed anywhere
    // below - that is the point of the scenario. Worker 1 will ALSO exit shortly
    // after, by design (a worker watches its coordinator connection), which is why
    // the recovery below restarts both workers, not just the killed one.
    c.ha_coordinator(0).kill_hard();
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    ASSERT_TRUE(clink::itest::await([&] { return !c.ha_coordinator(0).running(); },
                                    std::chrono::seconds(10)));

    // Reap the orphaned submitter; its verdict is about the dead connection.
    (void)sub->await_exit(std::chrono::seconds(15));

    // Fresh coordinator on the SAME ha-dir: it wins the leader lock, runs
    // recover_persisted_jobs(), and redeploys from the latest COMPLETED marker.
    ASSERT_TRUE(c.start_ha_coordinators(1));
    ASSERT_TRUE(c.restart_worker_ha(0));
    ASSERT_TRUE(c.restart_worker_ha(1));

    // 60s, NOT 120: ctest's per-test ceiling is 120s and a bound that equals it
    // means a genuine non-convergence is killed by ctest with no output at all -
    // which is exactly how this test's first version "failed", as a silent
    // 120.13s timeout. The bound must leave the failure message room to speak.
    ASSERT_TRUE(clink::itest::await(
        [&] {
            const auto v = verify_exactly_once(out_dir_, kTotalRecords);
            return v.duplicated.empty() && v.missing.empty();
        },
        std::chrono::seconds(60)))
        << [&] {
               const auto v = verify_exactly_once(out_dir_, kTotalRecords);
               std::ostringstream os;
               os << "output never converged to exactly-once after the compound failure: "
                  << v.duplicated.size() << " duplicated, " << v.missing.size() << " missing (of "
                  << kTotalRecords << ")";
               if (!v.duplicated.empty()) {
                   os << "; first duplicate: " << v.duplicated.front();
               }
               return os.str();
           }();
}

TEST_F(FaultRecoveryTest, AWorkerKilledInsideTheCommitWindowStaysExactlyOnce) {
    // Item 19's named gap: every kill test above lands BETWEEN commits.
    // This one lands INSIDE one - the window between the coordinator
    // completing a checkpoint (marker on disk, CommitCheckpoint broadcast)
    // and the sink publishing the staged transaction. The sink's first
    // commit is held open for 4s by a Delay at sink.before_commit (armed in
    // the worker processes via the environment, ordinal 1 so recovery's own
    // commits run free), and the worker is SIGKILLed inside that hold.
    // Recovery must re-commit the staged-but-unpublished transaction
    // (recover-and-re-commit at open), and the committed output must still
    // be each record exactly once.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator()) << "coordinator did not come up";
    ASSERT_TRUE(c.start_worker(0, {.fault = "sink.before_commit=delay:4000@1"}));
    ASSERT_TRUE(c.start_worker(1, {.fault = "sink.before_commit=delay:4000@1"}));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing()) << "the job never started";
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(30)))
        << "no checkpoint completed, so there is no commit window to kill inside";

    // The window is REAL at kill time: a checkpoint has completed (marker
    // above) while NOTHING has been published - the first commit is inside
    // its hold. Without this guard the kill could land after the publish
    // and the test would silently degrade into the between-commits case
    // the suite already covers.
    ASSERT_TRUE(committed_records(out_dir_).empty())
        << "the sink published before the kill - the hold did not produce a window";

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the mid-commit kill";
    ASSERT_EQ(*code, 0) << "the job did not recover from a kill inside the commit window";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << "the staged transaction was committed twice across the recovery: " << describe(v);
    EXPECT_TRUE(v.missing.empty())
        << "the transaction staged before the kill was never re-committed - records the "
           "completed checkpoint accounted for are gone: "
        << describe(v);
    EXPECT_TRUE(v.unexpected.empty()) << describe(v);
}

// A snapshot write killed BEFORE its fsync must not leave a completed
// checkpoint behind (followups item 58).
//
// checkpoint.before_fsync sits in write_fsync_rename, after the bytes have
// been written to the temp file and before the fsync that makes them
// durable. It was declared, wired, and armed by nothing - machinery built
// for a scenario nobody ran, which the fault-point call-site gate cannot
// detect (it proves the point is REACHABLE, not that any test opens the
// window).
//
// The invariant under test is item 51/F95's: an ok-ack implies the acked
// checkpoint's snapshot is already on disk. Dying here means the bytes
// reached the page cache and the rename never happened, so no snapshot file
// exists at that id. Recovery must therefore fall back to an earlier
// checkpoint and replay - which is only observable as exactly-once output,
// because a checkpoint wrongly treated as complete would resume PAST
// records whose state was never persisted, and they would be missing.
TEST_F(FaultRecoveryTest, ASnapshotKilledBeforeItsFsyncDoesNotCountAsCompleted) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    // Ordinal 4, not 1: the earlier writes let at least one checkpoint
    // complete cleanly, so recovery has a genuine fallback point. Arming at
    // the very first write would test a job with no checkpoint at all,
    // which is a different (and already covered) scenario.
    ASSERT_TRUE(c.start_worker(0, ProcOptions{.fault = "checkpoint.before_fsync=exit:71@4"}));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    // file:// for the same reason the state.before_restore test needs it:
    // the in-memory backend never reaches a durable write, so the armed
    // point would never fire and this would silently test nothing.
    auto sub = submit(c, /*max_restarts=*/3, "file:" + (c.root() / "state").string());
    ASSERT_NE(sub, nullptr);

    // Vacuity, checked rather than assumed: the armed worker must actually
    // have died AT the fault, which its distinctive exit code proves. If
    // the point is ever renamed or moved off this path, this fails here
    // instead of the test quietly becoming a plain worker-kill.
    ASSERT_TRUE(
        clink::itest::await([&] { return !c.worker(0).running(); }, std::chrono::seconds(60)))
        << "the armed worker never reached checkpoint.before_fsync, so the window never opened";
    const auto armed_exit = c.worker(0).poll_exit();
    ASSERT_TRUE(armed_exit.has_value());
    EXPECT_EQ(*armed_exit, 71)
        << "the worker exited for a reason other than the injected fault (expected _exit(71))";

    ASSERT_TRUE(c.restart_worker(0));

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the mid-snapshot kill";
    ASSERT_EQ(*code, 0) << "the job did not recover from a snapshot killed before its fsync";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.missing.empty())
        << "records were LOST: a checkpoint whose snapshot never reached disk was treated as a "
           "usable restore point, so recovery resumed past state it does not have: "
        << describe(v);
    EXPECT_TRUE(v.duplicated.empty())
        << "records were committed more than once across the recovery: " << describe(v);
    EXPECT_TRUE(v.unexpected.empty())
        << "output contains records the source never emitted: " << describe(v);
}

// The coordinator dies AFTER the COMPLETED marker is durable and BEFORE the
// commit broadcast (followups item 58).
//
// coordinator.after_completed_marker was declared, wired and armed by
// nothing. It brackets the narrowest window in the 2PC protocol, and the
// code comment immediately below it states the invariant it exists to
// test: "the marker write ordering matters - by the time workers commit
// their pre-staged transactions, the marker is durable, so a crash
// mid-broadcast still lets recovery find COMPLETED-N and commit on
// restore." Nothing checked that.
//
// What makes this window sharp rather than merely unlucky: at the moment
// of death a 2PC sink is holding a PREPARED transaction it was never told
// to commit, and the marker on disk says that checkpoint completed. If
// recovery does not drive the commit, those records are staged forever and
// the output is short - a silent loss whose evidence (a prepared
// transaction) lives in the sink, not in any log. HA is required because a
// bare coordinator restart abandons its jobs (F82); a fresh coordinator on
// the SAME ha-dir wins the lock and runs recover_persisted_jobs().
TEST_F(FaultRecoveryTest, ACoordinatorDyingAfterTheCompletedMarkerStillGetsTheCommitDone) {
    auto sp = spec();
    sp.ha = true;  // start_ha_coordinators refuses on a non-HA spec
    Cluster c(sp);
    ScopedDiagnostics diag(c);
    // Ordinal 2, not 1: the first completed checkpoint is allowed through
    // cleanly so the job has a recovery point that is not the one whose
    // broadcast we are about to lose. Arming at 1 would test a job whose
    // only completed checkpoint is the interrupted one - a narrower and
    // less representative scenario.
    ASSERT_TRUE(c.start_ha_coordinators(
        1, ProcOptions{.fault = "coordinator.after_completed_marker=exit:72@2"}));
    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.start_ha_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/3, "file:" + (c.root() / "state").string());
    ASSERT_NE(sub, nullptr);

    // Vacuity, proven not assumed: the coordinator must have died AT the
    // armed point, which its distinctive exit code shows. Without this the
    // test would still pass if the point were renamed - it would just be a
    // slow clean run.
    ASSERT_TRUE(clink::itest::await([&] { return !c.ha_coordinator(0).running(); },
                                    std::chrono::seconds(60)))
        << "the coordinator never reached coordinator.after_completed_marker, so the window "
           "between the durable marker and the commit broadcast never opened";
    const auto armed_exit = c.ha_coordinator(0).poll_exit();
    ASSERT_TRUE(armed_exit.has_value());
    EXPECT_EQ(*armed_exit, 72)
        << "the coordinator exited for a reason other than the injected fault (expected _exit(72))";

    // A marker for the interrupted checkpoint must be on disk - that is the
    // half of the window that DID happen, and recovery's whole job is to
    // honour it.
    EXPECT_GT(latest_completed(c.checkpoint_dir()), 0u)
        << "no COMPLETED marker survived, so the fault fired before the write rather than after "
           "it and this is testing the wrong side of the window";

    // The submitter's connection died with the coordinator; its exit code
    // describes the connection, not the job. The verdict is on disk.
    (void)sub->await_exit(std::chrono::seconds(15));

    ASSERT_TRUE(c.start_ha_coordinators(1));
    ASSERT_TRUE(c.restart_worker_ha(0));
    ASSERT_TRUE(c.restart_worker_ha(1));

    // 60s rather than the ctest ceiling, for the reason the compound-failure
    // test above documents: a bound equal to the ceiling turns a genuine
    // non-convergence into a silent kill with no message.
    ASSERT_TRUE(clink::itest::await(
        [&] {
            const auto v = verify_exactly_once(out_dir_, kTotalRecords);
            return v.duplicated.empty() && v.missing.empty();
        },
        std::chrono::seconds(60)))
        << [&] {
               const auto v = verify_exactly_once(out_dir_, kTotalRecords);
               std::ostringstream os;
               os << "output never converged after the coordinator died between the durable "
                     "COMPLETED marker and the commit broadcast - a prepared transaction was "
                     "left unpublished: "
                  << v.duplicated.size() << " duplicated, " << v.missing.size() << " missing (of "
                  << kTotalRecords << ")";
               if (!v.missing.empty()) {
                   os << "; first missing: " << v.missing.front();
               }
               return os.str();
           }();
}

// The coordinator dies after every ack but BEFORE the COMPLETED marker is
// written (followups item 58 - the last of the three unarmed points).
//
// The mirror image of its sibling above, and the assertion is the opposite
// one. Nothing durable records that this checkpoint completed, and the
// commit broadcast is downstream of the marker write, so NO sink may have
// published anything for it. Recovery must therefore fall back to the last
// checkpoint that DID get a marker and replay from there.
//
// The failure this guards against is a coordinator that broadcasts commits
// before the record of them is durable: sinks would publish output for a
// checkpoint that recovery cannot see, and replaying from the earlier point
// would then re-publish it - duplicates in the committed multiset, which is
// the at-least-once failure the whole 2PC protocol exists to avoid.
TEST_F(FaultRecoveryTest,
       ACoordinatorDyingBeforeTheCompletedMarkerCommitsNothingForThatCheckpoint) {
    auto sp = spec();
    sp.ha = true;
    Cluster c(sp);
    ScopedDiagnostics diag(c);
    // Ordinal 2: checkpoint one completes and marks cleanly, giving recovery
    // a fallback point; the second dies before its marker.
    ASSERT_TRUE(c.start_ha_coordinators(
        1, ProcOptions{.fault = "coordinator.before_completed_marker=exit:73@2"}));
    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.start_ha_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/3, "file:" + (c.root() / "state").string());
    ASSERT_NE(sub, nullptr);

    ASSERT_TRUE(clink::itest::await([&] { return !c.ha_coordinator(0).running(); },
                                    std::chrono::seconds(60)))
        << "the coordinator never reached coordinator.before_completed_marker";
    const auto armed_exit = c.ha_coordinator(0).poll_exit();
    ASSERT_TRUE(armed_exit.has_value());
    EXPECT_EQ(*armed_exit, 73)
        << "the coordinator exited for a reason other than the injected fault (expected _exit(73))";

    // The half of the window that DID happen is an ack set with no marker.
    // Exactly one marker should exist - checkpoint one's - and the
    // interrupted checkpoint must have left none. Capturing it here, before
    // recovery writes more, is what makes the claim checkable at all.
    const auto marker_at_death = latest_completed(c.checkpoint_dir());
    EXPECT_GT(marker_at_death, 0u)
        << "no marker at all survived, so the fault fired before the first checkpoint completed "
           "and recovery has no fallback - a different scenario from the one under test";

    (void)sub->await_exit(std::chrono::seconds(15));

    ASSERT_TRUE(c.start_ha_coordinators(1));
    ASSERT_TRUE(c.restart_worker_ha(0));
    ASSERT_TRUE(c.restart_worker_ha(1));

    ASSERT_TRUE(clink::itest::await(
        [&] {
            const auto v = verify_exactly_once(out_dir_, kTotalRecords);
            return v.duplicated.empty() && v.missing.empty();
        },
        std::chrono::seconds(60)))
        << [&] {
               const auto v = verify_exactly_once(out_dir_, kTotalRecords);
               std::ostringstream os;
               os << "output never converged after the coordinator died before the COMPLETED "
                     "marker. Duplicates here mean a sink published for a checkpoint no marker "
                     "records, and the replay from the earlier point published it again: "
                  << v.duplicated.size() << " duplicated, " << v.missing.size() << " missing (of "
                  << kTotalRecords << ")";
               if (!v.duplicated.empty()) {
                   os << "; first duplicate: " << v.duplicated.front();
               }
               return os.str();
           }();
}

// --- The hung-but-alive axis. Every kill above is SIGKILL: clean death,
// the failure mode the engine handles best and reality serves least. These
// two pause a process with SIGSTOP - alive, holding its sockets, its locks
// and its staged output - and resume it AFTER the cluster has moved on.

// A worker declared lost while merely paused RESUMES, with subtasks live,
// staged 2PC output on disk, and half-open sockets. The re-registration
// contract's own comment names the premise this violates: "the previous
// PROCESS is gone, so anything it had in flight can never report" - here it
// is not gone, and everything it had in flight can still report.
//
// What keeps the output exactly-once is structural, and this test exists to
// pin exactly that structure: commits happen only on coordinator triggers
// (a dead-ended control plane mints no new ones), the committed filename is
// derived from (subtask, checkpoint) with no attempt component (a zombie
// finishing an in-flight commit renames onto the path recovery already
// wrote - idempotent, not duplicating), and loss-declaration shutdown_read()s
// the zombie's socket so its resumed heartbeats go nowhere. Weaken any of
// those - attempt-unique filenames, say - and the zombie duplicates output,
// which is precisely what the final assertion is watching for.
TEST_F(FaultRecoveryTest, AWorkerResumedAfterBeingDeclaredLostCannotBreakExactlyOnce) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)))
        << "no checkpoint completed before the pause";

    c.worker(0).signal(SIGSTOP);

    // Vacuity: the watchdog must genuinely declare the loss (default
    // heartbeat_timeout is 2s), or everything below is a plain clean run.
    ASSERT_TRUE(clink::itest::await([&] { return c.coordinator().log_contains("worker lost"); },
                                    std::chrono::seconds(30)))
        << "the coordinator never declared the paused worker lost";

    // The job must recover onto worker 1 and complete WHILE worker 0 is
    // still frozen - proven, not assumed, by its process state.
    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the pause";
    ASSERT_EQ(*code, 0) << "the job did not recover from a paused worker";
    {
        std::string state_cmd = "ps -o state= -p " + std::to_string(c.worker(0).pid());
        FILE* p = ::popen(state_cmd.c_str(), "r");
        ASSERT_NE(p, nullptr);
        char buf[16] = {};
        (void)::fgets(buf, sizeof(buf), p);
        ::pclose(p);
        ASSERT_NE(std::string_view{buf}.find('T'), std::string_view::npos)
            << "worker 0 was not in the stopped state when the job completed (state: " << buf
            << "), so this tested an ordinary worker loss, not a zombie";
    }

    // Wake the zombie. Everything it does from here is the scenario: its
    // subtasks resume mid-record, its channels point at cancelled peers,
    // any in-flight commit dispatch completes against files recovery
    // already committed.
    c.worker(0).signal(SIGCONT);

    // Give it a damage window. The zombie is EXPECTED to notice its
    // dead-ended control plane and exit, but that is not the contract this
    // test gates - the discarded result makes this a bounded wait, not an
    // assertion, and the window exists so that if the zombie CAN corrupt
    // the output, it has had every chance to before we look.
    (void)clink::itest::await([&] { return !c.worker(0).running(); }, std::chrono::seconds(8));

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << "the resumed zombie re-published output recovery had already committed: " << describe(v);
    EXPECT_TRUE(v.missing.empty()) << "records were LOST across the zombie window: " << describe(v);
    EXPECT_TRUE(v.unexpected.empty())
        << "output contains records the source never emitted: " << describe(v);
    EXPECT_TRUE(c.coordinator().running())
        << "the coordinator did not survive the zombie's resumed traffic";

    c.worker(0).kill_and_reap();
}

// The coordinator itself pauses past its own heartbeat timeout and resumes.
// A pause longer than the worker heartbeat lease makes each worker retire its
// stale control session, drain its tasks and reconnect after the coordinator
// resumes. The worker processes themselves must stay alive throughout.
//
// Contract: the job completes exactly-once, both original worker PIDs survive,
// and the resumed coordinator does not independently declare either worker
// stale before accepting their replacement sessions.
TEST_F(FaultRecoveryTest, ACoordinatorPausedPastItsHeartbeatTimeoutStillDeliversExactlyOnce) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    bring_up(c);

    auto sub = submit(c, /*max_restarts=*/3);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await([&] { return latest_completed(c.checkpoint_dir()) > 0; },
                                    std::chrono::seconds(45)))
        << "no checkpoint completed before the pause";

    const auto worker_0_pid = c.worker(0).pid();
    const auto worker_1_pid = c.worker(1).pid();

    c.coordinator().signal(SIGSTOP);
    // Hold the pause for 3x the 2s default heartbeat timeout, using the
    // workers' continued liveness as the condition (they must NOT die just
    // because the coordinator went quiet - their sockets are merely
    // buffering). A fixed sleep would hide a worker that exits early.
    const auto pause_until = std::chrono::steady_clock::now() + std::chrono::seconds(6);
    ASSERT_TRUE(clink::itest::await(
        [&] {
            if (!c.worker(0).running() || !c.worker(1).running()) {
                return true;  // fail fast below - a worker died during the pause
            }
            return std::chrono::steady_clock::now() >= pause_until;
        },
        std::chrono::seconds(20)));
    ASSERT_TRUE(c.worker(0).running() && c.worker(1).running())
        << "a worker process exited during the coordinator pause";
    ASSERT_EQ(c.worker(0).pid(), worker_0_pid);
    ASSERT_EQ(c.worker(1).pid(), worker_1_pid);
    c.coordinator().signal(SIGCONT);

    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the coordinator resumed";
    ASSERT_EQ(*code, 0) << "the job did not complete after the coordinator resumed";
    EXPECT_EQ(c.worker(0).pid(), worker_0_pid)
        << "worker-0 was replaced instead of recovering its control session in-process";
    EXPECT_EQ(c.worker(1).pid(), worker_1_pid)
        << "worker-1 was replaced instead of recovering its control session in-process";

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << "a post-resume false loss triggered a recovery that re-published output: "
        << describe(v);
    EXPECT_TRUE(v.missing.empty()) << "records were LOST across the pause: " << describe(v);
    EXPECT_TRUE(v.unexpected.empty())
        << "output contains records the source never emitted: " << describe(v);

    // The watchdog must recognise its own pause before the reconnecting
    // sessions arrive. Session replacement can legitimately fold the old
    // tasks into one restart, but the watchdog must not independently evict
    // workers based on timestamps accumulated while it was stopped.
    EXPECT_TRUE(c.coordinator().log_contains("watchdog resumed after a suspension"))
        << "the pause never tripped the self-pause detector, so this run did not "
           "exercise the resume path it exists to gate";
    EXPECT_FALSE(c.coordinator().log_contains("[coordinator.watchdog] [warning] worker lost"))
        << "the resumed watchdog declared a healthy worker lost - self-pause "
           "detection has regressed";
    EXPECT_TRUE(c.coordinator().log_contains("re-registered; previous session retired"))
        << "the workers never replaced their expired control sessions";
}
