// Fault-pair combinatorics: two faults at once, sampled from the grid
// nobody hand-writes.
//
// Every fault point is armed individually by some test, and item 19 armed
// two hand-picked combinations. That leaves the rest of the ordered grid -
// window A held open while a process dies inside window B - explored by
// nothing, and both of the durability defects found this week were SINGLE
// faults in windows no test had opened. Pairs are the obvious next seam.
//
// Shape of one sample: BOTH workers arm the DELAY side (delay:150 at
// every hit of point A - short enough not to wedge, long enough that
// windows genuinely overlap; both workers because subtask PLACEMENT
// decides which process a point fires in, and this suite's own vacuity
// guard caught a first draft that armed one worker and sampled nothing
// for sink-side points). ONE worker additionally arms the EXIT side
// (exit:1 at the second hit of point B, so the first checkpoint usually
// completes and recovery has a fallback) - and if that worker survives
// the whole run untouched, the pair is retried once with the exit armed
// on the other worker, because placement put point B's subtask there.
// The exit clause is listed BEFORE the delay clause in the schedule:
// reach() is first-match-wins, and on diagonal pairs (A == B) a
// delay-first schedule would shadow the exit forever. The dying worker is
// NOT respawned: the survivor absorbs the job, bounding every sample at
// one death and one recovery. The verdict per pair is the only one that
// matters: the job completes and the committed output is exactly-once.
//
// Engagement is EVIDENCE, not assumption. The fault framework writes a
// fire-time witness line to unbuffered stderr precisely so this file can
// grep the worker logs for it: a sampled pair whose faults never fired is
// a clean run wearing a scary name, and it is REPORTED as unengaged
// rather than silently counted as coverage. The exit-side pool is curated
// to points that deterministically fire in this job, so exit-side
// engagement is asserted outright; the delay side may legitimately not
// fire (state.before_restore fires only if a restore happens) and is
// recorded per pair.
//
// Knobs, mirroring the SQL oracle: CLINK_FAULT_PAIRS_N samples per run
// (default 4), CLINK_FAULT_PAIRS_SEED (default fixed so CI reproduces),
// CLINK_FAULT_PAIRS_ALL=1 iterates the full ordered grid (a campaign, not
// a gate - ~90 cluster runs).

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <random>
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

constexpr int kTotalRecords = 40;

// The points that deterministically fire in a checkpointing 2PC run on a
// file-backed state backend. This list is CURATED, not enumerated from the
// framework: a point outside this job's paths would make its pair vacuous,
// and the exit-side engagement assertion below depends on every member
// genuinely firing. Extending the job extends the eligible pool.
const std::vector<std::string>& always_firing_points() {
    static const std::vector<std::string> kPoints{
        "checkpoint.before_write",
        "checkpoint.during_write",
        "checkpoint.before_fsync",
        "checkpoint.before_publish",
        "checkpoint.after_publish",
        "sink.before_prepare",
        "sink.after_prepare",
        "sink.before_commit",
        "sink.after_external_commit",
    };
    return kPoints;
}

// The delay side adds the restore point: it fires only when the survivor
// actually restores (a checkpoint must have completed first), which is
// exactly the kind of conditional window worth holding open - but its
// non-firing is legitimate, so it is recorded rather than asserted.
const std::vector<std::string>& delay_side_points() {
    static const std::vector<std::string> kPoints = [] {
        auto v = always_firing_points();
        v.push_back("state.before_restore");
        return v;
    }();
    return kPoints;
}

std::uint64_t pairs_seed() {
    if (const char* s = std::getenv("CLINK_FAULT_PAIRS_SEED"); s && *s) {
        return std::strtoull(s, nullptr, 10);
    }
    return 20260814ULL;
}
int pairs_n() {
    if (const char* s = std::getenv("CLINK_FAULT_PAIRS_N"); s && *s) {
        return std::max(1, std::atoi(s));
    }
    return 4;
}

std::uint64_t latest_completed(const std::filesystem::path& ckpt_root) {
    std::uint64_t best = 0;
    std::error_code ec;
    for (auto it = std::filesystem::recursive_directory_iterator(ckpt_root, ec);
         it != std::filesystem::recursive_directory_iterator();
         it.increment(ec)) {
        if (ec) {
            break;
        }
        const auto name = it->path().filename().string();
        if (name.rfind("COMPLETED-", 0) == 0) {
            best = std::max(best, std::strtoull(name.c_str() + 10, nullptr, 10));
        }
    }
    return best;
}

struct PairVerdict {
    bool submit_ok{false};
    bool exit_side_fired{false};
    bool delay_side_fired{false};
    clink::itest::OutputVerdict output;
    std::string detail;
};

class FaultPairs : public ::testing::Test {
protected:
    void SetUp() override {
        if (!binaries_available()) {
            GTEST_SKIP() << "cluster binaries or the 2PC job plugin are not built";
        }
    }

    static PairVerdict run_pair_once(const std::string& delay_point,
                                     const std::string& exit_point,
                                     int exit_worker,
                                     const std::string& tag) {
        const auto out_dir = std::filesystem::temp_directory_path() /
                             ("clink_fp_out_" + std::to_string(::getpid()) + "_" + tag);
        std::filesystem::remove_all(out_dir);
        std::filesystem::create_directories(out_dir);
        ::setenv("CLINK_2PC_OUT_DIR", out_dir.c_str(), 1);
        ::setenv("CLINK_2PC_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
        ::setenv("CLINK_2PC_TICK_MS", "50", 1);

        PairVerdict v;
        ClusterSpec spec;
        spec.node_binary = node_binary();
        spec.workers = 2;
        spec.slots_per_worker = 4;
        Cluster c(spec);
        ScopedDiagnostics diag(c);
        if (!c.start_coordinator()) {
            v.detail = "coordinator did not come up";
            return v;
        }
        // Exit clause FIRST: reach() is first-match-wins, and on a diagonal
        // pair a delay-first schedule shadows the exit at every hit.
        const std::string delay_clause = delay_point + "=delay:150";
        const std::string exit_and_delay = exit_point + "=exit:1@2," + delay_clause;
        for (int w = 0; w < 2; ++w) {
            const bool arms_exit = (w == exit_worker);
            if (!c.start_worker(w,
                                ProcOptions{.fault = arms_exit ? exit_and_delay : delay_clause})) {
                v.detail = "worker " + std::to_string(w) + " did not come up";
                return v;
            }
        }
        if (!c.await_workers_registered(2)) {
            v.detail = "workers did not register";
            return v;
        }

        auto sub = std::make_unique<Process>();
        std::vector<std::string> argv{submit_binary().string(),
                                      "--job=" + two_phase_commit_job().string(),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(c.coordinator_port()),
                                      "--wait-timeout-s=90",
                                      "--checkpoint-dir=" + c.checkpoint_dir().string(),
                                      "--checkpoint-interval-ms=150",
                                      "--max-restarts-on-worker-loss=3",
                                      "--state-backend=file:" + (c.root() / "state").string()};
        if (!sub->spawn("submit", submit_binary(), std::move(argv), c.log_dir())) {
            v.detail = "submitter did not spawn";
            return v;
        }

        const auto code = sub->await_exit(std::chrono::seconds(120));
        v.submit_ok = code.has_value() && *code == 0;
        if (!v.submit_ok) {
            v.detail = "submitter exit " +
                       (code.has_value() ? std::to_string(*code) : std::string{"(none)"});
        }
        v.exit_side_fired =
            c.worker(static_cast<std::size_t>(exit_worker)).log_contains("fired: " + exit_point);
        v.delay_side_fired = c.worker(0).log_contains("fired: " + delay_point) ||
                             c.worker(1).log_contains("fired: " + delay_point);
        v.output = clink::itest::verify_exactly_once(out_dir, kTotalRecords);

        std::error_code ec;
        std::filesystem::remove_all(out_dir, ec);
        return v;
    }

    // Placement-immune wrapper: if the exit-armed worker survived untouched
    // (its point's subtask was placed on the peer), retry once with the
    // arming swapped. Bounded at two cluster runs per pair.
    static PairVerdict run_pair(const std::string& delay_point,
                                const std::string& exit_point,
                                const std::string& tag) {
        auto v = run_pair_once(delay_point, exit_point, /*exit_worker=*/1, tag);
        if (!v.exit_side_fired && v.submit_ok) {
            v = run_pair_once(delay_point, exit_point, /*exit_worker=*/0, tag + "_swap");
        }
        return v;
    }

    static void assert_pair(const std::string& delay_point,
                            const std::string& exit_point,
                            const std::string& tag) {
        const auto v = run_pair(delay_point, exit_point, tag);
        const std::string what = "pair [hold " + delay_point + " | die-in " + exit_point + "]";
        EXPECT_TRUE(v.submit_ok) << what << ": the job did not complete (" << v.detail << ")";
        EXPECT_TRUE(v.exit_side_fired)
            << what << ": the exit-side fault never fired, so this sample was a clean run "
            << "wearing a scary name - the point has left this job's paths and must leave "
            << "the curated pool";
        EXPECT_TRUE(v.output.duplicated.empty())
            << what << ": records committed MORE than once: " << describe(v.output);
        EXPECT_TRUE(v.output.missing.empty()) << what << ": records LOST: " << describe(v.output);
        EXPECT_TRUE(v.output.unexpected.empty())
            << what << ": output contains records the source never emitted: " << describe(v.output);
        RecordProperty(("delay_fired_" + tag).c_str(), v.delay_side_fired ? "yes" : "no");
    }
};

// The engagement detector must be able to say NO. rescale.after_drain is a
// real, declared point that this job's paths never reach (nothing rescales
// here); a detector that reports it fired is matching noise, and every
// engagement assertion above would be meaningless.
TEST_F(FaultPairs, TheEngagementDetectorCanSayNo) {
    // run_pair_once, not run_pair: the swap-retry exists to chase placement,
    // and a point that fires NOWHERE would just be chased twice.
    const auto v = run_pair_once("sink.before_commit",
                                 "rescale.after_drain",
                                 /*exit_worker=*/1,
                                 "vacuity");
    EXPECT_TRUE(v.submit_ok) << "an unengaged exit fault must leave a clean run: " << v.detail;
    EXPECT_FALSE(v.exit_side_fired)
        << "the detector claims rescale.after_drain fired in a job that never rescales - "
           "the witness grep is matching noise and every engagement result is suspect";
    EXPECT_TRUE(v.delay_side_fired)
        << "sink.before_commit fires on every commit in this job; if the detector cannot "
           "see it, the witness line is not reaching the worker log";
}

// The sampled gate: N seeded pairs per run. Deterministic by default so a
// CI failure reproduces; the seed and every pair are named in the output.
TEST_F(FaultPairs, SampledPairsStayExactlyOnce) {
    const auto& delays = delay_side_points();
    const auto& exits = always_firing_points();
    std::vector<std::pair<std::string, std::string>> grid;
    for (const auto& d : delays) {
        for (const auto& e : exits) {
            grid.emplace_back(d, e);
        }
    }
    std::mt19937_64 rng{pairs_seed()};
    std::shuffle(grid.begin(), grid.end(), rng);
    const int n = std::min<int>(pairs_n(), static_cast<int>(grid.size()));
    for (int i = 0; i < n; ++i) {
        SCOPED_TRACE("seed=" + std::to_string(pairs_seed()) + " sample=" + std::to_string(i));
        assert_pair(grid[static_cast<std::size_t>(i)].first,
                    grid[static_cast<std::size_t>(i)].second,
                    "s" + std::to_string(i));
    }
}

// The full ordered grid - a campaign, not a gate. ~90 cluster runs; opt in
// with CLINK_FAULT_PAIRS_ALL=1 and expect ~20 minutes.
TEST_F(FaultPairs, FullGridCampaign) {
    if (const char* all = std::getenv("CLINK_FAULT_PAIRS_ALL"); all == nullptr || *all != '1') {
        GTEST_SKIP() << "set CLINK_FAULT_PAIRS_ALL=1 to run the full ordered grid";
    }
    const auto& delays = delay_side_points();
    const auto& exits = always_firing_points();
    int idx = 0;
    for (const auto& d : delays) {
        for (const auto& e : exits) {
            SCOPED_TRACE("grid pair " + std::to_string(idx) + ": hold " + d + " | die-in " + e);
            assert_pair(d, e, "g" + std::to_string(idx));
            ++idx;
        }
    }
}

}  // namespace
