// Cross-sink commit atomicity, under failure.
//
// docs/production-hardening-plan.md W22 records the gap: nothing verified
// that a job's two transactional sinks agree about what they published when
// a failure lands between their commits. The gating code was read, never
// exercised.
//
// Writing these tests answered a second question that reading could not.
// The property holds - and `commit_group`, which the guarantee analyser used
// to prescribe for it, has nothing to do with it. Running the worker-kill
// case with the group and with no group gives identical per-checkpoint
// agreement, because commit is one job-wide broadcast per checkpoint issued
// only after every subtask acked ok, and one failed ack aborts that
// checkpoint for every sink. `pending` in Coordinator::CheckpointGroupState
// is maintained and never read; there is no group-scoped commit. So the
// ungrouped case below is not a contrast, it is the same guarantee reached
// by the same mechanism, and both are asserted so a change that makes
// ungrouped sinks genuinely split is caught.
//
// The property, checkable from outside the process: for every checkpoint id,
// either BOTH sinks have a committed file or NEITHER does. One without the
// other is precisely the split a group exists to prevent - the state where
// two outputs disagree about what happened - and it is invisible to any test
// that only counts records, because the record totals can be complete while
// the two sides are internally inconsistent.
//
// The 2PC sink names its committed output `committed/sub<N>-<ckpt>.dat`, so
// the set of checkpoint ids each sink has published is readable from the
// filesystem. That is what makes this testable without instrumenting the
// engine.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/cluster_harness.hpp"

namespace {

using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
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
std::filesystem::path two_sink_job() {
#ifdef CLINK_TWO_SINK_JOB_PATH
    return std::filesystem::path{CLINK_TWO_SINK_JOB_PATH};
#else
    return {};
#endif
}

constexpr int kTotalRecords = 40;

// Checkpoint ids this sink has PUBLISHED, parsed from
// committed/sub<N>-<ckpt>.dat. Files still in staging/ are excluded: an
// uncommitted transaction is not visible to a consumer and must not count
// as published.
std::set<std::uint64_t> committed_checkpoints(const std::filesystem::path& out_dir) {
    std::set<std::uint64_t> ids;
    const auto dir = out_dir / "committed";
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return ids;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        // sub<N>-<ckpt>.dat
        const auto stem = entry.path().stem().string();
        const auto dash = stem.rfind('-');
        if (dash == std::string::npos) {
            continue;
        }
        try {
            ids.insert(std::stoull(stem.substr(dash + 1)));
        } catch (const std::exception&) {
            // A name that does not parse is not a checkpoint file.
        }
    }
    return ids;
}

std::vector<std::string> committed_records(const std::filesystem::path& out_dir) {
    std::vector<std::string> lines;
    const auto dir = out_dir / "committed";
    std::error_code ec;
    if (!std::filesystem::exists(dir, ec)) {
        return lines;
    }
    for (const auto& entry : std::filesystem::directory_iterator(dir, ec)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty()) {
                lines.push_back(line);
            }
        }
    }
    return lines;
}

// Wait until both sinks' committed sets have stopped changing, then return
// them. A commit in flight when the kill landed can complete a moment later,
// and comparing mid-commit would report a split that resolves itself.
//
// A fixed settle sleep was the obvious way to handle that and the wrong one:
// it is the timing dependence this suite exists to avoid, too short on a
// loaded runner and wasted time otherwise. This observes the actual thing -
// no change for a quiet period - so it returns as soon as the system is
// quiescent and only fails a test when nothing ever settles.
struct SinkSets {
    std::set<std::uint64_t> a;
    std::set<std::uint64_t> b;
};

SinkSets await_settled(const std::filesystem::path& out_a,
                       const std::filesystem::path& out_b,
                       std::chrono::milliseconds quiet_for = std::chrono::milliseconds{750}) {
    SinkSets last{committed_checkpoints(out_a), committed_checkpoints(out_b)};
    auto stable_since = std::chrono::steady_clock::now();
    const auto deadline = stable_since + std::chrono::seconds{30};
    while (std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
        SinkSets now{committed_checkpoints(out_a), committed_checkpoints(out_b)};
        if (now.a != last.a || now.b != last.b) {
            last = std::move(now);
            stable_since = std::chrono::steady_clock::now();
            continue;
        }
        if (std::chrono::steady_clock::now() - stable_since >= quiet_for) {
            return last;
        }
    }
    return last;
}

std::string describe_split(const std::set<std::uint64_t>& a, const std::set<std::uint64_t>& b) {
    std::ostringstream os;
    const auto join = [](const std::set<std::uint64_t>& s) {
        std::ostringstream o;
        for (auto it = s.begin(); it != s.end(); ++it) {
            o << (it == s.begin() ? "" : ",") << *it;
        }
        return o.str();
    };
    os << "sink A committed checkpoints {" << join(a) << "}, sink B {" << join(b) << "}";
    return os.str();
}

class CommitGroupAtomicityTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(two_sink_job())) {
            GTEST_SKIP() << "cluster binaries or the two-sink job are not built";
        }
        const auto base = std::filesystem::temp_directory_path() /
                          ("clink_cg_" + std::to_string(::getpid()) + "_" +
                           ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(base);
        out_a_ = base / "a";
        out_b_ = base / "b";
        std::filesystem::create_directories(out_a_);
        std::filesystem::create_directories(out_b_);
        base_ = base;
        ::setenv("CLINK_2SINK_OUT_A", out_a_.c_str(), 1);
        ::setenv("CLINK_2SINK_OUT_B", out_b_.c_str(), 1);
        ::setenv("CLINK_2SINK_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
        ::setenv("CLINK_2SINK_TICK_MS", "50", 1);
        ::setenv("CLINK_2SINK_GROUP", commit_group(), 1);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(base_, ec);
    }

    // Overridden by the ungrouped fixture. "" means the sinks declare no
    // commit_group at all.
    virtual const char* commit_group() const { return "atomic-out"; }

    static ClusterSpec spec() {
        ClusterSpec s;
        s.node_binary = node_binary();
        s.workers = 2;
        s.slots_per_worker = 4;
        return s;
    }

    std::unique_ptr<Process> submit(Cluster& c, int max_restarts) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{
            submit_binary().string(),
            "--job=" + two_sink_job().string(),
            "--coordinator-host=127.0.0.1",
            "--coordinator-port=" + std::to_string(c.coordinator_port()),
            "--wait-timeout-s=120",
            "--checkpoint-dir=" + c.checkpoint_dir().string(),
            "--checkpoint-interval-ms=150",
            "--max-restarts-on-worker-loss=" + std::to_string(max_restarts)};
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    std::filesystem::path base_;
    std::filesystem::path out_a_;
    std::filesystem::path out_b_;
};

}  // namespace

// The premise: on a clean run the two sinks publish the SAME set of
// checkpoints. Without this, a failure below could be the job, the fan-out,
// or the filename parsing.
TEST_F(CommitGroupAtomicityTest, TwoSinksAgreeOnACleanRun) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/0);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(std::chrono::seconds(120));
    ASSERT_TRUE(code.has_value()) << "submitter never exited";
    ASSERT_EQ(*code, 0);

    const auto a = committed_checkpoints(out_a_);
    const auto b = committed_checkpoints(out_b_);
    ASSERT_FALSE(a.empty()) << "sink A published nothing; the job did not run as expected";
    EXPECT_EQ(a, b) << "two sinks of one job published DIFFERENT checkpoints on a clean run: "
                    << describe_split(a, b);

    // And each sink independently owes the whole stream.
    for (const auto& [label, dir] : {std::pair{"A", out_a_}, std::pair{"B", out_b_}}) {
        std::map<std::string, int> seen;
        for (const auto& line : committed_records(dir)) {
            ++seen[line];
        }
        for (int i = 0; i < kTotalRecords; ++i) {
            const auto want = "record-" + std::to_string(i);
            EXPECT_EQ(seen[want], 1)
                << "sink " << label << " published " << seen[want] << " copies of " << want;
        }
    }
}

TEST_F(CommitGroupAtomicityTest, GroupedSinksNeverSplitAcrossAWorkerKill) {
    // The gap W22 recorded. A worker dies while the job is running; the job
    // must never end up with one sink having published a checkpoint the
    // other did not.
    //
    // Note what is NOT asserted: that the job completes, or that every
    // record arrives. A kill with a restart budget may or may not finish
    // inside the deadline, and that is not the property under test. What
    // must hold either way is that the two sinks AGREE - a job that fails
    // outright still must not leave two outputs disagreeing.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    // Wait until BOTH sinks have published something, so the kill lands
    // when there is real committed state to be inconsistent about. Waiting
    // on a checkpoint marker instead would make this premise
    // timing-dependent.
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return !committed_checkpoints(out_a_).empty() && !committed_checkpoints(out_b_).empty();
        },
        std::chrono::seconds(60)))
        << "the sinks never both published before the kill, so a split could not have been "
           "detected: "
        << describe_split(committed_checkpoints(out_a_), committed_checkpoints(out_b_));

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    (void)sub->await_exit(std::chrono::seconds(120));

    const auto settled = await_settled(out_a_, out_b_);
    const auto& a = settled.a;
    const auto& b = settled.b;
    EXPECT_EQ(a, b) << "the two sinks SPLIT across a worker kill: one published a checkpoint the "
                       "other did not, so the outputs disagree about what happened. "
                    << describe_split(a, b);

    sub->kill_and_reap();
}

TEST_F(CommitGroupAtomicityTest, NoSinkPublishesACheckpointTheOtherAborted) {
    // The same property viewed from the abort side. When any subtask fails
    // its pre-commit the coordinator withholds the completed marker and
    // aborts that checkpoint job-wide, so no sink may publish it.
    // Approximated from outside by the strongest observable statement: no
    // checkpoint id appears in exactly one sink's committed set once things
    // settle.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await([&] { return !committed_checkpoints(out_a_).empty(); },
                                    std::chrono::seconds(60)));

    // Kill the OTHER worker this time: whichever sink subtask it hosted
    // loses its in-flight transaction, which is the asymmetric case.
    c.worker(1).kill_hard();
    ASSERT_TRUE(c.await_process_gone(1));
    (void)sub->await_exit(std::chrono::seconds(120));

    const auto settled = await_settled(out_a_, out_b_);
    const auto& a = settled.a;
    const auto& b = settled.b;
    std::vector<std::uint64_t> only_one;
    for (const auto id : a) {
        if (b.find(id) == b.end()) {
            only_one.push_back(id);
        }
    }
    for (const auto id : b) {
        if (a.find(id) == a.end()) {
            only_one.push_back(id);
        }
    }
    EXPECT_TRUE(only_one.empty())
        << only_one.size()
        << " checkpoint(s) were published by exactly one of the two sinks, so they did not commit "
           "as a unit. "
        << describe_split(a, b);

    sub->kill_and_reap();
}

// The finding, made permanent. Same job, same kill, NO commit_group - and
// the same per-checkpoint agreement, because the guarantee never came from
// the group. Asserting it here means a future change that makes ungrouped
// sinks genuinely commit independently fails a test instead of quietly
// making the analyser's old advice true again.
//
// It also keeps the three cases above honest about what they demonstrate:
// with this alongside them, none of them can be read as evidence that
// setting a commit_group achieves anything.
class UngroupedSinkAtomicityTest : public CommitGroupAtomicityTest {
protected:
    const char* commit_group() const override { return ""; }
};

TEST_F(UngroupedSinkAtomicityTest, SinksWithNoCommitGroupAlsoNeverSplitAcrossAWorkerKill) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return !committed_checkpoints(out_a_).empty() && !committed_checkpoints(out_b_).empty();
        },
        std::chrono::seconds(60)))
        << "the sinks never both published before the kill: "
        << describe_split(committed_checkpoints(out_a_), committed_checkpoints(out_b_));

    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    (void)sub->await_exit(std::chrono::seconds(120));

    const auto settled = await_settled(out_a_, out_b_);
    const auto& a = settled.a;
    const auto& b = settled.b;
    EXPECT_EQ(a, b) << "two UNGROUPED sinks split across a worker kill. That is the behaviour the "
                       "engine's comments used to describe and the analyser used to warn about; if "
                       "it is now real, the grouped cases above are the only ones safe and the "
                       "analyser needs its commit_group advice back. "
                    << describe_split(a, b);

    sub->kill_and_reap();
}
