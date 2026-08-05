// A side output is held to the same exactly-once standard as the main branch,
// across a worker failure.
//
// Follow-up item 14. Side outputs were exercised on healthy runs ONLY. That is not
// a theoretical gap: F39 found side outputs silently failing to ATTACH on Linux, so
// the records went nowhere and every assertion still passed. A branch of the graph
// with no failure coverage is where the next defect is, and this workstream has
// repeatedly found that a suite which looks thorough can share one blind spot.
//
// The job (examples/side_output_recovery_job.cpp):
//
//   replayable source ──▶ fan-out ──main──▶ file_2pc_sink (main/)
//                                 └─side──▶ file_2pc_sink (side/)
//
// Both sinks are 2PC, which is what lets this assert EXACTLY-once rather than
// at-least-once: with a plain file sink a duplicate on the side branch after a kill
// would be indistinguishable from correct behaviour, and the test could not tell a
// defect from the contract. The source checkpoints its offset so a restart resumes
// instead of replaying from zero.
//
// Each input "record-N" produces "record-N" on the main branch and "side-N" on the
// side branch, so both are 1:1 with the source and a record that reached one branch
// but not the other is attributable to an index.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "tests/integration/cluster_harness.hpp"

using clink::itest::Cluster;
using clink::itest::ClusterSpec;
using clink::itest::Process;
using clink::itest::ScopedDiagnostics;

namespace {

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
std::filesystem::path side_output_job() {
#ifdef CLINK_SIDE_OUTPUT_RECOVERY_JOB_PATH
    return std::filesystem::path{CLINK_SIDE_OUTPUT_RECOVERY_JOB_PATH};
#else
    return {};
#endif
}

// Records the job emits, and therefore the exact multiset EACH branch must
// contain. Named once so the environment the job is given and the expectation the
// test checks cannot drift apart, which would make the comparison vacuous.
constexpr int kTotalRecords = 60;

std::vector<std::string> committed_lines(const std::filesystem::path& branch_dir) {
    std::vector<std::string> lines;
    const auto dir = branch_dir / "committed";
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

struct BranchVerdict {
    std::vector<std::string> missing;
    std::vector<std::string> duplicated;
    std::vector<std::string> unexpected;
    std::size_t total_lines{0};
};

// Compare a branch's committed output against "<prefix>-0".."<prefix>-(N-1)",
// each exactly once.
BranchVerdict verify_branch(const std::filesystem::path& branch_dir,
                            const std::string& prefix,
                            int total) {
    BranchVerdict v;
    std::map<std::string, int> seen;
    for (const auto& line : committed_lines(branch_dir)) {
        ++seen[line];
        ++v.total_lines;
    }
    for (int i = 0; i < total; ++i) {
        const auto want = prefix + "-" + std::to_string(i);
        const auto it = seen.find(want);
        if (it == seen.end()) {
            v.missing.push_back(want);
        } else if (it->second > 1) {
            v.duplicated.push_back(want + " x" + std::to_string(it->second));
        }
        seen.erase(want);
    }
    for (const auto& [line, count] : seen) {
        v.unexpected.push_back(line + " x" + std::to_string(count));
    }
    return v;
}

std::string describe(const BranchVerdict& v) {
    std::ostringstream os;
    os << v.total_lines << " committed lines";
    const auto list = [&os](const char* label, const std::vector<std::string>& xs) {
        if (xs.empty()) {
            return;
        }
        os << "; " << xs.size() << " " << label << ": ";
        for (std::size_t i = 0; i < xs.size() && i < 8; ++i) {
            os << (i ? ", " : "") << xs[i];
        }
        if (xs.size() > 8) {
            os << ", ... (+" << (xs.size() - 8) << ")";
        }
    };
    list("MISSING", v.missing);
    list("DUPLICATED", v.duplicated);
    list("UNEXPECTED", v.unexpected);
    return os.str();
}

class SideOutputRecoveryTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(side_output_job())) {
            GTEST_SKIP() << "cluster binaries or the side-output job plugin are not built";
        }
        out_dir_ = std::filesystem::temp_directory_path() /
                   ("clink_sor_" + std::to_string(::getpid()) + "_" +
                    ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(out_dir_);
        std::filesystem::create_directories(out_dir_);
        ::setenv("CLINK_SOR_OUT_DIR", out_dir_.c_str(), 1);
        ::setenv("CLINK_SOR_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
        ::setenv("CLINK_SOR_TICK_MS", "40", 1);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(out_dir_, ec);
    }

    static std::unique_ptr<Process> submit(Cluster& c, int max_restarts) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{
            submit_binary().string(),
            "--job=" + side_output_job().string(),
            "--coordinator-host=127.0.0.1",
            "--coordinator-port=" + std::to_string(c.coordinator_port()),
            "--wait-timeout-s=120",
            "--checkpoint-dir=" + c.checkpoint_dir().string(),
            "--checkpoint-interval-ms=150",
            "--max-restarts-on-worker-loss=" + std::to_string(max_restarts)};
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    static ClusterSpec spec() {
        ClusterSpec s;
        s.node_binary = node_binary();
        s.workers = 2;
        s.slots_per_worker = 4;
        return s;
    }

    [[nodiscard]] std::filesystem::path main_dir() const { return out_dir_ / "main"; }
    [[nodiscard]] std::filesystem::path side_dir() const { return out_dir_ / "side"; }

    std::filesystem::path out_dir_;
};

}  // namespace

// The premise. Without it, a side-branch failure below could be the job, the
// side-output wiring, or the sink, and there would be no way to tell which.
//
// It also pins the F39 failure mode directly: a side output that never attaches
// produces an EMPTY side directory while the main branch looks perfect.
TEST_F(SideOutputRecoveryTest, ACleanRunCommitsBothBranchesExactlyOnce) {
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

    const auto main_v = verify_branch(main_dir(), "record", kTotalRecords);
    EXPECT_TRUE(main_v.missing.empty() && main_v.duplicated.empty() && main_v.unexpected.empty())
        << "a CLEAN run did not commit the MAIN branch exactly once: " << describe(main_v);

    const auto side_v = verify_branch(side_dir(), "side", kTotalRecords);
    EXPECT_GT(side_v.total_lines, 0u)
        << "the side branch committed NOTHING on a clean run. That is the F39 shape: the side "
           "output never attached, the records went nowhere, and the main branch looks perfect.";
    EXPECT_TRUE(side_v.missing.empty() && side_v.duplicated.empty() && side_v.unexpected.empty())
        << "a CLEAN run did not commit the SIDE branch exactly once: " << describe(side_v);
}

// The thing this file exists for: kill a worker mid-stream and hold the SIDE
// branch to exactly-once, not just the main one.
//
// The kill is triggered by a CONDITION - a completed checkpoint plus committed
// output on both branches - rather than a sleep, so it lands at a point where
// there is genuinely something for a bad recovery to lose or duplicate, on every
// run and not just a lucky one.
TEST_F(SideOutputRecoveryTest, BothBranchesStayExactlyOnceAcrossAWorkerKill) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.start_worker(1));
    ASSERT_TRUE(c.await_workers_registered(2));

    auto sub = submit(c, /*max_restarts=*/2);
    ASSERT_NE(sub, nullptr);

    // Wait for BOTH branches to have committed something. Killing before the side
    // branch has published anything would leave nothing on it for a replay to
    // duplicate, and the assertion below would hold for the wrong reason.
    ASSERT_TRUE(clink::itest::await(
        [&] {
            return verify_branch(main_dir(), "record", kTotalRecords).total_lines > 0 &&
                   verify_branch(side_dir(), "side", kTotalRecords).total_lines > 0;
        },
        std::chrono::seconds(90)))
        << "neither branch committed anything before the kill, so the scenario never ran";
    const auto side_before = verify_branch(side_dir(), "side", kTotalRecords).total_lines;

    // Kill and do NOT bring the worker back: the job has to recover onto the
    // survivor, which is the pattern the other fault tests use and the one this
    // cluster is sized for (4 subtasks, 4 slots per worker).
    //
    // An earlier cut restarted the SAME worker immediately and the job hung
    // forever - the coordinator logged "worker-0 re-registered; previous session
    // retired" and never restarted the job's subtasks. That is recorded as its own
    // follow-up rather than worked around silently here; whatever it is, it is not
    // about side outputs, and leaving it in this test would mean this test never
    // measures what it is named for.
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));

    const auto code = sub->await_exit(std::chrono::seconds(180));
    ASSERT_TRUE(code.has_value()) << "submitter never exited after the worker kill";
    EXPECT_EQ(*code, 0) << "the job did not survive the worker kill";

    const auto main_v = verify_branch(main_dir(), "record", kTotalRecords);
    const auto side_v = verify_branch(side_dir(), "side", kTotalRecords);
    const std::string context = "main: " + describe(main_v) + " | side: " + describe(side_v) +
                                " | side committed before the kill: " + std::to_string(side_before);

    EXPECT_TRUE(main_v.duplicated.empty())
        << "the MAIN branch duplicated records across the kill. " << context;
    EXPECT_TRUE(main_v.missing.empty())
        << "the MAIN branch lost records across the kill. " << context;

    // The assertions this file was written for.
    EXPECT_TRUE(side_v.duplicated.empty())
        << "the SIDE branch duplicated records across the kill: work already published on the "
           "side output was replayed and committed again. "
        << context;
    EXPECT_TRUE(side_v.missing.empty())
        << "the SIDE branch lost records across the kill. A side output that stops attaching "
           "after a redeploy loses everything from that point silently - the main branch keeps "
           "working, so nothing else notices. "
        << context;
    EXPECT_TRUE(side_v.unexpected.empty())
        << "the SIDE branch committed records the job never produced. " << context;
}
