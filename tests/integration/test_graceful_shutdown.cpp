// SIGTERM against a node that is actually doing something.
//
// tests/integration/test_sigterm_shutdown.cpp already covers SIGTERM, and
// passes - but it signals an IDLE worker, one that registered and has been
// sitting in its poll loop ever since. Nothing there exercises the case
// that matters operationally: a worker with a job running on it.
//
// That distinction is the whole point of handling SIGTERM. A container
// runtime sends SIGTERM, waits out a grace period, and then sends SIGKILL.
// A node that exits promptly when idle and hangs when busy gets SIGKILLed
// in exactly the situation where a clean shutdown was worth something, and
// the idle test reports green throughout.
//
// The bound below is a FAILURE bound, not a wait: a correct shutdown
// completes in well under a second. It is set near a realistic Kubernetes
// terminationGracePeriodSeconds so that "passes the test" and "survives a
// real rollout" are the same claim.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <signal.h>
#include <string>
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
std::filesystem::path two_phase_commit_job() {
#ifdef CLINK_TWO_PHASE_COMMIT_JOB_PATH
    return std::filesystem::path{CLINK_TWO_PHASE_COMMIT_JOB_PATH};
#else
    return {};
#endif
}

// A grace period in the range a container runtime actually uses. Kubernetes
// defaults to 30 s; anything approaching that is already a bad shutdown, so
// 15 s is generous and still meaningful as a failure bound.
constexpr auto kGracePeriod = std::chrono::seconds{15};

class GracefulShutdownTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(two_phase_commit_job())) {
            GTEST_SKIP() << "cluster binaries or the 2PC job plugin are not built";
        }
        out_dir_ = std::filesystem::temp_directory_path() /
                   ("clink_gs_out_" + std::to_string(::getpid()) + "_" +
                    ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(out_dir_);
        std::filesystem::create_directories(out_dir_);
        ::setenv("CLINK_2PC_OUT_DIR", out_dir_.c_str(), 1);
        // Long enough that the job is unambiguously still running when the
        // signal lands. A job that had already finished would make this
        // test pass for the wrong reason.
        ::setenv("CLINK_2PC_TOTAL", "1000000", 1);
        ::setenv("CLINK_2PC_TICK_MS", "1", 1);
    }

    void TearDown() override {
        std::error_code ec;
        std::filesystem::remove_all(out_dir_, ec);
    }

    static ClusterSpec spec() {
        ClusterSpec s;
        s.node_binary = node_binary();
        s.workers = 1;
        s.slots_per_worker = 4;
        return s;
    }

    static std::unique_ptr<Process> submit(Cluster& c) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{submit_binary().string(),
                                      "--job=" + two_phase_commit_job().string(),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(c.coordinator_port()),
                                      "--wait-timeout-s=120",
                                      "--checkpoint-dir=" + c.checkpoint_dir().string(),
                                      "--checkpoint-interval-ms=150",
                                      "--max-restarts-on-worker-loss=0"};
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    std::filesystem::path out_dir_;
};

}  // namespace

TEST_F(GracefulShutdownTest, AWorkerRunningAJobExitsOnSigterm) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing())
        << "the job never started, so this would not have tested a BUSY worker";
    ASSERT_TRUE(c.worker(0).running());

    c.worker(0).signal(SIGTERM);
    const auto code = c.worker(0).await_exit(kGracePeriod);

    ASSERT_TRUE(code.has_value())
        << "a worker running a job did not exit within " << kGracePeriod.count()
        << "s of SIGTERM. A container runtime would SIGKILL it, which is precisely the "
           "situation graceful shutdown exists to avoid.";
    // 128+n means killed by signal n. Exiting BECAUSE of SIGTERM's default
    // action is not handling it.
    EXPECT_NE(*code, 128 + SIGTERM) << "the worker was terminated by SIGTERM's default action "
                                       "rather than shutting itself down";

    sub->kill_and_reap();
}

TEST_F(GracefulShutdownTest, ACoordinatorWithARunningJobExitsOnSigterm) {
    // The same question for the coordinator, which holds the accept thread,
    // a watchdog, a checkpoint timer and a reader thread per worker. Any
    // one of them failing to observe the stop flag wedges the process.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing());

    c.coordinator().signal(SIGTERM);
    const auto code = c.coordinator().await_exit(kGracePeriod);

    ASSERT_TRUE(code.has_value()) << "a coordinator with a running job did not exit within "
                                  << kGracePeriod.count() << "s of SIGTERM";
    EXPECT_NE(*code, 128 + SIGTERM);

    sub->kill_and_reap();
}

TEST_F(GracefulShutdownTest, ASecondSigtermDoesNotChangeTheOutcome) {
    // Operators and supervisors repeat the signal when a process looks
    // slow. A second SIGTERM arriving mid-shutdown must be absorbed, not
    // re-enter teardown or race the first one into a crash.
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_coordinator());
    ASSERT_TRUE(c.start_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(c.await_job_checkpointing());

    c.worker(0).signal(SIGTERM);
    c.worker(0).signal(SIGTERM);
    const auto code = c.worker(0).await_exit(kGracePeriod);
    ASSERT_TRUE(code.has_value()) << "a repeated SIGTERM wedged the shutdown";
    EXPECT_NE(*code, 128 + SIGABRT) << "the second signal crashed a process already shutting down";

    sub->kill_and_reap();
}
