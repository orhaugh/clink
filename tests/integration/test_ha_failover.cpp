// Coordinator failover, across real processes.
//
// The unit-level fencing tests (tests/test_coordinator_fencing.cpp) pin the
// RULE: a worker refuses a frame from a lower epoch than the one it bound
// at registration. They do it by scripting the transport, which is the only
// way to choose the epoch on each individual frame.
//
// What they cannot show is that the rule holds against epochs the SYSTEM
// produces rather than epochs a test hands it. That is what these are for:
// two coordinator processes contending for one lock, a real election, a
// real kill, and a real takeover. Two things have to be true afterwards and
// both are easy to get wrong in opposite directions:
//
//   1. The epoch actually advances. A failover that reuses the old epoch
//      fences nothing, and the unit tests would still pass.
//   2. Nothing is fenced that should not be. Adding fencing to a control
//      plane can break the very failover it exists to protect - a worker
//      that refuses the NEW leader is as dead as one obeying the old one,
//      and it fails silently, which is worse.
//
// Not covered here, and stated rather than implied: a genuine split brain,
// where the displaced leader is still ALIVE and still sending. Producing
// one needs the old process to keep its sockets while losing the lock,
// which the file coordinator cannot do (the fcntl lock is released only on
// process death, so a SIGSTOPped leader keeps it and no standby takes
// over). It needs a lease-based store - etcd - and is build-gated on it.
// See docs/production-hardening-plan.md, W15.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"

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

class HaFailoverTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::filesystem::exists(node_binary()) || !std::filesystem::exists(submit_binary()) ||
            !std::filesystem::exists(two_phase_commit_job())) {
            GTEST_SKIP() << "cluster binaries or the 2PC job plugin are not built";
        }
        out_dir_ = std::filesystem::temp_directory_path() /
                   ("clink_ha_out_" + std::to_string(::getpid()) + "_" +
                    ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(out_dir_);
        std::filesystem::create_directories(out_dir_);
        ::setenv("CLINK_2PC_OUT_DIR", out_dir_.c_str(), 1);
        ::setenv("CLINK_2PC_TOTAL", "40", 1);
        ::setenv("CLINK_2PC_TICK_MS", "50", 1);
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
        s.ha = true;
        return s;
    }

    static std::unique_ptr<Process> submit(Cluster& c) {
        auto p = std::make_unique<Process>();
        std::vector<std::string> argv{submit_binary().string(),
                                      "--job=" + two_phase_commit_job().string(),
                                      "--coordinator-host=127.0.0.1",
                                      "--coordinator-port=" + std::to_string(c.coordinator_port()),
                                      "--wait-timeout-s=90",
                                      "--checkpoint-dir=" + c.checkpoint_dir().string(),
                                      "--checkpoint-interval-ms=150",
                                      "--max-restarts-on-worker-loss=2"};
        const bool ok = p->spawn("submit", submit_binary(), std::move(argv), c.log_dir());
        return ok ? std::move(p) : nullptr;
    }

    std::filesystem::path out_dir_;
};

// Two coordinators, one lock: exactly one leads and the other holds the
// control port closed. Establishes the premise the rest of the file rests
// on - if both bound the port, nothing below would mean anything.
TEST_F(HaFailoverTest, ExactlyOneOfTwoCoordinatorsLeads) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(2)) << "no coordinator won the election";

    const auto leader = c.current_leader_index();
    ASSERT_TRUE(leader.has_value());
    const auto epoch = c.announced_epoch(*leader);
    ASSERT_TRUE(epoch.has_value()) << "the leader did not announce an epoch";
    EXPECT_GE(*epoch, 1U) << "a leader must take a non-zero epoch, or it fences nothing";

    // The standby must NOT have announced leadership.
    const std::size_t standby = *leader == 0 ? 1 : 0;
    EXPECT_FALSE(c.announced_epoch(standby).has_value())
        << "both coordinators believe they are the leader";
}

// The property the whole design turns on: leadership moving forward moves
// the epoch forward. A failover that reuses the epoch would leave the new
// leader indistinguishable from the old one on the wire.
TEST_F(HaFailoverTest, FailoverAdvancesTheEpoch) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(2));

    const auto first = c.current_leader_index();
    ASSERT_TRUE(first.has_value());
    const auto first_epoch = c.announced_epoch(*first);
    ASSERT_TRUE(first_epoch.has_value());

    const auto killed = c.kill_leader_and_await_failover();
    ASSERT_TRUE(killed.has_value()) << "no standby took over after the leader was killed";

    const auto second = c.current_leader_index();
    ASSERT_TRUE(second.has_value());
    ASSERT_NE(*second, *first);
    const auto second_epoch = c.announced_epoch(*second);
    ASSERT_TRUE(second_epoch.has_value());
    EXPECT_GT(*second_epoch, *first_epoch)
        << "the new leader took epoch " << *second_epoch << ", not above the old " << *first_epoch
        << "; a repeated epoch fences nothing";
}

// The direction that fencing can break rather than fix. A worker that
// registers with the NEW leader must bind the new epoch and be fenced by
// nothing, or the cluster is down and quiet about it.
TEST_F(HaFailoverTest, AWorkerJoiningTheNewLeaderIsNotFencedOff) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(2));

    const auto first_epoch = c.announced_epoch(*c.current_leader_index());
    ASSERT_TRUE(first_epoch.has_value());

    ASSERT_TRUE(c.kill_leader_and_await_failover().has_value());
    const auto second = c.current_leader_index();
    ASSERT_TRUE(second.has_value());
    const auto second_epoch = c.announced_epoch(*second);
    ASSERT_TRUE(second_epoch.has_value());

    // The worker discovers the leader through the election, which is the
    // only path that survives a failover.
    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1)) << "the worker never registered with the new leader";

    EXPECT_EQ(c.worker_discovered_epoch(0), second_epoch)
        << "the worker bound an epoch other than the new leader's";
    EXPECT_EQ(c.worker_fenced_frames(0), 0U)
        << "the new leader's own frames were refused by the worker it just registered";
}

// End to end at a non-zero epoch: a job submitted to the coordinator that
// won a failover must deploy, run, checkpoint and complete. Every control
// frame in that sequence is stamped, so any send path that forgot the epoch
// shows up here as a job that never starts.
TEST_F(HaFailoverTest, AJobSubmittedToTheNewLeaderRunsToCompletion) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(2));
    ASSERT_TRUE(c.kill_leader_and_await_failover().has_value());
    ASSERT_TRUE(c.await_coordinator_ready()) << "the new leader never opened the control port";

    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    const auto code = sub->await_exit(std::chrono::seconds(90));
    ASSERT_TRUE(code.has_value()) << "the submitter never exited";
    EXPECT_EQ(*code, 0) << "a job submitted to a fenced coordinator did not complete";
    EXPECT_EQ(c.worker_fenced_frames(0), 0U)
        << "the worker refused a frame from its own leader during a normal run";
}

// The metadata half. The manifest a leader writes records its epoch, so a
// superseded leader can be refused at the write. Read back off disk rather
// than through the API, because the file is what a recovering coordinator
// actually reads.
TEST_F(HaFailoverTest, TheJobManifestRecordsTheWritingLeadersEpoch) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(2));
    ASSERT_TRUE(c.kill_leader_and_await_failover().has_value());
    ASSERT_TRUE(c.await_coordinator_ready());
    const auto epoch = c.announced_epoch(*c.current_leader_index());
    ASSERT_TRUE(epoch.has_value());

    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));
    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);
    ASSERT_TRUE(sub->await_exit(std::chrono::seconds(90)).has_value());

    // Find the manifest the leader persisted for whatever job id it assigned.
    const auto jobs_dir = c.ha_dir() / "jobs";
    ASSERT_TRUE(std::filesystem::exists(jobs_dir)) << "no job was persisted under the HA dir";
    bool checked = false;
    std::error_code ec;
    for (const auto& e : std::filesystem::directory_iterator(jobs_dir, ec)) {
        const auto manifest = e.path() / "manifest.json";
        if (!std::filesystem::exists(manifest)) {
            continue;
        }
        EXPECT_EQ(clink::cluster::metadata_stored_epoch(manifest.string()), *epoch)
            << manifest.string() << " does not record the epoch of the leader that wrote it";
        checked = true;
    }
    EXPECT_TRUE(checked) << "no manifest.json was found to check";
}

}  // namespace
