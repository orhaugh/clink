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

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"

#include "tests/integration/await_port.hpp"
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

// Records the 2PC job emits, and therefore the exact multiset the committed
// output must contain after a failover.
constexpr int kTotalRecords = 40;

// Lines under <out>/committed/ - the output an external consumer sees. A
// file still in staging/ is a transaction nobody agreed to and must not be
// counted.
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

struct OutputVerdict {
    std::vector<std::string> duplicated;
    std::vector<std::string> missing;
    std::vector<std::string> unexpected;
    std::size_t total_lines{0};
    std::size_t distinct{0};
};

OutputVerdict verify_exactly_once(const std::filesystem::path& out_dir, int total) {
    OutputVerdict v;
    std::map<std::string, int> seen;
    for (const auto& line : committed_records(out_dir)) {
        ++seen[line];
        ++v.total_lines;
    }
    v.distinct = seen.size();
    for (int i = 0; i < total; ++i) {
        const auto want = "record-" + std::to_string(i);
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

std::string describe(const OutputVerdict& v) {
    std::ostringstream os;
    os << v.total_lines << " committed lines, " << v.distinct << " distinct";
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
    list("DUPLICATED", v.duplicated);
    list("MISSING", v.missing);
    list("UNEXPECTED", v.unexpected);
    return os.str();
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
        ::setenv("CLINK_2PC_TOTAL", std::to_string(kTotalRecords).c_str(), 1);
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

    // The manifest is persisted at submission and RETIRED at terminal (item
    // 69: what recovery would re-run is deleted once the job finishes, with
    // a TERMINAL tombstone in its place), so the epoch check has to read it
    // while the job is LIVE. The job runs for seconds against a 50ms poll,
    // so the window is wide; this used to read after exit and went red the
    // day retirement landed.
    const auto jobs_dir = c.ha_dir() / "jobs";
    const auto find_manifest = [&]() -> std::optional<std::filesystem::path> {
        std::error_code ec;
        if (!std::filesystem::exists(jobs_dir, ec)) {
            return std::nullopt;
        }
        for (const auto& e : std::filesystem::directory_iterator(jobs_dir, ec)) {
            const auto manifest = e.path() / "manifest.json";
            if (std::filesystem::exists(manifest)) {
                return manifest;
            }
        }
        return std::nullopt;
    };
    ASSERT_TRUE(clink::itest::await_condition([&] { return find_manifest().has_value(); },
                                              std::chrono::seconds{60}))
        << "no manifest.json appeared under the HA dir while the job ran";
    EXPECT_EQ(clink::cluster::metadata_stored_epoch(find_manifest()->string()), *epoch)
        << "the live manifest does not record the epoch of the leader that wrote it";

    ASSERT_TRUE(sub->await_exit(std::chrono::seconds(90)).has_value());

    // The terminal half of the same lifecycle: retirement removes the
    // manifest and leaves the tombstone, so a later takeover cannot
    // resurrect the finished job. Retirement runs inside completion
    // signalling; the bound covers a slow store, not a design delay.
    const auto tombstone_exists = [&] {
        std::error_code ec;
        for (const auto& e : std::filesystem::directory_iterator(jobs_dir, ec)) {
            if (std::filesystem::exists(e.path() / "TERMINAL")) {
                return true;
            }
        }
        return false;
    };
    EXPECT_TRUE(clink::itest::await_condition(
        [&] { return !find_manifest().has_value() && tombstone_exists(); },
        std::chrono::seconds{30}))
        << "the finished job was not retired: manifest "
        << (find_manifest().has_value() ? "still present" : "gone") << ", tombstone "
        << (tombstone_exists() ? "present" : "absent") << " (item 69)";
}

// --- exactly-once across a COORDINATOR failure ----------------------------
//
// The worker-failure case is covered by output equality in
// test_fault_recovery.cpp. This is the other half, and it exercises a
// different mechanism end to end: the surviving coordinator has to READ the
// last completed checkpoint off disk and resume the job from it. That is the
// path F29 broke - the marker was written flat and read job-scoped, so every
// completed checkpoint was invisible and a recovered job silently restarted
// from scratch. A test asserting only "the job ran again" would have passed
// throughout; only the output says whether it resumed or replayed.
//
// The submitter is deliberately NOT the signal. Its connection dies with the
// leader it was talking to, so its exit code reports the client's fate
// rather than the job's. The contract is the data: what ended up committed.
TEST_F(HaFailoverTest, ExactlyOnceSurvivesACoordinatorFailover) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    ASSERT_TRUE(c.start_ha_coordinators(2));
    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);

    // Wait for output to be COMMITTED, not merely for a checkpoint marker to
    // appear.
    //
    // Those are different moments: the marker means the coordinator
    // completed the checkpoint; the commit happens later, when the worker
    // processes the CommitCheckpoint broadcast. Killing on the marker made
    // this test's own premise timing-dependent - under the load of the full
    // suite the commit had not landed, so there was no published work for a
    // recovery to lose and the test failed on its premise rather than on the
    // engine. Waiting for committed bytes makes the premise a driven
    // condition instead of a hope.
    ASSERT_TRUE(clink::itest::await(
        [&] { return verify_exactly_once(out_dir_, kTotalRecords).total_lines > 0; },
        std::chrono::seconds(45)))
        << "nothing was committed before the failover, so a recovery could not lose or duplicate "
           "published work and this run would prove nothing";

    const auto committed_before = verify_exactly_once(out_dir_, kTotalRecords).total_lines;
    ASSERT_GT(committed_before, 0U);
    const auto worker_pid_before = c.worker(0).pid();

    // Kill the leader. The standby takes over and calls
    // recover_persisted_jobs(), which resolves a restore point from the
    // COMPLETED marker on disk.
    ASSERT_TRUE(c.kill_leader_and_await_failover().has_value())
        << "no standby took over after the leader was killed";

    // The worker process stays alive, fences and drains its old task session,
    // discovers the new leader, then re-registers a fresh session in-process.
    ASSERT_TRUE(c.await_workers_registered(2))
        << "the worker never re-registered with the new leader";
    ASSERT_TRUE(c.worker(0).running()) << "worker process exited during coordinator failover";
    EXPECT_EQ(c.worker(0).pid(), worker_pid_before)
        << "coordinator failover restarted the worker process instead of its control session";

    // Wait for the job to finish under the new leader, judged by the output
    // rather than by any process: every record committed, or a deadline.
    const bool finished = clink::itest::await(
        [&] { return verify_exactly_once(out_dir_, kTotalRecords).missing.empty(); },
        std::chrono::seconds(90));

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(v.duplicated.empty())
        << "records were committed MORE than once across the coordinator failover, so the "
           "recovered job replayed work that had already been published: "
        << describe(v);
    EXPECT_TRUE(v.unexpected.empty()) << describe(v);
    EXPECT_TRUE(finished) << "the job did not finish under the new leader: " << describe(v)
                          << " [processes: " << c.describe_coordinator_exits()
                          << ", worker-0=" << (c.worker(0).running() ? "running" : "gone") << "]";
    EXPECT_TRUE(v.missing.empty())
        << "records were LOST across the coordinator failover: " << describe(v);
    // committed_before is asserted non-zero above, so this run genuinely had
    // published work that a bad recovery could lose - which is exactly what
    // F34 lost.

    sub->kill_and_reap();
}

// A recovered job must WAIT for capacity, not die of it. The takeover races
// an external process supervisor restarting a worker that genuinely died;
// before the parked-recovery retry existed, a worker that returned
// after the submit slot-wait meant the recovery was logged-and-dropped and a
// RUNNING job silently ceased to exist until the next failover - observed
// live as "recovery failed: submit_job: insufficient free slots (need 2,
// have 0)" while the job's checkpoints sat intact on disk. Here the worker
// deliberately returns only AFTER the recovery has parked, and the job must
// still finish exactly once.
TEST_F(HaFailoverTest, ARecoveredJobParkedForCapacityRunsWhenAWorkerReturns) {
    Cluster c(spec());
    ScopedDiagnostics diag(c);
    // Shrink the submit slot-wait so the parked path is REACHED inside the
    // test budget instead of absorbed by the 15s default.
    ASSERT_TRUE(c.start_ha_coordinators(2, {.extra_args = {"--submit-wait-for-slots-ms=1500"}}));
    ASSERT_TRUE(c.start_ha_worker(0));
    ASSERT_TRUE(c.await_workers_registered(1));

    auto sub = submit(c);
    ASSERT_NE(sub, nullptr);

    // Published work exists before anything dies, so a recovery that never
    // runs is observable as loss rather than as an empty no-op.
    ASSERT_TRUE(clink::itest::await(
        [&] { return verify_exactly_once(out_dir_, kTotalRecords).total_lines > 0; },
        std::chrono::seconds(45)))
        << "nothing was committed before the failover; the run would prove nothing";

    // The worker dies FIRST, so the new leader recovers into a cluster with
    // zero slots; then the leader.
    c.worker(0).kill_hard();
    ASSERT_TRUE(c.await_process_gone(0));
    ASSERT_TRUE(c.kill_leader_and_await_failover().has_value())
        << "no standby took over after the leader was killed";
    ASSERT_TRUE(c.await_coordinator_ready());

    // Vacuity pin: the recovery genuinely hit the capacity wall and parked.
    // Without this line the test can pass through the plain recovery path
    // (worker back within the wait) and prove nothing about parking.
    ASSERT_TRUE(
        clink::itest::await([&] { return c.count_in_coordinator_log("parked for capacity") > 0; },
                            std::chrono::seconds(30)))
        << "the recovery never parked; the scenario under test never happened ["
        << c.describe_coordinator_exits() << "]";

    // The external process supervisor's move, deliberately AFTER the park.
    ASSERT_TRUE(c.restart_worker_ha(0)) << "the worker did not come back";
    ASSERT_TRUE(c.await_workers_registered(2))
        << "the restarted worker never registered with the new leader";

    const bool finished = clink::itest::await(
        [&] { return verify_exactly_once(out_dir_, kTotalRecords).missing.empty(); },
        std::chrono::seconds(90));

    const auto v = verify_exactly_once(out_dir_, kTotalRecords);
    EXPECT_TRUE(finished) << "the parked recovery never ran once capacity registered: "
                          << describe(v) << " [" << c.describe_coordinator_exits()
                          << ", worker-0=" << (c.worker(0).running() ? "running" : "gone") << "]";
    EXPECT_TRUE(v.duplicated.empty()) << describe(v);
    EXPECT_TRUE(v.missing.empty()) << describe(v);
    EXPECT_TRUE(v.unexpected.empty()) << describe(v);

    sub->kill_and_reap();
}

}  // namespace
