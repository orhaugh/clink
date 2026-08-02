// Coordinator fencing: a coordinator that has lost leadership must not be
// able to act on the cluster.
//
// The failure this guards against is split brain. HA leadership is held by
// a lock in a coordination store, and losing that lock is something the old
// leader can only find out by asking. A leader that is partitioned from the
// store - or merely paused long enough for its lease to lapse - keeps its
// TCP connections to every worker, keeps its in-memory job state, and keeps
// its checkpoint timer running. It has no idea it has been replaced.
//
// Before fencing, everything it did in that state landed: it could deploy a
// second copy of a job the new leader had also deployed, cancel a job the
// new leader had just started, trigger checkpoints numbered from its own
// stale counter into the same checkpoint directory, and broadcast
// CommitCheckpoint to 2PC sinks, publishing transactions the new leader
// never agreed to. The epoch existed in the leader-endpoint file and was
// read by nothing.
//
// The rule these tests pin: a worker binds the epoch of the coordinator
// that admitted it, and refuses any later control frame carrying a lower
// one. Zero means unfenced, so a non-HA cluster and a mid-upgrade cluster
// both behave exactly as before.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/worker.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

// Wait for `pred` or give up after `bound`. The deadline is a FAILURE
// BOUND, not a synchronisation mechanism: the assertions are about what
// happened, and the loop only decides how long to keep asking.
template <typename Pred>
bool fencing_await(Pred pred, std::chrono::milliseconds bound = 2s) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

// A Connection the test writes the coordinator's side of. Worker exposes
// set_connect_factory precisely so the transport can be substituted, which
// is what makes it possible to test the fencing rule against the REAL
// decoder and the REAL dispatch switch while choosing the epoch on each
// individual frame - something no arrangement of live Coordinators can do,
// because a coordinator only ever stamps one epoch at a time.
class FencingScriptedConnection final : public network::Connection {
public:
    // Queue a frame for the worker's reader to pick up.
    void deliver(const std::vector<std::byte>& framed) {
        {
            std::lock_guard lock(mu_);
            inbound_.insert(inbound_.end(), framed.begin(), framed.end());
        }
        cv_.notify_all();
    }

    bool send_all(const std::byte* /*buf*/, std::size_t len) override {
        sent_bytes_.fetch_add(len, std::memory_order_relaxed);
        return open_.load(std::memory_order_acquire);
    }

    // Bytes the worker wrote back. Used to prove the worker really did
    // register rather than the test having merely constructed one.
    [[nodiscard]] std::size_t sent_bytes() const noexcept {
        return sent_bytes_.load(std::memory_order_acquire);
    }

    bool recv_all(std::byte* buf, std::size_t len) override {
        std::unique_lock lock(mu_);
        cv_.wait(lock, [&] { return inbound_.size() >= len || !open_.load(); });
        if (inbound_.size() < len) {
            return false;  // closed
        }
        for (std::size_t i = 0; i < len; ++i) {
            buf[i] = inbound_.front();
            inbound_.pop_front();
        }
        return true;
    }

    void shutdown_write() override {}
    void shutdown_read() override { close(); }

    void close() override {
        open_.store(false, std::memory_order_release);
        cv_.notify_all();
    }

    bool is_open() const noexcept override { return open_.load(std::memory_order_acquire); }

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<std::byte> inbound_;
    std::atomic<bool> open_{true};
    std::atomic<std::size_t> sent_bytes_{0};
};

// A worker wired to a scripted coordinator, already registered at
// `register_epoch`.
struct ScriptedWorker {
    explicit ScriptedWorker(std::uint64_t register_epoch) {
        Worker::Config cfg;
        cfg.heartbeat_interval = 0ms;  // no background chatter to reason about
        worker = std::make_unique<Worker>("w", "127.0.0.1", cfg);
        worker->register_role("noop", [this](const DeploymentTask&) {
            deploys.fetch_add(1, std::memory_order_relaxed);
        });
        worker->set_connect_factory([this](const std::string&, std::uint16_t) {
            auto c = std::make_unique<FencingScriptedConnection>();
            conn = c.get();
            return c;
        });
        // connect_to_coordinator does a blocking read for the ack, so the
        // ack has to already be sitting in the queue when it runs. Building
        // the connection eagerly here would fight the factory, so seed it
        // from another thread the moment the factory hands the pointer over.
        std::thread seeder([this, register_epoch] {
            while (conn == nullptr) {
                std::this_thread::sleep_for(100us);
            }
            conn->deliver(encode_frame(
                MessageKind::RegisterAck,
                RegisterAckMsg{.ok = true, .message = "", .coordinator_epoch = register_epoch}));
        });
        worker->connect_to_coordinator("127.0.0.1", 1);
        seeder.join();
    }

    ~ScriptedWorker() {
        if (conn != nullptr) {
            conn->close();
        }
        worker->stop();
    }

    ScriptedWorker(const ScriptedWorker&) = delete;
    ScriptedWorker& operator=(const ScriptedWorker&) = delete;
    ScriptedWorker(ScriptedWorker&&) = delete;
    ScriptedWorker& operator=(ScriptedWorker&&) = delete;

    std::unique_ptr<Worker> worker;
    FencingScriptedConnection* conn{nullptr};
    std::atomic<int> deploys{0};
};

// One frame of every coordinator -> worker kind that changes what a worker
// is doing, stamped at `e`. Kept in one place so a new fenced message kind
// added to the protocol without a test here is a visible omission.
std::vector<std::pair<const char*, std::vector<std::byte>>> all_control_frames(std::uint64_t e) {
    std::vector<std::pair<const char*, std::vector<std::byte>>> out;

    DeployMsg d;
    d.job_id = 1;
    d.tasks.push_back(DeploymentTask{.role = "noop", .subtask_idx = 0});
    d.coordinator_epoch = e;
    out.emplace_back("Deploy", encode_frame(MessageKind::Deploy, d));

    PeerUpdateMsg pu;
    pu.job_id = 1;
    pu.coordinator_epoch = e;
    out.emplace_back("PeerUpdate", encode_frame(MessageKind::PeerUpdate, pu));

    CancelJobMsg cj;
    cj.job_id = 1;
    cj.coordinator_epoch = e;
    out.emplace_back("CancelJob", encode_frame(MessageKind::CancelJob, cj));

    out.emplace_back("TriggerCheckpoint",
                     encode_frame(MessageKind::TriggerCheckpoint,
                                  TriggerCheckpointMsg{
                                      .job_id = 1, .checkpoint_id = 5, .coordinator_epoch = e}));
    out.emplace_back(
        "CommitCheckpoint",
        encode_frame(MessageKind::CommitCheckpoint,
                     CommitCheckpointMsg{.job_id = 1, .checkpoint_id = 5, .coordinator_epoch = e}));
    out.emplace_back(
        "AbortCheckpoint",
        encode_frame(MessageKind::AbortCheckpoint,
                     AbortCheckpointMsg{.job_id = 1, .checkpoint_id = 5, .coordinator_epoch = e}));

    FinalCheckpointAssignedMsg fca;
    fca.job_id = 1;
    fca.role = "noop";
    fca.subtask_idx = 0;
    fca.final_checkpoint_id = 5;
    fca.coordinator_epoch = e;
    out.emplace_back("FinalCheckpointAssigned",
                     encode_frame(MessageKind::FinalCheckpointAssigned, fca));

    BeginRescaleMsg br;
    br.job_id = 1;
    br.op_id = "agg";
    br.target_parallelism = 2;
    br.cutover_checkpoint = 5;
    br.coordinator_epoch = e;
    out.emplace_back("BeginRescale", encode_frame(MessageKind::BeginRescale, br));

    return out;
}

}  // namespace

TEST(CoordinatorFencing, AWorkerBindsTheEpochOfTheCoordinatorThatAdmittedIt) {
    ScriptedWorker sw(7);
    EXPECT_GT(sw.conn->sent_bytes(), 0U) << "the worker never sent Register, so nothing was tested";
    EXPECT_EQ(sw.worker->bound_epoch(), 7U);
    EXPECT_EQ(sw.worker->fenced_frame_count(), 0U);
}

TEST(CoordinatorFencing, EveryControlFrameFromASupersededCoordinatorIsRefused) {
    // Deploy is the loudest, but not the most dangerous. CommitCheckpoint
    // publishes 2PC sink transactions externally; TriggerCheckpoint numbers
    // checkpoints from a stale counter into a shared directory; BeginRescale
    // redistributes state. All of them must be refused, so every kind is
    // driven individually rather than trusting that one representative
    // proves the rest.
    ScriptedWorker sw(50);
    const auto frames = all_control_frames(/*e=*/7);  // stale: 7 < 50
    ASSERT_EQ(frames.size(), 8U) << "a fenced message kind is missing from this table";

    std::uint64_t expected = 0;
    for (const auto& [name, framed] : frames) {
        sw.conn->deliver(framed);
        ++expected;
        EXPECT_TRUE(fencing_await([&] { return sw.worker->fenced_frame_count() >= expected; }))
            << name << " from a superseded coordinator was NOT refused";
    }
    EXPECT_EQ(sw.worker->fenced_frame_count(), 8U);
    EXPECT_EQ(sw.deploys.load(), 0) << "a superseded coordinator deployed a task";
    EXPECT_FALSE(sw.worker->was_cancelled()) << "a superseded coordinator cancelled a job";
    EXPECT_EQ(sw.worker->bound_epoch(), 50U) << "a refused frame must not lower the bound epoch";
}

TEST(CoordinatorFencing, TheSameFramesAtTheBoundEpochAreAllAccepted) {
    // The other side of the previous test, and the one that keeps it
    // honest: if the frames were being dropped for some reason other than
    // fencing - a decode failure, say - this would fail too.
    ScriptedWorker sw(7);
    for (const auto& [name, framed] : all_control_frames(/*e=*/7)) {
        sw.conn->deliver(framed);
        (void)name;
    }
    EXPECT_TRUE(fencing_await([&] { return sw.worker->was_cancelled(); }))
        << "the CancelJob at the bound epoch never took effect, so the frames were not being "
           "processed at all";
    EXPECT_EQ(sw.worker->fenced_frame_count(), 0U);
    EXPECT_EQ(sw.worker->bound_epoch(), 7U);
}

TEST(CoordinatorFencing, AHigherEpochRebindsTheWorkerAndIsAccepted) {
    // A failover that keeps the connection up shows up here as a frame
    // carrying an epoch ABOVE the bound one. Refusing it would fence the
    // worker off from the legitimate new leader, which is the same outage
    // as split brain and harder to diagnose.
    ScriptedWorker sw(4);
    CancelJobMsg cj;
    cj.job_id = 1;
    cj.coordinator_epoch = 11;
    sw.conn->deliver(encode_frame(MessageKind::CancelJob, cj));

    EXPECT_TRUE(fencing_await([&] { return sw.worker->was_cancelled(); }))
        << "a frame from a LATER epoch must be accepted";
    EXPECT_EQ(sw.worker->bound_epoch(), 11U) << "the worker must follow the new leader forward";
    EXPECT_EQ(sw.worker->fenced_frame_count(), 0U);

    // And having moved forward, it now fences the epoch it used to accept.
    CancelJobMsg stale;
    stale.job_id = 2;
    stale.coordinator_epoch = 4;
    sw.conn->deliver(encode_frame(MessageKind::CancelJob, stale));
    EXPECT_TRUE(fencing_await([&] { return sw.worker->fenced_frame_count() == 1U; }))
        << "epoch 4 was still accepted after the worker rebound to 11";
}

TEST(CoordinatorFencing, AnUnfencedCoordinatorIsNotFencedOff) {
    // The non-HA case, and the mid-upgrade case: both ends at epoch 0.
    // This is the majority of deployments and must be untouched.
    ScriptedWorker sw(0);
    EXPECT_EQ(sw.worker->bound_epoch(), 0U);
    for (const auto& [name, framed] : all_control_frames(/*e=*/0)) {
        sw.conn->deliver(framed);
        (void)name;
    }
    EXPECT_TRUE(fencing_await([&] { return sw.worker->was_cancelled(); }));
    EXPECT_EQ(sw.worker->fenced_frame_count(), 0U)
        << "an unfenced coordinator had its frames refused";
}

TEST(CoordinatorFencing, AWorkerBoundToARealEpochStillAcceptsAPreFencingCoordinator) {
    // The upgrade ordering that would otherwise bite: workers restart onto
    // the new build first and register under an HA leader at epoch N, then
    // the leader is rolled back or a pre-fencing standby takes over and
    // sends frames with no epoch field at all (which decode as 0).
    //
    // Those frames ARE refused, by design - 0 < N - and that is the correct
    // reading of "a coordinator that cannot prove it is current". What must
    // not happen is silence: the drop is counted and logged, so the
    // condition is visible rather than presenting as a hung cluster.
    ScriptedWorker sw(6);
    CancelJobMsg cj;
    cj.job_id = 1;
    cj.coordinator_epoch = 6;
    auto framed = encode_frame(MessageKind::CancelJob, cj);
    framed.resize(framed.size() - 8);  // what a pre-fencing peer puts on the wire
    // Repair the length prefix so the frame is well-formed but epoch-less.
    const auto body_len = static_cast<std::uint32_t>(framed.size() - 4);
    framed[0] = static_cast<std::byte>((body_len >> 24) & 0xFF);
    framed[1] = static_cast<std::byte>((body_len >> 16) & 0xFF);
    framed[2] = static_cast<std::byte>((body_len >> 8) & 0xFF);
    framed[3] = static_cast<std::byte>(body_len & 0xFF);
    sw.conn->deliver(framed);

    EXPECT_TRUE(fencing_await([&] { return sw.worker->fenced_frame_count() == 1U; }))
        << "an epoch-less frame reaching a worker bound to epoch 6 must be counted, not ignored";
    EXPECT_FALSE(sw.worker->was_cancelled());
}

// ----- the coordinator's half: it must stamp what it sends ---------------

TEST(CoordinatorFencing, TheCoordinatorStampsItsEpochOnWhatItSends) {
    // End to end over a real socket: the worker's bound epoch can only have
    // come from the RegisterAck the coordinator encoded.
    Coordinator coordinator;
    coordinator.set_epoch(23);
    EXPECT_EQ(coordinator.epoch(), 23U);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    Worker worker("w", "127.0.0.1");
    worker.register_role("noop", [](const DeploymentTask&) {});
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    EXPECT_EQ(worker.bound_epoch(), 23U);

    worker.stop();
    coordinator.stop();
}

TEST(CoordinatorFencing, AnEpochedCoordinatorDoesNotFenceOffItsOwnDeploy) {
    // The bug this exists for. Four separate paths build a DeployMsg -
    // submit, two rescale paths, and restart-after-failure - and the first
    // version of this change stamped only one of them. An UNSTAMPED frame
    // carries epoch 0, which a worker bound to a real epoch refuses, so the
    // omission presents as a deploy that silently never happens.
    //
    // The frames from a fenced coordinator to a worker it registered itself
    // must therefore never be refused. Any send path that forgets the epoch
    // shows up here as a non-zero fenced count.
    Coordinator coordinator;
    coordinator.set_epoch(5);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    std::atomic<int> deployed{0};
    Worker worker("w", "127.0.0.1");
    worker.register_role("noop", [&deployed](const DeploymentTask&) { ++deployed; });
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "",
        .role = "noop",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(2s)) << "the deploy never completed";
    EXPECT_EQ(deployed.load(), 1);
    EXPECT_EQ(worker.fenced_frame_count(), 0U)
        << "a coordinator fenced off its own worker: some send path is not stamping the epoch";

    worker.stop();
    coordinator.stop();
}

TEST(CoordinatorFencing, AnEpochedCoordinatorCanStillRestartAFailedTask) {
    // Same property on the restart-after-failure path, which builds its own
    // DeployMsg. Mirrors Cluster.RestartsFailingTaskUpToMaxRestarts, with a
    // non-zero epoch: if the redeploy frame were unstamped, the retry would
    // be refused and the job would never recover.
    Coordinator::Config cfg;
    cfg.max_restarts = 3;
    Coordinator coordinator(cfg);
    coordinator.set_epoch(5);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});

    std::atomic<int> attempts{0};
    Worker worker("w", "127.0.0.1");
    worker.register_role("flaky", [&attempts](const DeploymentTask&) {
        if (attempts.fetch_add(1) == 0) {
            throw std::runtime_error("first attempt fails");
        }
    });
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    JobPlan plan;
    plan.tasks.push_back(PlannedTask{
        .worker_id = "",
        .role = "flaky",
        .subtask_idx = 0,
        .data_port = 0,
        .peer_refs = {},
        .extra_config = "",
    });
    coordinator.deploy(plan);

    ASSERT_TRUE(coordinator.await_completion(5s)) << "the restart never completed";
    EXPECT_GE(attempts.load(), 2) << "the task was never redeployed";
    EXPECT_EQ(worker.fenced_frame_count(), 0U)
        << "the restart path's Deploy was not stamped with the coordinator's epoch";

    worker.stop();
    coordinator.stop();
}

TEST(CoordinatorFencing, ANonHaCoordinatorStaysAtEpochZero) {
    // Nothing sets an epoch unless leadership election does, so a plain
    // single-coordinator cluster never fences anything.
    Coordinator coordinator;
    EXPECT_EQ(coordinator.epoch(), 0U);
}

// ----- metadata fencing -------------------------------------------------

TEST(CoordinatorFencing, MetadataWrittenByALaterEpochIsNotOverwritten) {
    EXPECT_TRUE(metadata_write_allowed(9, 0)) << "an unstamped record predates fencing";
    EXPECT_TRUE(metadata_write_allowed(9, 9)) << "a coordinator may overwrite its own record";
    EXPECT_TRUE(metadata_write_allowed(9, 3))
        << "a record from an earlier epoch is ours to replace";
    EXPECT_FALSE(metadata_write_allowed(3, 9)) << "epoch 3 must not overwrite epoch 9's record";
}

TEST(CoordinatorFencing, TheStoredEpochIsReadBackFromTheManifest) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink-fencing-" + std::to_string(::getpid()) + "-" +
                      ::testing::UnitTest::GetInstance()->current_test_info()->name());
    std::filesystem::create_directories(dir);

    const auto path = (dir / "manifest.json").string();
    {
        std::ofstream out(path);
        out << R"({"graph_json":"{}","plugins":[],"coordinator_epoch":42})";
    }
    EXPECT_EQ(metadata_stored_epoch(path), 42U);

    // A record written before fencing has no field at all, and must read as
    // 0 so an upgraded coordinator can still take ownership of it. Reading
    // it as anything else would strand every job persisted by the old build.
    const auto legacy = (dir / "legacy.json").string();
    {
        std::ofstream out(legacy);
        out << R"({"graph_json":"{}","plugins":[]})";
    }
    EXPECT_EQ(metadata_stored_epoch(legacy), 0U);

    // Absent entirely - the first write of a new job.
    EXPECT_EQ(metadata_stored_epoch((dir / "missing.json").string()), 0U);

    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
