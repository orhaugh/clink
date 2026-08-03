// What "checkpoint complete" is allowed to mean.
//
// The COMPLETED-<id> marker is the definition of a checkpoint having
// reached global completion, and it is what recovery restores from. It was
// written whenever every subtask had ANSWERED - not whenever every subtask
// had SUCCEEDED.
//
// Those differ. A subtask whose snapshot throws catches the exception,
// reports `ok=false`, and carries on running; nothing fails the job. The
// coordinator erased its key from the pending set exactly as if it had
// succeeded, so the set emptied, the marker was written, the recovery
// point advanced, and CommitCheckpoint went out - for a checkpoint in
// which one operator's state was never written at all. A later restore
// would restore that operator from nowhere.
//
// `msg.ok` was consulted in precisely two places in the whole ack handler:
// aborting a commit_group, and incrementing a metric. Neither is on the
// completion path, so for the default case - no commit groups - a failed
// snapshot was indistinguishable from a successful one.
//
// These tests drive the real coordinator over a real socket, playing the
// worker themselves, because the failure is in what the coordinator
// concludes from a specific sequence of acks and nothing above the wire
// can produce that sequence on demand.

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/frame_io.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/runtime/network/connection.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

template <typename Pred>
bool ckpt_await(Pred pred, std::chrono::milliseconds bound = 3s) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

std::optional<std::vector<std::byte>> recv_frame(network::Connection& c) {
    std::array<std::byte, 4> hdr{};
    if (!c.recv_all(hdr.data(), hdr.size())) {
        return std::nullopt;
    }
    std::uint32_t len = 0;
    for (const auto b : hdr) {
        len = (len << 8) | static_cast<unsigned char>(b);
    }
    std::vector<std::byte> body(len);
    if (len > 0 && !c.recv_all(body.data(), body.size())) {
        return std::nullopt;
    }
    return body;
}

// A worker played by the test: it registers, accepts whatever the
// coordinator deploys, and acks checkpoints with whatever verdict the test
// chooses. Everything below the ack is real - real frames, real socket,
// real coordinator.
class FakeWorker {
public:
    FakeWorker(std::uint16_t port, std::string id) : id_(std::move(id)) {
        conn_ = network::connect_plain("127.0.0.1", port);
    }

    [[nodiscard]] bool valid() const { return conn_ != nullptr; }

    [[nodiscard]] bool register_and_ack() {
        RegisterMsg reg{.worker_id = id_, .data_host = "127.0.0.1", .slot_count = 4};
        if (!send_frame(*conn_, encode_frame(MessageKind::Register, reg))) {
            return false;
        }
        auto reply = recv_frame(*conn_);
        if (!reply.has_value()) {
            return false;
        }
        MessageReader r(std::move(*reply));
        if (static_cast<MessageKind>(r.read_u8()) != MessageKind::RegisterAck) {
            return false;
        }
        if (!decode_register_ack(r).ok) {
            return false;
        }
        // Only now: the handshake read above is synchronous and would
        // race the pump for the same bytes.
        start_reader();
        start_heartbeat();
        return true;
    }

    // Pump inbound frames on their own thread.
    //
    // Reading inline cannot be deadline-bounded: recv_all blocks until
    // bytes arrive, so a loop that checks a deadline between reads never
    // checks it. A real worker has a reader thread for the same reason,
    // so this is also the more faithful shape.
    // A real worker heartbeats, and a worker that does not is correctly
    // declared lost - which kills the job before any checkpoint can be
    // acked. Sending them keeps the watchdog enabled rather than turning
    // off a safety mechanism to make a test pass.
    void start_heartbeat() {
        heartbeat_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                {
                    std::lock_guard lock(send_mu_);
                    if (!send_frame(*conn_,
                                    encode_frame(MessageKind::Heartbeat, HeartbeatMsg{id_}))) {
                        return;
                    }
                }
                for (int i = 0; i < 20 && !stop_.load(std::memory_order_acquire); ++i) {
                    std::this_thread::sleep_for(10ms);
                }
            }
        });
    }

    void start_reader() {
        reader_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                auto frame = recv_frame(*conn_);
                if (!frame.has_value()) {
                    return;  // closed
                }
                std::lock_guard lock(mu_);
                inbox_.push_back(std::move(*frame));
            }
        });
    }

    // Take the first queued frame of `kind`, waiting up to `bound`.
    // Frames of other kinds are left in place: a later await for a
    // different kind must still find them.
    [[nodiscard]] std::optional<MessageReader> await_frame(MessageKind kind,
                                                           std::chrono::milliseconds bound = 5s) {
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (std::chrono::steady_clock::now() < deadline) {
            {
                std::lock_guard lock(mu_);
                for (auto it = inbox_.begin(); it != inbox_.end(); ++it) {
                    if (it->empty()) {
                        continue;
                    }
                    if (static_cast<MessageKind>((*it)[0]) == kind) {
                        MessageReader r(std::move(*it));
                        inbox_.erase(it);
                        (void)r.read_u8();  // consume the kind byte
                        return r;
                    }
                }
            }
            std::this_thread::sleep_for(1ms);
        }
        return std::nullopt;
    }

    // Report a listening port for a deployed subtask.
    //
    // Periodic checkpointing does not begin until every generic subtask
    // has reported, because before that the chain is not up and a barrier
    // would arrive before any source injector exists. A fake worker that
    // skips this gets a job that never checkpoints - which is correct
    // coordinator behaviour and a useless test.
    [[nodiscard]] bool report_listening(JobId job_id,
                                        const std::string& role,
                                        std::uint32_t subtask,
                                        std::uint16_t port) {
        SubtaskListeningMsg m;
        m.job_id = job_id;
        m.worker_id = id_;
        m.role = role;
        m.subtask_idx = subtask;
        m.host = "127.0.0.1";
        m.edge_ports.push_back(SubtaskListeningMsg::EdgePort{
            .upstream_role = role, .upstream_subtask_idx = 0, .port = port});
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::SubtaskListening, m));
    }

    [[nodiscard]] bool ack_checkpoint(JobId job_id,
                                      std::uint64_t ckpt_id,
                                      const std::string& role,
                                      std::uint32_t subtask,
                                      bool ok) {
        SubtaskCheckpointedMsg m;
        m.job_id = job_id;
        m.checkpoint_id = ckpt_id;
        m.role = role;
        m.subtask_idx = subtask;
        m.ok = ok;
        m.error = ok ? "" : "injected snapshot failure";
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::SubtaskCheckpointed, m));
    }

    void close() {
        stop_.store(true, std::memory_order_release);
        if (conn_) {
            conn_->shutdown_read();
            conn_->close();
        }
        if (reader_.joinable()) {
            reader_.join();
        }
        if (heartbeat_.joinable()) {
            heartbeat_.join();
        }
    }

    ~FakeWorker() { close(); }
    FakeWorker(const FakeWorker&) = delete;
    FakeWorker& operator=(const FakeWorker&) = delete;
    FakeWorker(FakeWorker&&) = delete;
    FakeWorker& operator=(FakeWorker&&) = delete;

private:
    std::string id_;
    std::unique_ptr<network::Connection> conn_;
    std::thread reader_;
    std::thread heartbeat_;
    // Acks and heartbeats are written from different threads.
    std::mutex send_mu_;
    std::atomic<bool> stop_{false};
    std::mutex mu_;
    std::vector<std::vector<std::byte>> inbox_;
};

// A graph the built-in registry can plan: a bounded source into a file
// sink. Its content does not matter - the fake worker never runs it - but
// it has to be plannable or there is no job to checkpoint.
JobGraphSpec two_subtask_graph(const std::filesystem::path& out) {
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1000000"}};  // long enough not to finish under us
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out.string()}};
    g.ops.push_back(snk);
    return g;
}

// A cluster with one fake worker and one submitted job, ready to be told
// its checkpoints have or have not succeeded.
struct CheckpointFixture {
    CheckpointFixture()
        : dir(std::filesystem::temp_directory_path() /
              ("clink_ckpt_completion_" + std::to_string(::getpid()) + "_" +
               ::testing::UnitTest::GetInstance()->current_test_info()->name())) {
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        coordinator = std::make_unique<Coordinator>();
        port = coordinator->start();
        coordinator->expect_workers({"w"});
    }

    // Register the fake worker and submit a job whose checkpoints the
    // test will answer for. Returns the job id, or 0 on failure.
    JobId bring_up() {
        worker = std::make_unique<FakeWorker>(port, "w");
        if (!worker->valid() || !worker->register_and_ack()) {
            return 0;
        }
        if (!coordinator->await_registrations(2s)) {
            return 0;
        }
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = dir.string();
        // Periodic triggers drive the test: a real checkpoint id, issued
        // by the real trigger loop, rather than one the test invented.
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = 0;
        const auto job_id = coordinator->submit_job(
            two_subtask_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
        if (job_id == 0) {
            return 0;
        }
        // Learn the task set from the Deploy the coordinator sent, rather
        // than assuming it: the planner decides roles and subtask
        // indices, and acking keys it is not waiting on would leave the
        // pending set non-empty and every assertion below vacuous.
        auto deploy = worker->await_frame(MessageKind::Deploy);
        if (!deploy.has_value()) {
            return 0;
        }
        std::uint16_t fake_port = 40000;
        for (const auto& t : decode_deploy(*deploy).tasks) {
            deployed_.emplace_back(t.role, t.subtask_idx);
            // The port is never connected to - no data flows in this test
            // - but it has to be reported for the coordinator to consider
            // the chain up and start checkpointing.
            if (!worker->report_listening(job_id, t.role, t.subtask_idx, fake_port++)) {
                return 0;
            }
        }
        return deployed_.empty() ? 0 : job_id;
    }

    // Ack every deployed subtask for `ckpt_id` with the same verdict.
    // The task list comes from the Deploy frame the coordinator actually
    // sent, so the keys match what it is waiting on.
    bool ack_all(JobId job_id, std::uint64_t ckpt_id, bool ok) {
        if (deployed_.empty()) {
            return false;
        }
        for (const auto& [role, subtask] : deployed_) {
            if (!worker->ack_checkpoint(job_id, ckpt_id, role, subtask, ok)) {
                return false;
            }
        }
        return true;
    }

    // Wait for a TriggerCheckpoint and return the id it carries.
    std::optional<std::uint64_t> await_trigger() {
        auto r = worker->await_frame(MessageKind::TriggerCheckpoint);
        if (!r.has_value()) {
            return std::nullopt;
        }
        return decode_trigger_checkpoint(*r).checkpoint_id;
    }

    ~CheckpointFixture() {
        if (worker) {
            worker->close();
        }
        if (coordinator) {
            coordinator->stop();
        }
        if (std::getenv("CLINK_KEEP_CKPT_DIR") == nullptr) {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    }

    CheckpointFixture(const CheckpointFixture&) = delete;
    CheckpointFixture& operator=(const CheckpointFixture&) = delete;

    // The marker the coordinator actually writes: FLAT under the
    // configured checkpoint dir, not under a per-job subdirectory.
    //
    // Worth stating because the engine's own recovery lookup
    // (latest_completed_id_on_disk) reads <dir>/<job_id>/COMPLETED-N
    // instead. Verified against the filesystem rather than the comments,
    // which disagree with each other. See the plan doc, F29.
    [[nodiscard]] bool marker_exists(JobId /*job_id*/, std::uint64_t ckpt_id) const {
        std::error_code ec;
        return std::filesystem::exists(dir / ("COMPLETED-" + std::to_string(ckpt_id)), ec);
    }

    std::filesystem::path dir;
    std::unique_ptr<Coordinator> coordinator;
    std::uint16_t port{};
    std::unique_ptr<FakeWorker> worker;

private:
    std::vector<std::pair<std::string, std::uint32_t>> deployed_;
};

}  // namespace

// The fixture is only worth having if the plumbing works, so this
// establishes it: a fake worker registers and a job really is submitted
// and really does start triggering checkpoints.
TEST(CheckpointCompletion, TheFixtureProducesRealCheckpointTriggers) {
    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    const auto ckpt_id = fx.await_trigger();
    ASSERT_TRUE(ckpt_id.has_value())
        << "no TriggerCheckpoint arrived; the tests below would assert nothing";
    EXPECT_GT(*ckpt_id, 0U);
}

TEST(CheckpointCompletion, AFailedSubtaskAckDoesNotCompleteTheCheckpoint) {
    // The defect. Every subtask answers, so the pending set empties - and
    // emptiness used to be the entire completion condition, regardless of
    // what the answers said.
    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    const auto ckpt_id = fx.await_trigger();
    ASSERT_TRUE(ckpt_id.has_value());

    ASSERT_TRUE(fx.ack_all(job_id, *ckpt_id, /*ok=*/false));

    // Asserting a NEGATIVE needs a window in which the bad thing would
    // have happened. Without one this passes on a coordinator that has
    // simply not processed the acks yet.
    EXPECT_FALSE(ckpt_await(
        [&] { return fx.coordinator->latest_completed_checkpoint(job_id) >= *ckpt_id; }, 750ms))
        << "checkpoint " << *ckpt_id
        << " became the job's recovery point even though a subtask reported it could not "
           "snapshot; a restore would restore that operator from a checkpoint it never wrote";
    EXPECT_FALSE(fx.marker_exists(job_id, *ckpt_id))
        << "a COMPLETED marker was written for a checkpoint a subtask failed to take";
}

TEST(CheckpointCompletion, AnAllSuccessCheckpointStillCompletes) {
    // The control. Written as "never complete", the test above would pass
    // and checkpointing would be dead.
    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    const auto ckpt_id = fx.await_trigger();
    ASSERT_TRUE(ckpt_id.has_value());

    ASSERT_TRUE(fx.ack_all(job_id, *ckpt_id, /*ok=*/true));

    EXPECT_TRUE(ckpt_await([&] {
        return fx.coordinator->latest_completed_checkpoint(job_id) >= *ckpt_id;
    })) << "a checkpoint every subtask acked successfully did not complete";
    EXPECT_TRUE(ckpt_await([&] { return fx.marker_exists(job_id, *ckpt_id); }))
        << "no COMPLETED marker for a fully successful checkpoint";
}

TEST(CheckpointCompletion, TheRecoveryPointSurvivesALaterFailedCheckpoint) {
    // The consequence that matters. One checkpoint succeeds and becomes
    // the recovery point; the next fails. The recovery point must stay
    // where it was - neither advancing to a checkpoint that does not
    // exist in full, nor being lost.
    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);

    const auto good = fx.await_trigger();
    ASSERT_TRUE(good.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *good, /*ok=*/true));
    ASSERT_TRUE(ckpt_await([&] {
        return fx.coordinator->latest_completed_checkpoint(job_id) == *good;
    })) << "the first checkpoint never completed, so this cannot show what a later failure does "
           "to a recovery point";

    const auto bad = fx.await_trigger();
    ASSERT_TRUE(bad.has_value());
    ASSERT_GT(*bad, *good);
    ASSERT_TRUE(fx.ack_all(job_id, *bad, /*ok=*/false));

    EXPECT_FALSE(ckpt_await(
        [&] { return fx.coordinator->latest_completed_checkpoint(job_id) != *good; }, 750ms))
        << "a failed checkpoint moved the recovery point off the last good one (now "
        << fx.coordinator->latest_completed_checkpoint(job_id) << ", was " << *good << ")";
    EXPECT_FALSE(fx.marker_exists(job_id, *bad));
    EXPECT_TRUE(fx.marker_exists(job_id, *good))
        << "the failed checkpoint took the good one's marker with it";
}
