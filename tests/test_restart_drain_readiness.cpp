// Restart-drain readiness is COVERAGE, not emptiness.
//
// Draining never empties restart_drain_expected - each ack lands in
// restart_drained_keys and readiness means the drained set COVERS the
// expected set. A second worker lost during the drain is FOLDED: its keys
// are erased from the expected set. Those two facts compose into a wedge
// when the interleave is
//
//   1. a subtask on the survivor errors -> whole-job restart, expected =
//      {survivor's other subtask, dead worker's subtask}
//   2. the survivor's other subtask drains FIRST (acks land in drained)
//   3. the dead worker re-registers -> fold erases ITS key from expected
//
// leaving expected = {survivor's key}, already covered by drained. No
// further SubtaskFinished will ever arrive to re-evaluate readiness, and a
// kick that fires on expected.empty() never fires - the job sits ready
// until the drain deadline fails it with "survivors did not drain", naming
// a key that drained twenty-five seconds earlier. That is the soak's
// three-CI-sighting wedge (watch item 63), reproduced here as a scripted
// interleave against the real coordinator over real sockets, because only
// frame-level control can order the drain ack before the fold on demand.
//
// The fake worker mirrors test_checkpoint_completion.cpp's (uniquely named:
// same-named anon-namespace classes across test TUs ODR-collide under g++).

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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

std::optional<std::vector<std::byte>> drain_recv_frame(network::Connection& c) {
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

// A worker played by the test, driving the coordinator with the exact frame
// order the wedge needs. Real frames, real socket, real coordinator.
class DrainFakeWorker {
public:
    DrainFakeWorker(std::uint16_t port, std::string id) : id_(std::move(id)) {
        conn_ = network::connect_plain("127.0.0.1", port);
    }

    [[nodiscard]] bool valid() const { return conn_ != nullptr; }

    [[nodiscard]] bool register_and_ack() {
        RegisterMsg reg{.worker_id = id_, .data_host = "127.0.0.1", .slot_count = 4};
        if (!send_frame(*conn_, encode_frame(MessageKind::Register, reg))) {
            return false;
        }
        auto reply = drain_recv_frame(*conn_);
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
        start_reader_();
        start_heartbeat_();
        return true;
    }

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
                        (void)r.read_u8();
                        return r;
                    }
                }
            }
            std::this_thread::sleep_for(1ms);
        }
        return std::nullopt;
    }

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

    // The frames the wedge is made of: an errored subtask (enters the
    // whole-job restart) and a clean one (the drain ack).
    [[nodiscard]] bool send_finished(JobId job_id,
                                     const std::string& role,
                                     std::uint32_t subtask,
                                     bool had_error) {
        SubtaskFinishedMsg m;
        m.job_id = job_id;
        m.worker_id = id_;
        m.role = role;
        m.subtask_idx = subtask;
        m.had_error = had_error;
        m.error_message = had_error ? "injected bridge failure (peer gone)" : "";
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::SubtaskFinished, m));
    }

    // A SIGKILLed process says no goodbye: drop the socket, stop the pumps.
    void die_silently() { close(); }

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

    ~DrainFakeWorker() { close(); }
    DrainFakeWorker(const DrainFakeWorker&) = delete;
    DrainFakeWorker& operator=(const DrainFakeWorker&) = delete;
    DrainFakeWorker(DrainFakeWorker&&) = delete;
    DrainFakeWorker& operator=(DrainFakeWorker&&) = delete;

private:
    void start_reader_() {
        reader_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                auto frame = drain_recv_frame(*conn_);
                if (!frame.has_value()) {
                    return;
                }
                std::lock_guard lock(mu_);
                inbox_.push_back(std::move(*frame));
            }
        });
    }

    void start_heartbeat_() {
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

    std::string id_;
    std::unique_ptr<network::Connection> conn_;
    std::thread reader_;
    std::thread heartbeat_;
    std::mutex send_mu_;
    std::atomic<bool> stop_{false};
    std::mutex mu_;
    std::vector<std::vector<std::byte>> inbox_;
};

// Three single-parallelism ops so the planner emits three tasks across two
// workers: one worker necessarily holds two. The content never runs - the
// fake workers answer for it.
JobGraphSpec three_task_graph(const std::filesystem::path& out) {
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1000000"}};
    g.ops.push_back(src);
    OperatorSpec mid;
    mid.type = "identity_int64";
    mid.id = "mid";
    mid.inputs = {"src"};
    mid.parallelism = 1;
    mid.out_channel = std::string{kChannelInt64};
    g.ops.push_back(mid);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"mid"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out.string()}};
    g.ops.push_back(snk);
    return g;
}

TEST(RestartDrainReadiness, AFoldAfterTheLastDrainAckStillFiresTheRestart) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_drain_ready_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"dw-a", "dw-b"});

    auto wa = std::make_unique<DrainFakeWorker>(port, "dw-a");
    auto wb = std::make_unique<DrainFakeWorker>(port, "dw-b");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();  // non-empty -> subtask errors take the whole-job path
    ckpt.max_restarts_on_worker_loss = 3;
    const auto job_id = coordinator.submit_job(
        three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    // Learn each worker's task set from its own Deploy frame; the planner
    // decides placement and three tasks over two workers puts two on one
    // of them. Frames the wedge needs are then addressed correctly no
    // matter which way the placement fell.
    struct Deployed {
        DrainFakeWorker* worker{nullptr};
        std::vector<std::pair<std::string, std::uint32_t>> tasks;
    };
    Deployed a{wa.get(), {}};
    Deployed b{wb.get(), {}};
    std::uint16_t fake_port = 41000;
    for (auto* d : {&a, &b}) {
        auto deploy = d->worker->await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value()) << "a worker never received its Deploy";
        for (const auto& t : decode_deploy(*deploy).tasks) {
            d->tasks.emplace_back(t.role, t.subtask_idx);
            ASSERT_TRUE(d->worker->report_listening(job_id, t.role, t.subtask_idx, fake_port++));
        }
    }
    Deployed& survivor = a.tasks.size() == 2 ? a : b;
    Deployed& victim = a.tasks.size() == 2 ? b : a;
    ASSERT_EQ(survivor.tasks.size(), 2U) << "expected a 2/1 split across the two workers";
    ASSERT_EQ(victim.tasks.size(), 1U);

    // The interleave, in the exact order the soak produced it:
    // (1) the victim dies silently (SIGKILL says no goodbye) ...
    victim.worker->die_silently();
    // (2) ... a survivor subtask errors (its bridge lost its peer) ->
    //     whole-job restart; expected = {survivor's other, victim's}.
    ASSERT_TRUE(survivor.worker->send_finished(
        job_id, survivor.tasks[0].first, survivor.tasks[0].second, /*had_error=*/true));
    // The restart drain is observable as the CancelJob broadcast.
    ASSERT_TRUE(survivor.worker->await_frame(MessageKind::CancelJob, 10s).has_value())
        << "the subtask error never started a restart drain";
    // (3) the survivor's OTHER subtask drains FIRST - its ack is credited
    //     while the victim's key still sits in the expected set.
    ASSERT_TRUE(survivor.worker->send_finished(
        job_id, survivor.tasks[1].first, survivor.tasks[1].second, /*had_error=*/false));
    // (4) the victim's replacement registers - the fold erases the victim's
    //     key, leaving the expected set fully covered by already-credited
    //     drains. Nothing further will EVER arrive; only a coverage-based
    //     kick can fire the restart now.
    auto wb2 = std::make_unique<DrainFakeWorker>(port, victim.worker == wb.get() ? "dw-b" : "dw-a");
    ASSERT_TRUE(wb2->valid());
    ASSERT_TRUE(wb2->register_and_ack());

    // The verdict: the redeploy must arrive well inside the 30s drain
    // deadline. With an emptiness-based kick the job sits ready-but-unfired
    // until the deadline fails it with "survivors did not drain", naming a
    // key that drained right here in step (3).
    const bool redeployed = survivor.worker->await_frame(MessageKind::Deploy, 15s).has_value() ||
                            wb2->await_frame(MessageKind::Deploy, 1s).has_value();
    EXPECT_TRUE(redeployed)
        << "the restart never fired: the drain was covered but a kick keyed on an EMPTY "
           "expected set cannot see it (watch item 63's wedge)";

    wb2->close();
    survivor.worker->close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

}  // namespace
