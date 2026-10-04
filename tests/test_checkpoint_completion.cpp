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

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
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
#include "clink/cluster/in_doubt_resolution.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/cluster/protocol_trace.hpp"
#include "clink/config/json.hpp"
#include "clink/connectors/capability.hpp"
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/orchestration_metrics.hpp"
#include "clink/metrics/otlp_export.hpp"
#include "clink/metrics/process_metrics.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/savepoint.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

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
    FakeWorker(std::uint16_t port, std::string id, std::uint32_t slots = 4)
        : id_(std::move(id)), slots_(slots) {
        conn_ = network::connect_plain("127.0.0.1", port);
    }

    [[nodiscard]] bool valid() const { return conn_ != nullptr; }

    [[nodiscard]] bool register_and_ack() {
        const auto ack = register_reply();
        return ack.has_value() && ack->ok;
    }

    // The coordinator's answer to this worker's Register, accepted or not;
    // nullopt when there was none.
    [[nodiscard]] std::optional<RegisterAckMsg> register_reply() {
        RegisterMsg reg{.worker_id = id_, .data_host = "127.0.0.1", .slot_count = slots_};
        if (!send_frame(*conn_, encode_frame(MessageKind::Register, reg))) {
            return std::nullopt;
        }
        auto reply = recv_frame(*conn_);
        if (!reply.has_value()) {
            return std::nullopt;
        }
        MessageReader r(std::move(*reply));
        if (static_cast<MessageKind>(r.read_u8()) != MessageKind::RegisterAck) {
            return std::nullopt;
        }
        auto ack = decode_register_ack(r);
        if (ack.ok) {
            // Only now: the handshake read above is synchronous and would
            // race the pump for the same bytes.
            start_reader();
            start_heartbeat();
        }
        return ack;
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

    // The kinds of the frames still queued, in the order they arrived on the
    // connection.
    [[nodiscard]] std::vector<MessageKind> queued_kinds() {
        std::lock_guard lock(mu_);
        std::vector<MessageKind> kinds;
        for (const auto& frame : inbox_) {
            if (!frame.empty()) {
                kinds.push_back(static_cast<MessageKind>(frame[0]));
            }
        }
        return kinds;
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

    [[nodiscard]] bool send_finished(JobId job_id, const std::string& role, std::uint32_t subtask) {
        SubtaskFinishedMsg m;
        m.job_id = job_id;
        m.worker_id = id_;
        m.role = role;
        m.subtask_idx = subtask;
        m.had_error = false;
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::SubtaskFinished, m));
    }

    // What a bounded source sends at the end of its input: the reply carries
    // the final checkpoint id it injects.
    [[nodiscard]] bool request_final_checkpoint(JobId job_id,
                                                const std::string& role,
                                                std::uint32_t subtask) {
        RequestFinalCheckpointMsg m;
        m.job_id = job_id;
        m.role = role;
        m.subtask_idx = subtask;
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::RequestFinalCheckpoint, m));
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

    // What a worker sends once a committed checkpoint's external commit has
    // executed, for a sink whose commit cannot be re-run after a crash.
    [[nodiscard]] bool send_commit_confirmed(JobId job_id,
                                             std::uint64_t ckpt_id,
                                             const std::string& role,
                                             std::uint32_t subtask) {
        CommitConfirmedMsg m;
        m.job_id = job_id;
        m.checkpoint_id = ckpt_id;
        m.role = role;
        m.subtask_idx = subtask;
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::CommitConfirmed, m));
    }

    // A frame exactly as given: its first byte is the kind. For frames no
    // real worker sends.
    [[nodiscard]] bool send_raw(const std::vector<std::byte>& frame) {
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, frame);
    }

    // A heartbeat carrying `sequence`, beside the periodic ones (which carry
    // 0). Its answer is a barrier: the coordinator has read every frame sent
    // before it.
    [[nodiscard]] bool send_heartbeat(std::uint64_t sequence) {
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_,
                          encode_frame(MessageKind::Heartbeat,
                                       HeartbeatMsg{.worker_id = id_, .sequence = sequence}));
    }

    [[nodiscard]] bool await_heartbeat_ack(std::uint64_t sequence,
                                           std::chrono::milliseconds bound = 5s) {
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (std::chrono::steady_clock::now() < deadline) {
            const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            auto r = await_frame(MessageKind::HeartbeatAck, std::max(left, 1ms));
            if (r.has_value() && decode_heartbeat_ack(*r).sequence == sequence) {
                return true;
            }
        }
        return false;
    }

    // Drop every queued frame of `kind`.
    void discard(MessageKind kind) {
        std::lock_guard lock(mu_);
        std::erase_if(inbox_, [kind](const std::vector<std::byte>& f) {
            return !f.empty() && static_cast<MessageKind>(f[0]) == kind;
        });
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
    std::uint32_t slots_;
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

// Where the coordinator WRITES its completion marker, established by
// running one rather than by reading the comments - which disagree.
// Exposed so the recovery test below can state the premise it depends on.
std::filesystem::path written_marker_path(const std::filesystem::path& checkpoint_dir,
                                          JobId job_id,
                                          std::uint64_t ckpt_id) {
    return checkpoint_dir / "_jobs" / std::to_string(job_id) /
           ("COMPLETED-" + std::to_string(ckpt_id));
}

// A cluster with one fake worker and one submitted job, ready to be told
// its checkpoints have or have not succeeded.
struct CheckpointFixture {
    explicit CheckpointFixture(std::optional<Coordinator::Config> cfg = std::nullopt)
        : dir(std::filesystem::temp_directory_path() /
              ("clink_ckpt_completion_" + std::to_string(::getpid()) + "_" +
               ::testing::UnitTest::GetInstance()->current_test_info()->name())) {
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        coordinator = cfg.has_value() ? std::make_unique<Coordinator>(std::move(*cfg))
                                      : std::make_unique<Coordinator>();
        port = coordinator->start();
        coordinator->expect_workers({"w"});
    }

    // Register the fake worker and submit a job whose checkpoints the
    // test will answer for. Returns the job id, or 0 on failure.
    // `max_restarts` is the job's restart budget: 0 keeps a failed
    // checkpoint from starting a rewind, which most tests here want out of
    // the way; the rewind tests pass a budget.
    // `graph` defaults to a source into a file sink.
    JobId bring_up(std::uint32_t max_restarts = 0,
                   std::int64_t interval_ms = 100,
                   std::optional<JobGraphSpec> graph = std::nullopt) {
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
        ckpt.interval_ms = interval_ms;
        ckpt.max_restarts_on_worker_loss = max_restarts;
        const auto job_id = coordinator->submit_job(
            graph.has_value() ? std::move(*graph) : two_subtask_graph(dir / "out.txt"),
            OperatorRegistry::default_instance(),
            {},
            ckpt);
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

    // <checkpoint_dir>/_jobs/<job_id>/COMPLETED-<id>: the path recovery reads,
    // and - since F29 - the path the coordinator writes.
    [[nodiscard]] bool marker_exists(JobId job_id, std::uint64_t ckpt_id) const {
        std::error_code ec;
        return std::filesystem::exists(
            dir / "_jobs" / std::to_string(job_id) / ("COMPLETED-" + std::to_string(ckpt_id)), ec);
    }

    // The tasks the coordinator deployed, as its Deploy frame named them.
    [[nodiscard]] const std::vector<std::pair<std::string, std::uint32_t>>& deployed() const {
        return deployed_;
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

TEST(CheckpointCompletion, ACompletedCheckpointRecordsAnOtlpLifecycleSpan) {
    // The span SITE, not the exporter (test_otlp_export.cpp owns that): a
    // checkpoint completing inside the real coordinator must land a
    // clink.checkpoint span in the buffer when an exporter has armed it,
    // carrying the ids an operator would filter traces by.
    auto& buf = clink::metrics::SpanBuffer::global();
    buf.set_enabled(true);
    (void)buf.drain();  // other suites may have left spans behind

    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    const auto ckpt_id = fx.await_trigger();
    ASSERT_TRUE(ckpt_id.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *ckpt_id, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [&] { return fx.coordinator->latest_completed_checkpoint(job_id) >= *ckpt_id; }));

    const auto spans = buf.drain();
    buf.set_enabled(false);
    // Select by name: the submit that brought the job up records its own
    // clink.submit span into the same buffer, and the order is the
    // lifecycle's, not this assertion's concern.
    const auto ckpt_span = std::find_if(
        spans.begin(), spans.end(), [](const auto& sp) { return sp.name == "clink.checkpoint"; });
    ASSERT_NE(ckpt_span, spans.end()) << "no span recorded for a completed checkpoint";
    const auto& s = *ckpt_span;
    EXPECT_LE(s.start_unix_nano, s.end_unix_nano);
    EXPECT_GT(s.end_unix_nano, 0U);
    const auto attr = [&](const std::string& key) -> std::string {
        for (const auto& [k, v] : s.attributes) {
            if (k == key) {
                return v;
            }
        }
        return {};
    };
    EXPECT_EQ(attr("clink.job_id"), std::to_string(job_id));
    EXPECT_EQ(attr("clink.checkpoint_id"), std::to_string(*ckpt_id));
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

// Found by the exactly-once model (formal/ExactlyOnce.tla, design record
// 012), not by a rig. The trigger loop does not wait for the previous
// checkpoint's acks, so two checkpoints are routinely in flight. Checkpoint k
// FAILS (a subtask could not snapshot) and the job begins its rewind; k+1's
// barrier was already at the sinks, its acks keep arriving through the drain,
// and it used to complete: a COMPLETED marker, then in-doubt resolution
// committing its transactions and confirming it, then a restore FROM k+1 -
// past interval k, whose staged transactions the failure had aborted and
// which only a rewind below k re-emits. Silent loss of one interval with
// every gate green. A checkpoint above a failed one must be discarded like
// the failed one: no marker, and an abort for its staged transactions.
TEST(CheckpointCompletion, ACheckpointAboveAFailedOneIsDiscardedDuringTheRewind) {
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/3);
    ASSERT_GT(job_id, 0U);

    // Two checkpoints in flight: neither is answered until both exist.
    const auto failed = fx.await_trigger();
    ASSERT_TRUE(failed.has_value());
    const auto above = fx.await_trigger();
    ASSERT_TRUE(above.has_value());
    ASSERT_GT(*above, *failed);

    // k fails; the rewind begins with a cancel to the worker, and k's staged
    // transactions are aborted.
    ASSERT_TRUE(fx.ack_all(job_id, *failed, /*ok=*/false));
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::CancelJob, 5s).has_value())
        << "a failed checkpoint with restart budget did not begin a rewind";
    bool aborted_failed = false;
    for (int i = 0; i < 4 && !aborted_failed; ++i) {
        auto abort = fx.worker->await_frame(MessageKind::AbortCheckpoint, 5s);
        ASSERT_TRUE(abort.has_value()) << "no AbortCheckpoint for the failed checkpoint";
        aborted_failed = decode_abort_checkpoint(*abort).checkpoint_id == *failed;
    }
    ASSERT_TRUE(aborted_failed);

    // k+1 answers ok from every subtask during the drain.
    ASSERT_TRUE(fx.ack_all(job_id, *above, /*ok=*/true));

    // It must not become a restore point: no completion, no marker...
    EXPECT_FALSE(ckpt_await(
        [&] { return fx.coordinator->latest_completed_checkpoint(job_id) >= *above; }, 750ms))
        << "checkpoint " << *above << " completed above failed checkpoint " << *failed
        << " during the rewind; a restore from it skips the aborted interval";
    EXPECT_FALSE(fx.marker_exists(job_id, *above))
        << "a COMPLETED marker was written above a failed checkpoint mid-rewind";
    // ...and its staged transactions are aborted, since the rewind re-emits
    // its interval as well.
    bool aborted_above = false;
    for (int i = 0; i < 4 && !aborted_above; ++i) {
        auto abort = fx.worker->await_frame(MessageKind::AbortCheckpoint, 5s);
        if (!abort.has_value()) {
            break;
        }
        aborted_above = decode_abort_checkpoint(*abort).checkpoint_id == *above;
    }
    EXPECT_TRUE(aborted_above)
        << "no AbortCheckpoint for the checkpoint discarded above the rewind floor";
}

// With no restart budget left there is no rewind to re-emit the failed
// checkpoint's aborted interval. The job used to carry on regardless: the
// checkpoint above the failed one completed and committed, every later one
// built on it, and the interval's output was gone with every gate green. The
// budget is spent by every recovery a job makes, so a long-running job got
// here sooner or later. It now fails, naming the cause, and nothing completes
// above the gap.
TEST(CheckpointCompletion, AFailedCheckpointWithNoRestartBudgetFailsTheJobInsteadOfSailingOn) {
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/0);
    ASSERT_GT(job_id, 0U);
    const auto failed = fx.await_trigger();
    ASSERT_TRUE(failed.has_value());
    const auto above = fx.await_trigger();
    ASSERT_TRUE(above.has_value());
    ASSERT_GT(*above, *failed);

    ASSERT_TRUE(fx.ack_all(job_id, *failed, /*ok=*/false));
    ASSERT_TRUE(fx.ack_all(job_id, *above, /*ok=*/true));

    EXPECT_TRUE(fx.worker->await_frame(MessageKind::CancelJob, 5s).has_value())
        << "a failed checkpoint with no budget to rewind must fail the job";
    EXPECT_FALSE(ckpt_await(
        [&] { return fx.coordinator->latest_completed_checkpoint(job_id) >= *above; }, 750ms))
        << "checkpoint " << *above << " completed above failed checkpoint " << *failed
        << " with no rewind to re-emit the aborted interval: a silent gap in the output";
    EXPECT_FALSE(fx.marker_exists(job_id, *above));
    bool named = false;
    for (const auto& e : fx.coordinator->job_errors(job_id)) {
        named = named || e.find("no restart budget left to rewind") != std::string::npos;
    }
    EXPECT_TRUE(named) << "the job's failure does not name the cause";
}

// --- an id is on record before any frame naming it leaves ---------------
//
// A takeover numbers its checkpoints above <checkpoint_dir>/_jobs/<job>/TRIGGERED
// as well as above the markers and snapshot files, because a barrier whose
// capture has not landed leaves nothing else a takeover could see. So no
// TriggerCheckpoint, and no reply carrying an end-of-input final id, may leave
// the coordinator before its id is on record. Each job's ids are claimed by a
// claimer of its own, the next one as soon as the last is allocated, so the
// trigger loop never waits on the store. A record that cannot be written is a
// directory where the record goes: its rename fails.
namespace {

std::filesystem::path triggered_record_path(const std::filesystem::path& checkpoint_dir,
                                            JobId job_id) {
    return checkpoint_dir / clink::cluster::triggered_record_key(job_id);
}

void obstruct_triggered_record(const std::filesystem::path& checkpoint_dir, JobId job_id) {
    const auto path = triggered_record_path(checkpoint_dir, job_id);
    std::filesystem::create_directories(path);
    std::ofstream(path / "obstruction").put('x');
}

void clear_triggered_record(const std::filesystem::path& checkpoint_dir, JobId job_id) {
    std::error_code ec;
    std::filesystem::remove_all(triggered_record_path(checkpoint_dir, job_id), ec);
}

std::int64_t log_cursor_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
               .count() -
           1;
}

// Every claim on the record passes the compare-and-set's critical section,
// so this point's hits count the claims (no other coordination record is
// written by a coordinator without an HA directory).
constexpr const char* kIdClaimPoint = clink::fault::points::kCoordinatorBeforeMetadataWrite;

}  // namespace

TEST(CheckpointCompletion, APeriodicCheckpointIsNotTriggeredBeforeItsIdIsOnRecord) {
    // Observed only, to count the claims the failing record gets.
    clink::fault::Registry::instance().reset();
    clink::fault::ScopedFault observe{
        clink::fault::Rule{.point = kIdClaimPoint, .action = clink::fault::Action::Observe}};
    CheckpointFixture fx;
    // A fresh coordinator: the job it is about to be given is job 1.
    obstruct_triggered_record(fx.dir, 1);
    const auto since_ms = log_cursor_ms();
    const auto job_id = fx.bring_up();
    ASSERT_EQ(job_id, 1U);

    EXPECT_FALSE(fx.worker->await_frame(MessageKind::TriggerCheckpoint, 1500ms).has_value())
        << "a barrier left before its checkpoint id was on record";
    const auto detail = fx.coordinator->snapshot_job(job_id);
    ASSERT_TRUE(detail.has_value());
    EXPECT_TRUE(detail->pending_checkpoint_ids.empty())
        << "a round whose id could not be recorded left a checkpoint waiting on acks";
    std::size_t reported = 0;
    for (const auto& rec :
         LogBuffer::global().tail(1000, "error", since_ms, "coordinator.checkpoint")) {
        reported += rec.message.find("could not record checkpoint id") != std::string::npos ? 1 : 0;
    }
    EXPECT_EQ(reported, 1U) << "the failing record must be reported once, not every round";
    // And by the coordinator alone: the store's own error line on every
    // retry buried that one report under ten a second.
    for (const auto& rec :
         LogBuffer::global().tail(1000, "error", since_ms, "coordinator.metadata")) {
        ADD_FAILURE() << "the store logged a failed claim itself: " << rec.message;
    }
    // Backed off: a claim that keeps failing is retried with a growing delay,
    // not on every pass of the trigger loop at the job's 100ms interval.
    const auto attempts = clink::fault::Registry::instance().hits(kIdClaimPoint);
    EXPECT_GE(attempts, 1U) << "no claim reached the store, so nothing above was tested";
    EXPECT_LE(attempts, 8U) << attempts << " claims in 1.5s: a failing record is not backed off";

    // Writable again: the round goes ahead, and the refused rounds consumed no id.
    clear_triggered_record(fx.dir, job_id);
    const auto first = fx.await_trigger();
    ASSERT_TRUE(first.has_value()) << "checkpoints did not resume once the id could be recorded";
    EXPECT_EQ(*first, 1U);
    EXPECT_GE(clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id), *first);
}

// The claim for a job's next id goes out as soon as the previous id is
// allocated, so the id is on record before it falls due and the trigger does
// not wait on the store. The trigger loop used to make the write itself when
// the id fell due, one job after another.
TEST(CheckpointCompletion, TheNextCheckpointIdIsOnRecordBeforeItFallsDue) {
    CheckpointFixture fx;
    // Ten minutes between checkpoints: the first is triggered at once and the
    // second not within this test.
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = fx.await_trigger();
    ASSERT_TRUE(first.has_value());
    EXPECT_TRUE(ckpt_await(
        [&] {
            return clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id) ==
                   *first + 1;
        },
        3s))
        << "the id after checkpoint " << *first
        << " was not on record ahead of its trigger; the record holds "
        << clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id);
    EXPECT_FALSE(fx.worker->await_frame(MessageKind::TriggerCheckpoint, 300ms).has_value())
        << "claiming the next id ahead of time triggered its checkpoint early";
}

// Nothing goes on record before the job is up: the record, like everything
// else in the checkpoint directory, appears once the job is running, which is
// what the integration harness takes the directory's first entry to mean. A
// claim made at deploy put it there while the Deploy frames were still going
// out, and a test that killed a worker on seeing it killed the deploy instead.
TEST(CheckpointCompletion, NoCheckpointIdIsClaimedBeforeTheJobIsUp) {
    CheckpointFixture fx;
    fx.worker = std::make_unique<FakeWorker>(fx.port, "w");
    ASSERT_TRUE(fx.worker->valid());
    ASSERT_TRUE(fx.worker->register_and_ack());
    ASSERT_TRUE(fx.coordinator->await_registrations(2s));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = fx.dir.string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 0;
    const auto job_id = fx.coordinator->submit_job(
        two_subtask_graph(fx.dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto deploy = fx.worker->await_frame(MessageKind::Deploy);
    ASSERT_TRUE(deploy.has_value());
    const auto tasks = decode_deploy(*deploy).tasks;
    // Deployed, and not yet reporting its listeners: not up.
    EXPECT_FALSE(ckpt_await(
        [&] { return std::filesystem::exists(triggered_record_path(fx.dir, job_id)); }, 500ms))
        << "a checkpoint id went on record before the job's subtasks were up";
    std::uint16_t port_seed = 41900;
    for (const auto& t : tasks) {
        ASSERT_TRUE(fx.worker->report_listening(job_id, t.role, t.subtask_idx, port_seed++));
    }
    EXPECT_TRUE(ckpt_await([&] {
        return clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id) >= 1;
    })) << "the job's first checkpoint id never went on record once it was up";
}

// A claim that does not return holds its own job's checkpoints and nobody
// else's: the store write runs on the job's claimer, not on the trigger loop,
// which used to wait on every due job's record in turn, so one slow or
// failing store stopped every job on the coordinator.
TEST(CheckpointCompletion, ARecordWriteThatHangsHoldsOnlyItsOwnJob) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_claim_isolation_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{dir};
    Coordinator c;
    const auto port = c.start();
    c.expect_workers({"w"});
    FakeWorker w(port, "w");
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(c.await_registrations(2s));
    // The run's first claim is the first job's first id, parked until
    // released. Armed after the coordinator so that on an early exit it is
    // reset, releasing the claim, before the coordinator joins its claimer.
    clink::fault::Registry::instance().reset();
    clink::fault::ScopedFault park{clink::fault::Rule{
        .point = kIdClaimPoint, .ordinal = 1, .action = clink::fault::Action::Block}};

    // A job brought up to where its periodic checkpoints are due.
    const auto submit = [&](const std::string& tag, std::uint16_t port_seed) -> JobId {
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = (dir / tag).string();
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = 0;
        const auto id = c.submit_job(two_subtask_graph(dir / (tag + ".txt")),
                                     OperatorRegistry::default_instance(),
                                     {},
                                     ckpt);
        if (id == 0) {
            return 0;
        }
        auto deploy = w.await_frame(MessageKind::Deploy);
        if (!deploy.has_value()) {
            return 0;
        }
        for (const auto& t : decode_deploy(*deploy).tasks) {
            if (!w.report_listening(id, t.role, t.subtask_idx, port_seed++)) {
                return 0;
            }
        }
        return id;
    };
    const auto held = submit("held", 41500);
    ASSERT_GT(held, 0U);
    ASSERT_TRUE(
        ckpt_await([] { return clink::fault::Registry::instance().hits(kIdClaimPoint) >= 1; }, 3s))
        << "the first job's claim never reached the store";
    const auto free_job = submit("free", 41600);
    ASSERT_GT(free_job, 0U);

    // The other job checkpoints at its interval, the held one not at all.
    for (int i = 0; i < 4; ++i) {
        auto trigger = w.await_frame(MessageKind::TriggerCheckpoint, 3s);
        ASSERT_TRUE(trigger.has_value())
            << "a record write held on one job stopped the other job's checkpoints";
        EXPECT_EQ(decode_trigger_checkpoint(*trigger).job_id, free_job)
            << "the held job's barrier left before its id was on record";
    }

    // Released: the held job's id lands and its checkpoints begin.
    EXPECT_EQ(clink::fault::Registry::instance().release(kIdClaimPoint), 1U);
    bool held_triggered = false;
    for (int i = 0; i < 100 && !held_triggered; ++i) {
        auto trigger = w.await_frame(MessageKind::TriggerCheckpoint, 3s);
        if (!trigger.has_value()) {
            break;
        }
        held_triggered = decode_trigger_checkpoint(*trigger).job_id == held;
    }
    EXPECT_TRUE(held_triggered) << "the held job never checkpointed once its claim was released";
    w.close();
    c.stop();
}

// Another coordinator of the job claims ids on the record too: a superseded
// leader whose trigger loop runs on. A worker that has re-registered with the
// leader refuses its barriers, but one that has not yet re-registered accepts
// them, and its late capture of such an id writes the path the leader's
// capture of the same id would. So the leader must never use an id another
// coordinator claimed: its own claim is refused, and it numbers above what the
// record holds. The test claims the ids itself, as that coordinator would.
TEST(CheckpointCompletion, IdsAnotherCoordinatorClaimedAreSkippedNotReused) {
    const auto trace_dir = std::filesystem::temp_directory_path() /
                           ("clink_ckpt_renumber_trace_" + std::to_string(::getpid()));
    std::filesystem::remove_all(trace_dir);
    std::filesystem::create_directories(trace_dir);
    ::setenv("CLINK_PROTOCOL_TRACE_DIR", trace_dir.c_str(), 1);
    clink::protocol_trace::reset_for_tests();
    struct TraceOff {
        std::filesystem::path dir;
        ~TraceOff() {
            ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
            clink::protocol_trace::reset_for_tests();
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } trace_off{trace_dir};

    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    ASSERT_TRUE(fx.await_trigger().has_value());
    // This coordinator holds at most the ids it has put on record, and can
    // claim one more before the claim below lands.
    const auto ours = clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id) + 1;
    constexpr std::uint64_t kElsewhere = 40;
    ASSERT_LT(ours, kElsewhere);
    ASSERT_TRUE(clink::cluster::record_triggered_id(
                    fx.dir.string(), job_id, kElsewhere, "another-coordinator")
                    .claimed);

    std::uint64_t last = 0;
    for (int i = 0; i < 40 && last <= kElsewhere; ++i) {
        const auto id = fx.await_trigger();
        ASSERT_TRUE(id.has_value()) << "checkpoints stopped after another coordinator's claim";
        last = *id;
        EXPECT_TRUE(*id <= ours || *id > kElsewhere)
            << "checkpoint id " << *id
            << " was sent although another coordinator of the job had claimed every id up to "
            << kElsewhere;
    }
    EXPECT_EQ(last, kElsewhere + 1) << "the job did not number from just above the record";

    // The renumber is a protocol step of its own, which the specification
    // admits only while a superseded coordinator's claims can stand above
    // the leader's next id.
    bool recorded = false;
    for (const auto& entry : std::filesystem::directory_iterator(trace_dir)) {
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.find("\"event\":\"Renumber\"") != std::string::npos &&
                line.find("\"job\":" + std::to_string(job_id) + ",") != std::string::npos &&
                line.find("\"next\":" + std::to_string(kElsewhere + 1)) != std::string::npos) {
                recorded = true;
            }
        }
    }
    EXPECT_TRUE(recorded) << "the renumber left no Renumber event in the protocol trace";
}

TEST(CheckpointCompletion, ASavepointWhoseIdCannotBeRecordedSendsNothing) {
    CheckpointFixture fx;
    obstruct_triggered_record(fx.dir, 1);
    const auto job_id = fx.bring_up();
    ASSERT_EQ(job_id, 1U);

    const auto ack = fx.coordinator->take_savepoint(job_id, 2s);
    EXPECT_FALSE(ack.ok);
    EXPECT_NE(ack.message.find("could not record checkpoint id"), std::string::npos) << ack.message;
    EXPECT_FALSE(fx.worker->await_frame(MessageKind::TriggerCheckpoint, 500ms).has_value())
        << "the savepoint's barrier left without its id on record";
    const auto detail = fx.coordinator->snapshot_job(job_id);
    ASSERT_TRUE(detail.has_value());
    EXPECT_TRUE(detail->pending_checkpoint_ids.empty())
        << "the refused savepoint left a checkpoint waiting on acks";
}

// The final id's barrier leaves with the reply: the source injects it the
// moment the reply lands. A request whose id is not on record yet is held, not
// declined, and answered once the job's claimer has put the id there, without
// the source asking again. While the record cannot be written the source's
// bounded wait runs out, and it fails its subtask so the restart replays the
// tail under a checkpoint.
TEST(CheckpointCompletion, AFinalCheckpointIdIsAnsweredOnlyOnceItIsOnRecord) {
    CheckpointFixture fx;
    obstruct_triggered_record(fx.dir, 1);
    const auto job_id = fx.bring_up();
    ASSERT_EQ(job_id, 1U);
    ASSERT_FALSE(fx.deployed().empty());
    const auto& [role, subtask] = fx.deployed().front();

    ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    EXPECT_FALSE(fx.worker->await_frame(MessageKind::FinalCheckpointAssigned, 1s).has_value())
        << "a final checkpoint id reached a source before it was on record";

    clear_triggered_record(fx.dir, job_id);
    auto reply = fx.worker->await_frame(MessageKind::FinalCheckpointAssigned, 5s);
    ASSERT_TRUE(reply.has_value())
        << "the held request went unanswered once its id could be recorded";
    const auto assigned = decode_final_checkpoint_assigned(*reply);
    EXPECT_EQ(assigned.decline, FinalCheckpointDecline::None);
    ASSERT_GT(assigned.final_checkpoint_id, 0U);
    EXPECT_GE(clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id),
              assigned.final_checkpoint_id);
}

// The request arrives on the worker's control reader, which reads that
// worker's heartbeats too. It used to claim an id not yet on record right
// there, behind the job's claimer whenever that claimer's store write was out,
// so a slow object-store write held the worker's heartbeats and could get the
// worker declared lost. Here the claimer's write for the next id is held open:
// the request is held rather than claimed for, the frames behind it on the
// same connection are served at once, and the request is answered once the
// write lands.
TEST(CheckpointCompletion, AFinalCheckpointRequestNeverHoldsTheWorkersReaderOnTheStore) {
    CheckpointFixture fx;
    // The run's second claim, the id after the first checkpoint's, parks
    // inside the record's compare-and-set until released. Armed after the
    // fixture so that on an early exit it is reset, releasing the claim,
    // before the coordinator joins its claimer.
    clink::fault::Registry::instance().reset();
    clink::fault::ScopedFault park{clink::fault::Rule{
        .point = kIdClaimPoint, .ordinal = 2, .action = clink::fault::Action::Block}};
    // Ten minutes between checkpoints: the first is triggered at once, and the
    // claim for the second goes out straight after it.
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = fx.await_trigger();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        ckpt_await([] { return clink::fault::Registry::instance().hits(kIdClaimPoint) >= 2; }, 3s))
        << "the claim for the id after checkpoint " << *first << " never reached the store";

    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    // Behind the request on the same connection: the first checkpoint's acks.
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    EXPECT_TRUE(ckpt_await(
        [&] { return fx.coordinator->latest_completed_checkpoint(job_id) == *first; }, 1500ms))
        << "the worker's reader stayed on the final-id request while the job's claim was out";
    EXPECT_FALSE(fx.worker->await_frame(MessageKind::FinalCheckpointAssigned, 200ms).has_value())
        << "a final checkpoint id was answered before it was on record";

    // The claim lands, and the held request is answered with the id it made.
    EXPECT_EQ(clink::fault::Registry::instance().release(kIdClaimPoint), 1U);
    auto reply = fx.worker->await_frame(MessageKind::FinalCheckpointAssigned, 5s);
    ASSERT_TRUE(reply.has_value()) << "the held request was not answered once its id landed";
    const auto assigned = decode_final_checkpoint_assigned(*reply);
    EXPECT_EQ(assigned.decline, FinalCheckpointDecline::None);
    EXPECT_EQ(assigned.final_checkpoint_id, *first + 1);
    EXPECT_GE(clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id),
              assigned.final_checkpoint_id);
}

// A claim whose write lands and whose answer is lost is retried, and the retry
// finds the id on record under this coordinator's own claimant (on S3 an
// ordinary SDK retry of the conditional write is refused; here the write's
// rename lands and the call then throws). The id is this coordinator's. It
// used to be read as another coordinator's: the job skipped it, warned that
// two coordinators were triggering it, and recorded a Renumber that trace
// validation rejects, since no superseded coordinator stood behind it.
TEST(CheckpointCompletion, AClaimWhoseAnswerWasLostKeepsItsId) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    // Every durable write publishes through this point. With no HA directory
    // and no acks the claims are the only ones, so the second is the claim
    // for the id after the first checkpoint.
    clink::fault::ScopedFault lose{
        clink::fault::Rule{.point = clink::fault::points::kCheckpointAfterPublish,
                           .ordinal = 2,
                           .action = clink::fault::Action::Throw}};
    const auto since_ms = log_cursor_ms();
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    const auto first = fx.await_trigger();
    ASSERT_TRUE(first.has_value());
    const auto second = fx.await_trigger();
    ASSERT_TRUE(second.has_value()) << "checkpoints stopped after a claim's answer was lost";
    ASSERT_GE(
        clink::fault::Registry::instance().hits(clink::fault::points::kCheckpointAfterPublish), 2U)
        << "the claim for the second id never reached the store, so nothing was tested";
    EXPECT_EQ(*second, *first + 1)
        << "the id whose claim landed with its answer lost was skipped as another coordinator's";
    for (const auto& rec :
         LogBuffer::global().tail(1000, "warn", since_ms, "coordinator.checkpoint")) {
        EXPECT_EQ(rec.message.find("numbering from"), std::string::npos)
            << "a lost answer was reported as another coordinator's claim: " << rec.message;
    }
}

// Each job claims its ids on one thread for its life, parked between claims.
// It used to start a thread per checkpoint, which at a 100 ms interval is ten
// a second for every job.
TEST(CheckpointCompletion, AJobClaimsItsIdsOnOneThreadForItsLife) {
    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    std::uint64_t last = 0;
    for (int i = 0; i < 6; ++i) {
        const auto id = fx.await_trigger();
        ASSERT_TRUE(id.has_value()) << "checkpoint " << i + 1 << " was not triggered";
        last = *id;
    }
    // The claim after the last trigger has landed, so every claim so far has
    // run on whatever thread the job's claims run on.
    ASSERT_TRUE(ckpt_await([&] {
        return clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id) > last;
    }));
    EXPECT_EQ(fx.coordinator->id_claimers_started(), 1U)
        << "the job's claims ran on more than one thread";
}

// --- a worker's heartbeats while its frames wait on the store -------------
//
// A worker connection's reader reads every frame and answers heartbeats
// itself; every other frame is handled, in the order read, on the
// connection's dispatch thread. The reader used to run the handlers too, so a
// handler waiting on the store held that worker's heartbeats: a write that
// outlasted heartbeat_timeout got a live worker declared lost and turned a
// completed checkpoint into a restart, and one under mu_ held every worker's
// heartbeats at once. Each hold below is a Block at a fault point, so what is
// parked is decided by the code under test, not by timing.

namespace {

// Longer than the watchdog's default heartbeat_timeout (2s).
constexpr auto kPastTheHeartbeatTimeout = 2600ms;

// Every window of a second across `span` brings each of `workers` a
// HeartbeatAck, as a real worker's lease (3s without a coordinator frame)
// needs. Acks queued before the call are discarded; at most one answered just
// before the hold can still arrive late, and it satisfies only the first
// window.
::testing::AssertionResult heartbeats_answered_throughout(const std::vector<FakeWorker*>& workers,
                                                          std::chrono::milliseconds span) {
    for (auto* w : workers) {
        w->discard(MessageKind::HeartbeatAck);
    }
    const auto start = std::chrono::steady_clock::now();
    int windows = 0;
    while (std::chrono::steady_clock::now() - start < span) {
        for (std::size_t i = 0; i < workers.size(); ++i) {
            if (!workers[i]
                     ->await_frame(
                         MessageKind::HeartbeatAck,
                         clink::test_support::scale_slack(std::chrono::milliseconds{1000}))
                     .has_value()) {
                return ::testing::AssertionFailure()
                       << "worker " << i << " had no HeartbeatAck for a second, after " << windows
                       << " answered windows";
            }
        }
        ++windows;
    }
    return ::testing::AssertionSuccess() << windows << " windows answered";
}

bool worker_was_lost(const Coordinator& c, const std::string& id) {
    const auto lost = c.lost_workers();
    return std::find(lost.begin(), lost.end(), id) != lost.end();
}

// True if `id` is declared lost at any point until `since + span`, so the
// answer covers the whole hold whatever the assertions before it took.
bool lost_within(const Coordinator& c,
                 const std::string& id,
                 std::chrono::steady_clock::time_point since,
                 std::chrono::milliseconds span) {
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
        since + span - std::chrono::steady_clock::now());
    return ckpt_await([&] { return worker_was_lost(c, id); }, std::max(left, 0ms));
}

std::uint64_t malformed_frames() {
    return MetricsRegistry::global().counter(clink::metrics::kMalformedFrames).value();
}

// Brought up to its first checkpoint with the id after it already on record,
// so no claim is in flight while a test holds something else, and no further
// checkpoint falls due within the test. Returns the first checkpoint's id.
std::optional<std::uint64_t> first_checkpoint_with_next_on_record(CheckpointFixture& fx,
                                                                  JobId job_id) {
    const auto first = fx.await_trigger();
    if (!first.has_value()) {
        return std::nullopt;
    }
    if (!ckpt_await([&] {
            return clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id) >=
                   *first + 1;
        })) {
        return std::nullopt;
    }
    return first;
}

// The commit-confirmation protocol's graph: a file_2pc sink, tracked once its
// declared commit is made non-recoverable.
JobGraphSpec confirm_tracked_graph(const std::filesystem::path& dir) {
    JobGraphSpec g;
    OperatorSpec src;
    src.id = "src";
    src.type = "int64_range_source";
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1000000"}};
    g.ops.push_back(std::move(src));
    OperatorSpec conv;
    conv.id = "conv";
    conv.type = "int64_to_string";
    conv.out_channel = std::string{kChannelString};
    conv.inputs = {"src"};
    g.ops.push_back(std::move(conv));
    OperatorSpec snk;
    snk.id = "snk";
    snk.type = "file_2pc_sink_string";
    snk.out_channel = std::string{kChannelString};
    snk.inputs = {"conv"};
    snk.params = {{"dir", dir.string()}};
    g.ops.push_back(std::move(snk));
    return g;
}

// Replaces a connector's capability record for one test and puts the original
// back whatever happens: the registry is process-wide.
class CompletionScopedRecordOverride {
public:
    explicit CompletionScopedRecordOverride(clink::connectors::ConnectorCapabilities replacement) {
        const auto* current =
            clink::connectors::CapabilityRegistry::instance().find(replacement.name);
        if (current != nullptr) {
            original_ = *current;
        }
        clink::connectors::declare_connector(std::move(replacement));
    }
    ~CompletionScopedRecordOverride() {
        if (original_.has_value()) {
            clink::connectors::declare_connector(*original_);
        }
    }
    CompletionScopedRecordOverride(const CompletionScopedRecordOverride&) = delete;
    CompletionScopedRecordOverride& operator=(const CompletionScopedRecordOverride&) = delete;
    CompletionScopedRecordOverride(CompletionScopedRecordOverride&&) = delete;
    CompletionScopedRecordOverride& operator=(CompletionScopedRecordOverride&&) = delete;

private:
    std::optional<clink::connectors::ConnectorCapabilities> original_;
};

constexpr const char* kCompletedMarkerPoint =
    clink::fault::points::kCoordinatorBeforeCompletedMarker;
constexpr const char* kFinalRequestPoint =
    clink::fault::points::kCoordinatorBeforeFinalCheckpointRequest;

}  // namespace

// The COMPLETED marker write runs on the dispatch thread of the worker whose
// ack completed the checkpoint. Held past heartbeat_timeout, that worker's
// heartbeats are still answered, it is not declared lost, and the frames it
// sent behind the ack wait their turn: nothing is broadcast before the marker
// is durable.
TEST(CheckpointCompletion, ACompletedMarkerWriteOutlastingTheHeartbeatTimeoutKeepsItsWorker) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    // Armed after the fixture, so an early exit releases the hold before the
    // coordinator joins the connection's threads.
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1;
    })) << "the completed-marker write was never reached";
    const auto parked_at = std::chrono::steady_clock::now();

    EXPECT_TRUE(heartbeats_answered_throughout({fx.worker.get()}, kPastTheHeartbeatTimeout))
        << "a COMPLETED marker write held the worker's heartbeats";
    EXPECT_FALSE(lost_within(*fx.coordinator, "w", parked_at, kPastTheHeartbeatTimeout))
        << "a live worker was declared lost while its ack's marker write was out";
    const auto held_kinds = fx.worker->queued_kinds();
    EXPECT_EQ(std::count(held_kinds.begin(), held_kinds.end(), MessageKind::CommitCheckpoint), 0)
        << "a commit was broadcast before its COMPLETED marker was durable";
    EXPECT_EQ(
        std::count(held_kinds.begin(), held_kinds.end(), MessageKind::FinalCheckpointAssigned), 0)
        << "a frame queued behind the completing ack was handled ahead of it";
    EXPECT_EQ(fx.coordinator->latest_completed_checkpoint(job_id), 0U);

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(ckpt_await([&] {
        const auto kinds = fx.worker->queued_kinds();
        return std::count(kinds.begin(), kinds.end(), MessageKind::FinalCheckpointAssigned) > 0;
    })) << "the final-checkpoint request queued behind the ack was never answered";
    const auto kinds = fx.worker->queued_kinds();
    const auto commit = std::find(kinds.begin(), kinds.end(), MessageKind::CommitCheckpoint);
    const auto final_reply =
        std::find(kinds.begin(), kinds.end(), MessageKind::FinalCheckpointAssigned);
    ASSERT_NE(commit, kinds.end()) << "no CommitCheckpoint once the marker was written";
    EXPECT_LT(commit, final_reply)
        << "the connection's frames were not handled in the order the worker sent them";
    auto commit_frame = fx.worker->await_frame(MessageKind::CommitCheckpoint);
    ASSERT_TRUE(commit_frame.has_value());
    EXPECT_EQ(decode_commit_checkpoint(*commit_frame).checkpoint_id, *first);
    EXPECT_EQ(fx.coordinator->latest_completed_checkpoint(job_id), *first);
    EXPECT_FALSE(worker_was_lost(*fx.coordinator, "w"));
}

// The CONFIRMED marker write runs on the dispatch thread of the worker whose
// CommitConfirmed drained the set. Held past heartbeat_timeout, the worker's
// heartbeats are still answered and it is not declared lost; the confirmed
// restore point moves only once the marker is durable.
TEST(CheckpointCompletion, AConfirmedMarkerWriteOutlastingTheHeartbeatTimeoutKeepsItsWorker) {
    clink::cluster::ensure_built_ins_registered();
    const auto* file_2pc = clink::connectors::CapabilityRegistry::instance().find("file_2pc");
    ASSERT_NE(file_2pc, nullptr) << "built-in capability records not declared";
    auto flagged = *file_2pc;
    flagged.commit_recoverable = false;
    CompletionScopedRecordOverride tracked(std::move(flagged));
    clink::fault::Registry::instance().reset();

    CheckpointFixture fx;
    const auto job_id = fx.bring_up(
        /*max_restarts=*/0, /*interval_ms=*/600'000, confirm_tracked_graph(fx.dir / "out"));
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    // Sent once the confirmation set is seeded.
    auto commit = fx.worker->await_frame(MessageKind::CommitCheckpoint);
    ASSERT_TRUE(commit.has_value());
    ASSERT_EQ(decode_commit_checkpoint(*commit).checkpoint_id, *first);

    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kCoordinatorBeforeConfirmedMarker,
                           .ordinal = 1,
                           .action = clink::fault::Action::Block}};
    // Every deployed task confirms; the untracked ones are ignored.
    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_commit_confirmed(job_id, *first, role, subtask));
    }
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorBeforeConfirmedMarker) >= 1;
    })) << "the confirmed-marker write was never reached; is the job tracked?";
    const auto parked_at = std::chrono::steady_clock::now();

    const auto confirmed_marker =
        fx.dir / "_jobs" / std::to_string(job_id) / ("CONFIRMED-" + std::to_string(*first));
    EXPECT_TRUE(heartbeats_answered_throughout({fx.worker.get()}, kPastTheHeartbeatTimeout))
        << "a CONFIRMED marker write held the worker's heartbeats";
    EXPECT_FALSE(lost_within(*fx.coordinator, "w", parked_at, kPastTheHeartbeatTimeout))
        << "a live worker was declared lost while its confirmation's marker write was out";
    EXPECT_FALSE(std::filesystem::exists(confirmed_marker));
    EXPECT_EQ(fx.coordinator->latest_confirmed_checkpoint(job_id), 0U)
        << "the confirmed restore point moved before its marker was durable";

    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeConfirmedMarker),
              1U);
    EXPECT_TRUE(ckpt_await([&] { return std::filesystem::exists(confirmed_marker); }))
        << "no CONFIRMED marker once the write was released";
    EXPECT_TRUE(
        ckpt_await([&] { return fx.coordinator->latest_confirmed_checkpoint(job_id) == *first; }));
    EXPECT_FALSE(worker_was_lost(*fx.coordinator, "w"));
}

// A job's completion records are written under mu_. Held past heartbeat_timeout,
// every worker's heartbeats are still answered: the one whose exit completed the
// job, parked in the write, and an idle one that hosts nothing. The watchdog's
// own judgement is covered by its self-pause rule; this is about the workers'
// leases, which nothing on the coordinator's side protects.
TEST(CheckpointCompletion, AJobCompletionRecordHeldUnderTheLockStillAnswersEveryWorkersHeartbeat) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto ha_dir = fx.dir / "ha";
    fx.coordinator->set_ha_dir(ha_dir.string());
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    ASSERT_TRUE(first_checkpoint_with_next_on_record(fx, job_id).has_value());
    // Registered after the deploy, so it hosts nothing.
    FakeWorker idle(fx.port, "w2");
    ASSERT_TRUE(idle.valid());
    ASSERT_TRUE(idle.register_and_ack());
    // The next fenced metadata write is the job's history record: the claimer
    // is parked with the next id on record, the interval is ten minutes, and
    // the submit-time manifest write is behind us. Declared after the fixture
    // and the idle worker, so it is reset first and releases the write, which
    // holds mu_, before either joins anything.
    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kCoordinatorBeforeMetadataWrite,
                           .ordinal = 1,
                           .action = clink::fault::Action::Block}};

    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_finished(job_id, role, subtask));
    }
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorBeforeMetadataWrite) >= 1;
    })) << "the job's completion record was never written";

    EXPECT_TRUE(heartbeats_answered_throughout({fx.worker.get(), &idle}, 3200ms))
        << "a completion record written under the lock held the workers' heartbeats";

    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeMetadataWrite),
              1U);
    EXPECT_TRUE(fx.coordinator->await_job_completion(job_id, 5s));
    EXPECT_TRUE(fx.coordinator->lost_workers().empty());
    EXPECT_TRUE(std::filesystem::exists(ha_dir / "history" / (std::to_string(job_id) + ".json")))
        << "the held write was not the job's history record";
}

// A frame that does not decode still costs the worker its connection, now that
// it is decoded on the dispatch thread: the reader stops reading, and so stops
// stamping, even while the worker goes on heartbeating.
TEST(CheckpointCompletion, AMalformedFrameStillCostsTheWorkerItsConnection) {
    CheckpointFixture fx;
    const auto job_id = fx.bring_up();
    ASSERT_GT(job_id, 0U);
    const auto before = malformed_frames();
    SubtaskCheckpointedMsg m;
    m.job_id = job_id;
    m.checkpoint_id = 1;
    m.role = "src";
    auto frame = encode_frame(MessageKind::SubtaskCheckpointed, m);
    frame.resize(3);  // the kind byte and two bytes of an eight-byte job id
    ASSERT_TRUE(fx.worker->send_raw(frame));
    EXPECT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{3500})))
        << "a worker that sent a frame that does not decode kept its connection";
    EXPECT_GT(malformed_frames(), before);
}

namespace {

// The frames queued for one connection are bounded. Overflowing either bound
// is handled as a malformed frame: the connection is given up, and nothing
// queued is handled.
void flood_behind_a_held_dispatch(Coordinator::Config cfg) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx(cfg);
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = kFinalRequestPoint, .action = clink::fault::Action::Observe});
    const auto before = malformed_frames();

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    const auto& [role, subtask] = fx.deployed().front();
    for (int i = 0; i < 200; ++i) {
        if (!fx.worker->request_final_checkpoint(job_id, role, subtask)) {
            break;  // the coordinator stopped reading and the buffers filled
        }
    }
    EXPECT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{3500})))
        << "a connection whose dispatch backlog overflowed kept its worker";
    EXPECT_GT(malformed_frames(), before) << "the overflow was not counted as a dropped frame";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    EXPECT_FALSE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kFinalRequestPoint) > 0; }, 500ms))
        << "frames from an overflowed backlog were handled";
}

}  // namespace

TEST(CheckpointCompletion, AWorkerFloodingFramesBehindAHeldDispatchLosesItsConnection) {
    Coordinator::Config cfg;
    cfg.max_worker_dispatch_backlog_frames = 32;
    flood_behind_a_held_dispatch(cfg);
}

TEST(CheckpointCompletion, AWorkerWhoseQueuedFramesOutgrowTheByteBoundLosesItsConnection) {
    Coordinator::Config cfg;
    cfg.max_worker_dispatch_backlog_bytes = 1024;
    flood_behind_a_held_dispatch(cfg);
}

// A same-id re-registration retires the old session once its dispatch thread
// has handled every frame the session's reader read: they were sent before the
// re-registration, so they are handled first, as they were when the reader ran
// the handlers itself. The retirement runs on the new session's dispatch
// thread, so while the old session's dispatch waits on the store, the new
// session's heartbeats are answered and it is not declared lost. Then the
// subtasks the old session still hosted are redeployed onto the new one.
TEST(CheckpointCompletion, FramesQueuedBehindASupersededSessionAreHandledBeforeItIsRetired) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = kFinalRequestPoint, .action = clink::fault::Action::Observe});

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    const auto& [role, subtask] = fx.deployed().front();
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    }
    // Answered by the reader, so every frame before it has been read and queued.
    ASSERT_TRUE(fx.worker->send_heartbeat(7));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(7))
        << "the reader did not read past a dispatch held on the store";

    const auto since_ms = log_cursor_ms();
    const auto retired = [&] {
        for (const auto& rec :
             LogBuffer::global().tail(1000, "info", since_ms, "coordinator.register")) {
            if (rec.message.find("worker=w re-registered") != std::string::npos) {
                return true;
            }
        }
        return false;
    };
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    const auto registered_at = std::chrono::steady_clock::now();
    EXPECT_TRUE(heartbeats_answered_throughout({&successor}, kPastTheHeartbeatTimeout))
        << "the new session's heartbeats waited on the old session's dispatch";
    EXPECT_FALSE(lost_within(*fx.coordinator, "w", registered_at, kPastTheHeartbeatTimeout))
        << "the new session was declared lost while the old one's dispatch was held";
    EXPECT_FALSE(retired()) << "the old session was retired while its dispatch was still running";
    EXPECT_EQ(clink::fault::Registry::instance().hits(kFinalRequestPoint), 0U);

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    EXPECT_TRUE(ckpt_await(retired, 5s)) << "the old session was never retired";
    EXPECT_GE(clink::fault::Registry::instance().hits(kFinalRequestPoint), 3U)
        << "frames the superseded session sent before the re-registration were dropped";
    EXPECT_TRUE(successor.await_frame(MessageKind::Deploy, 5s).has_value())
        << "the superseded session's subtasks were not redeployed onto its successor";
}

// A subtask whose SubtaskFinished the old session had read, but not yet
// handled, when the worker re-registered finished cleanly: it must not be
// folded into a restart as lost. Here the whole job had finished, and with no
// restart budget a fold would have failed it.
TEST(CheckpointCompletion, AFinishQueuedBehindASupersededSessionStillCompletesTheJob) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_finished(job_id, role, subtask));
    }
    ASSERT_TRUE(fx.worker->send_heartbeat(9));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(9))
        << "the reader did not read past a dispatch held on the store";

    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    EXPECT_FALSE(fx.coordinator->await_job_completion(job_id, 300ms))
        << "the job completed while the frames reporting it were still queued";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(fx.coordinator->await_job_completion(job_id, 5s));
    const auto errors = fx.coordinator->job_errors(job_id);
    EXPECT_TRUE(errors.empty()) << "a job whose every subtask reported a clean finish failed: "
                                << (errors.empty() ? std::string{} : errors.front());
    EXPECT_FALSE(successor.await_frame(MessageKind::Deploy, 500ms).has_value())
        << "a job that had finished was redeployed";
}

// A CONFIRMED marker write that outlasts a restart's redeploy belongs to the
// run before it. The marker lands, but the new run's confirmed restore point
// does not move and the protocol trace records no WriteConfirmed after the
// Redeploy: the specification's Redeploy forgets every broadcast checkpoint,
// so it has no such step.
TEST(CheckpointCompletion, AConfirmedMarkerLandingAfterARedeployLeavesTheNewRunAlone) {
    const auto trace_dir = std::filesystem::temp_directory_path() /
                           ("clink_ckpt_late_confirm_trace_" + std::to_string(::getpid()));
    std::filesystem::remove_all(trace_dir);
    std::filesystem::create_directories(trace_dir);
    ::setenv("CLINK_PROTOCOL_TRACE_DIR", trace_dir.c_str(), 1);
    clink::protocol_trace::reset_for_tests();
    struct TraceOff {
        std::filesystem::path dir;
        ~TraceOff() {
            ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
            clink::protocol_trace::reset_for_tests();
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } trace_off{trace_dir};

    clink::cluster::ensure_built_ins_registered();
    const auto* file_2pc = clink::connectors::CapabilityRegistry::instance().find("file_2pc");
    ASSERT_NE(file_2pc, nullptr) << "built-in capability records not declared";
    auto flagged = *file_2pc;
    flagged.commit_recoverable = false;
    CompletionScopedRecordOverride tracked(std::move(flagged));
    clink::fault::Registry::instance().reset();

    CheckpointFixture fx;
    const auto job_id = fx.bring_up(
        /*max_restarts=*/1, /*interval_ms=*/600'000, confirm_tracked_graph(fx.dir / "out"));
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::CommitCheckpoint).has_value());
    // Registered after the deploy, so it hosts nothing and is where the
    // restart redeploys.
    FakeWorker spare(fx.port, "w2");
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());

    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kCoordinatorBeforeConfirmedMarker,
                           .ordinal = 1,
                           .action = clink::fault::Action::Block}};
    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_commit_confirmed(job_id, *first, role, subtask));
    }
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorBeforeConfirmedMarker) >= 1;
    })) << "the confirmed-marker write was never reached; is the job tracked?";

    // The worker dies with the write out. Its reader stops stamping, the
    // watchdog declares it lost, and the restart redeploys onto the spare.
    fx.worker->close();
    ASSERT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{5000})));
    ASSERT_TRUE(spare.await_frame(MessageKind::Deploy, 20s).has_value())
        << "the restart never redeployed";
    const auto confirmed_at_redeploy = fx.coordinator->latest_confirmed_checkpoint(job_id);

    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeConfirmedMarker),
              1U);
    const auto confirmed_marker =
        fx.dir / "_jobs" / std::to_string(job_id) / ("CONFIRMED-" + std::to_string(*first));
    ASSERT_TRUE(ckpt_await([&] { return std::filesystem::exists(confirmed_marker); }))
        << "no CONFIRMED marker once the write was released";
    const auto late_advance = [&] {
        return fx.coordinator->latest_confirmed_checkpoint(job_id) != confirmed_at_redeploy;
    };
    EXPECT_FALSE(ckpt_await(late_advance, 500ms))
        << "a confirmation of the previous run moved the new run's restore point";

    bool redeployed = false;
    bool confirmed_after_redeploy = false;
    const auto job_field = "\"job\":" + std::to_string(job_id) + ",";
    for (const auto& entry : std::filesystem::directory_iterator(trace_dir)) {
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.find(job_field) == std::string::npos) {
                continue;
            }
            if (line.find("\"event\":\"Redeploy\"") != std::string::npos) {
                redeployed = true;
            } else if (redeployed &&
                       line.find("\"event\":\"WriteConfirmed\"") != std::string::npos) {
                confirmed_after_redeploy = true;
            }
        }
    }
    EXPECT_TRUE(redeployed) << "the restart left no Redeploy event in the protocol trace";
    EXPECT_FALSE(confirmed_after_redeploy)
        << "the trace has a WriteConfirmed after the Redeploy, which the specification forbids";
}

namespace {

// True once the log records that worker "w"'s previous session was retired,
// counting only records from `since_ms` on.
bool previous_session_retired(std::int64_t since_ms) {
    for (const auto& rec :
         LogBuffer::global().tail(1000, "info", since_ms, "coordinator.register")) {
        if (rec.message.find("worker=w re-registered") != std::string::npos) {
            return true;
        }
    }
    return false;
}

// Whether the protocol trace under `dir` records a WriteConfirmed for `job_id`
// after that job's Redeploy, and whether it records the Redeploy at all.
struct ConfirmAfterRedeploy {
    bool redeployed{false};
    bool confirmed_after{false};
};
ConfirmAfterRedeploy scan_trace_for_confirm_after_redeploy(const std::filesystem::path& dir,
                                                           JobId job_id) {
    ConfirmAfterRedeploy out;
    const auto job_field = "\"job\":" + std::to_string(job_id) + ",";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.find(job_field) == std::string::npos) {
                continue;
            }
            if (line.find("\"event\":\"Redeploy\"") != std::string::npos) {
                out.redeployed = true;
            } else if (out.redeployed &&
                       line.find("\"event\":\"WriteConfirmed\"") != std::string::npos) {
                out.confirmed_after = true;
            }
        }
    }
    return out;
}

}  // namespace

// Per worker id, frames are handled in one order across a re-registration: the
// superseded session's, then its retirement, then the new session's. A frame
// the new session sends while the old one's dispatch waits on the store is
// read at once (its heartbeats are answered) but handled only once the old
// session has been retired.
TEST(CheckpointCompletion, ASuccessorsFramesAreHandledOnlyAfterItsPredecessorIsRetired) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = kFinalRequestPoint, .ordinal = 1, .action = clink::fault::Action::Block});

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(successor.request_final_checkpoint(job_id, role, subtask));
    // Answered by the new session's reader, so the request before it is read.
    ASSERT_TRUE(successor.send_heartbeat(5));
    ASSERT_TRUE(successor.await_heartbeat_ack(5))
        << "the new session's reader waited on the old session's dispatch";
    EXPECT_FALSE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kFinalRequestPoint) > 0; }, 500ms))
        << "the new session's frame was handled while the old session's dispatch still ran";
    EXPECT_FALSE(previous_session_retired(since_ms));

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kFinalRequestPoint) >= 1; }, 5s))
        << "the new session's frame was never handled";
    // Its handling is parked at the point: whatever came before it has happened.
    EXPECT_TRUE(previous_session_retired(since_ms))
        << "the new session's frame was handled before the old session was retired";
    EXPECT_EQ(clink::fault::Registry::instance().release(kFinalRequestPoint), 1U);
}

// The new session is schedulable from its RegisterAck, so a job can be placed on
// it while the old session's frames are still being handled. Retiring the old
// session folds only what that session held: the job on the new session runs
// on, and the old session's frames give back the old session's slots, not the
// new one's.
TEST(CheckpointCompletion, AJobPlacedOnTheSuccessorWhileThePredecessorRetiresIsNotFolded) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    // No restart budget: the fold fails the old session's job, which is
    // what it should do, and would fail the new session's job the same way.
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    ASSERT_GE(fx.deployed().size(), 2U) << "the test needs a subtask to finish and one to fold";
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    // Queued behind the held ack: one subtask of the old session finishes.
    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(fx.worker->send_finished(job_id, role, subtask));
    ASSERT_TRUE(fx.worker->send_heartbeat(4));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(4))
        << "the reader did not read past a dispatch held on the store";

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    // No checkpoint directory: a fold would fail it at once.
    const auto placed = fx.coordinator->submit_job(two_subtask_graph(fx.dir / "placed.txt"),
                                                   OperatorRegistry::default_instance(),
                                                   {},
                                                   CheckpointConfig{});
    ASSERT_GT(placed, 0U);
    auto deploy = successor.await_frame(MessageKind::Deploy);
    ASSERT_TRUE(deploy.has_value()) << "the job was not placed on the new session";
    const auto placed_tasks = decode_deploy(*deploy).tasks;
    ASSERT_FALSE(placed_tasks.empty());

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(ckpt_await([&] { return previous_session_retired(since_ms); }, 5s))
        << "the old session was never retired";
    // The retirement folded the old session's job, with no budget to restart it.
    ASSERT_TRUE(fx.coordinator->await_job_completion(job_id, 5s));
    EXPECT_FALSE(fx.coordinator->job_errors(job_id).empty())
        << "the old session's unfinished subtask was not folded";

    const auto errors = fx.coordinator->job_errors(placed);
    EXPECT_TRUE(errors.empty()) << "a job placed on the new session was folded as lost: "
                                << (errors.empty() ? std::string{} : errors.front());
    EXPECT_FALSE(fx.coordinator->await_job_completion(placed, 300ms))
        << "a job placed on the new session ended without any of its subtasks finishing";
    EXPECT_EQ(fx.coordinator->free_slots(), 4U - placed_tasks.size())
        << "the old session's finished subtask gave back a slot the new session's job holds";

    for (const auto& t : placed_tasks) {
        ASSERT_TRUE(successor.send_finished(placed, t.role, t.subtask_idx));
    }
    ASSERT_TRUE(fx.coordinator->await_job_completion(placed, 5s));
    EXPECT_TRUE(fx.coordinator->job_errors(placed).empty());
    EXPECT_EQ(fx.coordinator->free_slots(), 4U);
}

// A worker that registers again while an earlier session of it is still being
// retired is refused, retryably, so superseded sessions never queue up behind
// one another. Once the retirement ends, it is admitted.
TEST(CheckpointCompletion, AReRegistrationWhileAnEarlierSessionIsStillRetiringIsRefusedRetryably) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));

    FakeWorker second(fx.port, "w");
    ASSERT_TRUE(second.valid());
    ASSERT_TRUE(second.register_and_ack());
    FakeWorker third(fx.port, "w");
    ASSERT_TRUE(third.valid());
    const auto refused = third.register_reply();
    ASSERT_TRUE(refused.has_value()) << "no answer to the registration";
    EXPECT_FALSE(refused->ok)
        << "a session was admitted while an earlier one was still being retired";
    EXPECT_TRUE(refused->retryable) << "the refusal would make a supervised worker give up";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    // What a supervisor does with a retryable refusal: register again.
    std::unique_ptr<FakeWorker> fourth;
    EXPECT_TRUE(ckpt_await(
        [&] {
            fourth = std::make_unique<FakeWorker>(fx.port, "w");
            return fourth->valid() && fourth->register_and_ack();
        },
        5s))
        << "the worker was still refused once the retirement had ended";
}

namespace {

std::int64_t slots_in_use_gauge() {
    return MetricsRegistry::global().gauge(clink::metrics::kCoordinatorSlotsInUse).value();
}

// Releases `point` and joins `thread` on every exit path, in that order: a
// thread parked at a Block cannot be joined until the point is released.
struct ReleaseThenJoin {
    const char* point;
    std::thread& thread;
    ~ReleaseThenJoin() {
        (void)clink::fault::Registry::instance().release(point);
        if (thread.joinable()) {
            thread.join();
        }
    }
};

}  // namespace

// A submit places its tasks, then reads and writes the checkpoint store before
// it publishes the job and sends the Deploy. A worker that re-registers in
// between has its old session retired at once, before the job exists for the
// retirement to find. The tasks go to the new session, which takes over their
// slots. Stamped with the old session and deployed on its half-closed socket,
// the job never started and nothing ever folded it.
TEST(CheckpointCompletion, AJobPlacedJustBeforeItsWorkerReRegistersIsDeployedOnTheNewSession) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    fx.worker = std::make_unique<FakeWorker>(fx.port, "w");
    ASSERT_TRUE(fx.worker->valid());
    ASSERT_TRUE(fx.worker->register_and_ack());
    ASSERT_TRUE(fx.coordinator->await_registrations(2s));
    const auto in_use_before = slots_in_use_gauge();

    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorDeployAfterPlacement,
         .ordinal = 1,
         .action = clink::fault::Action::Block});
    std::atomic<JobId> placed{0};
    std::thread submitter([&] {
        try {
            placed = fx.coordinator->submit_job(two_subtask_graph(fx.dir / "placed.txt"),
                                                OperatorRegistry::default_instance(),
                                                {},
                                                CheckpointConfig{});
        } catch (const std::exception&) {
            // placed stays 0, which the test reports.
        }
    });
    ReleaseThenJoin join_submitter{clink::fault::points::kCoordinatorDeployAfterPlacement,
                                   submitter};
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorDeployAfterPlacement) >= 1;
    })) << "the submit never placed its tasks";

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    ASSERT_TRUE(ckpt_await([&] { return previous_session_retired(since_ms); }, 5s))
        << "the old session, with nothing queued, was not retired";

    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorDeployAfterPlacement),
              1U);
    submitter.join();
    ASSERT_GT(placed.load(), 0U) << "the submit failed";
    auto deploy = successor.await_frame(MessageKind::Deploy, 5s);
    ASSERT_TRUE(deploy.has_value())
        << "the job was deployed on the session its worker had replaced";
    const auto tasks = decode_deploy(*deploy).tasks;
    ASSERT_FALSE(tasks.empty());
    EXPECT_EQ(fx.coordinator->free_slots(), 4U - tasks.size())
        << "the new session does not hold the slots of the tasks deployed on it";
    EXPECT_EQ(slots_in_use_gauge() - in_use_before, static_cast<std::int64_t>(tasks.size()))
        << "the slots-in-use gauge drifted across the move";

    for (const auto& t : tasks) {
        ASSERT_TRUE(successor.send_finished(placed, t.role, t.subtask_idx));
    }
    ASSERT_TRUE(fx.coordinator->await_job_completion(placed, 5s));
    EXPECT_TRUE(fx.coordinator->job_errors(placed).empty());
    EXPECT_EQ(fx.coordinator->free_slots(), 4U);
    EXPECT_EQ(slots_in_use_gauge(), in_use_before);
}

// A subtask error from a superseded session is handled after its successor
// registered, and the per-subtask retry it decides goes to the successor. The
// subtask moves with it: stamped with the successor, so the old session's
// retirement does not fold it as lost while it runs again, and charged to the
// successor, whose slot its finish frees. Left stamped with the old session,
// the retirement failed the job as "worker lost" while the retried subtask ran.
TEST(CheckpointCompletion, ARetryDecidedOnASupersededSessionMovesItsSubtaskToTheSuccessor) {
    clink::fault::Registry::instance().reset();
    Coordinator::Config cfg;
    cfg.max_restarts = 1;  // the per-subtask retry's budget
    CheckpointFixture fx(cfg);
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());

    // A role task with no checkpoint directory: the only kind an error retries.
    JobPlan plan;
    PlannedTask task;
    task.role = "retried";
    task.subtask_idx = 0;
    plan.tasks.push_back(task);
    fx.coordinator->deploy(plan);
    auto first_deploy = fx.worker->await_frame(MessageKind::Deploy);
    ASSERT_TRUE(first_deploy.has_value());
    const auto retried_job = decode_deploy(*first_deploy).job_id;
    ASSERT_NE(retried_job, job_id);

    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    SubtaskFinishedMsg failure;
    failure.job_id = retried_job;
    failure.worker_id = "w";
    failure.role = "retried";
    failure.subtask_idx = 0;
    failure.had_error = true;
    failure.error_message = "injected subtask failure";
    ASSERT_TRUE(fx.worker->send_raw(encode_frame(MessageKind::SubtaskFinished, failure)));
    ASSERT_TRUE(fx.worker->send_heartbeat(3));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(3))
        << "the reader did not read past a dispatch held on the store";

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    auto retry = successor.await_frame(MessageKind::Deploy, 5s);
    ASSERT_TRUE(retry.has_value()) << "the failed subtask was not retried on the new session";
    ASSERT_EQ(decode_deploy(*retry).job_id, retried_job);
    ASSERT_TRUE(ckpt_await([&] { return previous_session_retired(since_ms); }, 5s))
        << "the old session was never retired";

    const auto errors = fx.coordinator->job_errors(retried_job);
    EXPECT_TRUE(errors.empty()) << "the retried subtask was folded as lost while it ran again: "
                                << (errors.empty() ? std::string{} : errors.front());
    EXPECT_FALSE(fx.coordinator->await_job_completion(retried_job, 300ms))
        << "the job ended while its retried subtask was running";
    EXPECT_EQ(fx.coordinator->free_slots(), 3U)
        << "the retried subtask's slot was not charged to the session running it";

    ASSERT_TRUE(successor.send_finished(retried_job, "retried", 0));
    ASSERT_TRUE(fx.coordinator->await_job_completion(retried_job, 5s));
    EXPECT_TRUE(fx.coordinator->job_errors(retried_job).empty());
    EXPECT_EQ(fx.coordinator->free_slots(), 4U);
}

// A subtask error from a superseded session, handled after the successor was
// itself declared lost while it retired its predecessor, is not retried: there
// is no live session to run it. The error completes the subtask and the job
// fails with it. Moved onto the lost successor, the subtask was stamped with a
// session whose loss fold had already run, which its predecessor's retirement
// does not fold either, and the job never completed.
TEST(CheckpointCompletion, ARetryDecidedAfterTheSuccessorWasLostCompletesTheSubtaskFailed) {
    clink::fault::Registry::instance().reset();
    Coordinator::Config cfg;
    cfg.max_restarts = 1;  // the per-subtask retry's budget
    CheckpointFixture fx(cfg);
    const auto in_use_before = slots_in_use_gauge();
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());

    // A role task with no checkpoint directory: the only kind an error retries.
    JobPlan plan;
    PlannedTask task;
    task.role = "retried";
    task.subtask_idx = 0;
    plan.tasks.push_back(task);
    fx.coordinator->deploy(plan);
    auto first_deploy = fx.worker->await_frame(MessageKind::Deploy);
    ASSERT_TRUE(first_deploy.has_value());
    const auto retried_job = decode_deploy(*first_deploy).job_id;
    ASSERT_NE(retried_job, job_id);

    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    SubtaskFinishedMsg failure;
    failure.job_id = retried_job;
    failure.worker_id = "w";
    failure.role = "retried";
    failure.subtask_idx = 0;
    failure.had_error = true;
    failure.error_message = "injected subtask failure";
    ASSERT_TRUE(fx.worker->send_raw(encode_frame(MessageKind::SubtaskFinished, failure)));
    ASSERT_TRUE(fx.worker->send_heartbeat(3));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(3))
        << "the reader did not read past a dispatch held on the store";

    // The worker re-registers, and the new session dies while its dispatch
    // still waits for the old session's frames.
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    successor.close();
    ASSERT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{5000})))
        << "the dead new session was never declared lost";
    EXPECT_FALSE(fx.coordinator->await_job_completion(retried_job, 100ms))
        << "the subtask's error was handled before the frame reporting it";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(fx.coordinator->await_job_completion(retried_job, 5s))
        << "the subtask was retried onto a lost session and nothing ever completed it";
    const auto errors = fx.coordinator->job_errors(retried_job);
    ASSERT_FALSE(errors.empty()) << "the job completed without the subtask's error";
    EXPECT_NE(errors.front().find("injected subtask failure"), std::string::npos) << errors.front();
    // The checkpointed job's subtasks go with the old session's retirement.
    ASSERT_TRUE(fx.coordinator->await_job_completion(job_id, 5s));
    EXPECT_TRUE(ckpt_await([&] { return slots_in_use_gauge() == in_use_before; }))
        << "the worker still holds " << slots_in_use_gauge() - in_use_before << " slot(s)";
}

namespace {

// Turns the protocol trace on into a fresh directory for one test, and off
// again, with the directory removed, whatever happens.
struct ScopedTraceDir {
    explicit ScopedTraceDir(const std::string& tag)
        : dir(std::filesystem::temp_directory_path() /
              ("clink_ckpt_trace_" + tag + "_" + std::to_string(::getpid()))) {
        std::filesystem::remove_all(dir);
        std::filesystem::create_directories(dir);
        ::setenv("CLINK_PROTOCOL_TRACE_DIR", dir.c_str(), 1);
        clink::protocol_trace::reset_for_tests();
    }
    ~ScopedTraceDir() {
        ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
        clink::protocol_trace::reset_for_tests();
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }
    ScopedTraceDir(const ScopedTraceDir&) = delete;
    ScopedTraceDir& operator=(const ScopedTraceDir&) = delete;
    ScopedTraceDir(ScopedTraceDir&&) = delete;
    ScopedTraceDir& operator=(ScopedTraceDir&&) = delete;
    std::filesystem::path dir;
};

// `job_id`'s protocol trace lines under `dir`, in the order written. Every
// event here comes from this process, so file order is emission order.
std::vector<std::string> job_trace_lines(const std::filesystem::path& dir, JobId job_id) {
    std::vector<std::string> out;
    const auto job_field = "\"job\":" + std::to_string(job_id) + ",";
    for (const auto& entry : std::filesystem::directory_iterator(dir)) {
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.find(job_field) != std::string::npos) {
                out.push_back(line);
            }
        }
    }
    return out;
}

bool trace_event_is(const std::string& line, const std::string& event) {
    return line.find("\"event\":\"" + event + "\"") != std::string::npos;
}

// An unsigned field of a trace line, if it carries one.
std::optional<std::uint64_t> trace_field(const std::string& line, const std::string& key) {
    const auto needle = "\"" + key + "\":";
    const auto at = line.find(needle);
    if (at == std::string::npos) {
        return std::nullopt;
    }
    std::uint64_t value = 0;
    bool any = false;
    for (auto i = at + needle.size(); i < line.size() && line[i] >= '0' && line[i] <= '9'; ++i) {
        value = value * 10 + static_cast<std::uint64_t>(line[i] - '0');
        any = true;
    }
    return any ? std::optional<std::uint64_t>{value} : std::nullopt;
}

// The file_2pc capability record with its commit declared non-recoverable, so
// the job runs the commit-confirmed restore protocol.
clink::connectors::ConnectorCapabilities tracked_file_2pc() {
    clink::cluster::ensure_built_ins_registered();
    const auto* file_2pc = clink::connectors::CapabilityRegistry::instance().find("file_2pc");
    if (file_2pc == nullptr) {
        throw std::runtime_error("built-in capability records not declared");
    }
    auto flagged = *file_2pc;
    flagged.commit_recoverable = false;
    return flagged;
}

}  // namespace

// A SubtaskFinished read from a session, and dispatched, before the watchdog
// declared that session lost, but handled only after the loss fold: the fold
// has already queued the subtask for the restart (or completed it, failed),
// so the finish is dropped in the handler's own hold. Handled, it reported a
// drain of a subtask the loss had killed, which the trace module cannot match,
// and in a job with no checkpoint it counted the subtask a second time.
TEST(CheckpointCompletion, AFinishHandledAfterItsSessionWasDeclaredLostIsDropped) {
    clink::fault::Registry::instance().reset();
    const ScopedTraceDir trace("finish_after_loss");
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    constexpr const char* kFinishPoint = clink::fault::points::kCoordinatorBeforeSubtaskFinished;
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kFinishPoint, .ordinal = 1, .action = clink::fault::Action::Block}};

    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(fx.worker->send_finished(job_id, role, subtask));
    ASSERT_TRUE(ckpt_await([&] {
        return clink::fault::Registry::instance().hits(kFinishPoint) >= 1;
    })) << "the finish never reached its handler";
    // The worker dies with its finish dispatched but not yet handled.
    fx.worker->close();
    ASSERT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{5000})))
        << "the dead worker was never declared lost";

    EXPECT_EQ(clink::fault::Registry::instance().release(kFinishPoint), 1U);
    // stop() joins the connection's dispatch thread, which finishes the frame
    // it is handling: every trace line the finish could emit is written.
    fx.coordinator->stop();
    const auto lines = job_trace_lines(trace.dir, job_id);
    const auto dies = std::find_if(lines.begin(), lines.end(), [](const std::string& l) {
        return trace_event_is(l, "WorkerDies");
    });
    ASSERT_NE(dies, lines.end()) << "the loss was not traced";
    for (auto it = dies; it != lines.end(); ++it) {
        EXPECT_FALSE(trace_event_is(*it, "SubtaskDrained"))
            << "a lost session's finish was handled after the loss fold: " << *it;
    }
}

// A submit's tasks go to their worker's new session when the worker
// re-registered between placement and publication, but only while that session
// has room for them. It is placeable from its RegisterAck, so another submit
// can fill it first. Moving the tasks anyway put four subtasks on a two-slot
// worker, and every free-slot sum then wrapped round to some four billion. The
// deploy is refused retryably instead, and gives its slots back.
TEST(CheckpointCompletion, AJobPlacedBeforeItsWorkerReRegistersIsRefusedWhenTheNewSessionIsFull) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    fx.worker = std::make_unique<FakeWorker>(fx.port, "w", /*slots=*/2);
    ASSERT_TRUE(fx.worker->valid());
    ASSERT_TRUE(fx.worker->register_and_ack());
    ASSERT_TRUE(fx.coordinator->await_registrations(2s));
    const auto in_use_before = slots_in_use_gauge();

    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorDeployAfterPlacement,
         .ordinal = 1,
         .action = clink::fault::Action::Block});
    std::atomic<JobId> placed{0};
    std::atomic<bool> refused_for_slots{false};
    std::thread submitter([&] {
        try {
            placed = fx.coordinator->submit_job(two_subtask_graph(fx.dir / "placed.txt"),
                                                OperatorRegistry::default_instance(),
                                                {},
                                                CheckpointConfig{});
        } catch (const Coordinator::InsufficientSlotsError&) {
            refused_for_slots = true;
        } catch (const std::exception&) {
            // Neither flag set, which the test reports.
        }
    });
    ReleaseThenJoin join_submitter{clink::fault::points::kCoordinatorDeployAfterPlacement,
                                   submitter};
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorDeployAfterPlacement) >= 1;
    })) << "the submit never placed its tasks";

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(fx.port, "w", /*slots=*/2);
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    ASSERT_TRUE(ckpt_await([&] { return previous_session_retired(since_ms); }, 5s))
        << "the old session, with nothing queued, was not retired";
    // Another submit takes every slot of the new session.
    const auto filler = fx.coordinator->submit_job(two_subtask_graph(fx.dir / "filler.txt"),
                                                   OperatorRegistry::default_instance(),
                                                   {},
                                                   CheckpointConfig{});
    ASSERT_GT(filler, 0U);
    auto filler_deploy = successor.await_frame(MessageKind::Deploy, 5s);
    ASSERT_TRUE(filler_deploy.has_value());
    ASSERT_EQ(decode_deploy(*filler_deploy).tasks.size(), 2U);

    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorDeployAfterPlacement),
              1U);
    submitter.join();
    EXPECT_EQ(placed.load(), 0U) << "the job was deployed onto a session with no room for it";
    EXPECT_TRUE(refused_for_slots.load())
        << "the deploy was not refused as a retryable lack of slots";
    EXPECT_EQ(fx.coordinator->free_slots(), 0U)
        << "the full session's free-slot count is wrong (over-charged and wrapped round?)";
    EXPECT_EQ(slots_in_use_gauge() - in_use_before, 2)
        << "the slots-in-use gauge counts more than the two tasks the worker runs";
    EXPECT_FALSE(successor.await_frame(MessageKind::Deploy, 300ms).has_value())
        << "a second job was deployed onto the full session";
}

// A per-subtask retry decided on a superseded session goes to the successor
// even when the successor is full, as a retry on a session never replaced
// does, and charges it past its capacity. Every free-slot sum counts such a
// session as having no free slots. Unguarded, the unsigned difference read as
// some four billion: admission let any submit through, and placement then
// failed it with a plain error instead of the retryable lack of slots.
TEST(CheckpointCompletion, ASessionChargedPastItsCapacityCountsAsHavingNoFreeSlots) {
    clink::fault::Registry::instance().reset();
    Coordinator::Config cfg;
    cfg.max_restarts = 1;  // the per-subtask retry's budget
    CheckpointFixture fx(cfg);
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());

    JobPlan plan;
    PlannedTask task;
    task.role = "retried";
    task.subtask_idx = 0;
    plan.tasks.push_back(task);
    fx.coordinator->deploy(plan);
    auto first_deploy = fx.worker->await_frame(MessageKind::Deploy);
    ASSERT_TRUE(first_deploy.has_value());
    const auto retried_job = decode_deploy(*first_deploy).job_id;

    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    SubtaskFinishedMsg failure;
    failure.job_id = retried_job;
    failure.worker_id = "w";
    failure.role = "retried";
    failure.subtask_idx = 0;
    failure.had_error = true;
    failure.error_message = "injected subtask failure";
    ASSERT_TRUE(fx.worker->send_raw(encode_frame(MessageKind::SubtaskFinished, failure)));
    ASSERT_TRUE(fx.worker->send_heartbeat(8));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(8))
        << "the reader did not read past a dispatch held on the store";

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(fx.port, "w", /*slots=*/1);
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    // The successor's only slot is taken before the retry is decided.
    JobPlan filler_plan;
    PlannedTask filler_task;
    filler_task.role = "filler";
    filler_task.subtask_idx = 0;
    filler_plan.tasks.push_back(filler_task);
    fx.coordinator->deploy(filler_plan);
    ASSERT_TRUE(successor.await_frame(MessageKind::Deploy, 5s).has_value());
    ASSERT_EQ(fx.coordinator->free_slots(), 0U);

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    auto retry = successor.await_frame(MessageKind::Deploy, 5s);
    ASSERT_TRUE(retry.has_value()) << "the failed subtask was not retried on the new session";
    ASSERT_EQ(decode_deploy(*retry).job_id, retried_job);
    ASSERT_TRUE(ckpt_await([&] { return previous_session_retired(since_ms); }, 5s))
        << "the old session was never retired";

    EXPECT_EQ(fx.coordinator->free_slots(), 0U)
        << "a session charged past its capacity reported free slots";
    EXPECT_THROW((void)fx.coordinator->submit_job(two_subtask_graph(fx.dir / "admitted.txt"),
                                                  OperatorRegistry::default_instance(),
                                                  {},
                                                  CheckpointConfig{}),
                 Coordinator::InsufficientSlotsError)
        << "a submit to a full cluster was not refused as a retryable lack of slots";
}

// A session lost while it is still retiring its predecessor folds only what it
// held itself. The predecessor's subtasks wait for the predecessor's queued
// frames, or for its retirement once those are handled. Folded with the lost
// session, they were redeployed at once, and the predecessor's queued
// SubtaskFinished frames were then counted against the new run: here they
// completed it while every redeployed subtask was still running.
TEST(CheckpointCompletion,
     ASessionLostWhileRetiringItsPredecessorLeavesThePredecessorsSubtasksToIt) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    // Registered after the deploy, so it hosts nothing and is where the
    // restart redeploys.
    FakeWorker spare(fx.port, "w2");
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());

    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    // Queued behind the held write: every subtask of the old session finishes.
    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_finished(job_id, role, subtask));
    }
    ASSERT_TRUE(fx.worker->send_heartbeat(6));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(6))
        << "the reader did not read past a dispatch held on the store";

    // The worker re-registers, and the new session dies while its dispatch
    // still waits for the old session's frames.
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    successor.close();
    ASSERT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{5000})))
        << "the dead new session was never declared lost";
    EXPECT_FALSE(spare.await_frame(MessageKind::Deploy, 500ms).has_value())
        << "the old session's subtasks were redeployed while the frames reporting them were "
           "still queued";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    auto redeploy = spare.await_frame(MessageKind::Deploy, 20s);
    ASSERT_TRUE(redeploy.has_value())
        << "the restart never redeployed once the old session's frames had drained it";
    EXPECT_FALSE(fx.coordinator->await_job_completion(job_id, 500ms))
        << "the old session's finishes were counted against the redeployed run";
    for (const auto& t : decode_deploy(*redeploy).tasks) {
        ASSERT_TRUE(spare.send_finished(job_id, t.role, t.subtask_idx));
    }
    ASSERT_TRUE(fx.coordinator->await_job_completion(job_id, 5s));
    const auto errors = fx.coordinator->job_errors(job_id);
    EXPECT_TRUE(errors.empty()) << (errors.empty() ? std::string{} : errors.front());
}

// A restart held for in-doubt resolution walks up from the confirmed restore
// point its stage reported (RestartProceeds), which is where the
// specification's walk starts. A confirmation's advance landing between the
// stage and the resolution thread taking the job used to move the walk's
// start: here the walk began past the only completed checkpoint and walked
// nothing, where the specification walks it.
TEST(CheckpointCompletion, AHeldResolutionWalksFromThePointItsStageReported) {
    const ScopedTraceDir trace("held_walk_start");
    CompletionScopedRecordOverride tracked(tracked_file_2pc());
    clink::fault::Registry::instance().reset();

    CheckpointFixture fx;
    const auto job_id = fx.bring_up(
        /*max_restarts=*/1, /*interval_ms=*/600'000, confirm_tracked_graph(fx.dir / "out"));
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::CommitCheckpoint).has_value());
    FakeWorker spare(fx.port, "w2");
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());

    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kCoordinatorBeforeConfirmedMarker,
                           .ordinal = 1,
                           .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorBeforeInDoubtWalk,
         .ordinal = 1,
         .action = clink::fault::Action::Block});
    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_commit_confirmed(job_id, *first, role, subtask));
    }
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorBeforeConfirmedMarker) >= 1;
    })) << "the confirmed-marker write was never reached; is the job tracked?";

    // The worker dies with the write out: the restart drains at once and is
    // held for resolution, which waits at its fault point.
    fx.worker->close();
    ASSERT_TRUE(ckpt_await(
        [] {
            return clink::fault::Registry::instance().hits(
                       clink::fault::points::kCoordinatorBeforeInDoubtWalk) >= 1;
        },
        clink::test_support::scale_slack(std::chrono::milliseconds{5000})))
        << "the restart was not held for in-doubt resolution";
    // The confirmation's write and advance land before the walk starts.
    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeConfirmedMarker),
              1U);
    ASSERT_TRUE(ckpt_await([&] {
        return fx.coordinator->latest_confirmed_checkpoint(job_id) == *first;
    })) << "the confirmation did not advance the restore point while the restart was held";
    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeInDoubtWalk),
              1U);
    ASSERT_TRUE(spare.await_frame(MessageKind::Deploy, 20s).has_value())
        << "the restart never redeployed";

    const auto lines = job_trace_lines(trace.dir, job_id);
    std::optional<std::uint64_t> staged_from;
    std::optional<std::uint64_t> walked_first;
    for (const auto& line : lines) {
        if (!staged_from.has_value()) {
            if (trace_event_is(line, "RestartProceeds")) {
                staged_from = trace_field(line, "confirmed");
            }
            continue;
        }
        if (line.find("\"event\":\"Walk") != std::string::npos) {
            walked_first = trace_field(line, "ckpt");
            break;
        }
    }
    ASSERT_TRUE(staged_from.has_value()) << "no RestartProceeds in the protocol trace";
    EXPECT_LT(*staged_from, *first);
    ASSERT_TRUE(walked_first.has_value())
        << "the walk visited no checkpoint: it started past the point its stage reported";
    EXPECT_EQ(*walked_first, *staged_from + 1)
        << "the walk did not start above the point its stage reported";
}

// A restart held for in-doubt resolution walks once. When the walk has
// answered and the redeploy finds too few free slots, the restart waits for
// capacity alone: no second RestartProceeds and no second walk, and the job
// fails once its capacity deadline passes. Staged again on every watchdog
// tick, the walk re-ran each time, the trace recorded a RestartProceeds the
// specification has no step for, and each stage reset the capacity clock, so
// the job never failed.
TEST(CheckpointCompletion, ARestartWaitingForCapacityAfterItsWalkDoesNotWalkAgain) {
    const ScopedTraceDir trace("walk_once");
    CompletionScopedRecordOverride tracked(tracked_file_2pc());
    clink::fault::Registry::instance().reset();
    Coordinator::Config cfg;
    cfg.restart_capacity_timeout = 1500ms;
    CheckpointFixture fx(cfg);
    const auto job_id = fx.bring_up(
        /*max_restarts=*/1, /*interval_ms=*/600'000, confirm_tracked_graph(fx.dir / "out"));
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::CommitCheckpoint).has_value());
    // Completed and never confirmed: the restart is held for the walk.
    ASSERT_EQ(fx.coordinator->latest_confirmed_checkpoint(job_id), 0U);

    // The only worker dies, so the walk's redeploy has nowhere to go.
    fx.worker->close();
    ASSERT_TRUE(fx.coordinator->await_job_completion(
        job_id, clink::test_support::scale_slack(std::chrono::milliseconds{10000})))
        << "the restart waited for capacity past its deadline without failing the job";
    const auto errors = fx.coordinator->job_errors(job_id);
    ASSERT_FALSE(errors.empty()) << "the job completed without the capacity failure";
    EXPECT_NE(errors.front().find("no slot available"), std::string::npos) << errors.front();

    std::size_t stages = 0;
    std::size_t decides = 0;
    for (const auto& line : job_trace_lines(trace.dir, job_id)) {
        stages += trace_event_is(line, "RestartProceeds") ? 1U : 0U;
        decides += trace_event_is(line, "WalkDecides") ? 1U : 0U;
    }
    EXPECT_EQ(stages, 1U) << "the restart was held for resolution more than once";
    EXPECT_EQ(decides, 1U) << "the walk ran more than once for one restart";
}

// A job resumed from its own checkpoints by a rerun reports, in its takeover
// line, the confirmed restore point it adopts. A CONFIRMED marker at or below
// the run base belongs to an earlier run: this run neither restores from it nor
// seeds its confirmed restore point with it, so it reports none. Reporting the
// marker anyway pinned the validator's reseeded restore point to another run's
// checkpoint.
TEST(CheckpointCompletion, AResumedRunReportsNoConfirmedPointFromTheRunBeforeItsBase) {
    CompletionScopedRecordOverride tracked(tracked_file_2pc());
    clink::fault::Registry::instance().reset();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_resume_confirmed_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{dir};
    const auto jobs = dir / "ckpt" / "_jobs" / "1";
    std::filesystem::create_directories(jobs);
    const auto write = [](const std::filesystem::path& p, const std::string& body) {
        std::ofstream out(p);
        out << body;
    };
    // An earlier run confirmed checkpoint 1 and finished cleanly.
    write(jobs / "COMPLETED-1", "job=1\ncheckpoint=1\ngeneration=1\nsubtasks=0\n");
    write(jobs / "CONFIRMED-1", "job=1\ncheckpoint=1\n");
    write(jobs / "FINISHED", "1");

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (dir / "ckpt").string();
    ckpt.interval_ms = 600'000;
    ckpt.track_runs = true;
    const auto submit_on_fresh_coordinator = [&]() -> JobId {
        Coordinator c;
        const auto port = c.start();
        struct Stop {
            Coordinator& c;
            ~Stop() { c.stop(); }
        } stop_coordinator{c};
        c.expect_workers({"w"});
        FakeWorker w(port, "w");
        if (!w.valid() || !w.register_and_ack() || !c.await_registrations(2s)) {
            return 0;
        }
        const auto id = c.submit_job(
            confirm_tracked_graph(dir / "out"), OperatorRegistry::default_instance(), {}, ckpt);
        w.close();
        return id;
    };
    // The next run starts afresh above it, completes checkpoint 2 and confirms
    // nothing before it is killed.
    ASSERT_EQ(submit_on_fresh_coordinator(), 1U);
    ASSERT_TRUE(std::filesystem::exists(jobs / "run-base")) << "the run recorded no base";
    write(jobs / "COMPLETED-2", "job=1\ncheckpoint=2\ngeneration=1\nsubtasks=0\n");

    // The rerun after the kill resumes that run.
    const ScopedTraceDir trace("resume_confirmed");
    ASSERT_EQ(submit_on_fresh_coordinator(), 1U);
    std::optional<std::uint64_t> reported;
    for (const auto& line : job_trace_lines(trace.dir, 1)) {
        if (trace_event_is(line, "CoordRecovers")) {
            reported = trace_field(line, "confirmed");
            break;
        }
    }
    ASSERT_TRUE(reported.has_value()) << "the resumed run left no CoordRecovers line";
    EXPECT_EQ(*reported, 0U)
        << "the takeover reported the earlier run's CONFIRMED marker as its confirmed point";
}

// A checkpoint whose run the job redeployed past while its COMPLETED marker
// was written is committed nowhere: its sinks were torn down, and the new run
// neither tracks nor commits it. Deciding in one hold and seeding and sending
// in later ones let a restart that began and redeployed in between have the old
// run's checkpoint committed on the new run's workers, after the Redeploy line.
TEST(CheckpointCompletion, ACheckpointWhoseRunWasRedeployedPastIsNotCommittedOnTheNewRun) {
    const ScopedTraceDir trace("redeployed_past");
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    FakeWorker spare(fx.port, "w2");
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());

    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kCoordinatorBeforeCommitBroadcast,
                           .ordinal = 1,
                           .action = clink::fault::Action::Block}};
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorBeforeCommitBroadcast) >= 1;
    })) << "the checkpoint never reached its commit broadcast";

    // The worker dies with the broadcast held; the restart redeploys.
    fx.worker->close();
    ASSERT_TRUE(spare.await_frame(MessageKind::Deploy, 20s).has_value())
        << "the restart never redeployed";
    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeCommitBroadcast),
              1U);
    EXPECT_FALSE(spare.await_frame(MessageKind::CommitCheckpoint, 500ms).has_value())
        << "the previous run's checkpoint was committed on the new run's worker";

    bool redeployed = false;
    bool broadcast_after = false;
    for (const auto& line : job_trace_lines(trace.dir, job_id)) {
        if (trace_event_is(line, "Redeploy")) {
            redeployed = true;
        } else if (redeployed && trace_event_is(line, "Broadcast")) {
            broadcast_after = true;
        }
    }
    EXPECT_TRUE(redeployed) << "the restart left no Redeploy event in the protocol trace";
    EXPECT_FALSE(broadcast_after)
        << "the trace has a Broadcast after the Redeploy, which the specification forbids";
}

// A COMPLETED marker whose put outlives a restart that redeploys lands on disk
// and moves nothing: the new run restored from a point the checkpoint is not
// part of, and its trace has no WriteCompleted after the Redeploy, which the
// specification has forgotten the checkpoint by. The advance and the line used
// to follow the put whatever had happened meanwhile, so the new run's restore
// point became the old run's checkpoint.
TEST(CheckpointCompletion, ACompletedMarkerLandingAfterARedeployLeavesTheNewRunAlone) {
    const ScopedTraceDir trace("completed_after_redeploy");
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/1, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    FakeWorker spare(fx.port, "w2");
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());

    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorAfterCompletedMarker,
         .action = clink::fault::Action::Observe});
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));

    // The worker dies with the put out; the restart redeploys from what
    // memory holds, which the put has not moved.
    fx.worker->close();
    ASSERT_TRUE(spare.await_frame(MessageKind::Deploy, 20s).has_value())
        << "the restart never redeployed";
    const auto completed_at_redeploy = fx.coordinator->latest_completed_checkpoint(job_id);
    ASSERT_LT(completed_at_redeploy, *first);

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorAfterCompletedMarker) >= 1;
    })) << "the held put never finished";
    EXPECT_TRUE(fx.marker_exists(job_id, *first)) << "the marker did not land on disk";
    EXPECT_EQ(fx.coordinator->latest_completed_checkpoint(job_id), completed_at_redeploy)
        << "the previous run's checkpoint became the new run's restore point";

    bool redeployed = false;
    bool written_after = false;
    for (const auto& line : job_trace_lines(trace.dir, job_id)) {
        if (trace_event_is(line, "Redeploy")) {
            redeployed = true;
        } else if (redeployed && trace_event_is(line, "WriteCompleted")) {
            written_after = true;
        }
    }
    EXPECT_TRUE(redeployed) << "the restart left no Redeploy event in the protocol trace";
    EXPECT_FALSE(written_after)
        << "the trace has a WriteCompleted after the Redeploy, which the specification forbids";
}

// A COMPLETED marker landing while the restart is held for in-doubt resolution
// is written and does not move the completed point: the stage fixed the walk's
// range at the completed point it reported, and the walk runs to it. Advanced
// under the walk, memory went past the point the walk stopped at, where the
// specification's walk, reading memory, went on.
TEST(CheckpointCompletion, ACompletedMarkerLandingDuringAHeldResolutionLeavesItsRangeAlone) {
    CompletionScopedRecordOverride tracked(tracked_file_2pc());
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(
        /*max_restarts=*/1, /*interval_ms=*/100, confirm_tracked_graph(fx.dir / "out"));
    ASSERT_GT(job_id, 0U);
    FakeWorker spare(fx.port, "w2");
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());

    // The first checkpoint completes and is never confirmed: the gap a
    // restart holds for.
    const auto good = fx.await_trigger();
    ASSERT_TRUE(good.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *good, /*ok=*/true));
    ASSERT_TRUE(ckpt_await([&] {
        return fx.coordinator->latest_completed_checkpoint(job_id) == *good;
    })) << "the first checkpoint never completed";
    const auto second = fx.await_trigger();
    ASSERT_TRUE(second.has_value());
    ASSERT_GT(*second, *good);

    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorAfterCompletedMarker,
         .action = clink::fault::Action::Observe});
    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorBeforeInDoubtWalk,
         .ordinal = 1,
         .action = clink::fault::Action::Block});
    ASSERT_TRUE(fx.ack_all(job_id, *second, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));

    // The worker dies with the second checkpoint's put out: the restart is held
    // for resolution of the first, and the walk waits at its fault point.
    fx.worker->close();
    ASSERT_TRUE(ckpt_await(
        [] {
            return clink::fault::Registry::instance().hits(
                       clink::fault::points::kCoordinatorBeforeInDoubtWalk) >= 1;
        },
        clink::test_support::scale_slack(std::chrono::milliseconds{5000})))
        << "the restart was not held for in-doubt resolution";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    ASSERT_TRUE(ckpt_await([] {
        return clink::fault::Registry::instance().hits(
                   clink::fault::points::kCoordinatorAfterCompletedMarker) >= 1;
    })) << "the held put never finished";
    EXPECT_TRUE(fx.marker_exists(job_id, *second)) << "the marker did not land on disk";
    EXPECT_EQ(fx.coordinator->latest_completed_checkpoint(job_id), *good)
        << "a completion moved the range of the walk its restart was held for";

    EXPECT_EQ(clink::fault::Registry::instance().release(
                  clink::fault::points::kCoordinatorBeforeInDoubtWalk),
              1U);
    ASSERT_TRUE(spare.await_frame(MessageKind::Deploy, 20s).has_value())
        << "the restart never redeployed";
}

// A CommitConfirmed for the previous run's checkpoint whose first hold comes
// after the restart's Redeploy finds nothing to drain: the restart forgot the
// old run's confirmation sets, as the specification's Redeploy forgets every
// broadcast checkpoint. Kept, the set drained, the marker was written, the new
// run's confirmed restore point moved, and the trace had a WriteConfirmed after
// the Redeploy.
TEST(CheckpointCompletion, AConfirmationFirstHandledAfterARedeployIsNotTheNewRunsConfirmation) {
    const auto trace_dir = std::filesystem::temp_directory_path() /
                           ("clink_ckpt_confirm_after_redeploy_" + std::to_string(::getpid()));
    std::filesystem::remove_all(trace_dir);
    std::filesystem::create_directories(trace_dir);
    ::setenv("CLINK_PROTOCOL_TRACE_DIR", trace_dir.c_str(), 1);
    clink::protocol_trace::reset_for_tests();
    struct TraceOff {
        std::filesystem::path dir;
        ~TraceOff() {
            ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
            clink::protocol_trace::reset_for_tests();
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } trace_off{trace_dir};

    clink::cluster::ensure_built_ins_registered();
    const auto* file_2pc = clink::connectors::CapabilityRegistry::instance().find("file_2pc");
    ASSERT_NE(file_2pc, nullptr) << "built-in capability records not declared";
    auto flagged = *file_2pc;
    flagged.commit_recoverable = false;
    CompletionScopedRecordOverride tracked(std::move(flagged));
    clink::fault::Registry::instance().reset();

    CheckpointFixture fx;
    const auto job_id = fx.bring_up(
        /*max_restarts=*/1, /*interval_ms=*/600'000, confirm_tracked_graph(fx.dir / "out"));
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    // Sent once the confirmation set for `first` is seeded.
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::CommitCheckpoint).has_value());

    // A subtask fails before any task has confirmed `first`. The job restarts
    // on the same worker, which stays registered and keeps its connection.
    const auto& [failed_role, failed_subtask] = fx.deployed().front();
    SubtaskFinishedMsg failure;
    failure.job_id = job_id;
    failure.worker_id = "w";
    failure.role = failed_role;
    failure.subtask_idx = failed_subtask;
    failure.had_error = true;
    failure.error_message = "injected subtask failure";
    ASSERT_TRUE(fx.worker->send_raw(encode_frame(MessageKind::SubtaskFinished, failure)));
    for (std::size_t i = 1; i < fx.deployed().size(); ++i) {
        const auto& [role, subtask] = fx.deployed()[i];
        ASSERT_TRUE(fx.worker->send_finished(job_id, role, subtask));  // the drain
    }
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::Deploy, 20s).has_value())
        << "the restart never redeployed";
    const auto confirmed_at_redeploy = fx.coordinator->latest_confirmed_checkpoint(job_id);

    // The previous run's tasks confirm `first` only now.
    clink::fault::Registry::instance().arm(
        {.point = clink::fault::points::kCoordinatorBeforeConfirmedMarker,
         .action = clink::fault::Action::Observe});
    clink::fault::Registry::instance().arm(
        {.point = kFinalRequestPoint, .action = clink::fault::Action::Observe});
    for (const auto& [role, subtask] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_commit_confirmed(job_id, *first, role, subtask));
    }
    // The connection's frames are handled one at a time, in the order sent, so
    // once this request is handled every confirmation before it has been, the
    // marker write and the advance of any set they drained included.
    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kFinalRequestPoint) >= 1; }, 5s))
        << "the frame sent behind the confirmations was never handled";
    EXPECT_EQ(clink::fault::Registry::instance().hits(
                  clink::fault::points::kCoordinatorBeforeConfirmedMarker),
              0U)
        << "the previous run's confirmations drained a set the restart should have forgotten";
    EXPECT_EQ(fx.coordinator->latest_confirmed_checkpoint(job_id), confirmed_at_redeploy)
        << "a confirmation of the previous run moved the new run's restore point";

    const auto trace = scan_trace_for_confirm_after_redeploy(trace_dir, job_id);
    EXPECT_TRUE(trace.redeployed) << "the restart left no Redeploy event in the protocol trace";
    EXPECT_FALSE(trace.confirmed_after)
        << "the trace has a WriteConfirmed after the Redeploy, which the specification forbids";
}

// stop() lets the frame a dispatch thread is handling finish, and abandons
// whatever is queued behind it.
TEST(CheckpointCompletion, StopAbandonsAWorkersQueuedFrames) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(/*max_restarts=*/0, /*interval_ms=*/600'000);
    ASSERT_GT(job_id, 0U);
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kCompletedMarkerPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    clink::fault::Registry::instance().arm(
        {.point = kFinalRequestPoint, .action = clink::fault::Action::Observe});

    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kCompletedMarkerPoint) >= 1; }));
    const auto& [role, subtask] = fx.deployed().front();
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    }
    ASSERT_TRUE(fx.worker->send_heartbeat(11));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(11))
        << "the reader did not read past a dispatch held on the store";

    // stop() runs on its own thread: it waits for the held dispatch. Declared
    // after `hold`, so on an early exit the hold is released before the join.
    std::thread stopper([&] { fx.coordinator->stop(); });
    struct JoinStopper {
        std::thread& t;
        ~JoinStopper() {
            clink::fault::Registry::instance().release();
            if (t.joinable()) {
                t.join();
            }
        }
    } join_stopper{stopper};
    // The listener closes after stop() has begun, so a refused connection
    // means the queued frames are now behind a stopping coordinator.
    ASSERT_TRUE(
        ckpt_await([&] { return network::connect_plain("127.0.0.1", fx.port) == nullptr; }, 5s))
        << "stop() never closed the listener";

    EXPECT_EQ(clink::fault::Registry::instance().release(kCompletedMarkerPoint), 1U);
    stopper.join();
    EXPECT_EQ(clink::fault::Registry::instance().hits(kFinalRequestPoint), 0U)
        << "stop() let a worker's queued frames be handled";
}

// --- what recovery restores from ----------------------------------------

// The marker is written flat at <checkpoint_dir>/COMPLETED-N. The recovery
// lookup (latest_completed_id_on_disk) reads
// <checkpoint_dir>/_jobs/<job_id>/COMPLETED-N. Those cannot both be right.
//
// A code reading says recovery must therefore always resolve to 0 and
// restore a job from scratch, throwing away every completed checkpoint.
// That is too severe a claim to make from reading, so this establishes it
// by running the real thing: complete a checkpoint under one coordinator,
// recover the job in a second, and read the restore point off the Deploy
// frame the new coordinator actually sends.
TEST(CheckpointCompletion, RecoveryRestoresFromTheLastCompletedCheckpoint) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_ckpt_recovery_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    const auto ha_dir = root / "ha";
    const auto ckpt_dir = root / "ckpt";
    std::filesystem::create_directories(ha_dir);
    std::filesystem::create_directories(ckpt_dir);

    JobId job_id = 0;
    std::uint64_t completed = 0;

    // --- first leader: run a job and complete a checkpoint ---
    {
        Coordinator a;
        a.set_ha_dir(ha_dir.string());
        const auto port = a.start();
        a.expect_workers({"w"});

        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(a.await_registrations(2s));

        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = ckpt_dir.string();
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = 0;
        job_id = a.submit_job(
            two_subtask_graph(root / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
        ASSERT_GT(job_id, 0U);

        auto deploy = w.await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value());
        const auto tasks = decode_deploy(*deploy).tasks;
        ASSERT_FALSE(tasks.empty());
        std::uint16_t port_seed = 41000;
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
        }

        auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
        ASSERT_TRUE(trigger.has_value());
        completed = decode_trigger_checkpoint(*trigger).checkpoint_id;
        ASSERT_GT(completed, 0U);
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, completed, t.role, t.subtask_idx, /*ok=*/true));
        }
        ASSERT_TRUE(ckpt_await([&] { return a.latest_completed_checkpoint(job_id) == completed; }))
            << "the checkpoint never completed, so there is nothing for recovery to find";

        // The premise, stated rather than assumed: this is where the
        // marker landed.
        ASSERT_TRUE(ckpt_await([&] {
            return std::filesystem::exists(written_marker_path(ckpt_dir, job_id, completed));
        })) << "no marker at "
            << written_marker_path(ckpt_dir, job_id, completed).string();

        w.close();
        a.stop();
    }

    // --- second leader: recover, and see what it restores from ---
    {
        Coordinator b;
        b.set_ha_dir(ha_dir.string());
        const auto port = b.start();
        b.expect_workers({"w"});

        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(b.await_registrations(2s));

        b.recover_persisted_jobs();

        auto deploy = w.await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value()) << "the recovered job was never deployed";
        const auto msg = decode_deploy(*deploy);

        EXPECT_EQ(msg.restore_from_checkpoint_id, completed)
            << "recovery restored the job from checkpoint " << msg.restore_from_checkpoint_id
            << " when checkpoint " << completed
            << " had completed and its marker is on disk. The marker is written to "
            << written_marker_path(ckpt_dir, job_id, completed).string()
            << " and the recovery lookup reads <checkpoint_dir>/_jobs/<job_id>/COMPLETED-N; every "
               "completed checkpoint is invisible to recovery and the job restarts from "
               "scratch.";

        w.close();
        b.stop();
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// A takeover before the job has completed a checkpoint of its own. The new
// leader used to take the job's own latest id, 0, as the restore point; the
// deploy lint refuses a restore directory with id 0, so the recovery threw and
// the job was dropped (and an operator resubmitting it lost the savepoint it
// was started from). It must deploy from the restore point the job was
// submitted with, or fresh when it had none, as a restart does.
namespace {

DeployMsg recovered_deploy_before_any_checkpoint(const std::string& tag,
                                                 const std::string& restore_from_dir,
                                                 std::uint64_t restore_from_checkpoint_id) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_ckpt_early_recovery_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    const auto ha_dir = root / "ha";
    const auto ckpt_dir = root / "ckpt";
    std::filesystem::create_directories(ha_dir);
    std::filesystem::create_directories(ckpt_dir);
    DeployMsg out;
    {
        Coordinator a;
        a.set_ha_dir(ha_dir.string());
        const auto port = a.start();
        a.expect_workers({"w"});
        FakeWorker w(port, "w");
        EXPECT_TRUE(w.valid());
        EXPECT_TRUE(w.register_and_ack());
        EXPECT_TRUE(a.await_registrations(2s));
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = ckpt_dir.string();
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = 0;
        ckpt.restore_from_dir = restore_from_dir;
        ckpt.restore_from_checkpoint_id = restore_from_checkpoint_id;
        const auto job_id = a.submit_job(
            two_subtask_graph(root / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
        EXPECT_GT(job_id, 0U);
        auto deploy = w.await_frame(MessageKind::Deploy);
        EXPECT_TRUE(deploy.has_value());
        if (deploy.has_value()) {
            // The premise: the first deploy carried the submitted restore point.
            EXPECT_EQ(decode_deploy(*deploy).restore_from_checkpoint_id,
                      restore_from_checkpoint_id);
        }
        // No checkpoint is ever acked: the leader dies before the job's first.
        w.close();
        a.stop();
    }
    {
        Coordinator b;
        b.set_ha_dir(ha_dir.string());
        const auto port = b.start();
        b.expect_workers({"w"});
        FakeWorker w(port, "w");
        EXPECT_TRUE(w.valid());
        EXPECT_TRUE(w.register_and_ack());
        EXPECT_TRUE(b.await_registrations(2s));
        b.recover_persisted_jobs();
        auto deploy = w.await_frame(MessageKind::Deploy);
        EXPECT_TRUE(deploy.has_value()) << "the new leader dropped the job instead of deploying it";
        if (deploy.has_value()) {
            out = decode_deploy(*deploy);
        }
        w.close();
        b.stop();
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    return out;
}

}  // namespace

TEST(CheckpointCompletion, RecoveryBeforeTheFirstCheckpointKeepsTheSubmittedRestorePoint) {
    const auto savepoint = std::filesystem::temp_directory_path() /
                           ("clink_ckpt_early_recovery_sp_" + std::to_string(::getpid()));
    std::filesystem::create_directories(savepoint);
    const auto msg = recovered_deploy_before_any_checkpoint("sp", savepoint.string(), 7);
    EXPECT_EQ(msg.restore_from_dir, savepoint.string());
    EXPECT_EQ(msg.restore_from_checkpoint_id, 7U)
        << "the recovered job did not resume from the savepoint it was submitted with";
    std::error_code ec;
    std::filesystem::remove_all(savepoint, ec);
}

// The takeover's protocol trace reports, as `confirmed`, the newest CONFIRMED
// marker in the job's own directory: what the specification reseeds the
// confirmed restore point from. A job that has confirmed nothing of its own
// restores from the savepoint it was submitted with, and reporting that
// savepoint's id made trace validation diverge at the takeover.
TEST(CheckpointCompletion, ATakeoverReportsItsOwnConfirmedMarkerNotTheSavepointItRestoresFrom) {
    const auto trace_dir = std::filesystem::temp_directory_path() /
                           ("clink_ckpt_takeover_confirmed_" + std::to_string(::getpid()));
    std::filesystem::remove_all(trace_dir);
    std::filesystem::create_directories(trace_dir);
    ::setenv("CLINK_PROTOCOL_TRACE_DIR", trace_dir.c_str(), 1);
    clink::protocol_trace::reset_for_tests();
    struct TraceOff {
        std::filesystem::path dir;
        ~TraceOff() {
            ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
            clink::protocol_trace::reset_for_tests();
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } trace_off{trace_dir};
    const auto savepoint = std::filesystem::temp_directory_path() /
                           ("clink_ckpt_takeover_confirmed_sp_" + std::to_string(::getpid()));
    std::filesystem::create_directories(savepoint);
    const auto msg = recovered_deploy_before_any_checkpoint("confirmed", savepoint.string(), 7);
    EXPECT_EQ(msg.restore_from_checkpoint_id, 7U) << "the premise: it restores from the savepoint";
    std::error_code ec;
    std::filesystem::remove_all(savepoint, ec);

    std::vector<std::string> recovers;
    for (const auto& entry : std::filesystem::directory_iterator(trace_dir)) {
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            if (line.find("\"event\":\"CoordRecovers\"") != std::string::npos) {
                recovers.push_back(line);
            }
        }
    }
    ASSERT_EQ(recovers.size(), 1U) << "the takeover was not traced once";
    EXPECT_NE(recovers.front().find("\"confirmed\":0"), std::string::npos)
        << "the takeover reported something other than the job's own confirmed marker: "
        << recovers.front();
}

TEST(CheckpointCompletion, RecoveryBeforeTheFirstCheckpointOfAFreshJobStartsItFresh) {
    const auto msg = recovered_deploy_before_any_checkpoint("fresh", "", 0);
    EXPECT_TRUE(msg.restore_from_dir.empty());
    EXPECT_EQ(msg.restore_from_checkpoint_id, 0U);
}

// --- what a takeover numbers its checkpoints from -----------------------
//
// The leader dies with a checkpoint's barrier sent and the checkpoint never
// answered. A worker that outlives it may still be writing that capture, at
// the path the new run's capture of the same id would take, so the new
// leader must number above it although nothing on disk names it yet: CI's
// trace validation caught a takeover reusing such an id. The fake worker
// writes no snapshot at all, which is that shape exactly, so every id it was
// sent is visible to the takeover only through the record.
namespace {

struct TakeoverNumbering {
    std::uint64_t completed{0};     // the last checkpoint the dead leader completed
    std::uint64_t highest_sent{0};  // the highest id it sent the worker a barrier for
    std::uint64_t restore{0};       // the takeover's restore point
    std::uint64_t first{0};         // the takeover's first checkpoint id
};

TakeoverNumbering takeover_after_an_unanswered_barrier(
    const std::string& tag,
    bool complete_one_first,
    const std::function<void(const std::filesystem::path&, JobId)>& before_takeover = {}) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_ckpt_takeover_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    const auto ha_dir = root / "ha";
    const auto ckpt_dir = root / "ckpt";
    std::filesystem::create_directories(ha_dir);
    std::filesystem::create_directories(ckpt_dir);
    TakeoverNumbering out;
    JobId job_id = 0;
    {
        Coordinator a;
        a.set_ha_dir(ha_dir.string());
        const auto port = a.start();
        a.expect_workers({"w"});
        FakeWorker w(port, "w");
        EXPECT_TRUE(w.valid());
        EXPECT_TRUE(w.register_and_ack());
        EXPECT_TRUE(a.await_registrations(2s));
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = ckpt_dir.string();
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = 0;
        job_id = a.submit_job(
            two_subtask_graph(root / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
        EXPECT_GT(job_id, 0U);
        std::vector<DeploymentTask> tasks;
        if (auto deploy = w.await_frame(MessageKind::Deploy); deploy.has_value()) {
            tasks = decode_deploy(*deploy).tasks;
        }
        EXPECT_FALSE(tasks.empty());
        std::uint16_t port_seed = 41200;
        for (const auto& t : tasks) {
            EXPECT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
        }
        if (auto trigger = w.await_frame(MessageKind::TriggerCheckpoint); trigger.has_value()) {
            out.highest_sent = decode_trigger_checkpoint(*trigger).checkpoint_id;
        }
        EXPECT_GT(out.highest_sent, 0U);
        if (complete_one_first && out.highest_sent > 0) {
            const auto id = out.highest_sent;
            for (const auto& t : tasks) {
                EXPECT_TRUE(w.ack_checkpoint(job_id, id, t.role, t.subtask_idx, /*ok=*/true));
            }
            EXPECT_TRUE(ckpt_await([&] { return a.latest_completed_checkpoint(job_id) == id; }));
            out.completed = a.latest_completed_checkpoint(job_id);
            // The next barrier reaches the worker and is never answered.
            if (auto next = w.await_frame(MessageKind::TriggerCheckpoint); next.has_value()) {
                out.highest_sent = decode_trigger_checkpoint(*next).checkpoint_id;
            }
        }
        w.close();
        a.stop();
        // Barriers the loop sent while the leader was going down count too.
        while (auto more = w.await_frame(MessageKind::TriggerCheckpoint, 5ms)) {
            out.highest_sent =
                std::max(out.highest_sent, decode_trigger_checkpoint(*more).checkpoint_id);
        }
    }
    if (before_takeover) {
        before_takeover(ckpt_dir, job_id);
    }
    {
        Coordinator b;
        b.set_ha_dir(ha_dir.string());
        const auto port = b.start();
        b.expect_workers({"w"});
        FakeWorker w(port, "w");
        EXPECT_TRUE(w.valid());
        EXPECT_TRUE(w.register_and_ack());
        EXPECT_TRUE(b.await_registrations(2s));
        b.recover_persisted_jobs();
        if (auto deploy = w.await_frame(MessageKind::Deploy); deploy.has_value()) {
            const auto msg = decode_deploy(*deploy);
            out.restore = msg.restore_from_checkpoint_id;
            std::uint16_t port_seed = 41300;
            for (const auto& t : msg.tasks) {
                EXPECT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
            }
        } else {
            ADD_FAILURE() << "the new leader did not redeploy the job";
        }
        if (auto trigger = w.await_frame(MessageKind::TriggerCheckpoint); trigger.has_value()) {
            out.first = decode_trigger_checkpoint(*trigger).checkpoint_id;
        } else {
            ADD_FAILURE() << "the recovered job never triggered a checkpoint";
        }
        w.close();
        b.stop();
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    return out;
}

}  // namespace

TEST(CheckpointCompletion, ATakeoverNumbersAboveABarrierTheDeadLeaderSent) {
    const auto n = takeover_after_an_unanswered_barrier("sent", /*complete_one_first=*/true);
    ASSERT_GT(n.completed, 0U);
    ASSERT_GT(n.highest_sent, n.completed);
    EXPECT_EQ(n.restore, n.completed);
    EXPECT_GT(n.first, n.highest_sent)
        << "the takeover restored from " << n.restore << " and numbered its first checkpoint "
        << n.first << ", but the dead leader had sent a barrier for " << n.highest_sent
        << "; a worker still writing that capture would put it beside this run's";
}

// The takeover has nothing of its own to restore, which used to skip the id
// floor altogether: the job numbered from 1 again.
TEST(CheckpointCompletion, ATakeoverBeforeAnyCompletedCheckpointNumbersAboveTheIdsSent) {
    const auto n = takeover_after_an_unanswered_barrier("fresh", /*complete_one_first=*/false);
    ASSERT_GT(n.highest_sent, 0U);
    EXPECT_EQ(n.restore, 0U);
    EXPECT_GT(n.first, n.highest_sent)
        << "a takeover with no checkpoint of its own numbered its first checkpoint " << n.first
        << " although the dead leader had sent a barrier for " << n.highest_sent;
}

// The record alone sets the floor when it stands above every marker and
// snapshot: a leader whose triggered checkpoints all died with it.
TEST(CheckpointCompletion, ATakeoverNumbersAboveTheRecordWhenItIsTheHighestIdOnDisk) {
    const auto n = takeover_after_an_unanswered_barrier(
        "record", /*complete_one_first=*/true, [](const std::filesystem::path& dir, JobId job) {
            std::ofstream(dir / clink::cluster::triggered_record_key(job), std::ios::trunc) << "50";
        });
    ASSERT_GT(n.completed, 0U);
    ASSERT_LT(n.highest_sent, 50U);
    EXPECT_EQ(n.first, 51U);
}

// --- what a takeover's redeploy restores from, and reports --------------
//
// A takeover of a job whose sinks need their commits confirmed resolves the
// completed but unconfirmed checkpoints before it redeploys, and a walk that
// proves their commits moves the restore point up to them. The deploy then
// restores from there, out of the job's own directory, and the protocol event
// that records the takeover's redeploy names the same point: the
// specification takes it from the walk's result, and the Kafka takeover
// traces diverged while the event named the point from before the walk.
namespace {

constexpr const char* kTakeoverResolver = "ckpt_completion_takeover";

// A sink's resume handle for a transaction that committed before the leader
// died, staged in the snapshot of participant `subtask` for checkpoint `ckpt`.
void stage_committed_handle(const std::filesystem::path& ckpt_dir,
                            std::uint32_t subtask,
                            std::uint64_t ckpt) {
    const std::string handle = std::string{"{\"resolver\":\""} + kTakeoverResolver +
                               "\",\"ckpt\":\"" + std::to_string(ckpt) + "\"}";
    const std::string key = std::string(clink::connectors::kTxnResumeStateKeyPrefix) + "sub0";
    auto sp = clink::state_processor::Savepoint::create();
    sp.backend().put_operator_state(clink::OperatorId{42},
                                    clink::StateBackend::KeyView{key.data(), key.size()},
                                    clink::StateBackend::ValueView{handle.data(), handle.size()});
    const auto sub_dir = std::filesystem::path(clink::state_dir_for(ckpt_dir.string(), 1, subtask));
    std::filesystem::create_directories(sub_dir);
    sp.write_to_file(sub_dir / ("checkpoint-" + std::to_string(ckpt) + ".snap"));
}

struct WalkedTakeover {
    std::uint64_t first{0};      // the dead leader's first completed checkpoint
    std::uint64_t completed{0};  // the dead leader's newest completed checkpoint
    std::filesystem::path checkpoint_dir;
    std::uint64_t deploy_restore{0};  // what the new leader's deploy restores
    std::string deploy_restore_dir;   // and from where
    bool deployed{false};
    std::int64_t event_restore{-1};    // what its Redeploy event reports, -1 for none
    std::int64_t event_completed{-1};  // what its CoordRecovers reports, -1 for none
    // The takeover's walk events, as (event, checkpoint), in the order written.
    std::vector<std::pair<std::string, std::uint64_t>> walk_events;
    // With restart_after_deploy: the restart's deploy, and what the trace
    // recorded after the takeover's Redeploy.
    bool restarted{false};
    std::uint64_t restart_restore{0};
    std::size_t stages_after_redeploy{0};   // RestartProceeds lines
    std::size_t decides_after_redeploy{0};  // WalkDecides lines at `completed`
};

struct TakeoverShape {
    bool first_confirmed{false};
    // The newest checkpoint's COMPLETED put is still out when the takeover
    // reads the markers, and lands before its walk.
    bool newest_lands_during_takeover{false};
    // No worker is registered when the takeover runs, so its recovery parks
    // for capacity and completes when one registers.
    bool park_for_capacity{false};
    // The resolver finds the newest checkpoint's transaction not committed.
    bool newest_refused{false};
    // The job's sinks re-run their commits after a crash: the manifest is
    // left unflagged, and the job restores from its newest COMPLETED marker.
    bool recoverable{false};
    // The newest checkpoint's COMPLETED put is still out when the takeover
    // reads the markers, and lands before it loads the job's plugins.
    bool newest_lands_after_marker_read{false};
    // With park_for_capacity: the newest checkpoint's COMPLETED put (and, for
    // a flagged job, its CONFIRMED one) is still out when the takeover reads
    // the markers, and lands while the recovery is parked.
    bool newest_lands_during_park{false};
    // Once the takeover has deployed, the job's only worker dies before the
    // new run completes a checkpoint, and a worker registering again takes
    // the restart's deploy. The job's plan then carries a sink whose commit
    // needs confirming, so the restart itself runs the protocol, not only the
    // takeover's reading of the manifest.
    bool restart_after_deploy{false};
};

// The dead leader completes two checkpoints of a job; the test then makes it
// a job whose sinks need their commits confirmed, stages a committed handle
// in both checkpoints, and marks the first one CONFIRMED when
// `first_confirmed`. The new leader's takeover walks what is unconfirmed.
WalkedTakeover takeover_through_the_walk(const std::string& tag, const TakeoverShape& shape) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_ckpt_walked_takeover_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    const auto ha_dir = root / "ha";
    const auto trace_dir = root / "trace";
    WalkedTakeover out;
    out.checkpoint_dir = root / "ckpt";
    std::filesystem::create_directories(ha_dir);
    std::filesystem::create_directories(trace_dir);
    std::filesystem::create_directories(out.checkpoint_dir);
    clink::connectors::TxnResumeRegistry::instance().register_resolver(
        kTakeoverResolver, [](const std::string&) {
            return clink::connectors::InDoubtResolution{true, "committed before the leader died"};
        });

    std::optional<CompletionScopedRecordOverride> tracked;
    if (shape.restart_after_deploy) {
        tracked.emplace(tracked_file_2pc());
    }
    JobId job_id = 0;
    std::uint64_t first_completed = 0;
    std::uint32_t sink_participant = 0;
    {
        Coordinator a;
        a.set_ha_dir(ha_dir.string());
        const auto port = a.start();
        a.expect_workers({"w"});
        FakeWorker w(port, "w");
        EXPECT_TRUE(w.valid());
        EXPECT_TRUE(w.register_and_ack());
        EXPECT_TRUE(a.await_registrations(2s));
        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = out.checkpoint_dir.string();
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = shape.restart_after_deploy ? 1 : 0;
        job_id = a.submit_job(shape.restart_after_deploy ? confirm_tracked_graph(root / "out")
                                                         : two_subtask_graph(root / "out.txt"),
                              OperatorRegistry::default_instance(),
                              {},
                              ckpt);
        EXPECT_GT(job_id, 0U);
        std::vector<DeploymentTask> tasks;
        if (auto deploy = w.await_frame(MessageKind::Deploy); deploy.has_value()) {
            tasks = decode_deploy(*deploy).tasks;
        }
        EXPECT_FALSE(tasks.empty());
        std::uint16_t port_seed = 41700;
        for (const auto& t : tasks) {
            EXPECT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
            sink_participant = std::max(sink_participant, t.subtask_idx);
        }
        for (int n = 0; n < 2; ++n) {
            auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
            if (!trigger.has_value()) {
                ADD_FAILURE() << "the leader triggered no checkpoint";
                break;
            }
            const auto id = decode_trigger_checkpoint(*trigger).checkpoint_id;
            for (const auto& t : tasks) {
                EXPECT_TRUE(w.ack_checkpoint(job_id, id, t.role, t.subtask_idx, /*ok=*/true));
            }
            EXPECT_TRUE(ckpt_await([&] { return a.latest_completed_checkpoint(job_id) == id; }));
            (n == 0 ? first_completed : out.completed) = id;
        }
        w.close();
        a.stop();
    }

    // The manifest as it reads for a job whose sinks need their commits
    // confirmed, a committed handle in each completed checkpoint, and the
    // first one confirmed if asked.
    const auto manifest = ha_dir / "jobs" / std::to_string(job_id) / "manifest.json";
    std::string body;
    {
        std::ifstream in(manifest);
        body.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    const std::string unflagged = "\"requires_commit_confirmation\":false";
    const auto at = body.find(unflagged);
    // A plan with a tracked sink is flagged already.
    EXPECT_EQ(at == std::string::npos, shape.restart_after_deploy) << body;
    if (at != std::string::npos && !shape.recoverable) {
        body.replace(at, unflagged.size(), "\"requires_commit_confirmation\":true");
        std::ofstream(manifest, std::ios::trunc) << body;
    }
    stage_committed_handle(out.checkpoint_dir, sink_participant, first_completed);
    stage_committed_handle(out.checkpoint_dir, sink_participant, out.completed);
    out.first = first_completed;
    if (shape.first_confirmed) {
        std::ofstream(out.checkpoint_dir / "_jobs" / std::to_string(job_id) /
                      ("CONFIRMED-" + std::to_string(first_completed)))
            << "job=" << job_id << "\ncheckpoint=" << first_completed << "\n";
    }
    if (shape.newest_refused) {
        const auto refused = "\"ckpt\":\"" + std::to_string(out.completed) + "\"";
        clink::connectors::TxnResumeRegistry::instance().register_resolver(
            kTakeoverResolver, [refused](const std::string& handle) {
                if (handle.find(refused) != std::string::npos) {
                    return clink::connectors::InDoubtResolution{false, "aborted by the broker"};
                }
                return clink::connectors::InDoubtResolution{true,
                                                            "committed before the leader died"};
            });
    }
    const auto newest_marker = out.checkpoint_dir / "_jobs" / std::to_string(job_id) /
                               ("COMPLETED-" + std::to_string(out.completed));
    const auto newest_in_flight = root / "newest-marker-in-flight";
    if (shape.newest_lands_during_takeover || shape.newest_lands_after_marker_read ||
        shape.newest_lands_during_park) {
        std::filesystem::rename(newest_marker, newest_in_flight);
    }

    ::setenv("CLINK_PROTOCOL_TRACE_DIR", trace_dir.c_str(), 1);
    clink::protocol_trace::reset_for_tests();
    {
        clink::fault::Registry::instance().reset();
        const char* land_at = nullptr;
        if (shape.newest_lands_during_takeover) {
            // The put lands once the takeover has read and reported the
            // markers, before it walks.
            land_at = clink::fault::points::kCoordinatorTakeoverBeforeWalk;
        } else if (shape.newest_lands_after_marker_read) {
            // The put lands once the takeover has read the markers, before it
            // loads the plugins and reports them.
            land_at = clink::fault::points::kCoordinatorTakeoverAfterMarkerRead;
        }
        std::optional<clink::fault::ScopedFault> hold;
        std::thread lander;
        if (land_at != nullptr) {
            hold.emplace(clink::fault::Rule{
                .point = land_at, .ordinal = 1, .action = clink::fault::Action::Block});
            lander = std::thread([&] {
                const bool reached = ckpt_await(
                    [&] { return clink::fault::Registry::instance().hits(land_at) >= 1; }, 10s);
                EXPECT_TRUE(reached) << "the takeover never reached " << land_at;
                std::filesystem::rename(newest_in_flight, newest_marker);
                clink::fault::Registry::instance().release(land_at);
            });
        }
        Coordinator::Config cfg;
        if (shape.park_for_capacity) {
            cfg.recovery_worker_settle = 0ms;  // recover at once, with no worker
        }
        Coordinator b(cfg);
        b.set_ha_dir(ha_dir.string());
        const auto port = b.start();
        b.expect_workers({"w"});
        std::unique_ptr<FakeWorker> w;
        if (!shape.park_for_capacity) {
            w = std::make_unique<FakeWorker>(port, "w");
            EXPECT_TRUE(w->valid());
            EXPECT_TRUE(w->register_and_ack());
            EXPECT_TRUE(b.await_registrations(2s));
        }
        b.recover_persisted_jobs();
        if (lander.joinable()) {
            lander.join();
        }
        if (shape.park_for_capacity) {
            if (shape.newest_lands_during_park) {
                // A superseded coordinator's puts land while the recovery is
                // parked: the COMPLETED marker, and the CONFIRMED one after it
                // for a job whose sinks need their commits confirmed.
                std::filesystem::rename(newest_in_flight, newest_marker);
                if (!shape.recoverable) {
                    std::ofstream(out.checkpoint_dir / "_jobs" / std::to_string(job_id) /
                                  ("CONFIRMED-" + std::to_string(out.completed)))
                        << "job=" << job_id << "\ncheckpoint=" << out.completed << "\n";
                }
            }
            // Parked: the walk has run and the submit found no slot. A worker
            // registering resumes the same takeover.
            w = std::make_unique<FakeWorker>(port, "w");
            EXPECT_TRUE(w->valid());
            EXPECT_TRUE(w->register_and_ack());
        }
        if (auto deploy = w->await_frame(MessageKind::Deploy); deploy.has_value()) {
            const auto msg = decode_deploy(*deploy);
            out.deployed = true;
            out.deploy_restore = msg.restore_from_checkpoint_id;
            out.deploy_restore_dir = msg.restore_from_dir;
        }
        if (shape.restart_after_deploy && out.deployed) {
            // The only worker dies before the new run completes a checkpoint.
            w->close();
            EXPECT_TRUE(ckpt_await([&] { return worker_was_lost(b, "w"); },
                                   clink::test_support::scale_slack(5000ms)))
                << "the dead worker was never declared lost";
            w = std::make_unique<FakeWorker>(port, "w");
            EXPECT_TRUE(w->valid());
            EXPECT_TRUE(w->register_and_ack());
            if (auto deploy = w->await_frame(MessageKind::Deploy, 20s); deploy.has_value()) {
                out.restarted = true;
                out.restart_restore = decode_deploy(*deploy).restore_from_checkpoint_id;
            }
        }
        w->close();
        b.stop();
    }
    ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
    clink::protocol_trace::reset_for_tests();
    for (const auto& entry : std::filesystem::directory_iterator(trace_dir)) {
        std::ifstream in(entry.path());
        std::string line;
        bool after_redeploy = false;
        while (std::getline(in, line)) {
            const auto ev = clink::config::parse(line);
            if (ev.int_or("job", 0) != static_cast<std::int64_t>(job_id)) {
                continue;
            }
            const auto event = ev.string_or("event", "");
            if (after_redeploy) {
                out.stages_after_redeploy += event == "RestartProceeds" ? 1U : 0U;
                out.decides_after_redeploy +=
                    (event == "WalkDecides" &&
                     ev.int_or("ckpt", 0) == static_cast<std::int64_t>(out.completed))
                        ? 1U
                        : 0U;
            }
            if (event == "Redeploy") {
                if (out.event_restore < 0) {
                    out.event_restore = ev.int_or("restore", -1);
                }
                after_redeploy = true;
            } else if (event == "CoordRecovers") {
                out.event_completed = ev.int_or("completed", -1);
            }
            if (event.rfind("Walk", 0) == 0) {
                out.walk_events.emplace_back(event,
                                             static_cast<std::uint64_t>(ev.int_or("ckpt", 0)));
            }
        }
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    return out;
}

}  // namespace

TEST(CheckpointCompletion, ATakeoversRedeployReportsTheRestorePointItsWalkResolved) {
    const auto t = takeover_through_the_walk("confirmed", {.first_confirmed = true});
    ASSERT_TRUE(t.deployed) << "the new leader did not redeploy the job";
    ASSERT_GT(t.completed, 0U);
    EXPECT_EQ(t.deploy_restore, t.completed)
        << "the walk did not confirm the newest checkpoint, so this tested nothing";
    EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.deploy_restore))
        << "the takeover's Redeploy reported restore point " << t.event_restore
        << " while its deploy restored from " << t.deploy_restore;
}

// With nothing of its own confirmed, the job's restore point comes from the
// walk alone, and the takeover left no directory to restore it from: the
// deploy's lint refused an id without one, and the job was dropped.
TEST(CheckpointCompletion, ATakeoverWhoseWalkConfirmsItsFirstCheckpointsRestoresFromThem) {
    const auto t = takeover_through_the_walk("unconfirmed", {.first_confirmed = false});
    ASSERT_TRUE(t.deployed) << "the job was dropped at the takeover";
    EXPECT_EQ(t.deploy_restore, t.completed);
    EXPECT_EQ(t.deploy_restore_dir, t.checkpoint_dir.string());
    EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.completed));
}

// The takeover walks the range it read and reported, not one read again: a
// COMPLETED put still out when the takeover read the markers (its run
// redeployed past, or its coordinator superseded) can land before the walk.
// Here the takeover read checkpoint 1 as both completed and confirmed, so it
// recorded no resolution; walking to a second read took it to checkpoint 2
// with no RestartProceeds, confirmed 2, and restored from it.
TEST(CheckpointCompletion, ATakeoverWalksOnlyTheCompletedRangeItReported) {
    const auto t = takeover_through_the_walk(
        "lands_late", {.first_confirmed = true, .newest_lands_during_takeover = true});
    ASSERT_TRUE(t.deployed) << "the new leader did not redeploy the job";
    ASSERT_GT(t.completed, t.first);
    for (const auto& [event, ckpt] : t.walk_events) {
        ADD_FAILURE() << "the takeover walked " << event << " at checkpoint " << ckpt
                      << " beyond the completed point " << t.first << " it reported";
    }
    EXPECT_EQ(t.deploy_restore, t.first);
    EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.first));
}

// A takeover parked for capacity resumes when a worker registers, and resumes
// past its walk: the walk ran once, and a second one re-probed the handle the
// first had found aborted, under walk events the trace module has no step for
// once the takeover's walk has finished.
TEST(CheckpointCompletion, ATakeoverParkedForCapacityWalksOnce) {
    const auto t = takeover_through_the_walk(
        "parked", {.first_confirmed = true, .park_for_capacity = true, .newest_refused = true});
    ASSERT_TRUE(t.deployed) << "the parked takeover never redeployed the job";
    const auto decides =
        std::count_if(t.walk_events.begin(), t.walk_events.end(), [&](const auto& e) {
            return e.first == "WalkDecides" && e.second == t.completed;
        });
    EXPECT_EQ(decides, 1) << "the takeover walked checkpoint " << t.completed << " " << decides
                          << " times";
    EXPECT_EQ(t.deploy_restore, t.first) << "the refused checkpoint was restored from";
    EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.first));
}

// A takeover reads the job's COMPLETED markers once, and its restore point and
// its CoordRecovers line both follow that read. A superseded coordinator's put
// landing while the takeover loaded the job's plugins used to reach a second
// read: the takeover reported checkpoint 2 completed and restored from 1, a
// restore point the specification cannot reach from the completed point it
// was given.
TEST(CheckpointCompletion, ATakeoverReportsTheCompletedPointItRestoresFrom) {
    const auto t = takeover_through_the_walk(
        "one_read", {.recoverable = true, .newest_lands_after_marker_read = true});
    ASSERT_TRUE(t.deployed) << "the new leader did not redeploy the job";
    ASSERT_GT(t.completed, t.first);
    EXPECT_EQ(t.event_completed, static_cast<std::int64_t>(t.first))
        << "the takeover reported a completed point it did not restore from";
    EXPECT_EQ(t.deploy_restore, t.first);
    EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.first));
}

// A recovery parked for capacity deploys, when a worker registers, the restore
// point its takeover decided and reported, not one read again: a superseded
// coordinator's put landing during the park used to move the retry's restore
// point past the completed point the takeover's line named. Both families: the
// COMPLETED marker for a job whose sinks re-run their commits, the CONFIRMED
// one for a job whose sinks need their commits confirmed.
TEST(CheckpointCompletion, AParkedRecoveryDeploysTheRestorePointItsTakeoverDecided) {
    for (const bool recoverable : {true, false}) {
        SCOPED_TRACE(recoverable ? "recoverable" : "commit-confirmed");
        const auto t = takeover_through_the_walk(recoverable ? "park_land_recov" : "park_land_eos",
                                                 {.first_confirmed = true,
                                                  .park_for_capacity = true,
                                                  .recoverable = recoverable,
                                                  .newest_lands_during_park = true});
        ASSERT_TRUE(t.deployed) << "the parked takeover never redeployed the job";
        ASSERT_GT(t.completed, t.first);
        EXPECT_EQ(t.event_completed, static_cast<std::int64_t>(t.first));
        EXPECT_EQ(t.deploy_restore, t.first)
            << "the retry restored from a marker that landed during the park";
        EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.first));
    }
}

// A takeover whose walk leaves a completed checkpoint unconfirmed (here the
// broker refused it) leaves the gap open, and a restart before the new run
// completes a checkpoint walks it again, as a restart of the run that
// completed it would. The taken-over job had no completed checkpoint in
// memory, so the restart used to redeploy straight from the confirmed point,
// a redeploy the specification holds for a second walk.
TEST(CheckpointCompletion, ARestartAfterATakeoverWalksTheGapTheTakeoverLeft) {
    const auto t = takeover_through_the_walk(
        "gap_again",
        {.first_confirmed = true, .newest_refused = true, .restart_after_deploy = true});
    ASSERT_TRUE(t.deployed) << "the new leader did not redeploy the job";
    ASSERT_EQ(t.deploy_restore, t.first) << "the refused checkpoint was restored from";
    ASSERT_TRUE(t.restarted) << "the restart never redeployed";
    EXPECT_EQ(t.stages_after_redeploy, 1U)
        << "the restart was not held for in-doubt resolution over the takeover's gap";
    EXPECT_EQ(t.decides_after_redeploy, 1U)
        << "the restart did not walk checkpoint " << t.completed << " again";
    EXPECT_EQ(t.restart_restore, t.first);
}

// The same for a tracked run resumed from its own checkpoints by a rerun: its
// takeover line reports the completed point it resumed beside, and a restart
// before the resumed run completes a checkpoint walks the gap its resume left.
TEST(CheckpointCompletion, ARestartAfterAResumeWalksTheGapTheResumeLeft) {
    CompletionScopedRecordOverride tracked(tracked_file_2pc());
    clink::fault::Registry::instance().reset();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_resume_gap_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{dir};
    const auto ckpt_dir = dir / "ckpt";
    const auto jobs = ckpt_dir / "_jobs" / "1";
    std::filesystem::create_directories(jobs);
    clink::connectors::TxnResumeRegistry::instance().register_resolver(
        kTakeoverResolver, [](const std::string& handle) {
            if (handle.find("\"ckpt\":\"2\"") != std::string::npos) {
                return clink::connectors::InDoubtResolution{false, "aborted by the broker"};
            }
            return clink::connectors::InDoubtResolution{true, "committed before the kill"};
        });

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = ckpt_dir.string();
    ckpt.interval_ms = 600'000;
    ckpt.track_runs = true;
    ckpt.max_restarts_on_worker_loss = 1;

    // The first run starts from empty state, recording its base and its graph,
    // and is killed. It completed checkpoints 1 and 2 and confirmed 1.
    std::uint32_t sink_participant = 0;
    std::string participants;
    {
        Coordinator c;
        const auto port = c.start();
        c.expect_workers({"w"});
        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(c.await_registrations(2s));
        ASSERT_EQ(
            c.submit_job(
                confirm_tracked_graph(dir / "out"), OperatorRegistry::default_instance(), {}, ckpt),
            1U);
        auto deploy = w.await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value());
        for (const auto& t : decode_deploy(*deploy).tasks) {
            sink_participant = std::max(sink_participant, t.subtask_idx);
            participants += (participants.empty() ? "" : ",") + std::to_string(t.subtask_idx);
        }
        w.close();
        c.stop();
    }
    for (const std::uint64_t id : {1U, 2U}) {
        std::ofstream(jobs / ("COMPLETED-" + std::to_string(id)))
            << "job=1\ncheckpoint=" << id << "\ngeneration=1\nsubtasks=" << participants << "\n";
        stage_committed_handle(ckpt_dir, sink_participant, id);
    }
    std::ofstream(jobs / "CONFIRMED-1") << "job=1\ncheckpoint=1\n";

    // The rerun resumes from 1 (the resolver refuses 2), and its only worker
    // dies before the resumed run completes a checkpoint.
    const ScopedTraceDir trace("resume_gap");
    std::uint64_t restart_restore = 0;
    {
        Coordinator c;
        const auto port = c.start();
        c.expect_workers({"w"});
        auto w = std::make_unique<FakeWorker>(port, "w");
        ASSERT_TRUE(w->valid());
        ASSERT_TRUE(w->register_and_ack());
        ASSERT_TRUE(c.await_registrations(2s));
        ASSERT_EQ(
            c.submit_job(
                confirm_tracked_graph(dir / "out"), OperatorRegistry::default_instance(), {}, ckpt),
            1U);
        auto deploy = w->await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value());
        ASSERT_EQ(decode_deploy(*deploy).restore_from_checkpoint_id, 1U);
        w->close();
        ASSERT_TRUE(ckpt_await([&] { return worker_was_lost(c, "w"); },
                               clink::test_support::scale_slack(5000ms)))
            << "the dead worker was never declared lost";
        w = std::make_unique<FakeWorker>(port, "w");
        ASSERT_TRUE(w->valid());
        ASSERT_TRUE(w->register_and_ack());
        auto redeploy = w->await_frame(MessageKind::Deploy, 20s);
        ASSERT_TRUE(redeploy.has_value()) << "the restart never redeployed";
        restart_restore = decode_deploy(*redeploy).restore_from_checkpoint_id;
        w->close();
        c.stop();
    }
    std::size_t stages = 0;
    std::size_t walked_again = 0;
    bool after_redeploy = false;
    for (const auto& line : job_trace_lines(trace.dir, 1)) {
        if (trace_event_is(line, "Redeploy")) {
            after_redeploy = true;
        } else if (after_redeploy && trace_event_is(line, "RestartProceeds")) {
            ++stages;
        } else if (after_redeploy && trace_event_is(line, "WalkDecides") &&
                   trace_field(line, "ckpt") == std::optional<std::uint64_t>{2U}) {
            ++walked_again;
        }
    }
    EXPECT_EQ(stages, 1U) << "the restart was not held for in-doubt resolution over the gap";
    EXPECT_EQ(walked_again, 1U) << "the restart did not walk checkpoint 2 again";
    EXPECT_EQ(restart_restore, 1U);
}

// --- a hot cutover's arm frames -----------------------------------------
namespace {

// A job a hot cutover can take: a keyed operator with rescale bounds between
// a source and a sink, the parallelism mismatched on both of its edges.
JobGraphSpec hot_cutover_graph(const std::filesystem::path& out) {
    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    src.params = {{"count", "1000000"}};
    g.ops.push_back(src);
    OperatorSpec agg;
    agg.type = "identity_int64";
    agg.id = "agg";
    agg.inputs = {"src"};
    agg.parallelism = 2;
    agg.min_parallelism = 1;
    agg.max_parallelism = 8;
    agg.out_channel = std::string{kChannelInt64};
    agg.key_by = "identity";
    g.ops.push_back(agg);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"agg"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", out.string()}};
    g.ops.push_back(snk);
    return g;
}

}  // namespace

// The cutover is armed under the coordinator lock and its arm frames go out
// after it. The phase deadline, or a worker loss, can abort the cutover in
// between, and the abort's CancelJob and replan are then on their way, so the
// arm frames must be dropped: sent, they would arm subtasks to stop at a
// cutover checkpoint nobody will trigger.
TEST(CheckpointCompletion, ACutoverAbortedBeforeItsArmFramesLeaveSendsNone) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_hot_arm_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{dir};
    Coordinator::Config cfg;
    // The watchdog aborts a cutover stuck past this in any phase, Arming
    // included: well inside the window the fault point below holds open.
    cfg.hot_cutover_phase_timeout = 100ms;
    Coordinator c(cfg);
    const auto port = c.start();
    c.expect_workers({"w"});
    FakeWorker w(port, "w", /*slots=*/8);
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(c.await_registrations(2s));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (dir / "ckpt").string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 0;
    const auto job_id = c.submit_job(
        hot_cutover_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto deploy = w.await_frame(MessageKind::Deploy);
    ASSERT_TRUE(deploy.has_value());
    const auto tasks = decode_deploy(*deploy).tasks;
    std::uint16_t port_seed = 41800;
    for (const auto& t : tasks) {
        ASSERT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
    }
    // A completed checkpoint for the cutover to start from.
    auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
    ASSERT_TRUE(trigger.has_value());
    const auto completed = decode_trigger_checkpoint(*trigger).checkpoint_id;
    for (const auto& t : tasks) {
        ASSERT_TRUE(w.ack_checkpoint(job_id, completed, t.role, t.subtask_idx, /*ok=*/true));
    }
    ASSERT_TRUE(ckpt_await([&] { return c.latest_completed_checkpoint(job_id) == completed; }));

    clink::fault::Registry::instance().reset();
    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kHotCutoverBeforeArm,
                           .ordinal = 1,
                           .action = clink::fault::Action::Delay,
                           .arg = 1500}};
    const auto result = c.request_operator_rescale(job_id, "agg", 4);
    ASSERT_TRUE(result.ok) << result.reason;
    ASSERT_EQ(clink::fault::Registry::instance().hits(clink::fault::points::kHotCutoverBeforeArm),
              1U)
        << "the rescale did not take the hot path, so there was no window to test";
    EXPECT_TRUE(w.await_frame(MessageKind::CancelJob, 2s).has_value())
        << "the phase deadline did not abort the cutover inside the window";
    EXPECT_FALSE(w.await_frame(MessageKind::BeginRescale, 500ms).has_value())
        << "the arm frames of a cutover the coordinator had already aborted were sent";
    w.close();
    c.stop();
}

// The arm frames go out in the same coordinator lock hold as the check that
// the cutover is still the one being armed, so an abort, which runs under the
// lock and sends after it, reaches every worker after them. Sent after the
// lock, an abort landing between the check and the send got its CancelJob out
// first, and an arm held up past the abort's drain and redeploy reached the
// replan's new subtasks, arming them for a checkpoint never triggered. The
// send is held open here while the phase deadline passes.
TEST(CheckpointCompletion, ACutoverAbortedWhileItsArmFramesAreSentReachesTheWorkersAfterThem) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_hot_arm_send_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{dir};
    Coordinator::Config cfg;
    // The watchdog aborts a cutover still arming past this, well inside the
    // hold below: the fake worker never acks the arm.
    cfg.hot_cutover_phase_timeout = 100ms;
    Coordinator c(cfg);
    const auto port = c.start();
    c.expect_workers({"w"});
    FakeWorker w(port, "w", /*slots=*/8);
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(c.await_registrations(2s));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (dir / "ckpt").string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 0;
    const auto job_id = c.submit_job(
        hot_cutover_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto deploy = w.await_frame(MessageKind::Deploy);
    ASSERT_TRUE(deploy.has_value());
    const auto tasks = decode_deploy(*deploy).tasks;
    std::uint16_t port_seed = 41850;
    for (const auto& t : tasks) {
        ASSERT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
    }
    // A completed checkpoint for the cutover to start from.
    auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
    ASSERT_TRUE(trigger.has_value());
    const auto completed = decode_trigger_checkpoint(*trigger).checkpoint_id;
    for (const auto& t : tasks) {
        ASSERT_TRUE(w.ack_checkpoint(job_id, completed, t.role, t.subtask_idx, /*ok=*/true));
    }
    ASSERT_TRUE(ckpt_await([&] { return c.latest_completed_checkpoint(job_id) == completed; }));

    clink::fault::Registry::instance().reset();
    clink::fault::ScopedFault hold{
        clink::fault::Rule{.point = clink::fault::points::kHotCutoverArmSend,
                           .ordinal = 1,
                           .action = clink::fault::Action::Delay,
                           .arg = 800}};
    const auto result = c.request_operator_rescale(job_id, "agg", 4);
    ASSERT_TRUE(result.ok) << result.reason;
    ASSERT_EQ(clink::fault::Registry::instance().hits(clink::fault::points::kHotCutoverArmSend), 1U)
        << "the rescale did not take the hot path, so there was no send to hold";
    const auto has = [](const std::vector<MessageKind>& kinds, MessageKind kind) {
        return std::find(kinds.begin(), kinds.end(), kind) != kinds.end();
    };
    ASSERT_TRUE(ckpt_await(
        [&] {
            const auto kinds = w.queued_kinds();
            return has(kinds, MessageKind::BeginRescale) && has(kinds, MessageKind::CancelJob);
        },
        5s))
        << "the phase deadline did not abort the armed cutover";
    const auto kinds = w.queued_kinds();
    const auto arm = std::find(kinds.begin(), kinds.end(), MessageKind::BeginRescale);
    const auto cancel = std::find(kinds.begin(), kinds.end(), MessageKind::CancelJob);
    EXPECT_LT(arm - kinds.begin(), cancel - kinds.begin())
        << "the abort's CancelJob reached the worker ahead of the cutover's arm frame";
    w.close();
    c.stop();
}

// An old subtask of a hot cutover that ended at the cutover checkpoint before
// that checkpoint completed is held as an early drain, its slot still charged
// to the session that ran it. A re-registration's retirement aborts the
// cutover, and the abort frees the early drains' slots. The session they were
// charged to is the retired one, so nothing is taken off the successor, which
// already holds a job of its own. Released by worker id, the slot came off the
// successor and left it over-placed.
TEST(CheckpointCompletion, AnEarlyDrainReleasedByARetirementLeavesTheSuccessorsSlotsAlone) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_early_drain_slot_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    struct Cleanup {
        std::filesystem::path dir;
        ~Cleanup() {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
        }
    } cleanup{dir};
    clink::fault::Registry::instance().reset();
    Coordinator c;
    const auto port = c.start();
    struct Stop {
        Coordinator& c;
        ~Stop() {
            (void)clink::fault::Registry::instance().release();
            c.stop();
        }
    } stop_coordinator{c};
    c.expect_workers({"w"});
    FakeWorker w(port, "w", /*slots=*/8);
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(c.await_registrations(2s));
    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = (dir / "ckpt").string();
    ckpt.interval_ms = 600'000;
    ckpt.max_restarts_on_worker_loss = 0;
    const auto job_id = c.submit_job(
        hot_cutover_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto deploy = w.await_frame(MessageKind::Deploy);
    ASSERT_TRUE(deploy.has_value());
    const auto tasks = decode_deploy(*deploy).tasks;
    std::vector<DeploymentTask> agg_tasks;
    std::uint16_t port_seed = 41900;
    for (const auto& t : tasks) {
        ASSERT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
        if (t.extra_config.find("\"id\":\"agg\"") != std::string::npos) {
            agg_tasks.push_back(t);
        }
    }
    ASSERT_EQ(agg_tasks.size(), 2U) << "could not tell the rescaled operator's subtasks apart";
    auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
    ASSERT_TRUE(trigger.has_value());
    const auto completed = decode_trigger_checkpoint(*trigger).checkpoint_id;
    for (const auto& t : tasks) {
        ASSERT_TRUE(w.ack_checkpoint(job_id, completed, t.role, t.subtask_idx, /*ok=*/true));
    }
    ASSERT_TRUE(ckpt_await([&] { return c.latest_completed_checkpoint(job_id) == completed; }));

    // Armed and acked: the cutover checkpoint is triggered and awaits its cut.
    const auto result = c.request_operator_rescale(job_id, "agg", 4);
    ASSERT_TRUE(result.ok) << result.reason;
    auto arm = w.await_frame(MessageKind::BeginRescale, 5s);
    ASSERT_TRUE(arm.has_value()) << "the rescale did not take the hot path";
    const auto cut = decode_begin_rescale(*arm).cutover_checkpoint;
    BeginRescaleAckMsg armed;
    armed.job_id = job_id;
    armed.op_id = "agg";
    armed.worker_id = "w";
    armed.armed_callbacks = 2;  // the two agg subtasks
    armed.armed_groups = 1;     // the source feeding them
    armed.rebind_tasks = 1;     // the sink they feed
    ASSERT_TRUE(w.send_raw(encode_frame(MessageKind::BeginRescaleAck, armed)));
    std::optional<std::uint64_t> cut_triggered;
    ASSERT_TRUE(ckpt_await(
        [&] {
            auto frame = w.await_frame(MessageKind::TriggerCheckpoint, 100ms);
            if (frame.has_value()) {
                cut_triggered = decode_trigger_checkpoint(*frame).checkpoint_id;
            }
            return cut_triggered == cut;
        },
        5s))
        << "the cutover checkpoint was not triggered";

    // One old subtask ends at the cut before the cut completes: an early drain.
    // The request behind it is held at its fault point, so once it is hit the
    // drain has been handled and the old session's dispatch is parked.
    ASSERT_TRUE(w.send_finished(job_id, agg_tasks.front().role, agg_tasks.front().subtask_idx));
    clink::fault::Registry::instance().arm(
        {.point = kFinalRequestPoint, .ordinal = 1, .action = clink::fault::Action::Block});
    ASSERT_TRUE(w.request_final_checkpoint(job_id, tasks.front().role, tasks.front().subtask_idx));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kFinalRequestPoint) >= 1; }, 5s));

    const auto since_ms = log_cursor_ms();
    FakeWorker successor(port, "w", /*slots=*/16);
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    const auto placed = c.submit_job(
        two_subtask_graph(dir / "placed.txt"), OperatorRegistry::default_instance(), {}, {});
    ASSERT_GT(placed, 0U);
    auto placed_deploy = successor.await_frame(MessageKind::Deploy, 5s);
    ASSERT_TRUE(placed_deploy.has_value());
    const auto placed_tasks = decode_deploy(*placed_deploy).tasks.size();
    ASSERT_EQ(c.free_slots(), 16U - placed_tasks);

    EXPECT_EQ(clink::fault::Registry::instance().release(kFinalRequestPoint), 1U);
    ASSERT_TRUE(ckpt_await([&] { return previous_session_retired(since_ms); }, 5s))
        << "the old session was never retired";
    // The retirement aborted the cutover, and the job restarts at the
    // requested parallelism on the only session left.
    std::size_t redeployed = 0;
    ASSERT_TRUE(ckpt_await(
        [&] {
            auto frame = successor.await_frame(MessageKind::Deploy, 100ms);
            if (frame.has_value()) {
                const auto msg = decode_deploy(*frame);
                if (msg.job_id == job_id) {
                    redeployed = msg.tasks.size();
                }
            }
            return redeployed > 0;
        },
        clink::test_support::scale_slack(std::chrono::milliseconds{10000})))
        << "the aborted cutover's job was not redeployed";
    EXPECT_EQ(c.free_slots(), 16U - placed_tasks - redeployed)
        << "the early drain's slot, charged to the retired session, came off the successor";
}

// A session lost while it still retires its predecessor leaves the
// predecessor's subtasks to that predecessor's queued frames, here with a hot
// cutover in flight. The loss aborts the cutover, and the abort stages the
// replan's drain with the worker already lost, so it counted none of the
// worker's subtasks as draining and put them all up for redeploy. The restart
// then redeployed at once onto the spare while the old session's
// SubtaskFinished frames were still queued, to be handled against the new run.
TEST(CheckpointCompletion, ASessionLostWhileRetiringMidCutoverLeavesThePredecessorsSubtasksToIt) {
    clink::fault::Registry::instance().reset();
    CheckpointFixture fx;
    const auto job_id = fx.bring_up(
        /*max_restarts=*/1, /*interval_ms=*/600'000, hot_cutover_graph(fx.dir / "out.txt"));
    ASSERT_GT(job_id, 0U);
    // Registered after the deploy, so it hosts nothing and is where the
    // replan redeploys.
    FakeWorker spare(fx.port, "w2", /*slots=*/8);
    ASSERT_TRUE(spare.valid());
    ASSERT_TRUE(spare.register_and_ack());
    const auto first = first_checkpoint_with_next_on_record(fx, job_id);
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(fx.ack_all(job_id, *first, /*ok=*/true));
    ASSERT_TRUE(
        ckpt_await([&] { return fx.coordinator->latest_completed_checkpoint(job_id) == *first; }));

    // Armed and never acked: the cutover stays in flight.
    const auto result = fx.coordinator->request_operator_rescale(job_id, "agg", 4);
    ASSERT_TRUE(result.ok) << result.reason;
    ASSERT_TRUE(fx.worker->await_frame(MessageKind::BeginRescale, 5s).has_value())
        << "the rescale did not take the hot path";

    // The old session's dispatch is parked on a request, with every subtask's
    // finish queued behind it.
    clink::fault::ScopedFault hold{clink::fault::Rule{
        .point = kFinalRequestPoint, .ordinal = 1, .action = clink::fault::Action::Block}};
    const auto& [role, subtask] = fx.deployed().front();
    ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    ASSERT_TRUE(ckpt_await(
        [] { return clink::fault::Registry::instance().hits(kFinalRequestPoint) >= 1; }));
    for (const auto& [r, s] : fx.deployed()) {
        ASSERT_TRUE(fx.worker->send_finished(job_id, r, s));
    }
    ASSERT_TRUE(fx.worker->send_heartbeat(8));
    ASSERT_TRUE(fx.worker->await_heartbeat_ack(8))
        << "the reader did not read past a dispatch held at the request";

    // The worker re-registers, and the new session dies while its dispatch
    // still waits for the old session's frames.
    FakeWorker successor(fx.port, "w");
    ASSERT_TRUE(successor.valid());
    ASSERT_TRUE(successor.register_and_ack());
    successor.close();
    ASSERT_TRUE(ckpt_await([&] { return worker_was_lost(*fx.coordinator, "w"); },
                           clink::test_support::scale_slack(std::chrono::milliseconds{5000})))
        << "the dead new session was never declared lost";
    EXPECT_FALSE(spare.await_frame(MessageKind::Deploy, 500ms).has_value())
        << "the old session's subtasks were redeployed while the frames reporting them were "
           "still queued";

    EXPECT_EQ(clink::fault::Registry::instance().release(kFinalRequestPoint), 1U);
    auto redeploy = spare.await_frame(MessageKind::Deploy, 20s);
    ASSERT_TRUE(redeploy.has_value())
        << "the replan never redeployed once the old session's frames had drained it";
    const auto redeployed = decode_deploy(*redeploy);
    ASSERT_EQ(redeployed.job_id, job_id);
    EXPECT_FALSE(fx.coordinator->await_job_completion(job_id, 500ms))
        << "the old session's finishes were counted against the redeployed run";
    for (const auto& t : redeployed.tasks) {
        ASSERT_TRUE(spare.send_finished(job_id, t.role, t.subtask_idx));
    }
    ASSERT_TRUE(fx.coordinator->await_job_completion(job_id, 5s));
    const auto errors = fx.coordinator->job_errors(job_id);
    EXPECT_TRUE(errors.empty()) << (errors.empty() ? std::string{} : errors.front());
}

// HA recovery replans the graph the job was SUBMITTED with - the manifest is
// written once, at submit, and a rescale since then does not rewrite it - and
// every task then restores from its own index in the latest completed
// checkpoint. A checkpoint taken by a different layout (after a rescale, or by an
// engine version that planned the graph differently) would give the recovered
// operators each other's state, so the layout gate refuses the recovery instead:
// no Deploy goes out, and the coordinator's log names the checkpoint and why.
//
// The first leader completes a real checkpoint; its marker is then rewritten to
// record one participant more than the plan has, which is what a checkpoint taken
// after scaling an operator up records.
TEST(CheckpointCompletion, RecoveryIsRefusedWhenTheCheckpointsLayoutDiffersFromThePlan) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_ckpt_recovery_layout_" + std::to_string(::getpid()));
    std::filesystem::remove_all(root);
    const auto ha_dir = root / "ha";
    const auto ckpt_dir = root / "ckpt";
    std::filesystem::create_directories(ha_dir);
    std::filesystem::create_directories(ckpt_dir);

    JobId job_id = 0;
    std::uint64_t completed = 0;
    std::uint32_t highest_subtask = 0;

    // --- first leader: run a job and complete a checkpoint ---
    {
        Coordinator a;
        a.set_ha_dir(ha_dir.string());
        const auto port = a.start();
        a.expect_workers({"w"});

        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(a.await_registrations(2s));

        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = ckpt_dir.string();
        ckpt.interval_ms = 100;
        ckpt.max_restarts_on_worker_loss = 0;
        job_id = a.submit_job(
            two_subtask_graph(root / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
        ASSERT_GT(job_id, 0U);

        auto deploy = w.await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value());
        const auto tasks = decode_deploy(*deploy).tasks;
        ASSERT_FALSE(tasks.empty());
        std::uint16_t port_seed = 41100;
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
            highest_subtask = std::max(highest_subtask, t.subtask_idx);
        }

        auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
        ASSERT_TRUE(trigger.has_value());
        completed = decode_trigger_checkpoint(*trigger).checkpoint_id;
        ASSERT_GT(completed, 0U);
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, completed, t.role, t.subtask_idx, /*ok=*/true));
        }
        ASSERT_TRUE(ckpt_await([&] {
            return std::filesystem::exists(written_marker_path(ckpt_dir, job_id, completed));
        })) << "the checkpoint never completed, so there is nothing for recovery to find";

        w.close();
        a.stop();
    }

    // The checkpoint as a larger layout would have recorded it.
    {
        std::string subtasks;
        for (std::uint32_t i = 0; i <= highest_subtask + 1; ++i) {
            subtasks += (subtasks.empty() ? "" : ",") + std::to_string(i);
        }
        std::ofstream(written_marker_path(ckpt_dir, job_id, completed), std::ios::trunc)
            << "job=" << job_id << "\ncheckpoint=" << completed
            << "\ngeneration=1\nsubtasks=" << subtasks << "\n";
    }

    // --- second leader: recovery must refuse, loudly ---
    {
        Coordinator b;
        b.set_ha_dir(ha_dir.string());
        const auto port = b.start();
        b.expect_workers({"w"});

        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(b.await_registrations(2s));

        const auto since_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count() -
                              1;
        b.recover_persisted_jobs();

        EXPECT_FALSE(w.await_frame(MessageKind::Deploy, 1s).has_value())
            << "the job was recovered into a layout its checkpoint was not taken with";
        EXPECT_FALSE(b.snapshot_job(job_id).has_value())
            << "the refused recovery left a job behind";
        bool named = false;
        for (const auto& rec : LogBuffer::global().tail(1000, "warn", since_ms, "coordinator.ha")) {
            named = named || (rec.message.find("recovery failed for job_id=" +
                                               std::to_string(job_id)) != std::string::npos &&
                              rec.message.find("refusing to restore checkpoint " +
                                               std::to_string(completed)) != std::string::npos);
        }
        EXPECT_TRUE(named) << "the refusal must reach the coordinator's log, naming the checkpoint";

        w.close();
        b.stop();
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// A cancelled job must STAY cancelled across a coordinator takeover.
//
// Recovery redeploys every job whose HA manifest exists, and cancellation
// used to leave the manifest in place - so the next takeover resurrected
// jobs the operator had killed (followups item 69: QUAL-05's cancelled
// control arm came back mid-campaign and competed for the subject's
// slots; every campaign carried a manual manifest wipe as the
// workaround). Terminal signalling now tombstones the job's HA prefix and
// deletes the manifest; recovery honours the tombstone even when the
// deletion was interrupted, which this test simulates by putting the
// manifest BACK beside the tombstone before the second leader recovers.
TEST(CheckpointCompletion, ACancelledJobIsNotResurrectedByRecovery) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("clink_terminal_manifest_" + std::to_string(::getpid()));
    const auto ha_dir = root / "ha";
    const auto ckpt_dir = root / "ckpt";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(ha_dir);
    std::filesystem::create_directories(ckpt_dir);

    JobId job_id = 0;
    std::string manifest_bytes;
    {
        Coordinator a;
        a.set_ha_dir(ha_dir.string());
        const auto port = a.start();
        a.expect_workers({"w"});
        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(a.await_registrations(2s));

        CheckpointConfig ckpt;
        ckpt.checkpoint_dir = ckpt_dir.string();
        ckpt.interval_ms = 100;
        job_id = a.submit_job(
            two_subtask_graph(root / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
        ASSERT_GT(job_id, 0U);

        auto deploy = w.await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value());
        const auto tasks = decode_deploy(*deploy).tasks;
        ASSERT_FALSE(tasks.empty());
        std::uint16_t port_seed = 45000;
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.report_listening(job_id, t.role, t.subtask_idx, port_seed++));
        }

        // Complete one checkpoint BEFORE cancelling. The premise matters:
        // recovery restores from the latest COMPLETED-N on disk, and a job
        // with none is refused by config lint (restore id 0) long before
        // the tombstone is consulted - the first cut of this test passed
        // against a disabled tombstone check exactly that way. The
        // campaign jobs item 69 resurrected all had checkpoints.
        auto trigger = w.await_frame(MessageKind::TriggerCheckpoint);
        ASSERT_TRUE(trigger.has_value());
        const auto ckpt_id = decode_trigger_checkpoint(*trigger).checkpoint_id;
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, ckpt_id, t.role, t.subtask_idx, /*ok=*/true));
        }
        const auto ckpt_deadline = std::chrono::steady_clock::now() + 5s;
        while (a.latest_completed_checkpoint(job_id) != ckpt_id &&
               std::chrono::steady_clock::now() < ckpt_deadline) {
            std::this_thread::sleep_for(20ms);
        }
        ASSERT_EQ(a.latest_completed_checkpoint(job_id), ckpt_id)
            << "the checkpoint never completed; the resurrection premise is gone";

        // Capture the manifest while the job is live - it is the artefact
        // whose afterlife is under test.
        const auto manifest_path = ha_dir / "jobs" / std::to_string(job_id) / "manifest.json";
        ASSERT_TRUE(std::filesystem::exists(manifest_path))
            << "no manifest was persisted; the premise of the test is gone";
        {
            std::ifstream in(manifest_path, std::ios::binary);
            manifest_bytes.assign(std::istreambuf_iterator<char>(in),
                                  std::istreambuf_iterator<char>());
        }
        ASSERT_FALSE(manifest_bytes.empty());

        const auto ack = a.cancel_job(job_id);
        ASSERT_TRUE(ack.ok) << ack.message;
        ASSERT_TRUE(w.await_frame(MessageKind::CancelJob).has_value());
        for (const auto& t : tasks) {
            ASSERT_TRUE(w.send_finished(job_id, t.role, t.subtask_idx));
        }
        ASSERT_TRUE(a.await_job_completion(job_id, 10s));
        w.close();
        a.stop();
    }

    // The retirement itself: tombstone present, manifest gone.
    const auto job_prefix = ha_dir / "jobs" / std::to_string(job_id);
    EXPECT_TRUE(std::filesystem::exists(job_prefix / "TERMINAL"))
        << "terminal signalling wrote no tombstone";
    EXPECT_FALSE(std::filesystem::exists(job_prefix / "manifest.json"))
        << "the manifest survived the terminal transition";

    // Simulate the interrupted deletion: the tombstone landed, the
    // manifest did not go. Recovery must honour the tombstone alone.
    {
        std::ofstream out(job_prefix / "manifest.json", std::ios::binary);
        out << manifest_bytes;
    }

    {
        Coordinator b;
        b.set_ha_dir(ha_dir.string());
        const auto port = b.start();
        b.expect_workers({"w"});
        FakeWorker w(port, "w");
        ASSERT_TRUE(w.valid());
        ASSERT_TRUE(w.register_and_ack());
        ASSERT_TRUE(b.await_registrations(2s));

        b.recover_persisted_jobs();

        // 5s comfortably exceeds the 1s worker-settle recovery waits for;
        // a resurrected job's Deploy would land well inside it.
        EXPECT_FALSE(w.await_frame(MessageKind::Deploy, 5s).has_value())
            << "the cancelled job was resurrected by recovery (item 69)";
        w.close();
        b.stop();
    }

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// Item 77b. A whole-job restart repairs a TRANSIENT snapshot failure and
// only amplifies a persistent one: each rewind re-emits an interval
// through every sink, so a cause that does not heal (QUAL-09: the state
// volume at ENOSPC) used to crashloop indefinitely - ~100 rewind-restarts
// in 35 minutes, output visibly shrinking, bounded only by a restart
// budget sized for worker loss. The breaker: at N CONSECUTIVE
// failure-restarts with no completed checkpoint between them, the job
// FAILS carrying the cause.
namespace {

// Triggers keep arriving on the interval and survive restarts in the
// fake worker's inbox; a cycle must ack a FRESH id or its acks land on a
// checkpoint the coordinator already aborted.
std::optional<std::uint64_t> await_trigger_above(FakeWorker& w, std::uint64_t last) {
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline) {
        auto r = w.await_frame(MessageKind::TriggerCheckpoint, 2s);
        if (!r.has_value()) {
            continue;
        }
        const auto id = decode_trigger_checkpoint(*r).checkpoint_id;
        if (id > last) {
            return id;
        }
    }
    return std::nullopt;
}

}  // namespace

TEST(CheckpointCompletion, PersistentCheckpointFailureFailsTheJobInsteadOfCrashlooping) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_breaker_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.checkpoint_failure_restart_limit = 3;
    // Count-only semantics for this test: the wall-clock window (item 80)
    // exists so a TRANSIENT window is not read as persistent, and it gets
    // its own test below; here the subject is the count.
    cfg.checkpoint_failure_restart_window = std::chrono::milliseconds{0};
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});
    FakeWorker w(port, "w");
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 100000;  // the budget must NOT be the bound
    const auto job_id = coordinator.submit_job(
        two_subtask_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    std::vector<std::pair<std::string, std::uint32_t>> tasks;
    auto learn_deploy = [&]() -> bool {
        auto deploy = w.await_frame(MessageKind::Deploy, 10s);
        if (!deploy.has_value()) {
            return false;
        }
        tasks.clear();
        std::uint16_t fake_port = 46000;
        for (const auto& t : decode_deploy(*deploy).tasks) {
            tasks.emplace_back(t.role, t.subtask_idx);
            if (!w.report_listening(job_id, t.role, t.subtask_idx, fake_port++)) {
                return false;
            }
        }
        return !tasks.empty();
    };
    ASSERT_TRUE(learn_deploy());

    std::uint64_t last_ckpt = 0;
    for (int cycle = 1; cycle <= 3; ++cycle) {
        const auto id = await_trigger_above(w, last_ckpt);
        ASSERT_TRUE(id.has_value()) << "no fresh trigger in cycle " << cycle;
        last_ckpt = *id;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, *id, role, sub, /*ok=*/false));
        }
        // Every failure cancels the deployment: the first two to drain for
        // a restart, the third - the breaker - to fail the job.
        ASSERT_TRUE(w.await_frame(MessageKind::CancelJob, 10s).has_value())
            << "no cancel after failure " << cycle;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.send_finished(job_id, role, sub));
        }
        if (cycle < 3) {
            ASSERT_TRUE(learn_deploy()) << "no redeploy after failure " << cycle;
        }
    }

    EXPECT_TRUE(coordinator.await_job_completion(job_id, 10s))
        << "three consecutive failure-restarts and the job is still going: the crashloop "
           "has no circuit-breaker (item 77b)";
    const auto errors = coordinator.job_errors(job_id);
    bool named = false;
    for (const auto& e : errors) {
        named =
            named || e.find("consecutive restarts from failed checkpoints") != std::string::npos;
    }
    EXPECT_TRUE(named) << "the failure does not carry the persistent-cause verdict";

    w.close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// The other half of "consecutive": one completed checkpoint proves the
// cause was transient and must reset the count, or a long-lived job
// accumulates unrelated transients into a spurious failure.
TEST(CheckpointCompletion, ACompletedCheckpointResetsTheFailureRestartCount) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_breaker_reset_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.checkpoint_failure_restart_limit = 3;
    // Count-only: with the item-80 wall-clock window active, the window
    // (not the reset under test) would keep the job alive and this test
    // would pass against a broken reset.
    cfg.checkpoint_failure_restart_window = std::chrono::milliseconds{0};
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});
    FakeWorker w(port, "w");
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 100000;
    const auto job_id = coordinator.submit_job(
        two_subtask_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    std::vector<std::pair<std::string, std::uint32_t>> tasks;
    auto learn_deploy = [&]() -> bool {
        auto deploy = w.await_frame(MessageKind::Deploy, 10s);
        if (!deploy.has_value()) {
            return false;
        }
        tasks.clear();
        std::uint16_t fake_port = 47000;
        for (const auto& t : decode_deploy(*deploy).tasks) {
            tasks.emplace_back(t.role, t.subtask_idx);
            if (!w.report_listening(job_id, t.role, t.subtask_idx, fake_port++)) {
                return false;
            }
        }
        return !tasks.empty();
    };
    ASSERT_TRUE(learn_deploy());

    std::uint64_t last_ckpt = 0;
    auto fail_one_cycle = [&](int label) {
        const auto id = await_trigger_above(w, last_ckpt);
        ASSERT_TRUE(id.has_value()) << "no fresh trigger at step " << label;
        last_ckpt = *id;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, *id, role, sub, /*ok=*/false));
        }
        ASSERT_TRUE(w.await_frame(MessageKind::CancelJob, 10s).has_value());
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.send_finished(job_id, role, sub));
        }
        ASSERT_TRUE(learn_deploy()) << "no redeploy at step " << label;
    };

    // Two failures (limit is three), then one SUCCESS, then two more
    // failures. Without the reset this is four consecutive and the job
    // dies at the fourth; with it, the count stands at two.
    fail_one_cycle(1);
    fail_one_cycle(2);
    {
        const auto id = await_trigger_above(w, last_ckpt);
        ASSERT_TRUE(id.has_value());
        last_ckpt = *id;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, *id, role, sub, /*ok=*/true));
        }
        ASSERT_TRUE(ckpt_await([&] {
            return coordinator.latest_completed_checkpoint(job_id) >= last_ckpt;
        })) << "the healthy checkpoint never completed; the reset premise is gone";
    }
    fail_one_cycle(3);
    fail_one_cycle(4);

    EXPECT_FALSE(coordinator.await_job_completion(job_id, 2s))
        << "the job died after two-fail/success/two-fail: a completed checkpoint did not "
           "reset the consecutive count, so unrelated transients accumulate into a "
           "spurious failure";

    (void)coordinator.cancel_job(job_id);
    for (const auto& [role, sub] : tasks) {
        (void)w.send_finished(job_id, role, sub);
    }
    (void)coordinator.await_job_completion(job_id, 10s);
    w.close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// Item 80, QUAL-09's cloud run: the breaker's count alone reads a TRANSIENT
// full-disk window as a persistent cause - five failures one checkpoint
// interval apart is ~75 seconds, and the run watched a 109-second ENOSPC
// window get the job terminally failed 37 seconds before the window
// released. Persistence is a property of duration: with the wall-clock
// window configured, reaching the count within seconds must keep the job
// restarting (riding the transient out), not fail it.
TEST(CheckpointCompletion, ReachingTheFailureCountWithinTheWindowIsNotPersistent) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_breaker_window_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.checkpoint_failure_restart_limit = 2;
    cfg.checkpoint_failure_restart_window = std::chrono::minutes{10};
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});
    FakeWorker w(port, "w");
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 100000;
    const auto job_id = coordinator.submit_job(
        two_subtask_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    std::vector<std::pair<std::string, std::uint32_t>> tasks;
    auto learn_deploy = [&]() -> bool {
        auto deploy = w.await_frame(MessageKind::Deploy, 10s);
        if (!deploy.has_value()) {
            return false;
        }
        tasks.clear();
        std::uint16_t fake_port = 48000;
        for (const auto& t : decode_deploy(*deploy).tasks) {
            tasks.emplace_back(t.role, t.subtask_idx);
            if (!w.report_listening(job_id, t.role, t.subtask_idx, fake_port++)) {
                return false;
            }
        }
        return !tasks.empty();
    };
    ASSERT_TRUE(learn_deploy());

    // Four consecutive failures - twice the count limit - all inside one
    // ten-minute window. Every one must produce a RESTART (a redeploy),
    // never the terminal verdict.
    std::uint64_t last_ckpt = 0;
    for (int cycle = 1; cycle <= 4; ++cycle) {
        const auto id = await_trigger_above(w, last_ckpt);
        ASSERT_TRUE(id.has_value()) << "no fresh trigger in cycle " << cycle;
        last_ckpt = *id;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, *id, role, sub, /*ok=*/false));
        }
        ASSERT_TRUE(w.await_frame(MessageKind::CancelJob, 10s).has_value())
            << "no cancel after failure " << cycle;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.send_finished(job_id, role, sub));
        }
        ASSERT_TRUE(learn_deploy())
            << "no redeploy after failure " << cycle
            << ": the breaker fired inside the wall-clock window, reading a transient "
               "as persistent (item 80)";
    }
    EXPECT_FALSE(coordinator.await_job_completion(job_id, 2s))
        << "the job was terminally failed within seconds of its first failed checkpoint";

    (void)coordinator.cancel_job(job_id);
    for (const auto& [role, sub] : tasks) {
        (void)w.send_finished(job_id, role, sub);
    }
    (void)coordinator.await_job_completion(job_id, 10s);
    w.close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// The restart budget bounds a burst of failures, not a job's lifetime. It used
// to be a lifetime count that nothing reset, so a long-running self-healing job
// failed at its eleventh recovery however far apart they were, and once spent
// it left every later failure unrecoverable. A checkpoint completing after a
// clean run since the last restart now forgives it.
TEST(CheckpointCompletion, SeparatedRecoveredFaultsDoNotExhaustTheRestartBudget) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ckpt_budget_rate_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.checkpoint_failure_restart_limit = 0;  // the breaker is not the subject
    cfg.restart_budget_reset_after = std::chrono::milliseconds{0};
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"w"});
    FakeWorker w(port, "w");
    ASSERT_TRUE(w.valid());
    ASSERT_TRUE(w.register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    ckpt.interval_ms = 100;
    ckpt.max_restarts_on_worker_loss = 1;  // one restart per burst
    const auto job_id = coordinator.submit_job(
        two_subtask_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    std::vector<std::pair<std::string, std::uint32_t>> tasks;
    auto learn_deploy = [&]() -> bool {
        auto deploy = w.await_frame(MessageKind::Deploy, 10s);
        if (!deploy.has_value()) {
            return false;
        }
        tasks.clear();
        std::uint16_t fake_port = 47100;
        for (const auto& t : decode_deploy(*deploy).tasks) {
            tasks.emplace_back(t.role, t.subtask_idx);
            if (!w.report_listening(job_id, t.role, t.subtask_idx, fake_port++)) {
                return false;
            }
        }
        return !tasks.empty();
    };
    ASSERT_TRUE(learn_deploy());

    // Three bursts of one failure each, every one followed by a completed
    // checkpoint. A budget of one per lifetime has nothing left for the
    // second; a budget of one per burst restarts all three.
    std::uint64_t last_ckpt = 0;
    for (int cycle = 1; cycle <= 3; ++cycle) {
        const auto failed = await_trigger_above(w, last_ckpt);
        ASSERT_TRUE(failed.has_value()) << "no trigger in cycle " << cycle;
        last_ckpt = *failed;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, *failed, role, sub, /*ok=*/false));
        }
        ASSERT_TRUE(w.await_frame(MessageKind::CancelJob, 10s).has_value());
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.send_finished(job_id, role, sub));
        }
        ASSERT_TRUE(learn_deploy()) << "cycle " << cycle
                                    << ": no redeploy, so the failure exhausted a budget that "
                                       "the completed checkpoints before it should have reset";
        const auto good = await_trigger_above(w, last_ckpt);
        ASSERT_TRUE(good.has_value());
        last_ckpt = *good;
        for (const auto& [role, sub] : tasks) {
            ASSERT_TRUE(w.ack_checkpoint(job_id, *good, role, sub, /*ok=*/true));
        }
        ASSERT_TRUE(ckpt_await([&] {
            return coordinator.latest_completed_checkpoint(job_id) >= last_ckpt;
        })) << "cycle "
            << cycle << ": the healthy checkpoint never completed";
    }
    EXPECT_FALSE(coordinator.await_job_completion(job_id, 1s))
        << "the job ended although every failure was recovered and followed by a clean run";

    (void)coordinator.cancel_job(job_id);
    for (const auto& [role, sub] : tasks) {
        (void)w.send_finished(job_id, role, sub);
    }
    (void)coordinator.await_job_completion(job_id, 10s);
    w.close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}
