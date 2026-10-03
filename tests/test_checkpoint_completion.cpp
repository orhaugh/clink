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
#include "clink/connectors/txn_resume_registry.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/otlp_export.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/state/state_backend_factory.hpp"
#include "clink/state_processor/savepoint.hpp"

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
        RegisterMsg reg{.worker_id = id_, .data_host = "127.0.0.1", .slot_count = slots_};
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
    // `max_restarts` is the job's restart budget: 0 keeps a failed
    // checkpoint from starting a rewind, which most tests here want out of
    // the way; the rewind tests pass a budget.
    JobId bring_up(std::uint32_t max_restarts = 0, std::int64_t interval_ms = 100) {
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
    ASSERT_TRUE(clink::cluster::record_triggered_id(fx.dir.string(), job_id, kElsewhere).claimed);

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
// moment the reply lands. Unanswered, the source fails its subtask when its
// bounded wait runs out and the restart replays the tail under a checkpoint.
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
    ASSERT_TRUE(fx.worker->request_final_checkpoint(job_id, role, subtask));
    auto reply = fx.worker->await_frame(MessageKind::FinalCheckpointAssigned);
    ASSERT_TRUE(reply.has_value()) << "the request went unanswered once its id could be recorded";
    const auto assigned = decode_final_checkpoint_assigned(*reply);
    ASSERT_GT(assigned.final_checkpoint_id, 0U);
    EXPECT_GE(clink::cluster::latest_triggered_id_on_disk(fx.dir.string(), job_id),
              assigned.final_checkpoint_id);
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
    std::uint64_t completed{0};  // the dead leader's newest completed checkpoint
    std::filesystem::path checkpoint_dir;
    std::uint64_t deploy_restore{0};  // what the new leader's deploy restores
    std::string deploy_restore_dir;   // and from where
    bool deployed{false};
    std::int64_t event_restore{-1};  // what its Redeploy event reports, -1 for none
};

// The dead leader completes two checkpoints of a job; the test then makes it
// a job whose sinks need their commits confirmed, stages a committed handle
// in both checkpoints, and marks the first one CONFIRMED when
// `first_confirmed`. The new leader's takeover walks what is unconfirmed.
WalkedTakeover takeover_through_the_walk(const std::string& tag, bool first_confirmed) {
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
        ckpt.max_restarts_on_worker_loss = 0;
        job_id = a.submit_job(
            two_subtask_graph(root / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
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
    EXPECT_NE(at, std::string::npos) << body;
    if (at != std::string::npos) {
        body.replace(at, unflagged.size(), "\"requires_commit_confirmation\":true");
        std::ofstream(manifest, std::ios::trunc) << body;
    }
    stage_committed_handle(out.checkpoint_dir, sink_participant, first_completed);
    stage_committed_handle(out.checkpoint_dir, sink_participant, out.completed);
    if (first_confirmed) {
        std::ofstream(out.checkpoint_dir / "_jobs" / std::to_string(job_id) /
                      ("CONFIRMED-" + std::to_string(first_completed)))
            << "job=" << job_id << "\ncheckpoint=" << first_completed << "\n";
    }

    ::setenv("CLINK_PROTOCOL_TRACE_DIR", trace_dir.c_str(), 1);
    clink::protocol_trace::reset_for_tests();
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
            out.deployed = true;
            out.deploy_restore = msg.restore_from_checkpoint_id;
            out.deploy_restore_dir = msg.restore_from_dir;
        }
        w.close();
        b.stop();
    }
    ::unsetenv("CLINK_PROTOCOL_TRACE_DIR");
    clink::protocol_trace::reset_for_tests();
    for (const auto& entry : std::filesystem::directory_iterator(trace_dir)) {
        std::ifstream in(entry.path());
        std::string line;
        while (std::getline(in, line)) {
            const auto ev = clink::config::parse(line);
            if (ev.string_or("event", "") == "Redeploy" &&
                ev.int_or("job", 0) == static_cast<std::int64_t>(job_id)) {
                out.event_restore = ev.int_or("restore", -1);
            }
        }
    }
    std::error_code ec;
    std::filesystem::remove_all(root, ec);
    return out;
}

}  // namespace

TEST(CheckpointCompletion, ATakeoversRedeployReportsTheRestorePointItsWalkResolved) {
    const auto t = takeover_through_the_walk("confirmed", /*first_confirmed=*/true);
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
    const auto t = takeover_through_the_walk("unconfirmed", /*first_confirmed=*/false);
    ASSERT_TRUE(t.deployed) << "the job was dropped at the takeover";
    EXPECT_EQ(t.deploy_restore, t.completed);
    EXPECT_EQ(t.deploy_restore_dir, t.checkpoint_dir.string());
    EXPECT_EQ(t.event_restore, static_cast<std::int64_t>(t.completed));
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
