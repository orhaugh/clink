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
#include <fstream>
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
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/protocol.hpp"
#include "clink/metrics/otlp_export.hpp"
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

    // <checkpoint_dir>/_jobs/<job_id>/COMPLETED-<id>: the path recovery reads,
    // and - since F29 - the path the coordinator writes.
    [[nodiscard]] bool marker_exists(JobId job_id, std::uint64_t ckpt_id) const {
        std::error_code ec;
        return std::filesystem::exists(
            dir / "_jobs" / std::to_string(job_id) / ("COMPLETED-" + std::to_string(ckpt_id)), ec);
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
