// An unrecoverable subtask error fails the job by cancelling every peer and
// waiting for completed_count to reach expected_completion - a COUNT. Two
// defects lived behind that count (followups item 75, QUAL-06 run C, where a
// 292-task job sat RUNNING for 75 silent minutes at 291/292 with its verdict
// already recorded):
//
//   75a - the count had NO DEADLINE on the coordinator. A peer whose cancel
//         never lands parks it short forever, and nothing re-examines a job
//         whose broadcast already fired. The watchdog now force-completes
//         the job as FAILED when the error-cancel convergence outruns the
//         restart-drain timeout.
//   75b - the worker's CancelJob flips only the cancel tokens REGISTERED at
//         that moment, and task construction runs on task threads: a task
//         that finished constructing after the flip registered a token
//         nobody would ever set and ran on as an orphan of a cancelled
//         deployment. The worker now latches the job id and starts such a
//         task pre-cancelled.
//
// 75a is scripted with fake workers over real frames (only frame-level
// control can withhold exactly one SubtaskFinished on demand); 75b runs a
// REAL worker and holds a task in construction across the cancel with the
// worker.task_token_register fault point, because the defect lives in the
// worker's own interleaving.
//
// The fake worker mirrors test_restart_drain_readiness.cpp's (uniquely
// named: same-named anon-namespace classes across test TUs ODR-collide
// under g++).

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
#include "clink/cluster/worker.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/runtime/network/connection.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

std::optional<std::vector<std::byte>> ecc_recv_frame(network::Connection& c) {
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

class EccFakeWorker {
public:
    EccFakeWorker(std::uint16_t port, std::string id) : id_(std::move(id)) {
        conn_ = network::connect_plain("127.0.0.1", port);
    }

    [[nodiscard]] bool valid() const { return conn_ != nullptr; }

    [[nodiscard]] bool register_and_ack() {
        RegisterMsg reg{.worker_id = id_, .data_host = "127.0.0.1", .slot_count = 4};
        if (!send_frame(*conn_, encode_frame(MessageKind::Register, reg))) {
            return false;
        }
        auto reply = ecc_recv_frame(*conn_);
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

    [[nodiscard]] bool send_finished(JobId job_id,
                                     const std::string& role,
                                     std::uint32_t subtask,
                                     bool had_error,
                                     bool fatal = false,
                                     bool transport_only = false) {
        SubtaskFinishedMsg m;
        m.job_id = job_id;
        m.worker_id = id_;
        m.role = role;
        m.subtask_idx = subtask;
        m.had_error = had_error;
        m.fatal = fatal;
        m.transport_only = transport_only;
        m.error_message = had_error ? (fatal ? "injected FATAL restore refusal (checkpoint corrupt)"
                                             : "injected bridge failure (peer gone)")
                                    : "";
        std::lock_guard lock(send_mu_);
        return send_frame(*conn_, encode_frame(MessageKind::SubtaskFinished, m));
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

    ~EccFakeWorker() { close(); }
    EccFakeWorker(const EccFakeWorker&) = delete;
    EccFakeWorker& operator=(const EccFakeWorker&) = delete;
    EccFakeWorker(EccFakeWorker&&) = delete;
    EccFakeWorker& operator=(EccFakeWorker&&) = delete;

private:
    void start_reader_() {
        reader_ = std::thread([this] {
            while (!stop_.load(std::memory_order_acquire)) {
                auto frame = ecc_recv_frame(*conn_);
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

// Three single-parallelism ops -> three tasks over two workers: one worker
// necessarily holds two. The content never runs; the fake workers answer.
JobGraphSpec ecc_three_task_graph(const std::filesystem::path& out) {
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

}  // namespace

// 75a. A FATAL subtask error broadcasts the peer cancels and one peer never
// reports (alive, heartbeating, not exiting - the orphan). The job must
// complete as FAILED within the drain timeout, never park RUNNING at
// expected-1 with its verdict already recorded.
TEST(ErrorCancelConvergence, AFatalErrorWhosePeerNeverReportsStillFailsTheJob) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ecc_deadline_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.restart_drain_timeout = 2000ms;  // the convergence bound under test
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-a", "ecc-b"});

    auto wa = std::make_unique<EccFakeWorker>(port, "ecc-a");
    auto wb = std::make_unique<EccFakeWorker>(port, "ecc-b");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    const auto job_id = coordinator.submit_job(
        ecc_three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    struct Deployed {
        EccFakeWorker* worker{nullptr};
        std::vector<std::pair<std::string, std::uint32_t>> tasks;
    };
    Deployed a{wa.get(), {}};
    Deployed b{wb.get(), {}};
    std::uint16_t fake_port = 42000;
    for (auto* d : {&a, &b}) {
        auto deploy = d->worker->await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value()) << "a worker never received its Deploy";
        for (const auto& t : decode_deploy(*deploy).tasks) {
            d->tasks.emplace_back(t.role, t.subtask_idx);
            ASSERT_TRUE(d->worker->report_listening(job_id, t.role, t.subtask_idx, fake_port++));
        }
    }
    Deployed& survivor = a.tasks.size() == 2 ? a : b;
    Deployed& orphan_host = a.tasks.size() == 2 ? b : a;
    ASSERT_EQ(survivor.tasks.size(), 2U) << "expected a 2/1 split across the two workers";
    ASSERT_EQ(orphan_host.tasks.size(), 1U);

    // A FATAL error: takes the fail-the-job path (a restart cannot fix a
    // damaged restore point), which cancels every peer and waits on the
    // count.
    ASSERT_TRUE(survivor.worker->send_finished(job_id,
                                               survivor.tasks[0].first,
                                               survivor.tasks[0].second,
                                               /*had_error=*/true,
                                               /*fatal=*/true));
    ASSERT_TRUE(survivor.worker->await_frame(MessageKind::CancelJob, 5s).has_value())
        << "the fatal error never broadcast the peer cancels";
    ASSERT_TRUE(orphan_host.worker->await_frame(MessageKind::CancelJob, 5s).has_value());

    // One peer obeys its cancel; the other NEVER reports, while its worker
    // keeps heartbeating - the orphan shape. Without the convergence
    // deadline the count parks at 2 of 3 and this await runs out its bound
    // with the job still RUNNING.
    ASSERT_TRUE(survivor.worker->send_finished(
        job_id, survivor.tasks[1].first, survivor.tasks[1].second, /*had_error=*/false));

    EXPECT_TRUE(coordinator.await_job_completion(job_id, 10s))
        << "the job sat RUNNING at expected-1: the error-cancel convergence has no deadline "
           "(followups item 75a)";
    const auto errors = coordinator.job_errors(job_id);
    ASSERT_FALSE(errors.empty());
    bool has_verdict = false;
    bool has_convergence = false;
    for (const auto& e : errors) {
        has_verdict = has_verdict || e.find("FATAL restore refusal") != std::string::npos;
        has_convergence =
            has_convergence || e.find("terminal-cancel convergence timed out") != std::string::npos;
    }
    EXPECT_TRUE(has_verdict) << "the recorded verdict must survive the forced completion";
    EXPECT_TRUE(has_convergence) << "the forced completion must say what never reported";

    wa->close();
    wb->close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// Item 73's coordinator half: a CLIENT cancel also completes by counting,
// and QUAL-06 run B watched cancel_requested sit "ignored" for 40 minutes
// against a wedged deployment. A cancelled job whose peer never reports
// must still reach a terminal state within the drain timeout.
TEST(ErrorCancelConvergence, AClientCancelWhosePeerNeverReportsStillTerminates) {
    ensure_built_ins_registered();
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_ecc_cancel_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.restart_drain_timeout = 2000ms;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-c", "ecc-d"});

    auto wa = std::make_unique<EccFakeWorker>(port, "ecc-c");
    auto wb = std::make_unique<EccFakeWorker>(port, "ecc-d");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    const auto job_id = coordinator.submit_job(
        ecc_three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);

    struct Deployed {
        EccFakeWorker* worker{nullptr};
        std::vector<std::pair<std::string, std::uint32_t>> tasks;
    };
    Deployed a{wa.get(), {}};
    Deployed b{wb.get(), {}};
    std::uint16_t fake_port = 43000;
    for (auto* d : {&a, &b}) {
        auto deploy = d->worker->await_frame(MessageKind::Deploy);
        ASSERT_TRUE(deploy.has_value()) << "a worker never received its Deploy";
        for (const auto& t : decode_deploy(*deploy).tasks) {
            d->tasks.emplace_back(t.role, t.subtask_idx);
            ASSERT_TRUE(d->worker->report_listening(job_id, t.role, t.subtask_idx, fake_port++));
        }
    }
    Deployed& obedient = a.tasks.size() == 2 ? a : b;
    Deployed& silent = a.tasks.size() == 2 ? b : a;
    ASSERT_EQ(obedient.tasks.size(), 2U) << "expected a 2/1 split across the two workers";

    const auto ack = coordinator.cancel_job(job_id);
    ASSERT_TRUE(ack.ok);
    ASSERT_TRUE(obedient.worker->await_frame(MessageKind::CancelJob, 5s).has_value());
    ASSERT_TRUE(silent.worker->await_frame(MessageKind::CancelJob, 5s).has_value());

    // The obedient worker's tasks report; the silent one's never does,
    // while its worker keeps heartbeating.
    for (const auto& [role, sub] : obedient.tasks) {
        ASSERT_TRUE(obedient.worker->send_finished(job_id, role, sub, /*had_error=*/false));
    }

    EXPECT_TRUE(coordinator.await_job_completion(job_id, 10s))
        << "the cancelled job sat RUNNING at expected-1: a client cancel's convergence "
           "has no deadline (followups item 73)";

    wa->close();
    wb->close();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// 75b. A task still constructing when its job's CancelJob is processed used
// to register a cancel token nobody would ever set and run on as an orphan.
// The worker.task_token_register fault point holds every task of this job in
// construction across the cancel; with the latch each starts pre-cancelled
// and the job completes. Without it the source task runs its two-billion
// range and the job never completes.
TEST(ErrorCancelConvergence, ATaskConstructedAcrossTheCancelStartsPreCancelled) {
    ensure_built_ins_registered();
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_ecc_latch_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator coordinator;
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-latch"});
    Worker::Config worker_cfg;
    worker_cfg.slot_count = 8;
    Worker worker("ecc-latch", "127.0.0.1", worker_cfg);
    worker.connect_to_coordinator("127.0.0.1", port);
    ASSERT_TRUE(coordinator.await_registrations(2s));

    // Ordinal 0 = every occurrence: each of the job's tasks sits at the
    // fault point for 1200ms before registering its cancel token.
    const clink::fault::ScopedFault guard(clink::fault::Rule{
        .point = clink::fault::points::kWorkerTaskTokenRegister,
        .ordinal = 0,
        .action = clink::fault::Action::Delay,
        .arg = 1200,
    });

    JobGraphSpec g;
    OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelInt64};
    // PACED, not merely large: this factory pre-builds every record into a
    // vector at construction, so a huge bare count spends minutes in an
    // uncancellable build before the runner can observe anything (the first
    // cut of this test timed out against a WORKING latch that way). 200k
    // records at 10ms apart build instantly and emit for ~2000s - an orphan
    // surviving the cancel keeps this job incomplete far past the await,
    // while a pre-cancelled task exits on its first predicate check.
    src.params = {{"count", "200000"}, {"delay_ms", "10"}};
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelInt64};
    snk.params = {{"path", (dir / "out.txt").string()}};
    g.ops.push_back(snk);

    const auto job_id = coordinator.submit_job(g, OperatorRegistry::default_instance());
    ASSERT_GT(job_id, 0U);

    // The cancel lands while every task is still held at the fault point,
    // so the flip walks an empty token map - the exact interleave from the
    // rig. Only the latch can reach these tasks now.
    std::this_thread::sleep_for(300ms);
    const auto ack = coordinator.cancel_job(job_id);
    EXPECT_TRUE(ack.ok);

    EXPECT_TRUE(coordinator.await_job_completion(job_id, 15s))
        << "a task registered after the cancel ran on as an orphan of a cancelled "
           "deployment (followups item 75b)";

    worker.stop();
    coordinator.stop();
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
}

// ----- Item 83: a departed peer is a symptom, not a cause -----
//
// A network-bridge send refused because the peer went away is reported as
// an error, because those records did not reach the stream. But it must not
// START recovery: the real cause (the peer's own exit, or its worker's
// loss) is already travelling here, and restarting on the symptom races
// ahead of it. Measured before this split: one killed worker produced
// restart attempts 1 and 2 within a second, entered recovery from the
// bridge path instead of the worker-loss path, and the killed worker never
// wound down.
//
// The other half matters just as much. In a healthy job no cause is coming,
// and a refused send that nobody acts on is the silent short stream the
// bridge's throw exists to catch - so once the grace expires, the transport
// failure IS the cause.

namespace {

// Deploy the graph and record each worker's (role, subtask) pairs.
struct EccDeployed {
    std::vector<std::pair<std::string, std::uint32_t>> a, b;
};

EccDeployed ecc_deploy(EccFakeWorker& wa, EccFakeWorker& wb, JobId job_id) {
    EccDeployed d;
    std::uint16_t fake_port = 44000;
    EccFakeWorker* workers[2] = {&wa, &wb};
    std::vector<std::pair<std::string, std::uint32_t>>* slots[2] = {&d.a, &d.b};
    for (int i = 0; i < 2; ++i) {
        auto deploy = workers[i]->await_frame(MessageKind::Deploy);
        if (!deploy.has_value()) {
            return d;
        }
        for (const auto& t : decode_deploy(*deploy).tasks) {
            slots[i]->emplace_back(t.role, t.subtask_idx);
            (void)workers[i]->report_listening(job_id, t.role, t.subtask_idx, fake_port++);
        }
    }
    return d;
}

}  // namespace

TEST(ErrorCancelConvergence, ATransportFailureWithNoCauseBehindItStillDrivesRecovery) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ecc_transport_alone_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.transport_symptom_grace = 300ms;  // the wait under test
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-a", "ecc-b"});
    auto wa = std::make_unique<EccFakeWorker>(port, "ecc-a");
    auto wb = std::make_unique<EccFakeWorker>(port, "ecc-b");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    const auto job_id = coordinator.submit_job(
        ecc_three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto d = ecc_deploy(*wa, *wb, job_id);
    ASSERT_FALSE(d.a.empty() || d.b.empty()) << "the graph never deployed across both workers";

    // One subtask reports a transport-only failure, and nothing else happens:
    // no worker dies, no other subtask errors.
    ASSERT_TRUE(wa->send_finished(job_id,
                                  d.a[0].first,
                                  d.a[0].second,
                                  /*had_error=*/true,
                                  /*fatal=*/false,
                                  /*transport_only=*/true));

    // The grace expires with no cause behind it, so the transport failure IS
    // the cause and recovery starts on it. Without this, a refused send in a
    // healthy job is a log line and the stream stays silently short.
    EXPECT_TRUE(wb->await_frame(MessageKind::CancelJob, 5s).has_value())
        << "a transport failure with no cause behind it was never acted on: the job carried on "
           "with records missing from its stream";
    std::filesystem::remove_all(dir);
}

TEST(ErrorCancelConvergence, ATransportFailureDoesNotStartRecoveryAheadOfItsCause) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ecc_transport_absorbed_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    // Long enough that the grace CANNOT be what starts recovery here: anything
    // that happens inside this window was driven by the real cause.
    cfg.transport_symptom_grace = 60s;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-a", "ecc-b"});
    auto wa = std::make_unique<EccFakeWorker>(port, "ecc-a");
    auto wb = std::make_unique<EccFakeWorker>(port, "ecc-b");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    const auto job_id = coordinator.submit_job(
        ecc_three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto d = ecc_deploy(*wa, *wb, job_id);
    ASSERT_FALSE(d.a.empty() || d.b.empty());

    // The survivor notices the departing peer first. This ordering - symptom
    // ahead of cause - is what produced the cascade.
    ASSERT_TRUE(wa->send_finished(job_id,
                                  d.a[0].first,
                                  d.a[0].second,
                                  /*had_error=*/true,
                                  /*fatal=*/false,
                                  /*transport_only=*/true));

    // It must not start recovery on its own.
    EXPECT_FALSE(wb->await_frame(MessageKind::CancelJob, 1s).has_value())
        << "the symptom started recovery before its cause arrived, which is how one killed "
           "worker produced two restarts and never wound down";

    // Now the cause: an ordinary (non-transport) failure. Recovery starts from
    // THAT, well inside the 60s grace, and the held symptom is absorbed.
    ASSERT_TRUE(wb->send_finished(job_id, d.b[0].first, d.b[0].second, /*had_error=*/true));
    EXPECT_TRUE(wa->await_frame(MessageKind::CancelJob, 5s).has_value())
        << "the real cause did not drive recovery";
    std::filesystem::remove_all(dir);
}

// ----- Item 73: a worker lost WHILE a whole-job restart is draining -----
//
// QUAL-06 at 292 tasks: a startup bridge failure began a whole-job restart,
// and the chaos controller's first worker kill landed in the same second.
// The restart reported success and the job never made progress again -
// RUNNING for 40 minutes with the checkpoint id frozen. At 20 tasks QUAL-05
// folded three overlapping worker losses correctly, so the first question is
// whether the overlap itself is handled at small width: a restart mid-drain
// when one of the survivors it is waiting on dies. Two ways to die - the
// watchdog notices a silent worker, or the worker comes back under the same
// id before the watchdog does - and both must end in a redeploy, never in a
// job that waits for a drain report that can no longer come.

TEST(ErrorCancelConvergence, AWorkerLostMidDrainDoesNotWedgeTheRestart) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ecc_overlap_lost_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.watchdog_interval = 50ms;
    cfg.heartbeat_timeout = 600ms;
    cfg.restart_drain_timeout = 20s;  // long: the fold, not the deadline, must free the restart
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-a", "ecc-b"});
    auto wa = std::make_unique<EccFakeWorker>(port, "ecc-a");
    auto wb = std::make_unique<EccFakeWorker>(port, "ecc-b");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    const auto job_id = coordinator.submit_job(
        ecc_three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto d = ecc_deploy(*wa, *wb, job_id);
    ASSERT_FALSE(d.a.empty() || d.b.empty()) << "the graph never deployed across both workers";

    // A non-fatal error on worker A starts a whole-job restart. Every other
    // in-flight subtask - A's remaining ones and B's - is now expected to
    // drain before the redeploy.
    ASSERT_TRUE(wa->send_finished(job_id, d.a[0].first, d.a[0].second, /*had_error=*/true));
    ASSERT_TRUE(wa->await_frame(MessageKind::CancelJob, 5s).has_value())
        << "the restart never broadcast its cancels";

    // THE OVERLAP: worker B dies before it can report anything. Its drain
    // reports can never come. The watchdog must notice, fold B's subtasks out
    // of the drain expectation, and let the restart proceed.
    wb->close();
    wb.reset();
    // A's other subtasks obey the cancel.
    for (std::size_t i = 1; i < d.a.size(); ++i) {
        ASSERT_TRUE(wa->send_finished(job_id, d.a[i].first, d.a[i].second, /*had_error=*/false));
    }

    // The redeploy must reach the surviving worker. Not "eventually at the
    // drain deadline" - the fold is what frees it, well inside 20s.
    EXPECT_TRUE(wa->await_frame(MessageKind::Deploy, 10s).has_value())
        << "no redeploy within 10s of the overlap: the restart is waiting on a worker that "
           "is gone (item 73)";
    std::filesystem::remove_all(dir);
}

TEST(ErrorCancelConvergence, AWorkerReturningUnderItsOwnIdMidDrainDoesNotWedgeTheRestart) {
    ensure_built_ins_registered();
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_ecc_overlap_rereg_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    Coordinator::Config cfg;
    cfg.watchdog_interval = 50ms;
    // Generous: a fast same-id return must NOT depend on the watchdog ever
    // declaring the old session lost.
    cfg.heartbeat_timeout = 30s;
    cfg.restart_drain_timeout = 20s;
    Coordinator coordinator(cfg);
    const auto port = coordinator.start();
    coordinator.expect_workers({"ecc-a", "ecc-b"});
    auto wa = std::make_unique<EccFakeWorker>(port, "ecc-a");
    auto wb = std::make_unique<EccFakeWorker>(port, "ecc-b");
    ASSERT_TRUE(wa->valid() && wb->valid());
    ASSERT_TRUE(wa->register_and_ack());
    ASSERT_TRUE(wb->register_and_ack());
    ASSERT_TRUE(coordinator.await_registrations(2s));

    CheckpointConfig ckpt;
    ckpt.checkpoint_dir = dir.string();
    const auto job_id = coordinator.submit_job(
        ecc_three_task_graph(dir / "out.txt"), OperatorRegistry::default_instance(), {}, ckpt);
    ASSERT_GT(job_id, 0U);
    auto d = ecc_deploy(*wa, *wb, job_id);
    ASSERT_FALSE(d.a.empty() || d.b.empty());

    ASSERT_TRUE(wa->send_finished(job_id, d.a[0].first, d.a[0].second, /*had_error=*/true));
    ASSERT_TRUE(wa->await_frame(MessageKind::CancelJob, 5s).has_value());

    // THE OVERLAP, orchestrator-style: B is killed and comes straight back
    // under the same id, heartbeating happily, before any watchdog tick
    // could call it lost. Its previous session's drain reports are gone.
    wb->close();
    wb.reset();
    auto wb2 = std::make_unique<EccFakeWorker>(port, "ecc-b");
    ASSERT_TRUE(wb2->valid());
    ASSERT_TRUE(wb2->register_and_ack());
    for (std::size_t i = 1; i < d.a.size(); ++i) {
        ASSERT_TRUE(wa->send_finished(job_id, d.a[i].first, d.a[i].second, /*had_error=*/false));
    }

    // Redeploy lands on A or the new B; either is a restart that fired.
    bool redeployed = false;
    const auto deadline = std::chrono::steady_clock::now() + 10s;
    while (std::chrono::steady_clock::now() < deadline && !redeployed) {
        redeployed = wa->await_frame(MessageKind::Deploy, 500ms).has_value() ||
                     wb2->await_frame(MessageKind::Deploy, 500ms).has_value();
    }
    EXPECT_TRUE(redeployed)
        << "no redeploy within 10s: the restart is waiting on a session that was replaced "
           "(item 73, same-id return)";
    std::filesystem::remove_all(dir);
}
