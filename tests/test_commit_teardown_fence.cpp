// A commit dispatched from the worker must never reach a sink its runner has
// begun to tear down.
//
// The worker delivers CommitCheckpoint to a sink's commit callback on its
// commit-dispatch thread, through the task's CommitDispatchGate. The gate used
// to retire only when the task's executor was destroyed, while the sink's own
// runner had torn the sink down earlier: close() or close_cancelled(), then
// attach_runtime(nullptr). A commit dispatched into that window ran
// CommittingSink::finalise_ against a sink with no runtime, found no state
// backend, returned without committing and without throwing, and the worker
// counted the subtask as committed and sent CommitConfirmed for a commit that
// never ran. That is the QUAL-01 class of defect again: a confirmation the
// commit-confirmed restore protocol believes, for a transaction nothing will
// ever commit. A commit already running when the teardown began ran against a
// sink being closed under it.
//
// The rule these tests pin: once a sink's teardown begins, a commit already
// inside its external call finishes before close() proceeds, one that has not
// started it declines, and a later one is refused, logged and not confirmed,
// with the prepared handle left for restore-time recovery. The first three
// drive a real coordinator and worker and read what the worker confirmed off
// the wire. The last two hold the sink runner itself to the two edges of the
// rule: the end-of-stream wait still lets the tail's commit in, and an exit by
// exception still fences.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/commit_dispatch_gate.hpp"
#include "clink/cluster/coordinator.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/messages.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"
#include "clink/cluster/type_registry.hpp"
#include "clink/cluster/worker.hpp"
#include "clink/connectors/committing_sink.hpp"
#include "clink/fault/fault_injection.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/log_buffer.hpp"
#include "clink/runtime/network/connection.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "tests/test_helpers/sanitizer_slack.hpp"

using namespace clink;
using namespace clink::cluster;
using namespace std::chrono_literals;

namespace {

namespace points = clink::fault::points;

template <typename Pred>
bool fence_await(Pred pred,
                 std::chrono::milliseconds bound = clink::test_support::scale_slack(10s)) {
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// Every CommitConfirmed the worker put on the wire. Fed from the worker's own
// writes, so it records the worker's decision to confirm, not what the
// coordinator made of it (a job whose sinks can re-commit after a crash is not
// tracked, and the coordinator ignores its confirmations).
class FenceSentFrames {
public:
    void record(const std::byte* data, std::size_t len) {
        std::lock_guard lock(mu_);
        pending_.insert(pending_.end(), data, data + len);
        // A frame is a 4-byte big-endian length, then the body: kind first.
        while (pending_.size() >= 4) {
            std::size_t body_len = 0;
            for (std::size_t i = 0; i < 4; ++i) {
                body_len = (body_len << 8) | static_cast<unsigned char>(pending_[i]);
            }
            if (pending_.size() < 4 + body_len) {
                break;
            }
            const auto body_begin = pending_.begin() + 4;
            const auto body_end = body_begin + static_cast<std::ptrdiff_t>(body_len);
            std::vector<std::byte> body(body_begin, body_end);
            pending_.erase(pending_.begin(), body_end);
            if (!body.empty() &&
                static_cast<MessageKind>(body.front()) == MessageKind::CommitConfirmed) {
                MessageReader r(std::move(body));
                (void)r.read_u8();
                confirmed_.push_back(decode_commit_confirmed(r));
            }
        }
    }

    [[nodiscard]] std::size_t confirmations(JobId job,
                                            std::uint64_t checkpoint,
                                            std::uint32_t subtask) const {
        std::lock_guard lock(mu_);
        return static_cast<std::size_t>(
            std::count_if(confirmed_.begin(), confirmed_.end(), [&](const CommitConfirmedMsg& m) {
                return m.job_id == job && m.checkpoint_id == checkpoint && m.subtask_idx == subtask;
            }));
    }

private:
    mutable std::mutex mu_;
    std::vector<std::byte> pending_;
    std::vector<CommitConfirmedMsg> confirmed_;
};

// The worker's control connection, passed through, with every write recorded.
class FenceRecordingConnection final : public network::Connection {
public:
    FenceRecordingConnection(std::unique_ptr<network::Connection> inner,
                             std::shared_ptr<FenceSentFrames> sent)
        : inner_(std::move(inner)), sent_(std::move(sent)) {}

    bool send_all(const std::byte* buf, std::size_t len) override {
        sent_->record(buf, len);
        return inner_->send_all(buf, len);
    }
    bool recv_all(std::byte* buf, std::size_t len) override { return inner_->recv_all(buf, len); }
    void shutdown_write() override { inner_->shutdown_write(); }
    void shutdown_read() override { inner_->shutdown_read(); }
    void close() override { inner_->close(); }
    bool is_open() const noexcept override { return inner_->is_open(); }
    bool set_recv_timeout(std::chrono::milliseconds t) override {
        return inner_->set_recv_timeout(t);
    }
    bool set_send_timeout(std::chrono::milliseconds t) override {
        return inner_->set_send_timeout(t);
    }

private:
    std::unique_ptr<network::Connection> inner_;
    std::shared_ptr<FenceSentFrames> sent_;
};

// What the test sink did, in the order it did it. The sink's commit() is its
// external effect, so "commit txn-N" in this log is the commit having run.
class FenceEventLog {
public:
    void add(std::string event) {
        std::lock_guard lock(mu_);
        events_.push_back(std::move(event));
    }
    void clear() {
        std::lock_guard lock(mu_);
        events_.clear();
    }
    [[nodiscard]] std::size_t count(const std::string& event) const {
        std::lock_guard lock(mu_);
        return static_cast<std::size_t>(std::count(events_.begin(), events_.end(), event));
    }
    // Position of the first occurrence, or -1.
    [[nodiscard]] std::ptrdiff_t index_of(const std::string& event) const {
        std::lock_guard lock(mu_);
        const auto it = std::find(events_.begin(), events_.end(), event);
        return it == events_.end() ? -1 : it - events_.begin();
    }
    [[nodiscard]] std::string dump() const {
        std::lock_guard lock(mu_);
        std::string out;
        for (const auto& e : events_) {
            out += "\n  " + e;
        }
        return out;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::string> events_;
};

FenceEventLog& event_log() {
    static FenceEventLog log;
    return log;
}

std::atomic<std::uint64_t>& ticks_produced() {
    static std::atomic<std::uint64_t> n{0};
    return n;
}

// Paced and unbounded: the job runs for as long as the test needs it to.
class FenceTickingSource final : public Source<std::string> {
public:
    bool produce(Emitter<std::string>& out) override {
        if (this->cancelled()) {
            return false;
        }
        Batch<std::string> b;
        b.emplace("tick-" + std::to_string(next_++));
        out.emit_data(std::move(b));
        ticks_produced().fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(2ms);
        return true;
    }
    std::string name() const override { return "commit_fence_ticking_source"; }

private:
    std::uint64_t next_{0};
};

// A committing sink whose transaction for checkpoint N is "txn-N", and whose
// every step lands in the event log.
// Holds the test sink's commit() inside its external call - after
// "commit-begin", before "commit" - until the test releases it. One commit per
// arm.
class FenceCommitHold {
public:
    void arm() {
        std::lock_guard lock(mu_);
        armed_ = true;
        released_ = false;
    }
    void release() {
        {
            std::lock_guard lock(mu_);
            released_ = true;
        }
        cv_.notify_all();
    }
    void hold_if_armed() {
        std::unique_lock lock(mu_);
        if (!armed_) {
            return;
        }
        armed_ = false;
        cv_.wait(lock, [this] { return released_; });
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    bool armed_{false};
    bool released_{false};
};

FenceCommitHold& commit_hold() {
    static FenceCommitHold hold;
    return hold;
}

class FenceEventLogSink final : public CommittingSink<std::string, std::string> {
public:
    explicit FenceEventLogSink(std::uint32_t subtask) : CommittingSink(subtask) {}

    void write(const Batch<std::string>& /*batch*/) override {}
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        event_log().add("prepare txn-" + std::to_string(checkpoint_id));
        return "txn-" + std::to_string(checkpoint_id);
    }
    // The external commit: "commit-begin" when it starts, "commit" once it ran.
    bool commit(const std::string& txn) override {
        event_log().add("commit-begin " + txn);
        commit_hold().hold_if_armed();
        event_log().add("commit " + txn);
        return true;
    }
    void abort(const std::string& txn) override { event_log().add("abort " + txn); }
    std::string serialize(const std::string& txn) const override { return txn; }
    std::string deserialize(std::string_view bytes) const override { return std::string{bytes}; }
    void flush() override { event_log().add("flush"); }
    void close() override { event_log().add("close"); }
    std::string name() const override { return "commit_fence_event_log_sink"; }
};

// A committing sink whose first write fails, so its runner exits by exception.
class FenceFailingSink final : public CommittingSink<std::string, std::string> {
public:
    void write(const Batch<std::string>& /*batch*/) override {
        throw std::runtime_error("injected write failure");
    }
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        return "txn-" + std::to_string(checkpoint_id);
    }
    bool commit(const std::string& /*txn*/) override { return true; }
    std::string serialize(const std::string& txn) const override { return txn; }
    std::string deserialize(std::string_view bytes) const override { return std::string{bytes}; }
    std::string name() const override { return "commit_fence_failing_sink"; }
};

using FenceChannel = BoundedChannel<StreamElement<std::string>>;

StreamElement<std::string> fence_row(std::string value) {
    Batch<std::string> b;
    b.emplace(std::move(value));
    return StreamElement<std::string>::data(std::move(b));
}

// The test sink the factory built last, so a test can watch its teardown.
std::mutex& last_sink_mu() {
    static std::mutex mu;
    return mu;
}
std::weak_ptr<FenceEventLogSink>& last_sink() {
    static std::weak_ptr<FenceEventLogSink> sink;
    return sink;
}

void register_fence_test_ops() {
    static std::once_flag once;
    std::call_once(once, [] {
        ensure_built_ins_registered();
        clink::plugin::PluginRegistry reg(TypeRegistry::default_instance(),
                                          RunnerRegistry::default_instance(),
                                          SelectorRegistry::default_instance());
        reg.register_source<std::string>(
            "commit_fence_ticking_source",
            [](const clink::plugin::BuildContext&) -> std::shared_ptr<Source<std::string>> {
                return std::make_shared<FenceTickingSource>();
            });
        reg.register_sink<std::string>(
            "commit_fence_event_log_sink",
            [](const clink::plugin::BuildContext& ctx) -> std::shared_ptr<Sink<std::string>> {
                auto sink = std::make_shared<FenceEventLogSink>(ctx.subtask_idx);
                std::lock_guard lock(last_sink_mu());
                last_sink() = sink;
                return sink;
            });
    });
}

// Whether the most recently built test sink's runner has begun its teardown:
// the moment from which the runner waits for a dispatch already running.
bool sink_teardown_begun() {
    std::lock_guard lock(last_sink_mu());
    const auto sink = last_sink().lock();
    return sink != nullptr && sink->teardown_begun();
}

// source -> committing sink, one subtask each, on separate tasks.
JobGraphSpec fence_graph() {
    JobGraphSpec g;
    OperatorSpec src;
    src.id = "src";
    src.type = "commit_fence_ticking_source";
    src.parallelism = 1;
    src.out_channel = std::string{kChannelString};
    g.ops.push_back(src);
    OperatorSpec snk;
    snk.id = "snk";
    // Stable across the restore, so the restored sink finds its handles.
    snk.uid = "commit-fence-sink";
    snk.type = "commit_fence_event_log_sink";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{kChannelString};
    g.ops.push_back(snk);
    return g;
}

CheckpointConfig fence_checkpoints(const std::filesystem::path& dir) {
    CheckpointConfig c;
    c.checkpoint_dir = dir.string();
    // No periodic checkpoints: the one checkpoint under test is a savepoint,
    // so nothing completes after it and nothing purges it.
    c.interval_ms = 0;
    c.max_restarts_on_worker_loss = 0;
    return c;
}

// The sink's job-global subtask index: what CommitConfirmed names it by.
std::optional<std::uint32_t> await_sink_subtask(Coordinator& coordinator, JobId job) {
    std::optional<std::uint32_t> sub;
    fence_await([&] {
        const auto g = coordinator.snapshot_job_graph(job);
        if (!g.has_value()) {
            return false;
        }
        for (const auto& node : g->nodes) {
            if (node.id == "snk" && !node.subtasks.empty()) {
                sub = node.subtasks.front().subtask_idx;
                return true;
            }
        }
        return false;
    });
    return sub;
}

std::filesystem::path fence_scratch(const std::string& tag) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("clink_commit_fence_" + tag + "_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

bool worker_logged(std::int64_t since_ms, const std::string& needle) {
    for (const auto& rec : LogBuffer::global().tail(2000, "info", since_ms, "worker.commit")) {
        if (rec.message.find(needle) != std::string::npos) {
            return true;
        }
    }
    return false;
}

// A coordinator and one worker whose control connection is recorded.
struct FenceCluster {
    FenceCluster() : coordinator(coordinator_config()) {
        const auto port = coordinator.start();
        coordinator.expect_workers({"worker-commit-fence"});
        Worker::Config wcfg;
        wcfg.slot_count = 4;
        worker = std::make_unique<Worker>("worker-commit-fence", "127.0.0.1", wcfg);
        worker->set_connect_factory([sent_frames = sent](const std::string& host, std::uint16_t p) {
            return std::make_unique<FenceRecordingConnection>(network::connect_plain(host, p),
                                                              sent_frames);
        });
        worker->connect_to_coordinator("127.0.0.1", port);
    }
    ~FenceCluster() {
        // Release anything still parked before tearing down around it.
        clink::fault::Registry::instance().reset();
        worker->stop();
        coordinator.stop();
    }
    FenceCluster(const FenceCluster&) = delete;
    FenceCluster& operator=(const FenceCluster&) = delete;
    FenceCluster(FenceCluster&&) = delete;
    FenceCluster& operator=(FenceCluster&&) = delete;

    static Coordinator::Config coordinator_config() {
        Coordinator::Config cfg;
        cfg.max_restarts = 0;
        return cfg;
    }

    JobId submit(const CheckpointConfig& checkpoints) {
        return coordinator.submit_job(fence_graph(),
                                      OperatorRegistry::default_instance(),
                                      std::vector<PluginBinary>{},
                                      checkpoints);
    }

    std::shared_ptr<FenceSentFrames> sent = std::make_shared<FenceSentFrames>();
    Coordinator coordinator;
    std::unique_ptr<Worker> worker;
};

}  // namespace

// The silent confirmation, reproduced. The savepoint's CommitCheckpoint is held
// on the worker's dispatch thread before it reaches any callback, the job is
// cancelled, and every sink runner is held once it has closed its sink and
// detached the runtime, with the task's executor still standing. Only then
// does the dispatch go on. The sink was torn down before the commit reached
// it, so the commit must be refused and not confirmed; the handle it would
// have finalised is still in the savepoint, and a restore from there commits
// it.
//
// Before the fix the gate was still open in this window: the callback reached
// the detached sink, finalise_ found no state backend and returned, and the
// worker sent CommitConfirmed for a transaction that never committed.
TEST(CommitTeardownFence, ACommitArrivingAfterTheSinkWasTornDownIsRefusedNotConfirmed) {
    register_fence_test_ops();
    auto& faults = clink::fault::Registry::instance();
    faults.reset();
    event_log().clear();
    const auto root = fence_scratch("refused");

    // The first commit dispatch parks before any callback sees it.
    faults.arm({.point = points::kWorkerBeforeCommitDispatch,
                .ordinal = 1,
                .action = clink::fault::Action::Block});

    FenceCluster c;
    ASSERT_TRUE(c.coordinator.await_registrations(clink::test_support::scale_slack(5s)));
    const auto job = c.submit(fence_checkpoints(root / "ckpt"));
    const auto sink_sub = await_sink_subtask(c.coordinator, job);
    ASSERT_TRUE(sink_sub.has_value()) << "the sink subtask was never placed";
    // Records flow before the cut, so the transaction the savepoint seals holds
    // some.
    const auto ticks_before = ticks_produced().load();
    ASSERT_TRUE(fence_await([&] { return ticks_produced().load() >= ticks_before + 20; }));

    const auto sp = c.coordinator.take_savepoint(job, clink::test_support::scale_slack(30s));
    ASSERT_TRUE(sp.ok) << sp.message;
    const auto n = sp.checkpoint_id;
    const auto txn = "txn-" + std::to_string(n);
    ASSERT_EQ(event_log().count("prepare " + txn), 1u) << event_log().dump();
    ASSERT_TRUE(fence_await([&] { return faults.hits(points::kWorkerBeforeCommitDispatch) >= 1; }))
        << "the savepoint's commit never reached the worker's dispatch thread";

    // Every sink runner now parks once its sink is closed and detached: the
    // test sink, and the network bridge that carries the source task's output.
    faults.arm(
        {.point = points::kSinkAfterTeardown, .ordinal = 0, .action = clink::fault::Action::Block});
    ASSERT_TRUE(c.coordinator.cancel_job(job).ok);
    ASSERT_TRUE(fence_await([&] { return event_log().count("close") >= 1; }))
        << "the sink was never torn down";
    ASSERT_TRUE(fence_await([&] { return faults.hits(points::kSinkAfterTeardown) >= 2; }))
        << "the sink runners never finished their teardown";

    // The commit goes on, to a sink that is gone.
    const auto since = now_ms() - 1;
    faults.release(points::kWorkerBeforeCommitDispatch);
    const auto refusal = "checkpoint " + std::to_string(n) + " not dispatched to subtask " +
                         std::to_string(*sink_sub);
    ASSERT_TRUE(fence_await([&] {
        return c.sent->confirmations(job, n, *sink_sub) > 0 || worker_logged(since, refusal);
    })) << "the dispatch of checkpoint "
        << n << " neither confirmed nor refused";

    EXPECT_EQ(c.sent->confirmations(job, n, *sink_sub), 0u)
        << "the worker confirmed checkpoint " << n << " for the sink's subtask although the "
        << "commit reached the sink after its teardown and never ran. The commit-confirmed "
        << "restore protocol would advance past a transaction nothing will ever commit."
        << event_log().dump();
    EXPECT_TRUE(worker_logged(since, refusal))
        << "a refused commit must say so: the log line is the only witness";
    EXPECT_EQ(event_log().count("commit " + txn), 0u)
        << "the commit ran against a torn-down sink" << event_log().dump();

    faults.reset();
    ASSERT_TRUE(c.coordinator.await_job_completion(job, clink::test_support::scale_slack(30s)));

    // The handle is still in the savepoint: a restore from it commits it, once.
    auto restore = fence_checkpoints(root / "ckpt-restore");
    restore.restore_from_dir = (root / "ckpt").string();
    restore.restore_from_checkpoint_id = n;
    const auto job2 = c.submit(restore);
    EXPECT_TRUE(fence_await([&] { return event_log().count("commit " + txn) >= 1; }))
        << "the restore from checkpoint " << n << " did not re-commit its prepared handle"
        << event_log().dump();
    ASSERT_TRUE(c.coordinator.cancel_job(job2).ok);
    ASSERT_TRUE(c.coordinator.await_job_completion(job2, clink::test_support::scale_slack(30s)));
    EXPECT_EQ(event_log().count("commit " + txn), 1u) << event_log().dump();

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// The other half: a commit already running when the teardown begins. It is held
// inside its external call - the sink's commit() has started and not returned -
// when the cancel arrives. The sink must not be closed under it: close() waits
// until the commit has run, and the commit is confirmed exactly once.
//
// Before the fix the runner closed the sink at once and detached it while the
// commit was still in flight; the commit then ran against a closed sink.
TEST(CommitTeardownFence, TeardownWaitsForACommitAlreadyRunningWhichIsConfirmedOnce) {
    register_fence_test_ops();
    auto& faults = clink::fault::Registry::instance();
    faults.reset();
    event_log().clear();
    const auto root = fence_scratch("running");
    commit_hold().arm();
    struct ReleaseHold {
        ~ReleaseHold() { commit_hold().release(); }
    } release_on_exit;

    FenceCluster c;
    ASSERT_TRUE(c.coordinator.await_registrations(clink::test_support::scale_slack(5s)));
    const auto job = c.submit(fence_checkpoints(root / "ckpt"));
    const auto sink_sub = await_sink_subtask(c.coordinator, job);
    ASSERT_TRUE(sink_sub.has_value()) << "the sink subtask was never placed";

    const auto sp = c.coordinator.take_savepoint(job, clink::test_support::scale_slack(30s));
    ASSERT_TRUE(sp.ok) << sp.message;
    const auto n = sp.checkpoint_id;
    const auto commit_event = "commit txn-" + std::to_string(n);
    ASSERT_TRUE(fence_await([&] {
        return event_log().count("commit-begin txn-" + std::to_string(n)) > 0;
    })) << "the savepoint's commit never started"
        << event_log().dump();

    ASSERT_TRUE(c.coordinator.cancel_job(job).ok);
    // The runner has begun the teardown and is waiting for the commit. Without
    // that wait the sink would already be closed, under the commit.
    ASSERT_TRUE(fence_await([] { return sink_teardown_begun(); }))
        << "the sink's teardown never began";
    EXPECT_EQ(event_log().count("close"), 0u)
        << "the sink was closed while a commit dispatched to it was still running"
        << event_log().dump();

    commit_hold().release();
    ASSERT_TRUE(c.coordinator.await_job_completion(job, clink::test_support::scale_slack(30s)));
    ASSERT_TRUE(fence_await([&] { return event_log().count("close") > 0; })) << event_log().dump();

    EXPECT_EQ(event_log().count(commit_event), 1u) << event_log().dump();
    EXPECT_LT(event_log().index_of(commit_event), event_log().index_of("close"))
        << "the commit must complete before the sink is closed" << event_log().dump();
    EXPECT_TRUE(fence_await([&] { return c.sent->confirmations(job, n, *sink_sub) > 0; }))
        << "a commit that ran to completion was not confirmed";
    EXPECT_EQ(c.sent->confirmations(job, n, *sink_sub), 1u);

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// A dispatch that has entered the gate but not yet started its external commit
// when the teardown begins declines instead. The teardown waits for a running
// dispatch, so a commit that started now would hold the subtask for as long as
// the external system took to answer, indefinitely while it is down, and a
// restart drain with it (the Postgres and S3 outage-during-recovery gates hit
// exactly this). Declining is a refusal like any other: nothing committed,
// nothing confirmed, the handle left in the checkpoint for a restore.
TEST(CommitTeardownFence, ACommitNotYetStartedWhenTheTeardownBeginsIsRefusedNotWaitedFor) {
    register_fence_test_ops();
    auto& faults = clink::fault::Registry::instance();
    faults.reset();
    event_log().clear();
    const auto root = fence_scratch("not_started");

    // Handle loaded, nothing external done yet.
    faults.arm(
        {.point = points::kSinkBeforeCommit, .ordinal = 1, .action = clink::fault::Action::Block});

    FenceCluster c;
    ASSERT_TRUE(c.coordinator.await_registrations(clink::test_support::scale_slack(5s)));
    const auto job = c.submit(fence_checkpoints(root / "ckpt"));
    const auto sink_sub = await_sink_subtask(c.coordinator, job);
    ASSERT_TRUE(sink_sub.has_value()) << "the sink subtask was never placed";

    const auto sp = c.coordinator.take_savepoint(job, clink::test_support::scale_slack(30s));
    ASSERT_TRUE(sp.ok) << sp.message;
    const auto n = sp.checkpoint_id;
    const auto txn = "txn-" + std::to_string(n);
    ASSERT_TRUE(fence_await([&] { return faults.hits(points::kSinkBeforeCommit) >= 1; }))
        << "the savepoint's commit never reached the sink";

    ASSERT_TRUE(c.coordinator.cancel_job(job).ok);
    ASSERT_TRUE(fence_await([] { return sink_teardown_begun(); }))
        << "the sink's teardown never began";
    EXPECT_EQ(event_log().count("close"), 0u)
        << "the sink was closed while a dispatch was still running" << event_log().dump();

    const auto since = now_ms() - 1;
    faults.release(points::kSinkBeforeCommit);
    ASSERT_TRUE(c.coordinator.await_job_completion(job, clink::test_support::scale_slack(30s)))
        << "the teardown never finished";

    EXPECT_EQ(event_log().count("commit-begin " + txn), 0u)
        << "a commit that had not started when the teardown began went on to start"
        << event_log().dump();
    EXPECT_EQ(c.sent->confirmations(job, n, *sink_sub), 0u)
        << "a commit the sink declined was confirmed";
    bool refusal_logged = false;
    for (const auto& rec : LogBuffer::global().tail(2000, "error", since, "worker.commit")) {
        refusal_logged =
            refusal_logged ||
            (rec.message.find("checkpoint " + std::to_string(n)) != std::string::npos &&
             rec.message.find("teardown began") != std::string::npos);
    }
    EXPECT_TRUE(refusal_logged) << "the declined commit left no witness in the worker's log";

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

// The fence must not cost a bounded job its tail. At a clean end of stream the
// runner holds a committing sink open while a prepared handle awaits its
// commit (staged_commits_outstanding), because that commit arrives from another
// thread, possibly another worker. The commit arriving during that wait must
// run: the teardown, and the fence with it, comes after the wait.
TEST(CommitTeardownFence, ACommitArrivingDuringTheEndOfStreamWaitStillRuns) {
    event_log().clear();
    auto in = std::make_shared<FenceChannel>(16);
    auto sink = std::make_shared<FenceEventLogSink>(0);
    Dag dag;
    const auto stage = dag.add_sink<std::string>(StageHandle<std::string>{in, 0}, sink);
    // What the worker and the sink's commit registration set up.
    auto gate = std::make_shared<CommitDispatchGate>();
    sink->set_commit_dispatch_fence([gate] { gate->retire_and_drain(); });
    const auto commit = gated_dispatch(gate, [sink](std::uint64_t ckpt) { sink->on_commit(ckpt); });

    InMemoryStateBackend backend;
    const auto& runner = dag.runners()[stage.runner_index];
    RuntimeContext ctx(runner.id, runner.name, &backend, &MetricsRegistry::global());
    std::exception_ptr runner_failure;
    std::thread task([&] {
        try {
            runner.run(ctx, [] { return false; });
        } catch (...) {
            runner_failure = std::current_exception();
        }
    });

    in->push(fence_row("tail"));
    in->push(StreamElement<std::string>::barrier(CheckpointBarrier{CheckpointId{1}}));
    in->close(ChannelCloseReason::Finished);
    // flush() runs at a clean end of stream, just before the wait.
    ASSERT_TRUE(fence_await([] { return event_log().count("flush") > 0; })) << event_log().dump();
    EXPECT_NO_THROW(commit(1))
        << "the final checkpoint's commit was refused while the runner was waiting for it";
    task.join();

    EXPECT_FALSE(runner_failure) << "the runner gave up waiting for the tail's commit";
    EXPECT_EQ(event_log().count("commit txn-1"), 1u) << event_log().dump();
    EXPECT_LT(event_log().index_of("commit txn-1"), event_log().index_of("close"))
        << event_log().dump();
    // The teardown still fenced dispatch, and detached the sink, behind it.
    EXPECT_TRUE(gate->retired());
    EXPECT_TRUE(sink->runtime_detached());
    EXPECT_THROW(commit(2), CommitDispatchRefused);
}

// A sink runner that fails never closes its sink, but the RuntimeContext the
// sink holds is destroyed with the executor all the same. Its exit still fences
// commit dispatch and detaches the runtime, so a commit that comes later is
// refused rather than reaching through a dead context.
TEST(CommitTeardownFence, ARunnerThatThrowsStillFencesCommitDispatchAndDetachesItsSink) {
    auto in = std::make_shared<FenceChannel>(16);
    auto sink = std::make_shared<FenceFailingSink>();
    Dag dag;
    const auto stage = dag.add_sink<std::string>(StageHandle<std::string>{in, 0}, sink);
    auto gate = std::make_shared<CommitDispatchGate>();
    sink->set_commit_dispatch_fence([gate] { gate->retire_and_drain(); });
    const auto commit = gated_dispatch(gate, [sink](std::uint64_t ckpt) { sink->on_commit(ckpt); });

    InMemoryStateBackend backend;
    const auto& runner = dag.runners()[stage.runner_index];
    RuntimeContext ctx(runner.id, runner.name, &backend, &MetricsRegistry::global());
    in->push(fence_row("boom"));
    in->close(ChannelCloseReason::Finished);
    EXPECT_THROW(runner.run(ctx, [] { return false; }), std::runtime_error);

    EXPECT_TRUE(gate->retired()) << "a runner that failed left commit dispatch open to its sink";
    EXPECT_EQ(sink->runtime(), nullptr) << "a runner that failed left its sink attached";
    EXPECT_TRUE(sink->runtime_detached());
    EXPECT_THROW(commit(1), CommitDispatchRefused);
}
