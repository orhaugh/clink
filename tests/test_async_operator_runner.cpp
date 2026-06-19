// ASYNC-6: operator opt-in (supports_async / process_async) + the
// single-input runner's async branch. A reference KeyedCountOperator
// implements BOTH the synchronous process() and the async process_async()
// (submitting one coroutine per record to the AsyncExecutionController that
// co_awaits KeyedState::get_async). Running the same operator and input
// through the real DAG once on a plain backend (sync path) and once on an
// async-capable backend (async branch) must produce byte-identical output -
// the core ASYNC-6 acceptance.

#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/remote_read_backend.hpp"

using namespace clink;

namespace {

// A backend that reports it can defer reads (so the runner takes the async
// branch) but actually completes get_async inline via the StateBackend base
// default (co_return get()). This exercises the full async wiring with
// deterministic, ordered completion - the genuinely-blocking backend is
// ASYNC-8. InMemoryStateBackend is final, so it is composed and forwarded.
class InlineAsyncBackend : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        store_.restore(snap, kg_filter);
    }
    std::string description() const override { return "inline-async"; }

    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }

private:
    InMemoryStateBackend store_;
};

// Per-key occurrence counter: emits the running count for each record's key.
// Implements the sync process() and the opt-in async process_async().
class KeyedCountOperator final : public Operator<int, std::int64_t> {
public:
    void process(const StreamElement<int>& element, Emitter<std::int64_t>& out) override {
        if (element.is_data()) {
            auto kv = state_();
            Batch<std::int64_t> batch;
            for (const auto& rec : element.as_data()) {
                const std::int64_t key = rec.value();
                const std::int64_t c = kv.get(key).value_or(0) + 1;
                kv.put(key, c);
                batch.emplace(c);
            }
            out.emit_data(std::move(batch));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }

    [[nodiscard]] bool supports_async() const noexcept override { return true; }

    void process_async(const StreamElement<int>& element,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;  // the runner routes watermarks/barriers through the controller
        }
        for (const auto& rec : element.as_data()) {
            const std::int64_t key = rec.value();
            auto kv = state_();
            aec.submit(std::to_string(key), [kv, key, &out]() mutable -> async::Task<void> {
                auto cur = co_await kv.get_async(key);
                const std::int64_t c = cur.value_or(0) + 1;
                kv.put(key, c);
                Batch<std::int64_t> batch;
                batch.emplace(c);
                out.emit_data(std::move(batch));
                co_return;
            });
        }
    }

    std::string name() const override { return "keyed_count"; }

private:
    KeyedState<std::int64_t, std::int64_t> state_() {
        return this->runtime()->keyed_state<std::int64_t, std::int64_t>(
            "c", int64_codec(), int64_codec());
    }
};

std::vector<Record<int>> records(const std::vector<int>& xs) {
    std::vector<Record<int>> out;
    out.reserve(xs.size());
    for (int x : xs) {
        out.emplace_back(x);
    }
    return out;
}

std::vector<std::int64_t> run_once(const std::vector<int>& input,
                                   std::shared_ptr<StateBackend> backend) {
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(records(input));
    auto op = std::make_shared<KeyedCountOperator>();
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, std::int64_t>(h0, op);
    dag.add_sink<std::int64_t>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected();
}

}  // namespace

// The async runner branch produces byte-identical output to the synchronous
// path for the same operator and input. The plain InMemory backend reports
// supports_async_get()==false (sync path); the InlineAsyncBackend reports
// true (async branch via process_async + the controller).
TEST(AsyncOperatorRunner, AsyncPathMatchesSyncPath) {
    const std::vector<int> input = {1, 1, 2, 1, 3, 2, 1};
    // Per-key running counts in record order: 1,2,1,3,1,2,4.
    const std::vector<std::int64_t> expected = {1, 2, 1, 3, 1, 2, 4};

    const auto sync_out = run_once(input, std::make_shared<InMemoryStateBackend>());
    const auto async_out = run_once(input, std::make_shared<InlineAsyncBackend>());

    EXPECT_EQ(sync_out, expected);
    EXPECT_EQ(async_out, expected);
    EXPECT_EQ(async_out, sync_out);
}

// A non-opting operator on an async-capable backend stays on the synchronous
// path (supports_async() default false), so the async branch is inert unless
// the operator explicitly opts in.
TEST(AsyncOperatorRunner, NonOptingOperatorIgnoresAsyncBackend) {
    // MapOperator does not override supports_async(); even on the async
    // backend it runs through process(). A trivial passthrough proves the
    // pipeline still runs unchanged.
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(records({5, 6, 7}));
    auto op = std::make_shared<MapOperator<int, int>>([](int x) { return x * 2; });
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);
    JobConfig cfg;
    cfg.state_backend = std::make_shared<InlineAsyncBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    EXPECT_EQ(sink->collected(), (std::vector<int>{10, 12, 14}));
}

// async-timer gate tripwire: an operator that is BOTH async-capable AND fires
// state-touching timers is refused at runner start (the per-key-gated timer
// path is a deferred increment), so it fails loudly rather than silently racing
// a timer callback against an in-flight async read for the same key.
namespace {
class TimerBearingAsyncOperator final : public Operator<int, int> {
public:
    void process(const StreamElement<int>& element, Emitter<int>& out) override {
        if (element.is_data()) {
            out.emit_data(Batch<int>(element.as_data()));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }
    void process_async(const StreamElement<int>&,
                       Emitter<int>&,
                       AsyncExecutionController&) override {}
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    std::string name() const override { return "timer_bearing_async"; }
};
}  // namespace

TEST(AsyncOperatorRunner, RefusesAsyncOperatorThatFiresStateTouchingTimers) {
    // Sanity: the base default is false (a plain op never trips the guard).
    MapOperator<int, int> plain([](int x) { return x; });
    EXPECT_FALSE(plain.fires_state_touching_timers());

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(records({1, 2, 3}));
    auto op = std::make_shared<TimerBearingAsyncOperator>();
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);
    JobConfig cfg;
    cfg.state_backend = std::make_shared<InlineAsyncBackend>();  // supports_async_get()
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();  // the operator thread throws at setup; LocalExecutor records it

    const auto errs = exec.operator_errors();
    ASSERT_FALSE(errs.empty()) << "the async + timer-bearing operator must be refused";
    bool found = false;
    for (const auto& [op_name, msg] : errs) {
        if (msg.find("state-touching timers under async") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found) << "expected the async-timer-gate guard error";
}

// Production async link: the DAG runner must wire a deferring backend's
// resume scheduler to the per-subtask AsyncExecutionController. With it, a
// cold get_async suspends and loads on an IO thread, resuming on the runner
// thread; WITHOUT it, get_async falls back to an inline blocking load on the
// runner thread. We prove the wiring by asserting the loader ran on a thread
// other than the runner thread - impossible under the inline fallback.
namespace {
std::atomic<std::thread::id> g_rrb_runner_tid;
std::atomic<std::thread::id> g_rrb_loader_tid;

class RrbProbeOp final : public Operator<std::int64_t, std::int64_t> {
public:
    void process(const StreamElement<std::int64_t>& e, Emitter<std::int64_t>& out) override {
        // Synchronous fallback (not exercised here: the backend defers).
        if (e.is_data()) {
            Batch<std::int64_t> b;
            for (const auto& r : e.as_data())
                b.emplace(r.value());
            out.emit_data(std::move(b));
        } else if (e.is_watermark()) {
            this->on_watermark(e.as_watermark(), out);
        } else {
            this->on_barrier(e.as_barrier(), out);
        }
    }
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    void process_async(const StreamElement<std::int64_t>& e,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!e.is_data()) {
            return;
        }
        g_rrb_runner_tid.store(std::this_thread::get_id(), std::memory_order_relaxed);
        for (const auto& rec : e.as_data()) {
            const std::int64_t key = rec.value();
            auto kv = state_();
            aec.submit(std::to_string(key), [kv, key, &out]() mutable -> async::Task<void> {
                auto v = co_await kv.get_async(key);  // cold miss -> defers iff wired
                Batch<std::int64_t> b;
                b.emplace(v.value_or(key));
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    std::string name() const override { return "rrb_probe"; }

private:
    KeyedState<std::int64_t, std::int64_t> state_() {
        return this->runtime()->keyed_state<std::int64_t, std::int64_t>(
            "p", int64_codec(), int64_codec());
    }
};
}  // namespace

TEST(AsyncOperatorRunner, RunnerWiresResumeSchedulerSoColdReadsDeferOffRunner) {
    g_rrb_runner_tid.store(std::thread::id{});
    g_rrb_loader_tid.store(std::thread::id{});

    // Loader records its thread and returns nullopt (every key is a cold miss),
    // so every first get_async on a key is a remote (cold) read.
    auto backend = std::make_shared<RemoteReadBackend>(
        [](OperatorId, std::string) -> std::optional<StateBackend::Value> {
            g_rrb_loader_tid.store(std::this_thread::get_id(), std::memory_order_relaxed);
            return std::nullopt;
        });

    std::vector<Record<std::int64_t>> in;
    for (std::int64_t k : {std::int64_t{1}, std::int64_t{2}, std::int64_t{3}}) {
        in.emplace_back(k);
    }

    Dag dag;
    auto src = std::make_shared<VectorSource<std::int64_t>>(std::move(in));
    auto op = std::make_shared<RrbProbeOp>();
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    dag.add_sink<std::int64_t>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = backend;  // RemoteReadBackend, supports_async_get() == true
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_EQ(sink->collected().size(), 3u);
    EXPECT_GT(backend->remote_loads(), 0u) << "no cold reads happened";

    const auto runner_tid = g_rrb_runner_tid.load();
    const auto loader_tid = g_rrb_loader_tid.load();
    ASSERT_NE(runner_tid, std::thread::id{});
    ASSERT_NE(loader_tid, std::thread::id{});
    // The decisive check: the cold load ran on an IO thread, not the runner
    // thread. That can only happen if the runner wired set_async_resume_scheduler
    // (the inline fallback would run the loader on the runner thread).
    EXPECT_NE(loader_tid, runner_tid)
        << "cold load ran on the runner thread: the runner did not wire the async "
           "resume scheduler, so get_async fell back to an inline blocking load";
}
