// AsyncTumblingWindowOperator: the first WINDOW operator on the async-state
// execution path. It exercises what the append-only async adopters (keyed
// aggregate, SQL GROUP BY) do not: epoch-gated, watermark-driven firing. The
// per-record accumulator fold reads via co_await get_async (per-key gate); a
// watermark fires due windows ONLY after every record that arrived before it
// has drained (the AsyncExecutionController's release closure); the window
// enumeration rides the framework TimerService (checkpointed + restored).
//
// Coverage: sync/async output parity (LocalExecutor, end-of-stream firing); the
// refined async tripwire admits the event-time-only operator; epoch gating (a
// deferring backend proves firing observes all in-flight reads); and
// checkpoint/restore re-fires a still-open window from restored timers + state.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/async_tumbling_window_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

using KV = std::pair<std::int64_t, std::int64_t>;  // (key, value)
using KA = std::pair<std::int64_t, std::int64_t>;  // (key, agg) emitted

// Reports async-capable but completes get_async inline (StateBackend default).
// Forces the runner's async branch deterministically. Uniquely named to avoid
// an ODR clash with the like-named doubles in other test TUs of clink_core_tests.
class AtwInlineAsyncBackend : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        store_.restore(snap, kg);
    }
    std::string description() const override { return "atw-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

// Genuinely DEFERS every get_async on a worker thread, resuming the coroutine
// on the runner thread via the wired scheduler - so a read is in-flight when a
// watermark arrives, letting a test observe epoch gating (firing waits).
class AtwDeferringBackend : public StateBackend {
public:
    AtwDeferringBackend() : worker_([this] { loop_(); }) {}
    ~AtwDeferringBackend() override {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        worker_.join();
    }

    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        store_.restore(snap, kg);
    }
    std::string description() const override { return "atw-deferring"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { resume_ = std::move(s); }

    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Defer{this, op, std::string(key)};
    }

    std::size_t deferrals() const { return deferrals_.load(std::memory_order_relaxed); }

private:
    struct Defer {
        const AtwDeferringBackend* self;
        OperatorId op;
        std::string key;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            self->deferrals_.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lk(self->mu_);
            self->jobs_.push_back(h);
            self->cv_.notify_one();
        }
        std::optional<Value> await_resume() { return self->get(op, key); }
    };

    void loop_() {
        for (;;) {
            std::coroutine_handle<> h;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !jobs_.empty(); });
                if (stop_ && jobs_.empty()) {
                    return;
                }
                h = jobs_.front();
                jobs_.pop_front();
            }
            if (resume_) {
                resume_(h);  // hand back to the runner thread
            }
        }
    }

    InMemoryStateBackend store_;
    AsyncResumeScheduler resume_;
    mutable std::mutex mu_;
    mutable std::condition_variable cv_;
    mutable std::deque<std::coroutine_handle<>> jobs_;
    bool stop_{false};
    mutable std::atomic<std::size_t> deferrals_{0};
    std::thread worker_;
};

std::shared_ptr<AsyncTumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t>> make_op() {
    return std::make_shared<AsyncTumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
        /*window_size_ms=*/1000,
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        int64_codec(),
        int64_codec(),
        "win_sum");
}

// (key, value) @ event-time-ms records.
std::vector<Record<KV>> recs(
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>>& xs) {
    std::vector<Record<KV>> out;
    for (const auto& [k, v, ts] : xs) {
        out.emplace_back(KV{k, v}, EventTime{ts});
    }
    return out;
}

// Run through the real runner; bounded source emits max watermark at EOS, so
// every window fires. Returns the emitted (key, agg) pairs, sorted.
std::vector<KA> run_windows(const std::vector<Record<KV>>& input,
                            std::shared_ptr<StateBackend> backend) {
    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(input);
    auto op = make_op();
    auto sink = std::make_shared<CollectingSink<KA>>();
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KA>(h0, op);
    dag.add_sink<KA>(h1, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    return out;
}

template <typename T>
std::vector<StreamElement<T>> drain_ch(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<StreamElement<T>> out;
    while (auto e = ch.try_pop()) {
        out.push_back(std::move(*e));
    }
    return out;
}

}  // namespace

TEST(AsyncTumblingWindowOperator, SyncAndAsyncProduceIdenticalWindows) {
    const auto input = recs({
        {1, 10, 100},  // window [0,1000) key 1
        {1, 30, 200},  // window [0,1000) key 1
        {2, 5, 300},   // window [0,1000) key 2
        {1, 7, 1500},  // window [1000,2000) key 1
    });
    // Per (key, window) sums, fired at end-of-stream max watermark:
    //   key 1 [0,1000) = 40, key 2 [0,1000) = 5, key 1 [1000,2000) = 7.
    const std::vector<KA> expected = {{1, 7}, {1, 40}, {2, 5}};

    const auto sync_out = run_windows(input, std::make_shared<InMemoryStateBackend>());
    const auto async_out = run_windows(input, std::make_shared<AtwInlineAsyncBackend>());

    EXPECT_EQ(sync_out, expected) << "synchronous process() path";
    EXPECT_EQ(async_out, expected) << "async process_async() path";
    EXPECT_EQ(sync_out, async_out) << "sync and async must be identical";
}

TEST(AsyncTumblingWindowOperator, TripwireAdmitsEventTimeOnlyWindowUnderAsync) {
    auto op = make_op();
    // It DOES fire state-touching timers (the window fire), but only event-time
    // ones, which fire in the gated on_watermark release closure - so the async
    // runner admits it.
    EXPECT_TRUE(op->supports_async());
    EXPECT_TRUE(op->fires_state_touching_timers());
    EXPECT_FALSE(op->fires_state_touching_processing_time_timers());
    // And it actually runs on the async branch without tripping (no throw).
    EXPECT_NO_THROW(run_windows(recs({{1, 1, 10}}), std::make_shared<AtwInlineAsyncBackend>()));
}

TEST(AsyncTumblingWindowOperator, EpochGatedFiringObservesAllInFlightReads) {
    auto backend = std::make_shared<AtwDeferringBackend>();
    const OperatorId op_id{1};
    RuntimeContext ctx(op_id, "win_sum", backend.get(), nullptr);
    auto op = make_op();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });

    BoundedChannel<StreamElement<KA>> ch(256);
    Emitter<KA> em(&ch);

    // Two records for (key 1, window [0,1000)), each read DEFERRED on the worker.
    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{100});
    b.emplace(KV{1, 30}, EventTime{200});
    op->process_async(StreamElement<KV>::data(std::move(b)), em, aec);
    aec.poll();

    // Watermark at 1000 closes [0,1000). Its firing must run only AFTER both
    // deferred reads have drained, so the fired aggregate sees both records.
    const Watermark wm{EventTime{1000}};
    aec.on_watermark([&] { op->on_watermark(wm, em); });
    aec.drain();  // completes the in-flight reads, then releases the watermark + fires

    EXPECT_GE(backend->deferrals(), 2u);  // reads genuinely deferred

    std::optional<KA> fired;
    for (const auto& e : drain_ch(ch)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                fired = r.value();
            }
        }
    }
    ASSERT_TRUE(fired.has_value()) << "window [0,1000) must fire on the 1000 watermark";
    EXPECT_EQ(fired->first, 1);
    EXPECT_EQ(fired->second, 40) << "firing must observe BOTH in-flight records (10 + 30)";
}

TEST(AsyncTumblingWindowOperator, TimersAndAccumulatorsSurviveCheckpointRestore) {
    const OperatorId op_id{2};

    // Phase 1: ingest into an OPEN window (no firing watermark), then checkpoint
    // both the timers (operator-state) and the accumulators (keyed state).
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "win_sum", backend1.get(), nullptr);
        auto op = make_op();
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{100});
        b.emplace(KV{1, 30}, EventTime{200});
        op->process(StreamElement<KV>::data(std::move(b)), em);  // sync path; window stays open
        op->snapshot_timers(*backend1, op_id);                   // persist the event-time timer
        snap = backend1->snapshot(CheckpointId{1});  // persist accumulators + timer blob
        EXPECT_TRUE(drain_ch(ch).empty()) << "nothing fires before the window's watermark";
    }

    // Phase 2: a FRESH operator + backend restores the checkpoint, then a
    // watermark at 1000 must re-fire window [0,1000) with the restored sum (40).
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "win_sum", backend2.get(), nullptr);
    auto op2 = make_op();
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);  // re-register the event-time timer
    op2->open();

    BoundedChannel<StreamElement<KA>> ch2(64);
    Emitter<KA> em2(&ch2);
    op2->on_watermark(Watermark{EventTime{1000}}, em2);

    std::optional<KA> fired;
    for (const auto& e : drain_ch(ch2)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                fired = r.value();
            }
        }
    }
    ASSERT_TRUE(fired.has_value()) << "restored open window must re-fire";
    EXPECT_EQ(fired->first, 1);
    EXPECT_EQ(fired->second, 40) << "restored accumulator (10 + 30) must be intact";
}

TEST(AsyncTumblingWindowOperator, LateDropBoundarySurvivesRestore) {
    const OperatorId op_id{3};

    // Phase 1: ingest + FIRE window [0,1000) (watermark 1000), then checkpoint.
    // The window is now fired+purged; the late-drop boundary (1000) is persisted.
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "win_sum", backend1.get(), nullptr);
        auto op = make_op();
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{100});
        op->process(StreamElement<KV>::data(std::move(b)), em);
        op->on_watermark(Watermark{EventTime{1000}}, em);  // fires + purges [0,1000)
        op->snapshot_timers(*backend1, op_id);
        snap = backend1->snapshot(CheckpointId{1});
    }

    // Phase 2: restore, then a genuinely-late record for the already-fired
    // window must be DROPPED (boundary 1000 was restored), so a later watermark
    // emits NOTHING - no duplicate window.
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "win_sum", backend2.get(), nullptr);
    auto op2 = make_op();
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);
    op2->open();  // reloads current_watermark_ = 1000

    BoundedChannel<StreamElement<KA>> ch2(64);
    Emitter<KA> em2(&ch2);
    Batch<KV> late;
    late.emplace(KV{1, 99}, EventTime{200});  // window [0,1000), already fired
    op2->process(StreamElement<KV>::data(std::move(late)), em2);
    op2->on_watermark(Watermark{EventTime{2000}}, em2);

    bool any_data = false;
    for (const auto& e : drain_ch(ch2)) {
        if (e.is_data() && !e.as_data().empty()) {
            any_data = true;
        }
    }
    EXPECT_FALSE(any_data) << "a late record for a pre-restore-fired window must not re-emit";
}
