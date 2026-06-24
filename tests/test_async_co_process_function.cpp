// AsyncKeyedCoProcessFunction: the async-state two-input co-process function
// (the previously-deferred multi-input async path). Both keyed inputs share one
// keyed state and one AsyncExecutionController, so same-key records from either
// side serialise through the per-key gate while distinct keys overlap;
// process_element{1,2} co_await state reads; on_timer is synchronous.
//
// Coverage:
//   * SyncAsyncParity - same op on a sync backend (resume-to-completion) and an
//     inline-async backend produce identical output (disjoint keys -> order
//     independent), through the real add_co_operator runner.
//   * CrossInputPerKeySerialisation - a right-input record for key K parks
//     behind an in-flight left-input read for K and observes its write.
//   * DistinctKeyOverlapAcrossInputs - a left key and a right key run
//     concurrently.
//   * ProcessingTimeTimerGatedAcrossInputs - a timer parks behind a same-key
//     in-flight read.
//   * EventTimeTimerFiresOnMergedWatermark - placed via the merged-watermark
//     epoch release.

#include <algorithm>
#include <atomic>
#include <coroutine>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/async_co_process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"
#include "clink/state/remote_pool.hpp"
#include "clink/state/remote_read_backend.hpp"

using namespace clink;

namespace {

// Deferring backend, manual release (no worker thread) - an in-flight read
// stays suspended until the test calls release_all(). Uniquely named (Acpf*).
class AcpfManualBackend : public StateBackend {
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
    std::string description() const override { return "acpf-manual"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { resume_ = std::move(s); }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Defer{this, op, std::string(key)};
    }
    void release_all() {
        std::vector<std::coroutine_handle<>> hs;
        hs.swap(pending_);
        for (auto h : hs) {
            if (resume_) {
                resume_(h);
            }
        }
    }
    std::size_t pending() const noexcept { return pending_.size(); }

private:
    struct Defer {
        const AcpfManualBackend* self;
        OperatorId op;
        std::string key;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const { self->pending_.push_back(h); }
        std::optional<Value> await_resume() const { return self->get(op, key); }
    };
    InMemoryStateBackend store_;
    AsyncResumeScheduler resume_;
    mutable std::vector<std::coroutine_handle<>> pending_;
};

// Async-capable, completes get_async inline (forces the async branch).
class AcpfInlineAsyncBackend : public StateBackend {
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
    std::string description() const override { return "acpf-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

// Running SUM per key across BOTH inputs (process_element1/2 share state); each
// record optionally (re)registers a processing-time timer that adds 100 on fire.
// Value doubles as key (identity extractors).
class AcpfCoSumFn final
    : public AsyncKeyedCoProcessFunction<std::int64_t, std::int64_t, std::int64_t, std::int64_t> {
public:
    AcpfCoSumFn(std::int64_t proc_timer_ts, std::int64_t event_timer_ts)
        : proc_timer_ts_(proc_timer_ts), event_timer_ts_(event_timer_ts) {}

    void open(RuntimeContext& rt) override {
        state_.emplace(
            rt.keyed_state<std::int64_t, std::int64_t>("c", int64_codec(), int64_codec()));
    }

    async::Task<void> process_element1(const std::int64_t& key,
                                       const std::int64_t& value,
                                       AsyncKeyedProcessContext<std::int64_t>& ctx,
                                       Collector<std::int64_t>& out) override {
        co_await fold_(key, value, ctx, out);
        co_return;
    }
    async::Task<void> process_element2(const std::int64_t& key,
                                       const std::int64_t& value,
                                       AsyncKeyedProcessContext<std::int64_t>& ctx,
                                       Collector<std::int64_t>& out) override {
        co_await fold_(key, value, ctx, out);
        co_return;
    }

    void on_timer(const std::int64_t& key,
                  std::int64_t /*ts*/,
                  AsyncKeyedOnTimerContext<std::int64_t>& /*ctx*/,
                  Collector<std::int64_t>& out) override {
        const std::int64_t cur = state_->get(key).value_or(0);
        state_->put(key, cur + 100);
        out.collect(cur + 100);
    }

private:
    async::Task<void> fold_(std::int64_t key,
                            std::int64_t value,
                            AsyncKeyedProcessContext<std::int64_t>& ctx,
                            Collector<std::int64_t>& out) {
        auto cur = co_await state_->get_async(key);
        const std::int64_t total = cur.value_or(0) + value;
        state_->put(key, total);
        if (proc_timer_ts_ >= 0) {
            ctx.register_processing_time_timer(proc_timer_ts_);
        }
        if (event_timer_ts_ >= 0) {
            ctx.register_event_time_timer(event_timer_ts_);
        }
        out.collect(total);
        co_return;
    }

    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
    std::int64_t proc_timer_ts_;
    std::int64_t event_timer_ts_;
};

using Adapter = detail::
    AsyncKeyedCoProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t, std::int64_t>;

std::shared_ptr<Adapter> make_adapter(std::int64_t proc_timer_ts = -1,
                                      std::int64_t event_timer_ts = -1) {
    auto fn = std::make_shared<AcpfCoSumFn>(proc_timer_ts, event_timer_ts);
    return std::make_shared<Adapter>(
        fn,
        [](const std::int64_t& v) { return v; },
        [](const std::int64_t& v) { return v; },
        int64_codec(),
        "acpf");
}

StreamElement<std::int64_t> data1(std::int64_t v) {
    Batch<std::int64_t> b;
    b.emplace(v);
    return StreamElement<std::int64_t>::data(std::move(b));
}

void drain_releasing(AcpfManualBackend& backend, AsyncExecutionController& aec) {
    while (aec.in_flight() > 0 || aec.parked() > 0) {
        backend.release_all();
        aec.poll();
    }
    aec.poll();
}

std::vector<std::int64_t> drain_data(BoundedChannel<StreamElement<std::int64_t>>& ch) {
    std::vector<std::int64_t> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    return out;
}

// End-to-end through the real add_co_operator runner. Disjoint left/right keys
// keep the per-key running sums order-independent, so a sorted compare is
// deterministic across the two-channel interleaving.
std::vector<std::int64_t> run_co_e2e(const std::vector<std::int64_t>& left_vals,
                                     const std::vector<std::int64_t>& right_vals,
                                     std::shared_ptr<StateBackend> backend,
                                     std::int64_t proc_timer_ts) {
    Dag dag;
    std::vector<Record<std::int64_t>> lrecs;
    for (auto x : left_vals) {
        lrecs.emplace_back(x);
    }
    std::vector<Record<std::int64_t>> rrecs;
    for (auto x : right_vals) {
        rrecs.emplace_back(x);
    }
    auto left = std::make_shared<VectorSource<std::int64_t>>(lrecs);
    auto right = std::make_shared<VectorSource<std::int64_t>>(rrecs);
    auto h_l = dag.add_source<std::int64_t>(left);
    auto h_r = dag.add_source<std::int64_t>(right);
    auto op = make_adapter(proc_timer_ts);
    auto h_co = dag.add_co_operator<std::int64_t, std::int64_t, std::int64_t>(h_l, h_r, op);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_co, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(AsyncCoProcessFunction, SyncAsyncParity) {
    // Disjoint keys: left {1,2}, right {3,4}; each key sees one record (sum =
    // value), then its always-due processing-time timer fires (+100). Sorted
    // output is order-independent and identical on the sync and async paths.
    const std::vector<std::int64_t> expected = {1, 2, 3, 4, 101, 102, 103, 104};
    const auto sync_out =
        run_co_e2e({1, 2}, {3, 4}, std::make_shared<InMemoryStateBackend>(), /*proc_timer_ts=*/0);
    const auto async_out =
        run_co_e2e({1, 2}, {3, 4}, std::make_shared<AcpfInlineAsyncBackend>(), /*proc_timer_ts=*/0);
    EXPECT_EQ(sync_out, expected) << "synchronous co-op path";
    EXPECT_EQ(async_out, expected) << "async co-op path";
    EXPECT_EQ(sync_out, async_out);
}

TEST(AsyncCoProcessFunction, CrossInputPerKeySerialisation) {
    auto backend = std::make_shared<AcpfManualBackend>();
    const OperatorId op_id{1};
    RuntimeContext ctx(op_id, "acpf", backend.get(), nullptr);
    auto op = make_adapter();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // Left record key 7 suspends on its read; a right record key 7 must PARK
    // behind it (same gate), then observe its write.
    op->process_async1(data1(7), em, aec);
    op->process_async2(data1(7), em, aec);
    aec.poll();
    EXPECT_EQ(aec.in_flight(), 1u) << "only one record for key 7 runs at a time";
    EXPECT_EQ(aec.parked(), 1u) << "the right-input record parks behind the left-input read";

    drain_releasing(*backend, aec);
    auto out = drain_data(ch);
    std::sort(out.begin(), out.end());
    EXPECT_EQ(out, (std::vector<std::int64_t>{7, 14}))
        << "left sums to 7, right observes it and sums to 14 (cross-input shared state)";
}

TEST(AsyncCoProcessFunction, DistinctKeyOverlapAcrossInputs) {
    auto backend = std::make_shared<AcpfManualBackend>();
    const OperatorId op_id{2};
    RuntimeContext ctx(op_id, "acpf", backend.get(), nullptr);
    auto op = make_adapter();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // A left key (7) and a right key (9) run concurrently - distinct keys
    // overlap regardless of input side.
    op->process_async1(data1(7), em, aec);
    op->process_async2(data1(9), em, aec);
    aec.poll();
    EXPECT_EQ(aec.in_flight(), 2u) << "distinct keys overlap across inputs";
    EXPECT_EQ(aec.parked(), 0u);

    drain_releasing(*backend, aec);
    auto out = drain_data(ch);
    std::sort(out.begin(), out.end());
    EXPECT_EQ(out, (std::vector<std::int64_t>{7, 9}));
}

TEST(AsyncCoProcessFunction, ProcessingTimeTimerGatedAcrossInputs) {
    auto backend = std::make_shared<AcpfManualBackend>();
    const OperatorId op_id{3};
    RuntimeContext ctx(op_id, "acpf", backend.get(), nullptr);
    auto op = make_adapter();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // A processing-time timer for key 7 (registered with the adapter's gate
    // bytes) is due while a left-input record for key 7 is in flight.
    const auto gate7 = [] {
        const auto b = int64_codec().encode(7);
        return std::string(reinterpret_cast<const char*>(b.data()), b.size());
    }();
    ctx.timer_service()->register_processing_time_timer(50, gate7);
    op->process_async1(data1(7), em, aec);
    aec.poll();
    ASSERT_EQ(aec.in_flight(), 1u);

    detail::gated_fire_processing_time_timers(*op, em, /*now_ms=*/100, aec);
    EXPECT_EQ(aec.parked(), 1u) << "timer parks behind the in-flight read for key 7";

    drain_releasing(*backend, aec);
    auto out = drain_data(ch);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 7) << "the record's sum emits first";
    EXPECT_EQ(out[1], 107) << "the timer ran after the record, observing its write (7 + 100)";
}

TEST(AsyncCoProcessFunction, EventTimeTimerFiresOnMergedWatermark) {
    auto backend = std::make_shared<AcpfManualBackend>();
    const OperatorId op_id{4};
    RuntimeContext ctx(op_id, "acpf", backend.get(), nullptr);
    auto op = make_adapter(/*proc_timer_ts=*/-1, /*event_timer_ts=*/1000);
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // A left-input record for key 7 registers an event-time timer at 1000 and
    // stays in flight. Firing the merged-watermark epoch release must wait for
    // the read to drain, so on_timer observes the record's write (7 -> 107).
    op->process_async1(data1(7), em, aec);
    aec.poll();
    ASSERT_EQ(aec.in_flight(), 1u);
    aec.on_watermark([&] { op->on_watermark(Watermark{EventTime{2000}}, em); });
    drain_releasing(*backend, aec);

    auto out = drain_data(ch);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 7);
    EXPECT_EQ(out[1], 107) << "event-time timer fired after the epoch drained (7 + 100)";
}

// ---- ASYNC-10: coalescing wired into the co-operator runner -------------

namespace {

// Counts batched vs per-key reads via shared atomics (survive teardown).
class CountingCoBackend final : public StateBackend {
public:
    CountingCoBackend(std::shared_ptr<std::atomic<int>> many,
                      std::shared_ptr<std::atomic<int>> single)
        : many_(std::move(many)), single_(std::move(single)) {}
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
    [[nodiscard]] std::string description() const override { return "counting-co"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        single_->fetch_add(1, std::memory_order_relaxed);
        co_return store_.get(op, key);
    }
    async::Task<std::vector<std::optional<Value>>> get_many_async(
        OperatorId op, const std::vector<std::string>& keys) const override {
        many_->fetch_add(1, std::memory_order_relaxed);
        std::vector<std::optional<Value>> out;
        out.reserve(keys.size());
        for (const auto& k : keys) {
            out.push_back(store_.get(op, KeyView{k}));
        }
        co_return out;
    }

private:
    InMemoryStateBackend store_;
    std::shared_ptr<std::atomic<int>> many_;
    std::shared_ptr<std::atomic<int>> single_;
};

// A coalescing async co-op: per-record get_async on each input, emit 0.
class CoalescingCoOp final : public CoOperator<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->keyed_state<std::int64_t, std::int64_t>(
            "c", int64_codec(), int64_codec()));
    }
    void process_element1(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    void process_element2(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool coalesce_reads() const noexcept override { return true; }
    void process_async1(const StreamElement<std::int64_t>& el,
                        Emitter<std::int64_t>& out,
                        AsyncExecutionController& aec) override {
        submit_each(el, out, aec);
    }
    void process_async2(const StreamElement<std::int64_t>& el,
                        Emitter<std::int64_t>& out,
                        AsyncExecutionController& aec) override {
        submit_each(el, out, aec);
    }

private:
    void submit_each(const StreamElement<std::int64_t>& el,
                     Emitter<std::int64_t>& out,
                     AsyncExecutionController& aec) {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            auto ks = *state_;
            aec.submit(std::to_string(k), [ks, k, &out]() mutable -> async::Task<void> {
                (void)co_await ks.get_async(k);
                Batch<std::int64_t> b;
                b.emplace(0);
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

}  // namespace

// Through the real add_co_operator runner: a batch of distinct keys on ONE
// input collapses into ONE get_many_async on the (deferring) backend, never the
// per-key get_async.
TEST(AsyncCoProcessFunction, CoalescesInputBatchIntoOneGetMany) {
    auto many = std::make_shared<std::atomic<int>>(0);
    auto single = std::make_shared<std::atomic<int>>(0);

    Dag dag;
    std::vector<Record<std::int64_t>> lrecs;
    for (std::int64_t k : {10, 20, 30, 40}) {
        lrecs.emplace_back(k);
    }
    auto left = std::make_shared<VectorSource<std::int64_t>>(lrecs);
    auto right = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{});
    auto h_l = dag.add_source<std::int64_t>(left);
    auto h_r = dag.add_source<std::int64_t>(right);
    auto op = std::make_shared<CoalescingCoOp>();
    auto h_co = dag.add_co_operator<std::int64_t, std::int64_t, std::int64_t>(h_l, h_r, op);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_co, sink);
    JobConfig cfg;
    cfg.state_backend = std::make_shared<CountingCoBackend>(many, single);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_EQ(sink->collected().size(), 4u);  // every left record emitted
    EXPECT_EQ(many->load(), 1);               // one input batch -> one get_many
    EXPECT_EQ(single->load(), 0);             // never the per-key path
}

namespace {

// Test-only async co-operator that opts into the async EVENT-TIME fire path:
// both inputs accumulate their value into per-key state and register an
// event-time timer; the merged watermark fires every due key through
// on_event_time_timers_async (the co-op runner submits one fire coroutine per
// due key, so a deferring backend coalesces the reads). The synchronous fallback
// fires through the base on_watermark -> on_event_time_timer path. Proves the
// co-op runner's async-fire wiring end to end. Key/gate/timer use one string so
// ingest and fire address the same state slice.
class AsyncFireCoOp : public CoOperator<std::int64_t, std::int64_t, std::int64_t> {
public:
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool coalesce_reads() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    [[nodiscard]] bool fires_async_event_time_timers() const noexcept override { return true; }

    void process_element1(const StreamElement<std::int64_t>& el,
                          Emitter<std::int64_t>& /*out*/) override {
        ingest_sync_(el);
    }
    void process_element2(const StreamElement<std::int64_t>& el,
                          Emitter<std::int64_t>& /*out*/) override {
        ingest_sync_(el);
    }
    void process_async1(const StreamElement<std::int64_t>& el,
                        Emitter<std::int64_t>& /*out*/,
                        AsyncExecutionController& aec) override {
        ingest_async_(el, aec);
    }
    void process_async2(const StreamElement<std::int64_t>& el,
                        Emitter<std::int64_t>& /*out*/,
                        AsyncExecutionController& aec) override {
        ingest_async_(el, aec);
    }

    // Synchronous fire (non-deferring backend, via base on_watermark).
    void on_event_time_timer(std::int64_t /*ts*/,
                             const std::string& key,
                             Emitter<std::int64_t>& out) override {
        emit_(state_().get(key).value_or(0), out);
    }
    // Async batched fire (deferring backend): one get_async per due key.
    async::Task<void> on_event_time_timers_async(std::vector<std::int64_t> /*tss*/,
                                                 std::string key,
                                                 Emitter<std::int64_t>& out) override {
        auto kv = state_();
        auto cur = co_await kv.get_async(key);
        emit_(cur.value_or(0), out);
        co_return;
    }

private:
    KeyedState<std::string, std::int64_t> state_() {
        return this->runtime()->keyed_state<std::string, std::int64_t>(
            "c", string_codec(), int64_codec());
    }
    static std::string key_of_(std::int64_t v) { return std::to_string(v); }
    static void emit_(std::int64_t v, Emitter<std::int64_t>& out) {
        Batch<std::int64_t> b;
        b.push(Record<std::int64_t>{v});
        out.emit_data(std::move(b));
    }
    void ingest_sync_(const StreamElement<std::int64_t>& el) {
        if (!el.is_data()) {
            return;
        }
        for (const auto& rec : el.as_data()) {
            const std::int64_t v = rec.value();
            const std::string key = key_of_(v);
            auto kv = state_();
            kv.put(key, kv.get(key).value_or(0) + v);
            this->runtime()->timer_service()->register_event_time_timer(5, key);
        }
    }
    void ingest_async_(const StreamElement<std::int64_t>& el, AsyncExecutionController& aec) {
        if (!el.is_data()) {
            return;
        }
        for (const auto& rec : el.as_data()) {
            const std::int64_t v = rec.value();
            const std::string key = key_of_(v);
            auto kv = state_();
            auto* rt = this->runtime();
            auto factory = [kv, v, key, rt]() mutable -> async::Task<void> {
                auto cur = co_await kv.get_async(key);
                kv.put(key, cur.value_or(0) + v);
                rt->timer_service()->register_event_time_timer(5, key);
                co_return;
            };
            while (!aec.submit(key, factory)) {
                aec.poll();
            }
        }
    }
};

std::vector<std::int64_t> run_async_fire_co(const std::vector<std::int64_t>& left_vals,
                                            const std::vector<std::int64_t>& right_vals,
                                            std::shared_ptr<StateBackend> backend) {
    Dag dag;
    std::vector<Record<std::int64_t>> lrecs;
    for (auto x : left_vals) {
        lrecs.emplace_back(x);
    }
    std::vector<Record<std::int64_t>> rrecs;
    for (auto x : right_vals) {
        rrecs.emplace_back(x);
    }
    auto h_l = dag.add_source<std::int64_t>(std::make_shared<VectorSource<std::int64_t>>(lrecs));
    auto h_r = dag.add_source<std::int64_t>(std::make_shared<VectorSource<std::int64_t>>(rrecs));
    auto op = std::make_shared<AsyncFireCoOp>();
    auto h_co = dag.add_co_operator<std::int64_t, std::int64_t, std::int64_t>(h_l, h_r, op);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    dag.add_sink<std::int64_t>(h_co, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

// The co-op runner's async EVENT-TIME fire wiring, end to end. Four distinct
// keys (left {1,2}, right {3,4}) each accumulate their value and register an
// event-time timer; the merged max watermark at EOS fires all four. The
// synchronous in-memory path (fire inside the merged-watermark release) and the
// async deferring path (gate-routed coroutines forwarded by a second epoch-tied
// release) produce identical output, and the async run routed every read through
// the coalescer (no single-key get_async reached the backend).
TEST(AsyncCoProcessFunction, EventTimeFireMatchesSyncAndCoalesces) {
    const std::vector<std::int64_t> expected = {1, 2, 3, 4};

    const auto sync_out =
        run_async_fire_co({1, 2}, {3, 4}, std::make_shared<InMemoryStateBackend>());

    auto pool = std::make_shared<InMemoryRemotePool>();
    auto rrb = std::make_shared<RemoteReadBackend>(pool, /*io_threads=*/1, /*hot_max_bytes=*/0);
    const auto async_out = run_async_fire_co({1, 2}, {3, 4}, rrb);

    EXPECT_EQ(sync_out, expected) << "synchronous co-op event-time fire";
    EXPECT_EQ(async_out, expected) << "async co-op event-time fire (the runner wiring)";
    EXPECT_EQ(async_out, sync_out);
    EXPECT_EQ(rrb->get_async_calls(), 0u) << "a read bypassed the coalescer as a single-key read";
    EXPECT_GT(rrb->get_many_async_calls(), 0u) << "no batched reads (async fire path inactive?)";
}
