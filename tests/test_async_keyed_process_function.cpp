// AsyncKeyedProcessFunction: the async-state analogue of KeyedProcessFunction
// (first general-purpose production adopter of the per-key-gated record path +
// the gated processing-time timer path). process_element co_awaits keyed-state
// reads; on_timer is synchronous and rides the gated/epoch timer paths.
//
// Coverage:
//   * SyncAsyncParity - one user coroutine, two equivalent drives (sync
//     process() resumes to inline completion; async via the controller) -> the
//     same output, including a fired timer.
//   * PerKeyRecordSerialisation - a second same-key record parks behind the
//     first's in-flight read and observes its write.
//   * DistinctKeyOverlap - distinct-key records run concurrently; each observes
//     its OWN key (the current_key race is structurally impossible).
//   * ProcessingTimeTimerSerialisesBehindInFlightRead - a due processing-time
//     timer parks behind a same-key in-flight read and runs after it.
//   * EventTimeTimerFiresInEpochRelease - an event-time timer fires only after
//     its epoch drains, observing the record write.
//   * CheckpointRestoreStateAndTimers - user keyed state + a pending timer
//     survive checkpoint/restore.

#include <algorithm>
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
#include "clink/operators/async_process_function.hpp"
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

using namespace clink;

namespace {

// Deferring backend with manual release (no worker thread) - an in-flight read
// stays suspended until the test calls release_all(). Uniquely named (Akpf*).
class AkpfManualBackend : public StateBackend {
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
    std::string description() const override { return "akpf-manual"; }
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
        const AkpfManualBackend* self;
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

// Async-capable, completes get_async inline (forces the async branch under the
// real runner without deferring).
class AkpfInlineAsyncBackend : public StateBackend {
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
    std::string description() const override { return "akpf-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

// Test vehicle: per-key running SUM of record values via co_await get_async;
// optionally registers a processing-time and/or event-time timer that, when it
// fires, adds 100 to the key's running total and emits it. The record's value
// doubles as its key (key_fn = identity), so distinct values are distinct keys.
class AkpfSumFn final : public AsyncKeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    AkpfSumFn(std::int64_t proc_timer_ts, std::int64_t event_timer_ts)
        : proc_timer_ts_(proc_timer_ts), event_timer_ts_(event_timer_ts) {}

    void open(RuntimeContext& rt) override {
        state_.emplace(
            rt.keyed_state<std::int64_t, std::int64_t>("c", int64_codec(), int64_codec()));
    }

    async::Task<void> process_element(const std::int64_t& key,
                                      const std::int64_t& value,
                                      AsyncKeyedProcessContext<std::int64_t>& ctx,
                                      Collector<std::int64_t>& out) override {
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

    void on_timer(const std::int64_t& key,
                  std::int64_t /*ts*/,
                  AsyncKeyedOnTimerContext<std::int64_t>& /*ctx*/,
                  Collector<std::int64_t>& out) override {
        const std::int64_t cur = state_->get(key).value_or(0);
        state_->put(key, cur + 100);
        out.collect(cur + 100);
    }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
    std::int64_t proc_timer_ts_;
    std::int64_t event_timer_ts_;
};

using Adapter = detail::AsyncKeyedProcessFunctionAdapter<std::int64_t, std::int64_t, std::int64_t>;

std::shared_ptr<Adapter> make_adapter(std::int64_t proc_timer_ts = -1,
                                      std::int64_t event_timer_ts = -1) {
    auto fn = std::make_shared<AkpfSumFn>(proc_timer_ts, event_timer_ts);
    return std::make_shared<Adapter>(
        fn, [](const std::int64_t& v) { return v; }, int64_codec(), "akpf");
}

// The gate bytes the adapter uses for key k (bare int64_codec encoding).
std::string gate_of(std::int64_t k) {
    const auto b = int64_codec().encode(k);
    return std::string(reinterpret_cast<const char*>(b.data()), b.size());
}

// Drive every suspended read to completion: release the manual backend's
// pending reads, poll, and repeat - a promoted (previously-parked) record
// suspends on its OWN read after promotion, so a single release + drain would
// block. Loops until no record is in flight or parked.
void drain_releasing(AkpfManualBackend& backend, AsyncExecutionController& aec) {
    while (aec.in_flight() > 0 || aec.parked() > 0) {
        backend.release_all();
        aec.poll();
    }
    aec.poll();  // settle any final watermark release / bookkeeping
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

StreamElement<std::int64_t> data1(std::int64_t v, std::optional<std::int64_t> ts = std::nullopt) {
    Batch<std::int64_t> b;
    if (ts.has_value()) {
        b.emplace(v, EventTime{*ts});
    } else {
        b.emplace(v);
    }
    return StreamElement<std::int64_t>::data(std::move(b));
}

std::vector<std::int64_t> run_e2e(const std::vector<std::int64_t>& input,
                                  std::shared_ptr<StateBackend> backend,
                                  std::int64_t proc_timer_ts) {
    Dag dag;
    std::vector<Record<std::int64_t>> recs;
    for (auto x : input) {
        recs.emplace_back(x);
    }
    auto src = std::make_shared<VectorSource<std::int64_t>>(recs);
    auto op = make_adapter(proc_timer_ts);
    auto sink = std::make_shared<CollectingSink<std::int64_t>>();
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    dag.add_sink<std::int64_t>(h1, sink);
    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    return out;
}

}  // namespace

TEST(AsyncKeyedProcessFunction, SyncAsyncParity) {
    // Single record (deterministic): sum=1 emits, then the always-due
    // processing-time timer fires once (1 + 100 = 101). Sync and async agree.
    const auto sync_out =
        run_e2e({1}, std::make_shared<InMemoryStateBackend>(), /*proc_timer_ts=*/0);
    const auto async_out =
        run_e2e({1}, std::make_shared<AkpfInlineAsyncBackend>(), /*proc_timer_ts=*/0);
    const std::vector<std::int64_t> expected = {1, 101};
    EXPECT_EQ(sync_out, expected) << "synchronous drive (resume-to-inline-completion)";
    EXPECT_EQ(async_out, expected) << "async drive (per-key gate)";
    EXPECT_EQ(sync_out, async_out);
}

TEST(AsyncKeyedProcessFunction, PerKeyRecordSerialisation) {
    auto backend = std::make_shared<AkpfManualBackend>();
    const OperatorId op_id{1};
    RuntimeContext ctx(op_id, "akpf", backend.get(), nullptr);
    auto op = make_adapter();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // Two records for the SAME key (value 1 -> key 1). Both suspend on the read;
    // the second must park behind the first.
    op->process_async(data1(1), em, aec);
    op->process_async(data1(1), em, aec);
    aec.poll();
    EXPECT_EQ(aec.in_flight(), 1u) << "only one same-key record runs at a time";
    EXPECT_EQ(aec.parked(), 1u) << "the second same-key record parks behind the first";

    drain_releasing(*backend, aec);
    const auto out = drain_data(ch);
    EXPECT_EQ(out, (std::vector<std::int64_t>{1, 2}))
        << "the second record observed the first's write (1 then 2, never 1 then 1)";
}

TEST(AsyncKeyedProcessFunction, DistinctKeyOverlap) {
    auto backend = std::make_shared<AkpfManualBackend>();
    const OperatorId op_id{2};
    RuntimeContext ctx(op_id, "akpf", backend.get(), nullptr);
    auto op = make_adapter();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // Distinct keys (1 and 2) run concurrently - neither parks.
    op->process_async(data1(1), em, aec);
    op->process_async(data1(2), em, aec);
    aec.poll();
    EXPECT_EQ(aec.in_flight(), 2u) << "distinct keys overlap";
    EXPECT_EQ(aec.parked(), 0u);

    drain_releasing(*backend, aec);
    auto out = drain_data(ch);
    std::sort(out.begin(), out.end());
    // key 1 sums to 1, key 2 sums to 2 (value == key here). A shared-current_key
    // leak would attribute both to one key and yield {1,3} or {2,3}, never {1,2}.
    EXPECT_EQ(out, (std::vector<std::int64_t>{1, 2}))
        << "each coroutine observed its OWN key (no shared current_key leak)";
}

TEST(AsyncKeyedProcessFunction, ProcessingTimeTimerSerialisesBehindInFlightRead) {
    auto backend = std::make_shared<AkpfManualBackend>();
    const OperatorId op_id{3};
    RuntimeContext ctx(op_id, "akpf", backend.get(), nullptr);
    auto op = make_adapter();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // A processing-time timer for key 1, registered (under the adapter's gate
    // bytes) in a prior turn, is due. A record for key 1 is in flight.
    ctx.timer_service()->register_processing_time_timer(50, gate_of(1));
    op->process_async(data1(1), em, aec);
    aec.poll();
    ASSERT_EQ(aec.in_flight(), 1u);

    detail::gated_fire_processing_time_timers(*op, em, /*now_ms=*/100, aec);
    EXPECT_EQ(aec.parked(), 1u) << "timer parks behind the in-flight read for key 1";
    EXPECT_TRUE(drain_data(ch).empty()) << "timer callback has not run yet";

    drain_releasing(*backend, aec);
    const auto out = drain_data(ch);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 1) << "the record's sum emits first";
    EXPECT_EQ(out[1], 101) << "the timer ran AFTER the record, observing its write (1 + 100)";
}

TEST(AsyncKeyedProcessFunction, EventTimeTimerFiresInEpochRelease) {
    auto backend = std::make_shared<AkpfManualBackend>();
    const OperatorId op_id{4};
    RuntimeContext ctx(op_id, "akpf", backend.get(), nullptr);
    auto op = make_adapter(/*proc_timer_ts=*/-1, /*event_timer_ts=*/1000);
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();
    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // Record for key 1 in flight; it registers an event-time timer at 1000.
    op->process_async(data1(1), em, aec);
    aec.poll();
    ASSERT_EQ(aec.in_flight(), 1u);

    // A watermark at 2000 must fire the event-time timer only AFTER the epoch
    // drains (the record completes), so on_timer observes the record's write.
    aec.on_watermark([&] { op->on_watermark(Watermark{EventTime{2000}}, em); });
    drain_releasing(*backend, aec);

    const auto out = drain_data(ch);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 1) << "the record emits first";
    EXPECT_EQ(out[1], 101) << "the event-time timer fired after the epoch drained (1 + 100)";
}

TEST(AsyncKeyedProcessFunction, CheckpointRestoreStateAndTimers) {
    const OperatorId op_id{5};

    // Phase 1: ingest (sum=1 for key 1) and leave a pending event-time timer at
    // 1000 (no firing watermark yet), then checkpoint state + timers.
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "akpf", backend1.get(), nullptr);
        auto op = make_adapter(/*proc_timer_ts=*/-1, /*event_timer_ts=*/1000);
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<std::int64_t>> ch(64);
        Emitter<std::int64_t> em(&ch);
        op->process(data1(1), em);  // sync path, sum=1, registers timer @1000
        op->snapshot_timers(*backend1, op_id);
        snap = backend1->snapshot(CheckpointId{1});
        EXPECT_EQ(drain_data(ch), (std::vector<std::int64_t>{1}));
    }

    // Phase 2: a fresh adapter+fn+backend restores, then a watermark past 1000
    // re-fires the restored timer, observing the restored sum (1 + 100 = 101).
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "akpf", backend2.get(), nullptr);
    auto op2 = make_adapter(/*proc_timer_ts=*/-1, /*event_timer_ts=*/1000);
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);
    op2->open();

    BoundedChannel<StreamElement<std::int64_t>> ch2(64);
    Emitter<std::int64_t> em2(&ch2);
    op2->on_watermark(Watermark{EventTime{2000}}, em2);
    EXPECT_EQ(drain_data(ch2), (std::vector<std::int64_t>{101}))
        << "restored timer re-fired and observed the restored state (1 + 100)";
}
