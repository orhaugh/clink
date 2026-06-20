// Per-key-gated PROCESSING-TIME timer fire path for async-state operators
// (clink::detail::gated_fire_processing_time_timers + the lifted runner
// tripwire). Under async, a state-touching processing-time timer callback must
// not interleave with an in-flight async read for the same key; the runner now
// routes processing-time timers through the AEC per-key gate so a timer for K
// serialises behind any in-flight record for K.
//
// Coverage:
//   * SerialisesTimerBehindInFlightRead - the core race proof: a timer for K
//     fired while a record for K is suspended on a deferred read PARKS, and runs
//     only after the record completes, observing its write (no lost update).
//   * DistinctKeyTimerRunsWhileOtherKeyInFlight - distinct keys overlap.
//   * MultipleDueTimersAllFireNoneDropped - collect-then-submit drops nothing.
//   * EndToEndGatedTimerMatchesSyncPath - the operator is ADMITTED (no throw)
//     and the gated async path matches the synchronous path.

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
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/gated_timer_fire.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

// Async backend whose get_async DEFERS and resumes only when the test calls
// release_all() - so an in-flight read stays suspended deterministically (no
// worker thread, no timing). Uniquely named (Gtf*) per the ODR discipline.
class GtfManualBackend : public StateBackend {
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
    std::string description() const override { return "gtf-manual"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { resume_ = std::move(s); }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Defer{this, op, std::string(key)};
    }

    // Test-driven: schedule every suspended read for resumption on the runner.
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
        const GtfManualBackend* self;
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
class GtfInlineAsyncBackend : public StateBackend {
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
    std::string description() const override { return "gtf-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

// The test vehicle: the first operator that is BOTH async-capable AND fires a
// state-touching PROCESSING-TIME timer. Per-key running count; each record also
// (re)registers a processing-time timer that, when it fires, adds 100 to the
// key's count and emits it. CRITICAL: the timer is registered with the SAME key
// string used as the AEC record gate (std::to_string(k)), honouring the gate-key
// contract so the runner's gating serialises it against same-key records.
class GatedProcTimerOp final : public Operator<std::int64_t, std::int64_t> {
public:
    explicit GatedProcTimerOp(std::int64_t timer_ts) : timer_ts_(timer_ts) {}

    void process(const StreamElement<std::int64_t>& element, Emitter<std::int64_t>& out) override {
        if (element.is_data()) {
            auto kv = state_();
            Batch<std::int64_t> batch;
            for (const auto& rec : element.as_data()) {
                const std::int64_t k = rec.value();
                const std::int64_t c = kv.get(k).value_or(0) + 1;
                kv.put(k, c);
                this->runtime()->timer_service()->register_processing_time_timer(timer_ts_,
                                                                                 std::to_string(k));
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

    void process_async(const StreamElement<std::int64_t>& element,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const std::int64_t k = rec.value();
            const std::string gate = std::to_string(k);  // record gate key == timer key
            const std::int64_t ts_reg = timer_ts_;
            auto kv = state_();
            auto factory = [this, kv, k, gate, ts_reg, &out]() mutable -> async::Task<void> {
                auto cur = co_await kv.get_async(k);
                const std::int64_t c = cur.value_or(0) + 1;
                kv.put(k, c);
                this->runtime()->timer_service()->register_processing_time_timer(ts_reg, gate);
                Batch<std::int64_t> batch;
                batch.emplace(c);
                out.emit_data(std::move(batch));
                co_return;
            };
            while (!aec.submit(gate, factory)) {
                aec.poll();
            }
        }
    }

    void on_processing_time_timer(std::int64_t /*ts*/,
                                  const std::string& key,
                                  Emitter<std::int64_t>& out) override {
        const std::int64_t k = std::stoll(key);
        auto kv = state_();
        const std::int64_t cur = kv.get(k).value_or(0);
        kv.put(k, cur + 100);
        Batch<std::int64_t> batch;
        batch.emplace(cur + 100);
        out.emit_data(std::move(batch));
    }

    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_processing_time_timers() const noexcept override {
        return true;
    }
    std::string name() const override { return "gated_proc_timer"; }

private:
    KeyedState<std::int64_t, std::int64_t> state_() {
        return this->runtime()->keyed_state<std::int64_t, std::int64_t>(
            "c", int64_codec(), int64_codec());
    }
    std::int64_t timer_ts_;
};

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

std::vector<std::int64_t> run_e2e(const std::vector<std::int64_t>& input,
                                  std::shared_ptr<StateBackend> backend) {
    Dag dag;
    std::vector<Record<std::int64_t>> recs;
    for (auto x : input) {
        recs.emplace_back(x);
    }
    auto src = std::make_shared<VectorSource<std::int64_t>>(recs);
    auto op = std::make_shared<GatedProcTimerOp>(/*timer_ts=*/0);  // always due
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

TEST(GatedTimerFire, SerialisesTimerBehindInFlightRead) {
    auto backend = std::make_shared<GtfManualBackend>();
    const OperatorId op_id{1};
    RuntimeContext ctx(op_id, "gated_proc_timer", backend.get(), nullptr);
    auto op =
        std::make_shared<GatedProcTimerOp>(/*timer_ts=*/1'000'000);  // record's own timer far off
    op->set_id(op_id);
    op->attach_runtime(&ctx);

    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // A processing-time timer for key 1 was set in a prior turn (registered
    // directly, due at 50).
    ctx.timer_service()->register_processing_time_timer(50, "1");

    // A record for key 1 is now in flight, suspended on a deferred read.
    Batch<std::int64_t> b;
    b.emplace(1);
    op->process_async(StreamElement<std::int64_t>::data(std::move(b)), em, aec);
    aec.poll();
    ASSERT_EQ(aec.in_flight(), 1u) << "the record is suspended, key 1 held";
    ASSERT_EQ(backend->pending(), 1u);

    // Fire the gated processing-time timer at now=100: it must PARK behind the
    // in-flight read for key 1, not run.
    detail::gated_fire_processing_time_timers(*op, em, /*now_ms=*/100, aec);
    EXPECT_EQ(aec.parked(), 1u) << "timer for key 1 parks behind the in-flight read";
    EXPECT_TRUE(drain_data(ch).empty()) << "the timer callback has not run yet";

    // Complete the read: the record writes count=1, then the parked timer runs
    // and observes that write (1 -> 101). If the timer had run first it would
    // have seen no value and emitted 100.
    backend->release_all();
    aec.drain();

    const auto out = drain_data(ch);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0], 1) << "the record's count emits first";
    EXPECT_EQ(out[1], 101) << "the timer ran AFTER the record and observed its write (1 + 100)";
}

TEST(GatedTimerFire, DistinctKeyTimerRunsWhileOtherKeyInFlight) {
    auto backend = std::make_shared<GtfManualBackend>();
    const OperatorId op_id{2};
    RuntimeContext ctx(op_id, "gated_proc_timer", backend.get(), nullptr);
    auto op = std::make_shared<GatedProcTimerOp>(/*timer_ts=*/1'000'000);
    op->set_id(op_id);
    op->attach_runtime(&ctx);

    AsyncExecutionController aec;
    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // Record for key 1 in flight (suspended).
    Batch<std::int64_t> b;
    b.emplace(1);
    op->process_async(StreamElement<std::int64_t>::data(std::move(b)), em, aec);
    aec.poll();
    ASSERT_EQ(aec.in_flight(), 1u);

    // A timer for a DIFFERENT key (2) must run immediately - distinct keys do
    // not block each other.
    ctx.timer_service()->register_processing_time_timer(50, "2");
    detail::gated_fire_processing_time_timers(*op, em, /*now_ms=*/100, aec);

    EXPECT_EQ(aec.parked(), 0u) << "the key-2 timer did not park behind key 1";
    EXPECT_EQ(aec.in_flight(), 1u) << "the key-1 record is still suspended";
    const auto out = drain_data(ch);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0], 100) << "key-2 timer ran on empty state (0 + 100)";

    backend->release_all();
    aec.drain();  // clean up the key-1 record
}

TEST(GatedTimerFire, MultipleDueTimersAllFireNoneDropped) {
    // No in-flight records; two due timers for distinct keys both fire (proves
    // collect-then-submit handles a batch and drops nothing).
    auto backend = std::make_shared<GtfInlineAsyncBackend>();
    const OperatorId op_id{3};
    RuntimeContext ctx(op_id, "gated_proc_timer", backend.get(), nullptr);
    auto op = std::make_shared<GatedProcTimerOp>(/*timer_ts=*/1'000'000);
    op->set_id(op_id);
    op->attach_runtime(&ctx);

    AsyncExecutionController aec;
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    ctx.timer_service()->register_processing_time_timer(10, "1");
    ctx.timer_service()->register_processing_time_timer(20, "9");
    detail::gated_fire_processing_time_timers(*op, em, /*now_ms=*/100, aec);
    aec.drain();

    auto out = drain_data(ch);
    std::sort(out.begin(), out.end());
    EXPECT_EQ(out, (std::vector<std::int64_t>{100, 100})) << "both timers fired exactly once";
    EXPECT_EQ(ctx.timer_service()->size(), 0u) << "both due timers consumed";
}

TEST(GatedTimerFire, EndToEndGatedTimerMatchesSyncPath) {
    // The operator is ADMITTED on the async path (the tripwire is gone) and the
    // gated processing-time timer fires through the real runner. A single record
    // is fully deterministic: count=1 emits, then the always-due timer fires once
    // (1 + 100 = 101). Sync and async must agree.
    const auto sync_out = run_e2e({1}, std::make_shared<InMemoryStateBackend>());
    const auto async_out = run_e2e({1}, std::make_shared<GtfInlineAsyncBackend>());

    const std::vector<std::int64_t> expected = {1, 101};
    EXPECT_EQ(sync_out, expected) << "synchronous path";
    EXPECT_EQ(async_out, expected) << "gated async path";
    EXPECT_EQ(sync_out, async_out);
}
