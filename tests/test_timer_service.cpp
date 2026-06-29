// Unit tests for TimerService (the processing-time timer surface
// exposed to operators via RuntimeContext::timer_service()).
//
// Also covers the end-to-end "register a timer in process(), have it
// fire on_processing_time_timer() between input pops" path via a Dag
// running through LocalExecutor.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/timer_service.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

TEST(TimerService, RegistersAndDedupesByTimestampKey) {
    TimerService ts;
    EXPECT_TRUE(ts.empty());
    ts.register_processing_time_timer(100, "k1");
    ts.register_processing_time_timer(100, "k1");  // duplicate
    ts.register_processing_time_timer(200, "k2");
    EXPECT_EQ(ts.size(), 2u);
    EXPECT_EQ(ts.next_timestamp().value(), 100);
}

TEST(TimerService, DeleteRemovesOnlyMatchingEntry) {
    TimerService ts;
    ts.register_processing_time_timer(100, "k1");
    ts.register_processing_time_timer(100, "k2");
    ts.delete_processing_time_timer(100, "k1");
    EXPECT_EQ(ts.size(), 1u);
    EXPECT_EQ(ts.next_timestamp().value(), 100);
}

TEST(TimerService, PollDueFiresInTimestampOrderAndPops) {
    TimerService ts;
    ts.register_processing_time_timer(300, "c");
    ts.register_processing_time_timer(100, "a");
    ts.register_processing_time_timer(200, "b");

    std::vector<std::pair<std::int64_t, std::string>> fired;
    const auto n =
        ts.poll_due(250, [&](std::int64_t t, const std::string& k) { fired.emplace_back(t, k); });
    EXPECT_EQ(n, 2u);
    EXPECT_EQ(fired, (std::vector<std::pair<std::int64_t, std::string>>{{100, "a"}, {200, "b"}}));
    EXPECT_EQ(ts.size(), 1u);
    EXPECT_EQ(ts.next_timestamp().value(), 300);
}

TEST(TimerService, NewTimerInsideCallbackNotFiredInSamePoll) {
    TimerService ts;
    ts.register_processing_time_timer(50, "first");
    std::vector<std::int64_t> fired;
    ts.poll_due(100, [&](std::int64_t t, const std::string&) {
        fired.push_back(t);
        // Register a timer that's already due (10 <= 100). Must NOT
        // fire until the next poll - matches semantics.
        ts.register_processing_time_timer(10, "follow_up");
    });
    EXPECT_EQ(fired, (std::vector<std::int64_t>{50}));
    EXPECT_EQ(ts.size(), 1u);
}

TEST(TimerService, InjectedClockAllowsDeterministicNow) {
    std::int64_t now = 0;
    TimerService ts([&now] { return now; });
    ts.register_processing_time_timer(10);
    ts.register_processing_time_timer(20);
    now = 15;
    std::vector<std::int64_t> fired;
    ts.poll_due_now([&](std::int64_t t, const std::string&) { fired.push_back(t); });
    EXPECT_EQ(fired, (std::vector<std::int64_t>{10}));
    now = 25;
    ts.poll_due_now([&](std::int64_t t, const std::string&) { fired.push_back(t); });
    EXPECT_EQ(fired, (std::vector<std::int64_t>{10, 20}));
}

// End-to-end: an operator registers a timer in open() and emits a
// record from on_processing_time_timer when it fires. The DAG runner
// must wake on the timer's deadline (not just on input arrival) so the
// fired count matches the operator's expectations.
class TimerRecordingOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        auto* ts = this->runtime()->timer_service();
        const auto now = ts->now_ms();
        // Three timers spaced 10ms apart, well within test runtime.
        ts->register_processing_time_timer(now + 5, "a");
        ts->register_processing_time_timer(now + 15, "b");
        ts->register_processing_time_timer(now + 25, "c");
    }
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        // Forward data unchanged.
        if (el.is_data()) {
            out.emit_data(el.as_data());
        } else if (el.is_watermark()) {
            out.emit_watermark(el.as_watermark());
        } else {
            out.emit_barrier(el.as_barrier());
        }
    }
    void on_processing_time_timer(std::int64_t /*ts*/,
                                  const std::string& key,
                                  Emitter<std::int64_t>& /*out*/) override {
        keys_fired_.push_back(key);
    }
    std::string name() const override { return "timer_recorder"; }
    std::vector<std::string> keys_fired_;
};

// A simple source that emits a single batch then stays alive briefly
// so the operator runner has time to fire its registered timers.
class SlowSource final : public Source<std::int64_t> {
public:
    bool produce(Emitter<std::int64_t>& out) override {
        if (emitted_) {
            std::this_thread::sleep_for(60ms);
            return false;
        }
        Batch<std::int64_t> b;
        b.emplace(1);
        out.emit_data(std::move(b));
        emitted_ = true;
        return true;
    }
    std::string name() const override { return "slow_source"; }

private:
    bool emitted_{false};
};

class CountingSink final : public Sink<std::int64_t> {
public:
    void on_data(const Batch<std::int64_t>& b) override {
        for (const auto& r : b) {
            received_.push_back(r.value());
        }
    }
    std::string name() const override { return "counting_sink"; }
    std::vector<std::int64_t> received_;
};

TEST(TimerService, SerializeRoundTripPreservesBothTimerSets) {
    TimerService ts;
    ts.register_processing_time_timer(100, "p1");
    ts.register_processing_time_timer(250, "");  // empty key
    ts.register_event_time_timer(200, "e1");
    ts.register_event_time_timer(200, "e2");
    ts.register_event_time_timer(300, "e1");

    const auto bytes = ts.serialize();

    TimerService restored;
    ASSERT_TRUE(restored.restore_from(bytes));
    EXPECT_EQ(restored.size(), 2u);
    EXPECT_EQ(restored.event_timers_size(), 3u);
    EXPECT_EQ(restored.next_timestamp().value(), 100);
    EXPECT_EQ(restored.next_event_timestamp().value(), 200);

    // The restored timers fire exactly like the originals.
    std::vector<std::pair<std::int64_t, std::string>> fired;
    restored.poll_due_event_time(
        250, [&](std::int64_t t, const std::string& k) { fired.emplace_back(t, k); });
    EXPECT_EQ(fired.size(), 2u);  // the two t=200 entries; t=300 stays
    EXPECT_EQ(restored.next_event_timestamp().value(), 300);
}

TEST(TimerService, SerializeRoundTripOnEmptyServiceIsEmpty) {
    TimerService ts;
    TimerService restored;
    restored.register_processing_time_timer(1, "stale");
    ASSERT_TRUE(restored.restore_from(ts.serialize()));
    EXPECT_TRUE(restored.empty());
    EXPECT_TRUE(restored.event_timers_empty());
}

TEST(TimerService, RestoreFromRejectsTruncatedBlob) {
    TimerService ts;
    ts.register_event_time_timer(200, "e1");
    auto bytes = ts.serialize();
    bytes.pop_back();  // truncate the last key byte

    TimerService restored;
    restored.register_processing_time_timer(5, "keep");
    EXPECT_FALSE(restored.restore_from(bytes));
    // A rejected restore leaves the service untouched.
    EXPECT_EQ(restored.size(), 1u);
    EXPECT_TRUE(restored.event_timers_empty());
}

TEST(TimerService, ProcessingTimeTimerFiresInOperatorRunner) {
    Dag dag;
    auto src = std::make_shared<SlowSource>();
    auto op = std::make_shared<TimerRecordingOperator>();
    auto sink = std::make_shared<CountingSink>();
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, op);
    dag.add_sink<std::int64_t>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // All three timers should have fired before the source closed.
    EXPECT_EQ(op->keys_fired_, (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{1}));
}

// CoOperator variant: timers registered from open() must fire from the
// CoOperator runner (which polls two input channels instead of one).
class TimerRecordingCoOperator final : public CoOperator<std::int64_t, std::int64_t, std::int64_t> {
public:
    void open() override {
        auto* ts = this->runtime()->timer_service();
        const auto now = ts->now_ms();
        ts->register_processing_time_timer(now + 5, "x");
        ts->register_processing_time_timer(now + 15, "y");
    }
    void process_element1(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    void process_element2(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    void on_processing_time_timer(std::int64_t /*ts*/,
                                  const std::string& key,
                                  Emitter<std::int64_t>& /*out*/) override {
        keys_fired_.push_back(key);
    }
    std::string name() const override { return "timer_co_recorder"; }
    std::vector<std::string> keys_fired_;
};

TEST(TimerService, ProcessingTimeTimerFiresInCoOperatorRunner) {
    Dag dag;
    auto src1 = std::make_shared<SlowSource>();
    auto src2 = std::make_shared<SlowSource>();
    auto op = std::make_shared<TimerRecordingCoOperator>();
    auto sink = std::make_shared<CountingSink>();
    auto h1 = dag.add_source<std::int64_t>(src1);
    auto h2 = dag.add_source<std::int64_t>(src2);
    auto hc = dag.add_co_operator<std::int64_t, std::int64_t, std::int64_t>(h1, h2, op);
    dag.add_sink<std::int64_t>(hc, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(op->keys_fired_, (std::vector<std::string>{"x", "y"}));
}

// Identity passthrough used as the tail of a chain so we can wire two
// operators into a ChainedOperator without changing the channel type.
class PassthroughInt64Operator final : public Operator<std::int64_t, std::int64_t> {
public:
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (el.is_data()) {
            out.emit_data(el.as_data());
        } else if (el.is_watermark()) {
            out.emit_watermark(el.as_watermark());
        } else {
            out.emit_barrier(el.as_barrier());
        }
    }
    std::string name() const override { return "passthrough"; }
};

// Regression test for the "ChainedOperator inner-op timers don't fire"
// gap. When two ops are chained into a single subtask via
// ChainedOperator, the inner ops share a runner but each must keep its
// own TimerService - otherwise a timer registered by the LEAD inner op
// in open() never fires because the runner only polls the outer op's
// (empty) TimerService. With the fix: each inner op has its own
// RuntimeContext (with its own TimerService) and the runner polls them
// via Operator::fire_due_timers / Operator::next_timer_deadline_ms.
TEST(TimerService, ProcessingTimeTimerFiresInChainedOperatorInnerOp) {
    Dag dag;
    auto src = std::make_shared<SlowSource>();
    auto inner_a = std::make_shared<TimerRecordingOperator>();
    auto inner_b = std::make_shared<PassthroughInt64Operator>();
    auto chained = std::make_shared<ChainedOperator<std::int64_t, std::int64_t, std::int64_t>>(
        inner_a, inner_b, "chained_a_b");
    auto sink = std::make_shared<CountingSink>();
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, chained);
    dag.add_sink<std::int64_t>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // All three timers registered by the LEAD inner op should fire,
    // even though the runner only sees the outer chained operator.
    EXPECT_EQ(inner_a->keys_fired_, (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(sink->received_, (std::vector<std::int64_t>{1}));
}

// Inner ops at BOTH positions of the chain register timers; verify each
// fires through its own per-RC TimerService (no cross-talk between the
// two inner ops' timer queues).
class TimerRecordingTailOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        auto* ts = this->runtime()->timer_service();
        const auto now = ts->now_ms();
        ts->register_processing_time_timer(now + 8, "tail-1");
        ts->register_processing_time_timer(now + 18, "tail-2");
    }
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (el.is_data()) {
            out.emit_data(el.as_data());
        } else if (el.is_watermark()) {
            out.emit_watermark(el.as_watermark());
        } else {
            out.emit_barrier(el.as_barrier());
        }
    }
    void on_processing_time_timer(std::int64_t /*ts*/,
                                  const std::string& key,
                                  Emitter<std::int64_t>& /*out*/) override {
        keys_fired_.push_back(key);
    }
    std::string name() const override { return "timer_tail_recorder"; }
    std::vector<std::string> keys_fired_;
};

TEST(TimerService, ChainedOperatorTimersAreIsolatedPerInnerOp) {
    Dag dag;
    auto src = std::make_shared<SlowSource>();
    auto inner_a = std::make_shared<TimerRecordingOperator>();
    auto inner_b = std::make_shared<TimerRecordingTailOperator>();
    auto chained = std::make_shared<ChainedOperator<std::int64_t, std::int64_t, std::int64_t>>(
        inner_a, inner_b, "chained_two_timers");
    auto sink = std::make_shared<CountingSink>();
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_operator<std::int64_t, std::int64_t>(h0, chained);
    dag.add_sink<std::int64_t>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // Each inner op sees only its own timer keys - no leakage from the
    // sibling's TimerService.
    EXPECT_EQ(inner_a->keys_fired_, (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(inner_b->keys_fired_, (std::vector<std::string>{"tail-1", "tail-2"}));
}

}  // namespace
