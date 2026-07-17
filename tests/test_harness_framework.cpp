// Self-tests for the public testing framework (clink::test) - increment 1:
// OutputCapture, OneInputOperatorHarness, deterministic time and timers,
// lifecycle enforcement. These pin the framework's own contract; the
// user-facing guide lives in docs/internals/testing-framework.md.

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/operator_base.hpp"
#include "clink/test/one_input_harness.hpp"
#include "clink/test/output_capture.hpp"

using namespace clink;

namespace {

// A stateless flat-map: emits v and v * 10 per input (collector-style test).
class FanOutOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    void process(const StreamElement<std::int64_t>& element, Emitter<std::int64_t>& out) override {
        if (element.is_data()) {
            Batch<std::int64_t> b;
            for (const auto& rec : element.as_data()) {
                if (rec.event_time()) {
                    b.emplace(rec.value(), *rec.event_time());
                    b.emplace(rec.value() * 10, *rec.event_time());
                } else {
                    b.emplace(rec.value());
                    b.emplace(rec.value() * 10);
                }
            }
            out.emit_data(std::move(b));
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }
    std::string name() const override { return "fan-out"; }
};

// A timer-driven operator: every input registers an event-time timer at
// ts + 100 and a processing-time timer at now + 50, keyed by the value;
// timer fires emit sentinel values (-ts for event time, +ts for
// processing time) so tests can distinguish the paths and their order.
class TimerOperator final : public Operator<std::int64_t, std::int64_t> {
public:
    void process(const StreamElement<std::int64_t>& element, Emitter<std::int64_t>& out) override {
        if (element.is_data()) {
            for (const auto& rec : element.as_data()) {
                const auto ts = rec.event_time() ? rec.event_time()->millis() : 0;
                runtime()->timer_service()->register_event_time_timer(ts + 100,
                                                                      std::to_string(rec.value()));
                runtime()->timer_service()->register_processing_time_timer(
                    runtime()->timer_service()->now_ms() + 50, std::to_string(rec.value()));
            }
        } else if (element.is_watermark()) {
            this->on_watermark(element.as_watermark(), out);
        } else {
            this->on_barrier(element.as_barrier(), out);
        }
    }
    void on_event_time_timer(std::int64_t ts,
                             const std::string& key,
                             Emitter<std::int64_t>& out) override {
        Batch<std::int64_t> b;
        b.emplace(-(ts * 1000 + std::stoll(key)));
        out.emit_data(std::move(b));
    }
    void on_processing_time_timer(std::int64_t ts,
                                  const std::string& key,
                                  Emitter<std::int64_t>& out) override {
        Batch<std::int64_t> b;
        b.emplace(ts * 1000 + std::stoll(key));
        out.emit_data(std::move(b));
    }
    std::string name() const override { return "timers"; }
};

// Lifecycle probe: records open/close ordering.
class LifecycleProbe final : public Operator<int, int> {
public:
    explicit LifecycleProbe(std::vector<std::string>* log) : log_(log) {}
    void open() override { log_->push_back("open"); }
    void close() override { log_->push_back("close"); }
    void process(const StreamElement<int>&, Emitter<int>&) override { log_->push_back("process"); }
    std::string name() const override { return "probe"; }

private:
    std::vector<std::string>* log_;
};

}  // namespace

// ---- OutputCapture as the stateless-function collector ----

TEST(TestFramework, OutputCaptureCollectsValuesRecordsAndWatermarksInOrder) {
    test::OutputCapture<std::int64_t> cap;
    FanOutOperator op;

    Batch<std::int64_t> b;
    b.emplace(1, EventTime{500});
    op.process(StreamElement<std::int64_t>::data(std::move(b)), cap.emitter());
    cap.emitter().emit_watermark(Watermark{EventTime{999}});
    op.process(StreamElement<std::int64_t>::data([] {
                   Batch<std::int64_t> u;
                   u.emplace(2);
                   return u;
               }()),
               cap.emitter());

    EXPECT_EQ(cap.values(), (std::vector<std::int64_t>{1, 10, 2, 20}));
    EXPECT_EQ(cap.value_count(), 4u);
    ASSERT_EQ(cap.watermarks().size(), 1u);
    EXPECT_EQ(cap.watermarks()[0].timestamp().millis(), 999);

    const auto recs = cap.records();
    ASSERT_EQ(recs.size(), 4u);
    EXPECT_EQ(recs[0].event_time_ms, std::optional<std::int64_t>{500});  // stamped
    EXPECT_EQ(recs[2].event_time_ms, std::nullopt);                      // unstamped

    EXPECT_TRUE(cap.any_value([](auto v) { return v == 20; }));
    EXPECT_EQ(cap.count_values([](auto v) { return v >= 10; }), 2u);

    auto drained = cap.take_events();
    EXPECT_EQ(drained.size(), 3u);  // two data batches + one watermark
    EXPECT_TRUE(cap.empty());
}

// ---- Harness basics: records, timestamps, ordering ----

TEST(TestFramework, HarnessProcessesElementsAndPreservesOrderAndTimestamps) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(FanOutOperator{});
    h.open();
    h.process_element(7);
    h.process_element(8, 1234);

    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{7, 70, 8, 80}));
    const auto recs = h.output().records();
    EXPECT_EQ(recs[3].event_time_ms, std::optional<std::int64_t>{1234});
    h.close();
}

// ---- Event-time timers fire through the production watermark path ----

TEST(TestFramework, WatermarkFiresDueEventTimeTimersThenForwards) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(TimerOperator{});
    h.open();
    h.process_element(1, 1000);  // registers event-time timer @1100 key "1"
    h.process_element(2, 2000);  // ... @2100 key "2"
    ASSERT_EQ(h.event_time_timers().size(), 2u);

    h.process_watermark(1500);  // only the first is due

    // The fire lands BEFORE the forwarded watermark in the event log.
    const auto& events = h.output().events();
    ASSERT_EQ(events.size(), 2u);
    EXPECT_TRUE(events[0].is_data());
    EXPECT_TRUE(events[1].is_watermark());
    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{-(1100 * 1000 + 1)}));
    EXPECT_EQ(h.event_time_timers().size(), 1u);  // @2100 still registered
    EXPECT_EQ(h.current_watermark_ms(), std::optional<std::int64_t>{1500});
}

// ---- Processing-time timers: manual clock, deterministic order ----

TEST(TestFramework, AdvanceProcessingTimeFiresDueTimersInTimestampThenKeyOrder) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(
        TimerOperator{}, {.initial_processing_time_ms = 1000});
    h.open();
    EXPECT_EQ(h.processing_time_ms(), 1000);
    h.process_element(2);  // proc timer @1050 key "2"
    h.process_element(1);  // proc timer @1050 key "1" - same deadline
    ASSERT_EQ(h.processing_time_timers().size(), 2u);

    h.advance_processing_time_by(49);
    EXPECT_TRUE(h.output_values().empty());  // 1049 < 1050: nothing due

    h.advance_processing_time_by(1);
    // Tie at 1050 fires in lexicographic KEY order: "1" before "2".
    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{1050 * 1000 + 1, 1050 * 1000 + 2}));
    EXPECT_TRUE(h.processing_time_timers().empty());

    EXPECT_THROW(h.set_processing_time(100), std::logic_error);  // no time travel
}

// ---- Lifecycle enforcement and RAII close ----

TEST(TestFramework, LifecycleIsEnforcedAndDestructorCloses) {
    std::vector<std::string> log;
    {
        auto h = test::OneInputOperatorHarness<int, int>::create(LifecycleProbe{&log});
        EXPECT_THROW(h.process_element(1), std::logic_error);  // before open
        h.open();
        EXPECT_THROW(h.open(), std::logic_error);  // double open
        h.process_element(1);
        // No explicit close: the destructor must close.
    }
    EXPECT_EQ(log, (std::vector<std::string>{"open", "process", "close"}));

    std::vector<std::string> log2;
    auto h2 = test::OneInputOperatorHarness<int, int>::create(LifecycleProbe{&log2});
    h2.open();
    h2.close();
    EXPECT_THROW(h2.process_element(1), std::logic_error);  // after close
    EXPECT_THROW(h2.close(), std::logic_error);             // double close
}

// ---- Increment 2: keyed harness + ProcessFunction factories ----

#include "clink/test/keyed_harness.hpp"

namespace {

struct Purchase {
    std::string user;
    std::int64_t amount;
};

// Counts purchases per user in keyed state; registers an event-time
// timer 1000ms after each record which emits "<user>:<count>" when it
// fires. The canonical keyed stateful pattern.
class CountPerUser final : public clink::KeyedProcessFunction<std::string, Purchase, std::string> {
public:
    void open(RuntimeContext& ctx) override {
        counts_.emplace(
            ctx.keyed_state<std::string, std::int64_t>("count", string_codec(), int64_codec()));
    }
    void process_element(const Purchase& p,
                         clink::ProcessFunctionContext<std::string>& ctx,
                         Collector<std::string>& out) override {
        const auto next = counts_->get(p.user).value_or(0) + 1;
        counts_->put(p.user, next);
        const auto ts = ctx.timestamp() ? ctx.timestamp()->millis() : 0;
        ctx.timer_service()->register_event_time_timer(ts + 1000, p.user);
        (void)out;
    }
    void on_timer(std::int64_t ts,
                  clink::OnTimerContext<std::string>& ctx,
                  Collector<std::string>& out) override {
        (void)ts;
        (void)ctx;
        out.collect(current_key() + ":" + std::to_string(counts_->get(current_key()).value_or(0)));
    }

private:
    std::optional<KeyedState<std::string, std::int64_t>> counts_;
};

}  // namespace

TEST(TestFramework, KeyedHarnessIsolatesStatePerKeyAndFiresKeyScopedTimers) {
    auto h = test::make_keyed_process_function_harness(
        CountPerUser{},
        [](const Purchase& p) { return p.user; },
        [](const std::string& timer_key) { return timer_key; });  // timer key IS the user key
    h.open();

    h.process_element(Purchase{"alice", 10}, 1000);
    h.process_element(Purchase{"bob", 20}, 1100);
    h.process_element(Purchase{"alice", 30}, 1200);

    // State isolated per key, inspected through the production read path.
    EXPECT_EQ(h.state_value<std::int64_t>("alice", "count"), 3 - 1);  // 2 purchases
    EXPECT_EQ(h.state_value<std::int64_t>("bob", "count"), 1);
    EXPECT_EQ(h.state_value<std::int64_t>("carol", "count"), std::nullopt);
    // known_keys follows the backend's key encoding (key-group order), so
    // sort before comparing.
    auto keys = h.known_keys<std::int64_t>("count");
    std::sort(keys.begin(), keys.end());
    EXPECT_EQ(keys, (std::vector<std::string>{"alice", "bob"}));

    // Key-scoped timer queries.
    EXPECT_TRUE(h.has_event_time_timer(2000, "alice"));
    EXPECT_TRUE(h.has_event_time_timer(2100, "bob"));
    EXPECT_EQ(h.event_time_timers_for("alice"), (std::vector<std::int64_t>{2000, 2200}));

    // Watermark past alice's timers only: two fires, both routed to
    // alice's key (current_key recovered from the timer key).
    h.process_watermark(2050);
    EXPECT_EQ(h.output_values(), (std::vector<std::string>{"alice:2"}));
    h.output().clear();
    h.process_watermark(3000);
    EXPECT_EQ(h.output_values(), (std::vector<std::string>{"bob:1", "alice:2"}));

    // Seeding + clearing state through the harness.
    h.seed_state<std::int64_t>("dave", "count", 41);
    EXPECT_EQ(h.state_value<std::int64_t>("dave", "count"), 41);
    h.clear_state("dave", "count");
    EXPECT_EQ(h.state_value<std::int64_t>("dave", "count"), std::nullopt);
}

TEST(TestFramework, ProcessFunctionFactoryDrivesNonKeyedFunction) {
    struct Doubler final : clink::ProcessFunction<std::int64_t, std::int64_t> {
        void process_element(const std::int64_t& v,
                             clink::ProcessFunctionContext<std::int64_t>&,
                             Collector<std::int64_t>& out) override {
            out.collect(v * 2);
        }
    };
    auto h = test::make_process_function_harness(Doubler{});
    h.open();
    h.process_element(21);
    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{42}));
}

// ---- Increment 3: two-input harness with real watermark combination ----

#include "clink/test/two_input_harness.hpp"

namespace {

// A join-ish co-operator: buffers left values in keyed state per key,
// emits "l+r" pairs on right arrivals; registers an event-time timer
// per left record which emits "expired:<key>" when fired.
class PairJoiner final : public CoOperator<std::int64_t, std::int64_t, std::string> {
public:
    void process_element1(const StreamElement<std::int64_t>& e,
                          Emitter<std::string>& out) override {
        if (!e.is_data()) {
            return;
        }
        for (const auto& rec : e.as_data()) {
            left_.push_back(rec.value());
            const auto ts = rec.event_time() ? rec.event_time()->millis() : 0;
            runtime()->timer_service()->register_event_time_timer(ts + 500,
                                                                  std::to_string(rec.value()));
        }
        (void)out;
    }
    void process_element2(const StreamElement<std::int64_t>& e,
                          Emitter<std::string>& out) override {
        if (!e.is_data()) {
            return;
        }
        Batch<std::string> b;
        for (const auto& rec : e.as_data()) {
            for (const auto l : left_) {
                b.emplace(std::to_string(l) + "+" + std::to_string(rec.value()));
            }
        }
        if (!b.empty()) {
            out.emit_data(std::move(b));
        }
    }
    void on_event_time_timer(std::int64_t,
                             const std::string& key,
                             Emitter<std::string>& out) override {
        Batch<std::string> b;
        b.emplace("expired:" + key);
        out.emit_data(std::move(b));
    }
    std::string name() const override { return "pair-joiner"; }

private:
    std::vector<std::int64_t> left_;
};

}  // namespace

TEST(TestFramework, TwoInputHarnessCombinesWatermarksAsTheRunningMinimum) {
    auto h = test::TwoInputOperatorHarness<std::int64_t, std::int64_t, std::string>::create(
        PairJoiner{});
    h.open();

    h.process_left(1, 1000);  // event-time timer @1500 key "1"
    h.process_right(7);
    EXPECT_EQ(h.output_values(), (std::vector<std::string>{"1+7"}));
    h.output().clear();

    // Left watermark alone: combined min is still the right input's
    // Watermark::min() - nothing forwards, no timer fires.
    EXPECT_EQ(h.process_left_watermark(2000), std::nullopt);
    EXPECT_EQ(h.current_watermark_ms(), std::nullopt);
    EXPECT_TRUE(h.output().watermarks().empty());
    ASSERT_EQ(h.event_time_timers().size(), 1u);

    // Right watermark: min(2000, 1600) = 1600 - forwards, fires @1500.
    auto combined = h.process_right_watermark(1600);
    ASSERT_TRUE(combined.has_value());
    EXPECT_EQ(combined->timestamp().millis(), 1600);
    EXPECT_EQ(h.current_watermark_ms(), std::optional<std::int64_t>{1600});
    EXPECT_EQ(h.output_values(), (std::vector<std::string>{"expired:1"}));
    ASSERT_EQ(h.output().watermarks().size(), 1u);
    EXPECT_EQ(h.output().watermarks()[0].timestamp().millis(), 1600);
    h.output().clear();

    // One input running ahead: right jumps to 5000; combined follows the
    // slower left (2000).
    combined = h.process_right_watermark(5000);
    ASSERT_TRUE(combined.has_value());
    EXPECT_EQ(combined->timestamp().millis(), 2000);

    // Idleness: left going idle releases the minimum to the right's 5000.
    combined = h.mark_left_idle();
    ASSERT_TRUE(combined.has_value());
    EXPECT_EQ(combined->timestamp().millis(), 5000);
}

// ---- Increment 4: snapshot/restore, failure injection, lifecycle log ----

TEST(TestFramework, SnapshotRestoreRoundTripsStateAndTimers) {
    const auto key_fn = [](const Purchase& p) { return p.user; };
    const auto timer_key_fn = [](const std::string& k) { return k; };
    auto h = test::make_keyed_process_function_harness(CountPerUser{}, key_fn, timer_key_fn);
    h.open();
    h.process_element(Purchase{"alice", 10}, 1000);  // timer @2000
    h.process_element(Purchase{"bob", 20}, 1100);    // timer @2100
    h.process_element(Purchase{"alice", 30}, 1200);  // timer @2200

    const auto snap = h.snapshot(1);

    // Divergence after the snapshot must not leak into the restore.
    h.process_element(Purchase{"alice", 99}, 1300);
    EXPECT_EQ(h.state_value<std::int64_t>("alice", "count"), 3);

    // Restore into a FRESH harness around a fresh function instance -
    // the recovery model: state and timers come from the snapshot alone.
    auto h2 = test::make_keyed_process_function_harness(CountPerUser{}, key_fn, timer_key_fn);
    h2.restore_from(snap);
    h2.open();

    EXPECT_EQ(h2.state_value<std::int64_t>("alice", "count"), 2);
    EXPECT_EQ(h2.state_value<std::int64_t>("bob", "count"), 1);
    EXPECT_EQ(h2.event_time_timers().size(), 3u);
    EXPECT_TRUE(h2.has_event_time_timer(2000, "alice"));
    EXPECT_TRUE(h2.has_event_time_timer(2100, "bob"));

    // The restored timers FIRE with the restored state behind them.
    h2.process_watermark(2100);
    EXPECT_EQ(h2.output_values(), (std::vector<std::string>{"alice:2", "bob:1"}));
}

TEST(TestFramework, StaticRestoreRebuildsAPlainHarness) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(TimerOperator{});
    h.open();
    h.process_element(4, 1000);  // event-time timer @1100, proc timer @50
    const auto snap = h.snapshot(7);

    auto h2 =
        test::OneInputOperatorHarness<std::int64_t, std::int64_t>::restore(TimerOperator{}, snap);
    h2.open();
    ASSERT_EQ(h2.event_time_timers().size(), 1u);
    h2.process_watermark(1100);
    EXPECT_EQ(h2.output_values(), (std::vector<std::int64_t>{-(1100 * 1000 + 4)}));
}

TEST(TestFramework, FailureInjectionBeforeProcessLeavesTheElementUnprocessed) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(FanOutOperator{});
    h.open();
    h.failures().fail_once(test::FailurePoint::BeforeProcessElement);

    EXPECT_THROW(h.process_element(1), test::InjectedFailure);
    EXPECT_TRUE(h.output().empty());  // the operator never saw the element

    h.process_element(2);  // one-shot rule is exhausted
    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{2, 20}));
    EXPECT_EQ(h.failures().injected_count(), 1u);
}

TEST(TestFramework, FailureInjectionAfterProcessFailsWithTheEffectApplied) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(FanOutOperator{});
    h.open();
    h.failures().fail_on_nth(test::FailurePoint::AfterProcessElement, 2);

    h.process_element(1);
    EXPECT_THROW(h.process_element(2), test::InjectedFailure);
    // The second element WAS processed before the failure - the
    // "crash after the effect" shape used to test replay/idempotence.
    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{1, 10, 2, 20}));
}

TEST(TestFramework, FailureInjectionOnTimersAndSnapshots) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(TimerOperator{});
    h.open();
    h.process_element(3, 1000);  // event-time @1100, processing-time @50

    // An armed event-time-timer failure throws BEFORE the fire: the
    // timer stays registered, so the retry fires it.
    h.failures().fail_once(test::FailurePoint::OnEventTimeTimer);
    EXPECT_THROW(h.process_watermark(1100), test::InjectedFailure);
    ASSERT_EQ(h.event_time_timers().size(), 1u);
    h.process_watermark(1100);
    EXPECT_EQ(h.output_values(), (std::vector<std::int64_t>{-(1100 * 1000 + 3)}));
    h.output().clear();

    h.failures().fail_once(test::FailurePoint::OnProcessingTimeTimer);
    EXPECT_THROW(h.advance_processing_time_to(50), test::InjectedFailure);

    h.failures().fail_once(test::FailurePoint::DuringSnapshot);
    EXPECT_THROW(h.snapshot(1), test::InjectedFailure);
    (void)h.snapshot(2);  // next attempt succeeds
}

TEST(TestFramework, CheckpointRecoveryLoopReplaysToTheSameState) {
    const auto key_fn = [](const Purchase& p) { return p.user; };

    // First run: checkpoint mid-stream, then crash before the next element.
    auto h = test::make_keyed_process_function_harness(CountPerUser{}, key_fn);
    h.open();
    h.process_element(Purchase{"alice", 10}, 1000);
    const auto checkpoint = h.snapshot(1);
    h.failures().fail_once(test::FailurePoint::BeforeProcessElement);
    EXPECT_THROW(h.process_element(Purchase{"alice", 20}, 2000), test::InjectedFailure);

    // Recovery: restore the checkpoint, replay from after it.
    auto h2 = test::make_keyed_process_function_harness(CountPerUser{}, key_fn);
    h2.restore_from(checkpoint);
    h2.open();
    h2.process_element(Purchase{"alice", 20}, 2000);

    EXPECT_EQ(h2.state_value<std::int64_t>("alice", "count"), 2);
}

TEST(TestFramework, TransitionsLogRecordsWhatTheHarnessDrove) {
    auto h = test::OneInputOperatorHarness<std::int64_t, std::int64_t>::create(FanOutOperator{});
    h.open();
    h.process_element(1);
    h.process_watermark(100);
    (void)h.snapshot(1);
    h.close();
    EXPECT_EQ(h.transitions(),
              (std::vector<std::string>{"open", "process", "watermark", "snapshot", "close"}));
}

TEST(TestFramework, TwoInputSnapshotRestoreAndFailureInjection) {
    auto h = test::TwoInputOperatorHarness<std::int64_t, std::int64_t, std::string>::create(
        PairJoiner{});
    h.open();
    h.process_left(1, 1000);  // event-time timer @1500 key "1"
    const auto snap = h.snapshot(1);

    // PairJoiner's left buffer is a member (not keyed state), so only
    // the TIMER travels through the snapshot - which is exactly what
    // this pins: restored timers fire on the restored co-operator.
    auto h2 = test::TwoInputOperatorHarness<std::int64_t, std::int64_t, std::string>::restore(
        PairJoiner{}, snap);
    h2.open();
    ASSERT_EQ(h2.event_time_timers().size(), 1u);
    h2.process_left_watermark(2000);
    auto combined = h2.process_right_watermark(2000);
    ASSERT_TRUE(combined.has_value());
    EXPECT_EQ(h2.output_values(), (std::vector<std::string>{"expired:1"}));

    h2.failures().fail_once(test::FailurePoint::BeforeProcessElement);
    EXPECT_THROW(h2.process_right(9), test::InjectedFailure);
    EXPECT_EQ(h2.failures().injected_count(), 1u);
}

// ---- Increment 5: test sources and sinks ----

#include "clink/test/sources_and_sinks.hpp"

TEST(TestFramework, TestSourceEmitsItsScriptOneEntryPerProduce) {
    test::TestSource<std::int64_t> src;
    src.emit(1, 1000).watermark(1500).emit(2);
    ASSERT_EQ(src.script_size(), 3u);
    EXPECT_TRUE(src.is_bounded());

    test::OutputCapture<std::int64_t> cap;
    EXPECT_TRUE(src.produce(cap.emitter()));   // value 1
    EXPECT_TRUE(src.produce(cap.emitter()));   // watermark
    EXPECT_TRUE(src.produce(cap.emitter()));   // value 2
    EXPECT_FALSE(src.produce(cap.emitter()));  // exhausted

    EXPECT_EQ(cap.values(), (std::vector<std::int64_t>{1, 2}));
    const auto recs = cap.records();
    EXPECT_EQ(recs[0].event_time_ms, std::optional<std::int64_t>{1000});
    EXPECT_EQ(recs[1].event_time_ms, std::nullopt);
    ASSERT_EQ(cap.watermarks().size(), 1u);
    EXPECT_EQ(cap.watermarks()[0].timestamp().millis(), 1500);

    // cancel() ends the stream regardless of remaining script.
    test::TestSource<std::int64_t> cancelled({7, 8});
    cancelled.cancel();
    EXPECT_FALSE(cancelled.produce(cap.emitter()));
}

TEST(TestFramework, TestSourceOffsetRoundTripResumesAfterTheCheckpoint) {
    InMemoryStateBackend backend;
    test::TestSource<std::int64_t> src({1, 2, 3, 4});
    test::OutputCapture<std::int64_t> cap;
    src.produce(cap.emitter());
    src.produce(cap.emitter());
    src.snapshot_offset(backend, OperatorId{42}, CheckpointId{1});

    // Recovery: a fresh source with the same script resumes after the
    // checkpointed cursor - nothing is re-emitted, nothing is skipped.
    test::TestSource<std::int64_t> resumed({1, 2, 3, 4});
    EXPECT_TRUE(resumed.restore_offset(backend, OperatorId{42}));
    test::OutputCapture<std::int64_t> cap2;
    while (resumed.produce(cap2.emitter())) {
    }
    EXPECT_EQ(cap2.values(), (std::vector<std::int64_t>{3, 4}));

    // No checkpointed offset for a different operator id.
    test::TestSource<std::int64_t> other({1});
    EXPECT_FALSE(other.restore_offset(backend, OperatorId{7}));
}

TEST(TestFramework, CollectSinkGathersRecordsAndFailingSinkThrowsAfterN) {
    test::CollectSink<std::int64_t> sink;
    Batch<std::int64_t> b;
    b.emplace(1, EventTime{100});
    b.emplace(2);
    sink.on_data(b);
    sink.on_watermark(Watermark{EventTime{500}});

    EXPECT_EQ(sink.values(), (std::vector<std::int64_t>{1, 2}));
    EXPECT_EQ(sink.value_count(), 2u);
    EXPECT_EQ(sink.records()[0].event_time_ms, std::optional<std::int64_t>{100});
    EXPECT_EQ(sink.watermarks(), (std::vector<std::int64_t>{500}));

    test::FailingSink<std::int64_t> failing(/*pass_first=*/2);
    Batch<std::int64_t> c;
    c.emplace(10);
    c.emplace(11);
    c.emplace(12);
    EXPECT_THROW(failing.on_data(c), test::InjectedFailure);
    EXPECT_EQ(failing.values(), (std::vector<std::int64_t>{10, 11}));  // crash on the third
    EXPECT_TRUE(failing.failed());
}

TEST(TestFramework, TransactionalTestSinkModelsTheTwoPhaseCommitLifecycle) {
    test::TransactionalTestSink<std::int64_t> sink;

    Batch<std::int64_t> e1;
    e1.emplace(1);
    e1.emplace(2);
    sink.on_data(e1);
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});  // stage epoch 1
    Batch<std::int64_t> e2;
    e2.emplace(3);
    sink.on_data(e2);
    sink.on_barrier(CheckpointBarrier{CheckpointId{2}});  // stage epoch 2

    // Nothing is durable until the commit phase.
    EXPECT_TRUE(sink.committed_values().empty());
    EXPECT_EQ(sink.pending_checkpoints(), (std::vector<std::uint64_t>{1, 2}));

    sink.on_commit(1);
    EXPECT_EQ(sink.committed_values(), (std::vector<std::int64_t>{1, 2}));
    sink.on_commit(1);  // idempotent, as the engine requires
    EXPECT_EQ(sink.committed_values(), (std::vector<std::int64_t>{1, 2}));

    sink.on_abort(2);  // epoch 2 rolls back; its records never commit
    EXPECT_EQ(sink.committed_values(), (std::vector<std::int64_t>{1, 2}));
    EXPECT_TRUE(sink.pending_checkpoints().empty());

    // A TERMINAL barrier commits its epoch immediately (bounded-stream
    // contract: no coordinator round-trip after end-of-stream).
    Batch<std::int64_t> e3;
    e3.emplace(4);
    sink.on_data(e3);
    EXPECT_EQ(sink.uncommitted_values(), (std::vector<std::int64_t>{4}));
    sink.on_barrier(CheckpointBarrier{CheckpointId{3}, /*terminal=*/true});
    EXPECT_EQ(sink.committed_values(), (std::vector<std::int64_t>{1, 2, 4}));
    EXPECT_EQ(sink.commits(), (std::vector<std::uint64_t>{1, 3}));
    EXPECT_EQ(sink.aborts(), (std::vector<std::uint64_t>{2}));
}

// ---- Increment 6: LocalTestEnvironment and TestCluster ----

#include <filesystem>
#include <fstream>

#include "clink/operators/map_operator.hpp"
#include "clink/test/local_environment.hpp"
#include "clink/test/test_cluster.hpp"

TEST(TestFramework, LocalTestEnvironmentRunsAPipelineOnTheRealRuntime) {
    test::LocalTestEnvironment env;
    auto src = std::make_shared<test::TestSource<std::int64_t>>();
    src->emit(1, 1000).emit(2, 2000).watermark(2500).emit(3, 3000);
    auto doubler = std::make_shared<MapOperator<std::int64_t, std::int64_t>>(
        [](const std::int64_t& v) { return v * 2; }, "doubler");
    auto sink = std::make_shared<test::CollectSink<std::int64_t>>();

    auto h0 = env.dag().add_source<std::int64_t>(src);
    auto h1 = env.dag().add_operator<std::int64_t, std::int64_t>(h0, doubler);
    env.dag().add_sink<std::int64_t>(h1, sink);

    env.execute();  // bounded source: runs to completion

    EXPECT_EQ(sink->values(), (std::vector<std::int64_t>{2, 4, 6}));
    // Event times and the scripted watermark rode the real channels.
    EXPECT_EQ(sink->records()[0].event_time_ms, std::optional<std::int64_t>{1000});
    EXPECT_FALSE(sink->watermarks().empty());
    EXPECT_TRUE(env.errors().empty());
}

TEST(TestFramework, LocalTestEnvironmentSurfacesOperatorFailures) {
    test::LocalTestEnvironment env;
    auto src =
        std::make_shared<test::TestSource<std::int64_t>>(std::vector<std::int64_t>{1, 2, 3, 4});
    auto sink = std::make_shared<test::FailingSink<std::int64_t>>(/*pass_first=*/2);
    auto h0 = env.dag().add_source<std::int64_t>(src);
    env.dag().add_sink<std::int64_t>(h0, sink);

    const auto errors = env.execute_collecting_errors();
    ASSERT_EQ(errors.size(), 1u);
    EXPECT_NE(errors[0].second.find("injected failure"), std::string::npos);
    EXPECT_EQ(sink->values(), (std::vector<std::int64_t>{1, 2}));  // what landed pre-crash

    // The throwing form raises PipelineFailure with the same detail.
    test::LocalTestEnvironment env2;
    auto src2 = std::make_shared<test::TestSource<std::int64_t>>(std::vector<std::int64_t>{1});
    auto sink2 = std::make_shared<test::FailingSink<std::int64_t>>(0);
    env2.dag().add_sink<std::int64_t>(env2.dag().add_source<std::int64_t>(src2), sink2);
    EXPECT_THROW(env2.execute(), test::PipelineFailure);
}

TEST(TestFramework, TestClusterRunsAJobGraphSpecToCompletion) {
    test::TestCluster mini({.workers = 1, .slots_per_worker = 4});

    const auto out_path =
        std::filesystem::temp_directory_path() / "clink_test_framework_test_cluster.txt";
    std::filesystem::remove(out_path);

    cluster::JobGraphSpec g;
    cluster::OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{cluster::kChannelInt64};
    src.params = {{"count", "5"}};  // 1..5 (start defaults to 1)
    g.ops.push_back(src);
    cluster::OperatorSpec snk;
    snk.type = "file_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{cluster::kChannelInt64};
    snk.params = {{"path", out_path.string()}};
    g.ops.push_back(snk);

    mini.execute(g);  // submit + await + assert no job errors

    std::ifstream in(out_path);
    ASSERT_TRUE(in.good());
    std::vector<std::int64_t> written;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            written.push_back(std::stoll(line));
        }
    }
    std::sort(written.begin(), written.end());
    EXPECT_EQ(written, (std::vector<std::int64_t>{1, 2, 3, 4, 5}));
    std::filesystem::remove(out_path);
}

// ---- Increment 7: assertions, sequences, side outputs, dogfooding ----

#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/test/assertions.hpp"
#include "clink/test/sequence.hpp"

namespace {

using KV = std::pair<std::string, std::int64_t>;

std::shared_ptr<TumblingWindowOperator<std::string, std::int64_t, std::int64_t>> make_sum_window() {
    return std::make_shared<TumblingWindowOperator<std::string, std::int64_t, std::int64_t>>(
        std::chrono::milliseconds{1000},
        []() -> std::int64_t { return 0; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        string_codec(),
        int64_codec());
}

}  // namespace

TEST(TestFramework, AssertionHelpersReportPreciseFailures) {
    test::OutputCapture<std::int64_t> cap;
    Batch<std::int64_t> b;
    b.emplace(1);
    b.emplace(2);
    cap.emitter().emit_data(std::move(b));
    cap.emitter().emit_watermark(Watermark{EventTime{100}});
    cap.emitter().emit_watermark(Watermark{EventTime{50}});  // regression

    EXPECT_TRUE(test::values_are(cap, {1, 2}));
    const auto wrong_order = test::values_are(cap, {2, 1});
    EXPECT_FALSE(wrong_order.ok);
    EXPECT_NE(wrong_order.message.find("expected values"), std::string::npos);

    EXPECT_TRUE(test::values_are_unordered(cap, {2, 1}));
    EXPECT_FALSE(test::values_are_unordered(cap, {1, 1}).ok);

    EXPECT_TRUE(test::contains_value(cap, std::int64_t{2}));
    EXPECT_FALSE(test::contains_value(cap, std::int64_t{9}).ok);

    const auto mono = test::watermarks_are_monotonic(cap);
    EXPECT_FALSE(mono.ok);
    EXPECT_NE(mono.message.find("regressed"), std::string::npos);
}

// Dogfood: the framework testing a real production operator - the
// keyed event-time tumbling window - through every phase: on-time
// firing, in-lateness re-fire, past-lateness side output, end-of-input
// flush. This is the acceptance proof that the harness drives
// production semantics, not a parallel mock runtime.
TEST(TestFramework, DogfoodTumblingWindowThroughTheHarness) {
    OutputTag<std::int64_t> late{"late"};
    auto window = make_sum_window();
    window->allowed_lateness(std::chrono::milliseconds{500});
    window->late_output_tag(late);

    auto h = test::OneInputOperatorHarness<KV, KV>::create(window);
    h.register_side_output(late);
    h.open();

    h.process_element({"alice", 5}, 100);
    h.process_element({"bob", 7}, 900);
    h.process_element({"alice", 6}, 1100);  // next window [1000, 2000)

    h.process_watermark(999);  // window [0, 1000) not yet complete
    EXPECT_TRUE(h.output_values().empty());

    h.process_watermark(1000);  // fires [0, 1000)
    auto fired = test::values_are_unordered(h.output(), {KV{"alice", 5}, KV{"bob", 7}});
    EXPECT_TRUE(fired) << fired.message;
    h.output().clear();

    // Within allowed lateness: the aggregate updates and re-emits.
    h.process_element({"alice", 100}, 500);
    auto refired = test::values_are(h.output(), {KV{"alice", 105}});
    EXPECT_TRUE(refired) << refired.message;
    EXPECT_TRUE(h.side_output_values(late).empty());
    h.output().clear();

    // Past window_end + lateness the window purges; a later record for
    // it routes to the late side output (value-typed), not the stream.
    h.process_watermark(1500);
    h.output().clear();
    h.process_element({"carol", 1}, 200);
    EXPECT_TRUE(h.output_values().empty());
    EXPECT_EQ(h.side_output_values(late), (std::vector<std::int64_t>{1}));

    // End of input drains the open window [1000, 2000).
    h.flush();
    auto flushed = test::values_are(h.output(), {KV{"alice", 6}});
    EXPECT_TRUE(flushed) << flushed.message;
}

// Dogfood: the real window operator's state rides HarnessSnapshot -
// a fresh operator restored from the checkpoint converges to the same
// result as the original.
TEST(TestFramework, DogfoodWindowStateSurvivesSnapshotRestore) {
    auto h = test::OneInputOperatorHarness<KV, KV>::create(make_sum_window());
    h.open();
    h.process_element({"alice", 5}, 100);
    const auto checkpoint = h.snapshot(1);
    h.process_element({"alice", 7}, 200);
    h.process_watermark(1000);
    EXPECT_EQ(h.output_values(), (std::vector<KV>{KV{"alice", 12}}));

    auto h2 = test::OneInputOperatorHarness<KV, KV>::restore(make_sum_window(), checkpoint);
    h2.open();
    h2.process_element({"alice", 7}, 200);  // replay the post-checkpoint input
    h2.process_watermark(1000);
    EXPECT_EQ(h2.output_values(), (std::vector<KV>{KV{"alice", 12}}));
}

// Property test: the window aggregate is insensitive to arrival order
// within a window. deterministic_shuffle makes every seed reproducible
// on every platform.
TEST(TestFramework, PropertyWindowAggregateIsOrderInsensitive) {
    const std::vector<KV> inputs = {{"a", 1}, {"a", 2}, {"b", 3}, {"a", 4}, {"b", 5}};

    for (std::uint64_t seed = 0; seed < 10; ++seed) {
        auto h = test::OneInputOperatorHarness<KV, KV>::create(make_sum_window());
        h.open();
        test::TestSequence<KV> seq;
        for (const auto& p : test::deterministic_shuffle(inputs, seed)) {
            seq.element(p, 100);
        }
        seq.watermark(1000);
        seq.replay(h);
        auto r = test::values_are_unordered(h.output(), {KV{"a", 7}, KV{"b", 8}});
        EXPECT_TRUE(r) << "seed " << seed << ": " << r.message;
    }

    // The shuffle itself is deterministic: same items + seed, same order.
    EXPECT_EQ(test::deterministic_shuffle(inputs, 3), test::deterministic_shuffle(inputs, 3));
}
