// Self-tests for the public testing framework (clink::test) - increment 1:
// OutputCapture, OneInputOperatorHarness, deterministic time and timers,
// lifecycle enforcement. These pin the framework's own contract; the
// user-facing guide lives in docs/internals/testing-framework.md.

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
        if (!element.is_data()) {
            return;
        }
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
        if (!element.is_data()) {
            return;
        }
        for (const auto& rec : element.as_data()) {
            const auto ts = rec.event_time() ? rec.event_time()->millis() : 0;
            runtime()->timer_service()->register_event_time_timer(ts + 100,
                                                                  std::to_string(rec.value()));
            runtime()->timer_service()->register_processing_time_timer(
                runtime()->timer_service()->now_ms() + 50, std::to_string(rec.value()));
        }
        (void)out;
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
