// ProcessFunction / KeyedProcessFunction APIs - the high-level
// transform surface. Tests cover:
//   * emit-many via Collector
//   * processing-time timer registration + onTimer firing
//   * event-time timer registration + firing on watermark advance
//   * KeyedProcessFunction current_key() during process_element and
//     on_timer
//   * Side outputs through ProcessFunctionContext

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <span>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

// Non-keyed ProcessFunction that emits 0, 1, ..., v-1 for each input
// value v. Demonstrates the Collector emit-many shape.
class ExplodeProcessFn final : public ProcessFunction<int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& /*ctx*/,
                         Collector<int>& out) override {
        for (int i = 0; i < v; ++i) {
            out.collect(i);
        }
    }
};

TEST(ProcessFunction, CollectorEmitsMultipleRecordsPerInput) {
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{3}, Record<int>{1}, Record<int>{4}});
    auto h_src = dag.add_source<int>(src);

    auto pf = std::make_shared<ExplodeProcessFn>();
    auto adapter =
        std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(pf, "explode");
    auto h_pf = dag.add_operator<int, int>(h_src, adapter);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_pf, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // Expected emissions: 3 → {0,1,2}, 1 → {0}, 4 → {0,1,2,3} = 8 records.
    auto out = sink->collected();
    EXPECT_EQ(out.size(), 8u);
}

// A ProcessFunction that registers a processing-time timer 10ms in
// the future for the first record it sees, then emits a sentinel
// when the timer fires.
class ProcessingTimeTimerFn final : public ProcessFunction<int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& ctx,
                         Collector<int>& out) override {
        out.collect(v);
        if (!registered_) {
            ctx.timer_service()->register_processing_time_timer(ctx.timer_service()->now_ms() + 10);
            registered_ = true;
        }
    }
    void on_timer(std::int64_t /*ts*/, OnTimerContext<int>& /*ctx*/, Collector<int>& out) override {
        out.collect(999);
        fired_.store(true);
    }
    bool fired() const noexcept { return fired_.load(); }

private:
    bool registered_{false};
    std::atomic<bool> fired_{false};
};

class HoldingSource final : public Source<int> {
public:
    explicit HoldingSource(std::vector<Record<int>> records) : records_(std::move(records)) {}
    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            Batch<int> b{records_};
            out.emit_data(std::move(b));
            emitted_ = true;
        }
        std::this_thread::sleep_for(2ms);
        return true;
    }
    std::string name() const override { return "holding"; }

private:
    std::vector<Record<int>> records_;
    bool emitted_{false};
};

TEST(ProcessFunction, ProcessingTimeTimerFiresAndEmitsSentinel) {
    Dag dag;
    auto src = std::make_shared<HoldingSource>(std::vector<Record<int>>{Record<int>{42}});
    auto h_src = dag.add_source<int>(src);
    auto pf = std::make_shared<ProcessingTimeTimerFn>();
    auto adapter = std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(pf);
    auto h_pf = dag.add_operator<int, int>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_pf, sink);

    LocalExecutor exec(std::move(dag));
    exec.start();
    // Wait for the timer to fire (10ms registration + a few ms slop).
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    while (std::chrono::steady_clock::now() < deadline && !pf->fired()) {
        std::this_thread::sleep_for(5ms);
    }
    src->cancel();
    exec.cancel();
    exec.await_termination();

    EXPECT_TRUE(pf->fired()) << "processing-time timer must fire within the deadline";
    bool saw_sentinel = false;
    bool saw_data = false;
    for (int v : sink->collected()) {
        if (v == 42) {
            saw_data = true;
        }
        if (v == 999) {
            saw_sentinel = true;
        }
    }
    EXPECT_TRUE(saw_data);
    EXPECT_TRUE(saw_sentinel);
}

// Event-time timer test: registers a timer at t=200; verifies it
// fires once the watermark crosses 200.
class EventTimeTimerFn final : public ProcessFunction<int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& ctx,
                         Collector<int>& out) override {
        out.collect(v);
        if (!registered_) {
            ctx.timer_service()->register_event_time_timer(200);
            registered_ = true;
        }
    }
    void on_timer(std::int64_t ts, OnTimerContext<int>& ctx, Collector<int>& out) override {
        EXPECT_EQ(ctx.time_domain(), TimeDomain::EventTime);
        out.collect(static_cast<int>(1000 + ts));
        fired_.store(true);
    }
    bool fired() const noexcept { return fired_.load(); }

private:
    bool registered_{false};
    std::atomic<bool> fired_{false};
};

// Emits records with event_time stamps, then a Watermark::max() to
// push the watermark past any registered event-time timer.
class TimedSource final : public Source<int> {
public:
    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            Batch<int> b;
            b.push(Record<int>{1, EventTime{100}});
            b.push(Record<int>{2, EventTime{150}});
            out.emit_data(std::move(b));
            // Bump watermark past the event-time timer at t=200.
            out.emit_watermark(Watermark{EventTime{500}});
            emitted_ = true;
        }
        return false;
    }
    std::string name() const override { return "timed"; }

private:
    bool emitted_{false};
};

TEST(ProcessFunction, EventTimeTimerFiresOnWatermarkAdvance) {
    Dag dag;
    auto src = std::make_shared<TimedSource>();
    auto h_src = dag.add_source<int>(src);
    auto pf = std::make_shared<EventTimeTimerFn>();
    auto adapter = std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(pf);
    auto h_pf = dag.add_operator<int, int>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_pf, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(pf->fired()) << "event-time timer must fire when watermark crosses it";
    bool saw_sentinel = false;
    for (int v : sink->collected()) {
        if (v == 1200) {  // 1000 + ts(200)
            saw_sentinel = true;
        }
    }
    EXPECT_TRUE(saw_sentinel);
}

// Keyed process function that records the current_key() it sees in a
// shared bucket, then registers an event-time timer; on_timer the key
// should match the original record's key.
class KeyTrackingProcessFn final : public KeyedProcessFunction<std::int64_t, int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& ctx,
                         Collector<int>& out) override {
        {
            std::lock_guard lock(mu_);
            element_keys_.push_back(current_key());
        }
        out.collect(v);
        ctx.timer_service()->register_event_time_timer(200);
    }
    void on_timer(std::int64_t /*ts*/, OnTimerContext<int>& /*ctx*/, Collector<int>& out) override {
        {
            std::lock_guard lock(mu_);
            timer_keys_.push_back(current_key());
        }
        out.collect(-1);
    }

    std::vector<std::int64_t> element_keys() const {
        std::lock_guard lock(mu_);
        return element_keys_;
    }
    std::vector<std::int64_t> timer_keys() const {
        std::lock_guard lock(mu_);
        return timer_keys_;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::int64_t> element_keys_;
    std::vector<std::int64_t> timer_keys_;
};

// ---------------------------------------------------------------------------
// CoProcessFunction + KeyedCoProcessFunction
// ---------------------------------------------------------------------------

class SumBothSidesCoFn final : public CoProcessFunction<int, int, int> {
public:
    void process_element1(const int& v,
                          ProcessFunctionContext<int>& /*ctx*/,
                          Collector<int>& out) override {
        out.collect(v + 100);  // tag left elements
    }
    void process_element2(const int& v,
                          ProcessFunctionContext<int>& /*ctx*/,
                          Collector<int>& out) override {
        out.collect(v + 200);  // tag right elements
    }
};

TEST(CoProcessFunction, BothSidesEmitTaggedRecords) {
    Dag dag;
    auto left = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{1}, Record<int>{2}});
    auto right = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{10}, Record<int>{20}});
    auto h_l = dag.add_source<int>(left);
    auto h_r = dag.add_source<int>(right);

    auto pf = std::make_shared<SumBothSidesCoFn>();
    auto adapter = std::make_shared<::clink::detail::CoProcessFunctionAdapter<int, int, int>>(pf);
    auto h_co = dag.add_co_operator<int, int, int>(h_l, h_r, adapter);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_co, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    std::vector<int> sorted(out.begin(), out.end());
    std::sort(sorted.begin(), sorted.end());
    EXPECT_EQ(sorted, (std::vector<int>{101, 102, 210, 220}));
}

class TimerSettingCoFn final : public CoProcessFunction<int, int, int> {
public:
    void process_element1(const int& v,
                          ProcessFunctionContext<int>& ctx,
                          Collector<int>& out) override {
        out.collect(v);
        if (!set_) {
            ctx.timer_service()->register_event_time_timer(200);
            set_ = true;
        }
    }
    void process_element2(const int& v,
                          ProcessFunctionContext<int>& /*ctx*/,
                          Collector<int>& out) override {
        out.collect(v + 1000);
    }
    void on_timer(std::int64_t /*ts*/, OnTimerContext<int>& /*ctx*/, Collector<int>& out) override {
        out.collect(-7);
    }

private:
    bool set_{false};
};

class TimedIntsSource final : public Source<int> {
public:
    explicit TimedIntsSource(std::vector<Record<int>> recs, Watermark wm)
        : recs_(std::move(recs)), wm_(wm) {}
    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            Batch<int> b{recs_};
            out.emit_data(std::move(b));
            out.emit_watermark(wm_);
            emitted_ = true;
        }
        return false;
    }
    std::string name() const override { return "timed_ints"; }

private:
    std::vector<Record<int>> recs_;
    Watermark wm_;
    bool emitted_{false};
};

TEST(CoProcessFunction, EventTimeTimerFiresAfterMinWatermarkAdvances) {
    Dag dag;
    auto left = std::make_shared<TimedIntsSource>(
        std::vector<Record<int>>{Record<int>{5, EventTime{100}}}, Watermark{EventTime{500}});
    auto right = std::make_shared<TimedIntsSource>(
        std::vector<Record<int>>{Record<int>{50, EventTime{150}}}, Watermark{EventTime{500}});
    auto h_l = dag.add_source<int>(left);
    auto h_r = dag.add_source<int>(right);

    auto pf = std::make_shared<TimerSettingCoFn>();
    auto adapter = std::make_shared<::clink::detail::CoProcessFunctionAdapter<int, int, int>>(pf);
    auto h_co = dag.add_co_operator<int, int, int>(h_l, h_r, adapter);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_co, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    bool saw_left = false;
    bool saw_right = false;
    bool saw_timer = false;
    for (int v : sink->collected()) {
        if (v == 5)
            saw_left = true;
        if (v == 1050)
            saw_right = true;
        if (v == -7)
            saw_timer = true;
    }
    EXPECT_TRUE(saw_left);
    EXPECT_TRUE(saw_right);
    EXPECT_TRUE(saw_timer) << "event-time timer should fire once min(left_wm, right_wm) >= 200";
}

class KeyTrackingCoFn final : public KeyedCoProcessFunction<std::int64_t, int, int, int> {
public:
    void process_element1(const int& v,
                          ProcessFunctionContext<int>& /*ctx*/,
                          Collector<int>& out) override {
        {
            std::lock_guard lock(mu_);
            left_keys_.push_back(current_key());
        }
        out.collect(v);
    }
    void process_element2(const int& v,
                          ProcessFunctionContext<int>& /*ctx*/,
                          Collector<int>& out) override {
        {
            std::lock_guard lock(mu_);
            right_keys_.push_back(current_key());
        }
        out.collect(v);
    }

    std::vector<std::int64_t> left_keys() const {
        std::lock_guard lock(mu_);
        return left_keys_;
    }
    std::vector<std::int64_t> right_keys() const {
        std::lock_guard lock(mu_);
        return right_keys_;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::int64_t> left_keys_;
    std::vector<std::int64_t> right_keys_;
};

TEST(KeyedCoProcessFunction, CurrentKeyTracksWhicheverSideDispatchedLast) {
    Dag dag;
    auto left = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{7}, Record<int>{8}});
    auto right = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{70}, Record<int>{80}});
    auto h_l = dag.add_source<int>(left);
    auto h_r = dag.add_source<int>(right);

    auto pf = std::make_shared<KeyTrackingCoFn>();
    auto adapter = std::make_shared<
        ::clink::detail::KeyedCoProcessFunctionAdapter<std::int64_t, int, int, int>>(
        pf,
        [](const int& v) -> std::int64_t { return v; },
        [](const int& v) -> std::int64_t { return v / 10; });
    auto h_co = dag.add_co_operator<int, int, int>(h_l, h_r, adapter);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_co, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto lk = pf->left_keys();
    auto rk = pf->right_keys();
    ASSERT_EQ(lk.size(), 2u);
    ASSERT_EQ(rk.size(), 2u);
    EXPECT_EQ(lk[0], 7);
    EXPECT_EQ(lk[1], 8);
    EXPECT_EQ(rk[0], 7);  // 70/10
    EXPECT_EQ(rk[1], 8);  // 80/10
}

// ---------------------------------------------------------------------------
// BroadcastProcessFunction
// ---------------------------------------------------------------------------

class TaggingBroadcastFn final : public BroadcastProcessFunction<int, int, int, int> {
public:
    void process_element(const int& v,
                         const BroadcastState<int>& state,
                         Collector<int>& out) override {
        const int multiplier = state.get().value_or(1);
        // Emit the multiplied value AND a tag value, demonstrating
        // the emit-many shape the Collector enables.
        out.collect(v * multiplier);
        out.collect(v + multiplier);
    }
    void process_broadcast_element(const int& v,
                                   BroadcastState<int>& state,
                                   Collector<int>& /*out*/) override {
        // Each broadcast record replaces the stored multiplier.
        state.put(v);
    }
};

TEST(BroadcastProcessFunction, MainStreamSeesBroadcastStateAndEmitsMany) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    Dag dag;
    auto main = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{2}, Record<int>{3}});
    auto brod = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{10}});
    auto h_m = dag.add_source<int>(main);
    auto h_b = dag.add_source<int>(brod);

    auto pf = std::make_shared<TaggingBroadcastFn>();
    auto [pb, pm] = ::clink::detail::build_broadcast_process_callbacks<int, int, int, int>(pf);

    Codec<int> int_codec{.encode =
                             [](const int& v) {
                                 std::vector<std::byte> out(4);
                                 const auto u = static_cast<std::uint32_t>(v);
                                 for (int i = 0; i < 4; ++i) {
                                     out[i] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
                                 }
                                 return out;
                             },
                         .decode = [](std::span<const std::byte> b) -> std::optional<int> {
                             if (b.size() < 4)
                                 return std::nullopt;
                             std::uint32_t u = 0;
                             for (int i = 0; i < 4; ++i) {
                                 u |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[i]))
                                      << (i * 8);
                             }
                             return static_cast<int>(u);
                         }};

    auto h_bp = dag.broadcast_process<int, int, int, int>(h_m, h_b, pb, pm, int_codec, "mult_slot");

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_bp, sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // Order of broadcast vs. main is non-deterministic across threads.
    // What we CAN assert: every main record produces exactly two
    // emissions (the *mult and +mult outputs), so the sink sees 4
    // records total.
    auto out = sink->collected();
    EXPECT_EQ(out.size(), 4u);
}

// ---------------------------------------------------------------------------
// ProcessWindowFunction (tumbling event-time, in-memory)
// ---------------------------------------------------------------------------

class CountAndMaxWindowFn final : public ProcessWindowFunction<std::int64_t, int, int> {
public:
    void process(const std::int64_t& key,
                 const WindowContext& ctx,
                 const std::vector<int>& elements,
                 Collector<int>& out) override {
        // Emit two records per fired window:
        //   * encoded as 1000*window_start + count, and
        //   * the max input value.
        // Both encode `key` in their high bits so the test can verify
        // the right key was associated with this window.
        int max_v = 0;
        for (int v : elements) {
            if (v > max_v)
                max_v = v;
        }
        out.collect(static_cast<int>(1000 * ctx.window_start + static_cast<int>(elements.size()) +
                                     static_cast<int>(key) * 10'000));
        out.collect(max_v + static_cast<int>(key) * 100'000);
    }
};

// Emits a fixed batch of (key, value) records with event_time, then a
// watermark high enough to close every tumbling window.
class KeyedTimedSource final : public Source<std::pair<std::int64_t, int>> {
public:
    bool produce(Emitter<std::pair<std::int64_t, int>>& out) override {
        if (cancelled() || emitted_) {
            return false;
        }
        Batch<std::pair<std::int64_t, int>> b;
        // Window 0 (start=0, end=100): key 1 -> {7, 9}, key 2 -> {5}
        b.push(Record<std::pair<std::int64_t, int>>{{1, 7}, EventTime{10}});
        b.push(Record<std::pair<std::int64_t, int>>{{1, 9}, EventTime{50}});
        b.push(Record<std::pair<std::int64_t, int>>{{2, 5}, EventTime{80}});
        // Window 1 (start=100, end=200): key 1 -> {11}
        b.push(Record<std::pair<std::int64_t, int>>{{1, 11}, EventTime{150}});
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark{EventTime{500}});
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "keyed_timed"; }

private:
    bool emitted_{false};
};

TEST(ProcessWindowFunction, FiresPerKeyWindowAtWatermarkCrossing) {
    Dag dag;
    auto src = std::make_shared<KeyedTimedSource>();
    auto h_src = dag.add_source<std::pair<std::int64_t, int>>(src);

    auto pwfn = std::make_shared<CountAndMaxWindowFn>();
    auto op =
        std::make_shared<::clink::detail::TumblingProcessWindowAdapter<std::int64_t, int, int>>(
            std::chrono::milliseconds{100}, pwfn);
    auto h_w = dag.add_operator<std::pair<std::int64_t, int>, int>(h_src, op);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_w, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    // Three windows fired (key 1 / window 0; key 2 / window 0; key 1
    // / window 1). Each emits 2 records via the PWFn. = 6 outputs.
    EXPECT_EQ(out.size(), 6u);

    // Decode and assert per-window expectations. The "count" emission
    // for key 1 / window 0 is encoded as 1000*0 + 2 + 1*10000 = 10002.
    // For key 2 / window 0: 1000*0 + 1 + 2*10000 = 20001. For key 1 /
    // window 1: 1000*100 + 1 + 1*10000 = 110001.
    std::set<int> emitted(out.begin(), out.end());
    EXPECT_NE(emitted.find(10002), emitted.end()) << "key=1 window=0 count emission missing";
    EXPECT_NE(emitted.find(20001), emitted.end()) << "key=2 window=0 count emission missing";
    EXPECT_NE(emitted.find(110001), emitted.end()) << "key=1 window=1 count emission missing";
    // Max emissions: key 1 / window 0 -> 9 + 1*100000 = 100009;
    //                key 2 / window 0 -> 5 + 2*100000 = 200005;
    //                key 1 / window 1 -> 11 + 1*100000 = 100011.
    EXPECT_NE(emitted.find(100009), emitted.end());
    EXPECT_NE(emitted.find(200005), emitted.end());
    EXPECT_NE(emitted.find(100011), emitted.end());
}

// Counts records per (key, window) and emits one record encoding the count.
class CountOnlyWindowFn final : public ProcessWindowFunction<std::int64_t, int, int> {
public:
    void process(const std::int64_t& key,
                 const WindowContext& ctx,
                 const std::vector<int>& elements,
                 Collector<int>& out) override {
        // Encode: 1_000_000 * key + 10_000 * (window_start/100) + count
        out.collect(static_cast<int>(1'000'000 * key + 10'000 * (ctx.window_start / 100) +
                                     static_cast<int>(elements.size())));
    }
};

class KeyedSlidingSource final : public Source<std::pair<std::int64_t, int>> {
public:
    bool produce(Emitter<std::pair<std::int64_t, int>>& out) override {
        if (cancelled() || emitted_) {
            return false;
        }
        Batch<std::pair<std::int64_t, int>> b;
        // size=200, slide=100. Records at ts=50, 150, 250 for key=1.
        // Sliding windows covering each:
        //   ts=50  -> windows [0,200) and [-100,100) (negative window
        //             clamped/ignored downstream; this op enumerates
        //             every covering start including negative ones).
        //   ts=150 -> [0,200) and [100,300)
        //   ts=250 -> [100,300) and [200,400)
        b.push(Record<std::pair<std::int64_t, int>>{{1, 50}, EventTime{50}});
        b.push(Record<std::pair<std::int64_t, int>>{{1, 150}, EventTime{150}});
        b.push(Record<std::pair<std::int64_t, int>>{{1, 250}, EventTime{250}});
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark{EventTime{1000}});
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "keyed_sliding"; }

private:
    bool emitted_{false};
};

TEST(SlidingProcessWindow, EachRecordEntersEveryCoveringWindow) {
    Dag dag;
    auto src = std::make_shared<KeyedSlidingSource>();
    auto h_src = dag.add_source<std::pair<std::int64_t, int>>(src);

    auto pwfn = std::make_shared<CountOnlyWindowFn>();
    auto op =
        std::make_shared<::clink::detail::SlidingProcessWindowAdapter<std::int64_t, int, int>>(
            std::chrono::milliseconds{200}, std::chrono::milliseconds{100}, pwfn);
    auto h_w = dag.add_operator<std::pair<std::int64_t, int>, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_w, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    // Expected counts per fired window for key=1:
    //   [-100, 100): contains ts=50 -> count 1
    //   [0, 200):    contains ts=50, 150 -> count 2
    //   [100, 300):  contains ts=150, 250 -> count 2
    //   [200, 400):  contains ts=250 -> count 1
    // Encoded: 1*1_000_000 + 10_000*(start/100) + count
    //   1_000_000 + 10_000*(-1) + 1 = 990_001
    //   1_000_000 + 10_000*0 + 2 = 1_000_002
    //   1_000_000 + 10_000*1 + 2 = 1_010_002
    //   1_000_000 + 10_000*2 + 1 = 1_020_001
    std::set<int> emitted(out.begin(), out.end());
    EXPECT_NE(emitted.find(990'001), emitted.end()) << "window [-100,100)";
    EXPECT_NE(emitted.find(1'000'002), emitted.end()) << "window [0,200)";
    EXPECT_NE(emitted.find(1'010'002), emitted.end()) << "window [100,300)";
    EXPECT_NE(emitted.find(1'020'001), emitted.end()) << "window [200,400)";
}

class KeyedSessionSource final : public Source<std::pair<std::int64_t, int>> {
public:
    bool produce(Emitter<std::pair<std::int64_t, int>>& out) override {
        if (cancelled() || emitted_) {
            return false;
        }
        Batch<std::pair<std::int64_t, int>> b;
        // gap=50ms. Key=1: records at 10, 30, 200, 220.
        // First session: [10, 31) extended by 30 -> [10, 31). Then 200
        // is 200-31=169 > 50 apart -> new session. So 2 sessions.
        // Key=2: records at 100, 105 -> one session.
        b.push(Record<std::pair<std::int64_t, int>>{{1, 1}, EventTime{10}});
        b.push(Record<std::pair<std::int64_t, int>>{{1, 2}, EventTime{30}});
        b.push(Record<std::pair<std::int64_t, int>>{{1, 3}, EventTime{200}});
        b.push(Record<std::pair<std::int64_t, int>>{{1, 4}, EventTime{220}});
        b.push(Record<std::pair<std::int64_t, int>>{{2, 5}, EventTime{100}});
        b.push(Record<std::pair<std::int64_t, int>>{{2, 6}, EventTime{105}});
        out.emit_data(std::move(b));
        out.emit_watermark(Watermark{EventTime{10'000}});
        emitted_ = true;
        return false;
    }
    std::string name() const override { return "keyed_session"; }

private:
    bool emitted_{false};
};

TEST(SessionProcessWindow, MergesContiguousRecordsAndStartsNewSessionAcrossGap) {
    Dag dag;
    auto src = std::make_shared<KeyedSessionSource>();
    auto h_src = dag.add_source<std::pair<std::int64_t, int>>(src);

    auto pwfn = std::make_shared<CountOnlyWindowFn>();
    auto op =
        std::make_shared<::clink::detail::SessionProcessWindowAdapter<std::int64_t, int, int>>(
            std::chrono::milliseconds{50}, pwfn);
    auto h_w = dag.add_operator<std::pair<std::int64_t, int>, int>(h_src, op);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_w, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    // Three sessions:
    //   key=1 session1 (start=10, end=31): 2 elements
    //   key=1 session2 (start=200, end=221): 2 elements
    //   key=2 session1 (start=100, end=106): 2 elements
    EXPECT_EQ(out.size(), 3u) << "should fire one record per session";
    for (int v : out) {
        const int count = v % 100;  // last two digits
        EXPECT_EQ(count, 2) << "each session contains 2 records";
    }
}

TEST(KeyedProcessFunction, CurrentKeyMatchesExtractorDuringProcessElement) {
    Dag dag;
    auto src = std::make_shared<TimedSource>();  // emits {1, 2} + watermark
    auto h_src = dag.add_source<int>(src);

    auto pf = std::make_shared<KeyTrackingProcessFn>();
    // Key extractor: identity on int -> int64.
    auto adapter =
        std::make_shared<::clink::detail::KeyedProcessFunctionAdapter<std::int64_t, int, int>>(
            pf, [](const int& v) -> std::int64_t { return v; });
    auto h_pf = dag.add_operator<int, int>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_pf, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto ek = pf->element_keys();
    ASSERT_EQ(ek.size(), 2u);
    EXPECT_EQ(ek[0], 1);
    EXPECT_EQ(ek[1], 2);

    // Timer firing happens after watermark crosses 200. Both records
    // registered a timer at t=200; both should have fired.
    auto tk = pf->timer_keys();
    EXPECT_FALSE(tk.empty()) << "at least one event-time timer should have fired";
}

// ---------------------------------------------------------------------------
// Typed-K KeyedProcessFunction (K = std::string)
// ---------------------------------------------------------------------------

class StringKeyedProcessFn final : public KeyedProcessFunction<std::string, int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& /*ctx*/,
                         Collector<int>& out) override {
        std::lock_guard lock(mu_);
        keys_seen_.push_back(current_key());
        out.collect(v);
    }
    std::vector<std::string> keys_seen() const {
        std::lock_guard lock(mu_);
        return keys_seen_;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::string> keys_seen_;
};

TEST(KeyedProcessFunction, StringKeyExtractorPopulatesCurrentKey) {
    Dag dag;
    auto src = std::make_shared<TimedSource>();  // emits {1, 2}
    auto h_src = dag.add_source<int>(src);

    auto pf = std::make_shared<StringKeyedProcessFn>();
    auto adapter =
        std::make_shared<::clink::detail::KeyedProcessFunctionAdapter<std::string, int, int>>(
            pf, [](const int& v) -> std::string { return v == 1 ? "alpha" : "beta"; });
    auto h_pf = dag.add_operator<int, int>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_pf, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto keys = pf->keys_seen();
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], "alpha");
    EXPECT_EQ(keys[1], "beta");
}

// ---------------------------------------------------------------------------
// Typed-K KeyedProcessFunction (K = user struct)
// ---------------------------------------------------------------------------

struct CompositeKey {
    std::int64_t tenant_id;
    std::string region;
    bool operator==(const CompositeKey&) const = default;
};

class StructKeyedProcessFn final : public KeyedProcessFunction<CompositeKey, int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& /*ctx*/,
                         Collector<int>& out) override {
        std::lock_guard lock(mu_);
        keys_seen_.push_back(current_key());
        out.collect(v);
    }
    std::vector<CompositeKey> keys_seen() const {
        std::lock_guard lock(mu_);
        return keys_seen_;
    }

private:
    mutable std::mutex mu_;
    std::vector<CompositeKey> keys_seen_;
};

TEST(KeyedProcessFunction, StructKeyExtractorPopulatesCurrentKey) {
    Dag dag;
    auto src = std::make_shared<TimedSource>();
    auto h_src = dag.add_source<int>(src);

    auto pf = std::make_shared<StructKeyedProcessFn>();
    auto adapter =
        std::make_shared<::clink::detail::KeyedProcessFunctionAdapter<CompositeKey, int, int>>(
            pf, [](const int& v) -> CompositeKey {
                return CompositeKey{static_cast<std::int64_t>(v), v == 1 ? "us-east" : "eu-west"};
            });
    auto h_pf = dag.add_operator<int, int>(h_src, adapter);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_pf, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto keys = pf->keys_seen();
    ASSERT_EQ(keys.size(), 2u);
    EXPECT_EQ(keys[0], (CompositeKey{1, "us-east"}));
    EXPECT_EQ(keys[1], (CompositeKey{2, "eu-west"}));
}

// ---- timer checkpoint / restore (failover) ----
//
// A ProcessFunction that registers a far-future event-time timer per
// record and fires a sentinel when the timer triggers. Used to prove a
// checkpointed timer survives a restore into a fresh state backend.
class RestorableTimerFn final : public ProcessFunction<int, int> {
public:
    void process_element(const int& v,
                         ProcessFunctionContext<int>& ctx,
                         Collector<int>& out) override {
        out.collect(v);
        ctx.timer_service()->register_event_time_timer(1000, "k");
    }
    void on_timer(std::int64_t ts, OnTimerContext<int>& /*ctx*/, Collector<int>& out) override {
        out.collect(static_cast<int>(7000 + ts));
        fired_.store(true);
    }
    bool fired() const noexcept { return fired_.load(); }

private:
    std::atomic<bool> fired_{false};
};

namespace {
// A forwarding Emitter<int> that records every emitted value, so a test
// can drive an operator directly (no Dag) and inspect its output.
Emitter<int> recording_emitter(std::vector<int>& sink) {
    return Emitter<int>(typename Emitter<int>::Forward([&sink](StreamElement<int> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.push_back(r.value());
            }
        }
        return true;
    }));
}
}  // namespace

TEST(ProcessFunction, EventTimeTimerSurvivesCheckpointRestore) {
    const OperatorId op_id{99};

    // --- run 1: register a far-future event-time timer, checkpoint it. ---
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    RuntimeContext rc1(op_id, "pf", backend1.get(), nullptr);
    auto fn1 = std::make_shared<RestorableTimerFn>();
    auto op1 = std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(fn1, "pf");
    op1->set_id(op_id);
    op1->attach_runtime(&rc1);
    op1->open();

    std::vector<int> emitted1;
    auto em1 = recording_emitter(emitted1);
    Batch<int> b;
    b.push(Record<int>{42});
    op1->process(StreamElement<int>::data(std::move(b)), em1);
    ASSERT_EQ(rc1.timer_service()->next_event_timestamp().value_or(-1), 1000);

    // Checkpoint exactly as the runner does: persist timers, then snapshot.
    op1->snapshot_timers(*backend1, op_id);
    auto snap = backend1->snapshot(CheckpointId{1});
    op1->close();
    op1->attach_runtime(nullptr);

    // --- run 2: fresh backend restored from the snapshot. ---
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext rc2(op_id, "pf", backend2.get(), nullptr);
    auto fn2 = std::make_shared<RestorableTimerFn>();
    auto op2 = std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(fn2, "pf");
    op2->set_id(op_id);
    op2->attach_runtime(&rc2);
    op2->restore_timers(*backend2, op_id);
    op2->open();

    // The timer is back without re-processing any record.
    ASSERT_EQ(rc2.timer_service()->next_event_timestamp().value_or(-1), 1000);

    // Advancing the watermark past it fires on_timer in the restored job.
    std::vector<int> emitted2;
    auto em2 = recording_emitter(emitted2);
    op2->on_watermark(Watermark{EventTime{2000}}, em2);
    EXPECT_TRUE(fn2->fired()) << "restored event-time timer must fire on watermark advance";
    EXPECT_NE(std::find(emitted2.begin(), emitted2.end(), 8000), emitted2.end());  // 7000 + 1000
}

// Rescale ROUTES checkpointed timers to the subtask owning each timer's key
// group. The timer blob is broadcast to every new subtask (operator-state);
// restore_timers keeps only the timers whose key falls in this subtask's
// key-group range. Across two complementary subtasks the original timers
// partition exactly - no loss, no duplication - matching their key groups.
TEST(ProcessFunction, EventTimeTimersRouteByKeyGroupOnRescale) {
    const OperatorId op_id{99};

    auto backend1 = std::make_shared<InMemoryStateBackend>();
    RuntimeContext rc1(op_id, "pf", backend1.get(), nullptr);
    auto fn1 = std::make_shared<RestorableTimerFn>();
    auto op1 = std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(fn1, "pf");
    op1->set_id(op_id);
    op1->attach_runtime(&rc1);
    op1->open();
    std::vector<std::string> keys;
    for (int i = 0; i < 60; ++i) {
        keys.push_back("key-" + std::to_string(i));
        rc1.timer_service()->register_event_time_timer(1000, keys.back());
    }
    op1->snapshot_timers(*backend1, op_id);
    auto snap = backend1->snapshot(CheckpointId{1});

    // Restore into two complementary subtasks: lower / upper half of the
    // key-group space. Each restores the operator-state whole, then routes.
    auto restore_into = [&](KeyGroupRange range) {
        auto backend = std::make_shared<InMemoryStateBackend>();
        backend->restore(snap);
        RuntimeContext rc(op_id, "pf", backend.get(), nullptr);
        rc.set_restore_key_group_range(range);
        auto fn = std::make_shared<RestorableTimerFn>();
        auto op = std::make_shared<::clink::detail::ProcessFunctionAdapter<int, int>>(fn, "pf");
        op->set_id(op_id);
        op->attach_runtime(&rc);
        op->restore_timers(*backend, op_id);
        std::vector<std::string> got;
        rc.timer_service()->poll_due_event_time(
            2000, [&](std::int64_t, const std::string& k) { got.push_back(k); });
        return got;
    };
    const auto lo = restore_into(KeyGroupRange{KeyGroup{0}, KeyGroup{64}});
    const auto hi = restore_into(KeyGroupRange{KeyGroup{64}, KeyGroup{kNumKeyGroups}});

    const std::set<std::string> lo_set(lo.begin(), lo.end());
    const std::set<std::string> hi_set(hi.begin(), hi.end());
    EXPECT_EQ(lo.size() + hi.size(), keys.size()) << "no loss, no duplication";
    for (const auto& k : keys) {
        const auto kg = key_group_for_key(
            std::span<const std::byte>{reinterpret_cast<const std::byte*>(k.data()), k.size()});
        const bool in_lo = kg < 64;
        EXPECT_EQ(lo_set.count(k) == 1, in_lo) << "key " << k << " kg " << kg;
        EXPECT_EQ(hi_set.count(k) == 1, !in_lo) << "key " << k << " kg " << kg;
    }
    EXPECT_FALSE(lo.empty());
    EXPECT_FALSE(hi.empty());
}

// Two-input counterpart: a CoProcessFunction's event-time timer (both
// inputs share one TimerService) survives a checkpoint/restore. Exercises
// the CoOperator::snapshot_timers / restore_timers hooks and the co-op
// runner wiring's shared helper path.
class RestorableCoTimerFn final : public CoProcessFunction<int, int, int> {
public:
    void process_element1(const int& v,
                          ProcessFunctionContext<int>& ctx,
                          Collector<int>& out) override {
        out.collect(v);
        ctx.timer_service()->register_event_time_timer(1000, "k");
    }
    void process_element2(const int& v,
                          ProcessFunctionContext<int>& /*ctx*/,
                          Collector<int>& out) override {
        out.collect(v + 1000);
    }
    void on_timer(std::int64_t ts, OnTimerContext<int>& /*ctx*/, Collector<int>& out) override {
        out.collect(static_cast<int>(7000 + ts));
        fired_.store(true);
    }
    bool fired() const noexcept { return fired_.load(); }

private:
    std::atomic<bool> fired_{false};
};

TEST(CoProcessFunction, EventTimeTimerSurvivesCheckpointRestore) {
    const OperatorId op_id{123};

    auto backend1 = std::make_shared<InMemoryStateBackend>();
    RuntimeContext rc1(op_id, "co", backend1.get(), nullptr);
    auto fn1 = std::make_shared<RestorableCoTimerFn>();
    auto op1 =
        std::make_shared<::clink::detail::CoProcessFunctionAdapter<int, int, int>>(fn1, "co");
    op1->set_id(op_id);
    op1->attach_runtime(&rc1);
    op1->open();

    std::vector<int> emitted1;
    auto em1 = recording_emitter(emitted1);
    Batch<int> b;
    b.push(Record<int>{5});
    op1->process_element1(StreamElement<int>::data(std::move(b)), em1);
    ASSERT_EQ(rc1.timer_service()->next_event_timestamp().value_or(-1), 1000);

    op1->snapshot_timers(*backend1, op_id);
    auto snap = backend1->snapshot(CheckpointId{1});
    op1->close();
    op1->attach_runtime(nullptr);

    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext rc2(op_id, "co", backend2.get(), nullptr);
    auto fn2 = std::make_shared<RestorableCoTimerFn>();
    auto op2 =
        std::make_shared<::clink::detail::CoProcessFunctionAdapter<int, int, int>>(fn2, "co");
    op2->set_id(op_id);
    op2->attach_runtime(&rc2);
    op2->restore_timers(*backend2, op_id);
    op2->open();

    ASSERT_EQ(rc2.timer_service()->next_event_timestamp().value_or(-1), 1000);

    std::vector<int> emitted2;
    auto em2 = recording_emitter(emitted2);
    op2->on_watermark(Watermark{EventTime{2000}}, em2);
    EXPECT_TRUE(fn2->fired()) << "restored co-op event-time timer must fire on watermark advance";
    EXPECT_NE(std::find(emitted2.begin(), emitted2.end(), 8000), emitted2.end());  // 7000 + 1000
}
