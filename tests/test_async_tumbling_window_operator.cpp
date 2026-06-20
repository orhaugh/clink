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
#include <chrono>
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
#include "clink/core/pane_info.hpp"
#include "clink/operators/async_tumbling_window_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/async_execution_controller.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/output_tag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

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

// One emitted pane: value, event-time-ms, and PaneInfo (window ops always set it).
struct AtwEmit {
    KA value;
    std::int64_t ts;
    PaneInfo pane;
    bool operator==(const AtwEmit&) const = default;
};

std::vector<AtwEmit> collect_emits(BoundedChannel<StreamElement<KA>>& ch) {
    std::vector<AtwEmit> out;
    for (const auto& e : drain_ch(ch)) {
        if (!e.is_data()) {
            continue;
        }
        for (const auto& r : e.as_data()) {
            out.push_back(AtwEmit{r.value(),
                                  r.event_time().value_or(EventTime{0}).millis(),
                                  r.pane().value_or(PaneInfo{})});
        }
    }
    return out;
}

// Attach a RuntimeContext owning a typed side-output channel for `tag` (mirrors
// what the executor wires up). Returned ctx must outlive every op call.
template <typename Op>
RuntimeContext attach_late_channel(Op& op,
                                   StateBackend* backend,
                                   BoundedChannel<StreamElement<std::int64_t>>& late_ch,
                                   const std::string& tag) {
    RuntimeContext ctx(OperatorId{1}, "win_sum", backend, nullptr);
    SideOutputChannelMap channels;
    auto shared =
        std::shared_ptr<BoundedChannel<StreamElement<std::int64_t>>>(&late_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(shared);
    entry.close_fn = [shared] { shared->close(); };
    channels.emplace(tag, std::move(entry));
    ctx.set_side_output_channels(std::move(channels));
    return ctx;
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

// ---- allowed-lateness + late panes + late-output-tag ----------------------

// gap-style helper: a window-sum op with allowed_lateness set.
std::shared_ptr<AsyncTumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t>> make_late_op(
    std::int64_t lateness_ms) {
    auto op =
        std::make_shared<AsyncTumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
            /*window_size_ms=*/1000,
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            int64_codec(),
            int64_codec(),
            "win_sum");
    op->allowed_lateness(std::chrono::milliseconds{lateness_ms});
    return op;
}

TEST(AsyncTumblingWindowOperator, AllowedLatenessOnTimeThenLatePaneThenPurge) {
    // window [0,1000), lateness 500 -> purge deadline 1500.
    auto backend = std::make_shared<InMemoryStateBackend>();
    const OperatorId op_id{10};
    RuntimeContext ctx(op_id, "win_sum", backend.get(), nullptr);
    auto op = make_late_op(500);
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{100});
    op->process(StreamElement<KV>::data(std::move(b)), em);
    op->on_watermark(Watermark{EventTime{1000}}, em);  // on-time fire, window KEPT
    auto e1 = collect_emits(ch);
    ASSERT_EQ(e1.size(), 1u);
    EXPECT_EQ(e1[0].value, (KA{1, 10}));
    EXPECT_EQ(e1[0].ts, 999);  // window max timestamp
    EXPECT_EQ(e1[0].pane.timing, PaneInfo::Timing::OnTime);
    EXPECT_EQ(e1[0].pane.pane_index, 0);
    EXPECT_TRUE(e1[0].pane.is_first);
    EXPECT_FALSE(e1[0].pane.is_last) << "lateness>0 so the on-time pane is not the last";

    // Within-band late record (watermark 1000 < purge 1500): re-fire a Late pane.
    Batch<KV> late;
    late.emplace(KV{1, 5}, EventTime{200});
    op->process(StreamElement<KV>::data(std::move(late)), em);
    auto e2 = collect_emits(ch);
    ASSERT_EQ(e2.size(), 1u) << "a within-band late record must re-emit immediately";
    EXPECT_EQ(e2[0].value, (KA{1, 15})) << "late pane carries the updated aggregate";
    EXPECT_EQ(e2[0].pane.timing, PaneInfo::Timing::Late);
    EXPECT_EQ(e2[0].pane.pane_index, 1);
    EXPECT_FALSE(e2[0].pane.is_first);
    EXPECT_FALSE(e2[0].pane.is_last);

    // Cross the purge deadline: the window is erased, no further emission.
    op->on_watermark(Watermark{EventTime{1500}}, em);
    EXPECT_TRUE(collect_emits(ch).empty()) << "purge emits nothing";

    // A now-late-late record for the purged window is dropped (no tag).
    Batch<KV> latelate;
    latelate.emplace(KV{1, 99}, EventTime{300});
    op->process(StreamElement<KV>::data(std::move(latelate)), em);
    op->on_watermark(Watermark{EventTime{3000}}, em);
    EXPECT_TRUE(collect_emits(ch).empty()) << "a record for a purged window must not re-open it";
}

TEST(AsyncTumblingWindowOperator, LateLateRecordRoutedToSideOutputNotAggregated) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    BoundedChannel<StreamElement<std::int64_t>> late_ch(64);
    const OperatorId op_id{11};
    auto op = make_late_op(500);
    OutputTag<std::int64_t> tag("late");
    op->late_output_tag(tag);
    auto ctx = attach_late_channel(*op, backend.get(), late_ch, "late");
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{100});
    op->process(StreamElement<KV>::data(std::move(b)), em);
    op->on_watermark(Watermark{EventTime{1700}}, em);  // fires + purges [0,1000)
    (void)collect_emits(ch);

    // Past the purge deadline (1500): route to the side output, do NOT aggregate.
    Batch<KV> latelate;
    latelate.emplace(KV{1, 99}, EventTime{200});
    op->process(StreamElement<KV>::data(std::move(latelate)), em);
    op->on_watermark(Watermark{EventTime{3000}}, em);
    EXPECT_TRUE(collect_emits(ch).empty()) << "late-late record must not feed the aggregate";

    std::vector<std::pair<std::int64_t, std::int64_t>> side;
    for (const auto& e : drain_ch(late_ch)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                side.emplace_back(r.value(), r.event_time().value_or(EventTime{0}).millis());
            }
        }
    }
    ASSERT_EQ(side.size(), 1u);
    EXPECT_EQ(side[0].first, 99);
    EXPECT_EQ(side[0].second, 200) << "raw value + original event_time preserved";
}

TEST(AsyncTumblingWindowOperator, AsyncPathEmitsLatePaneFromCoroutine) {
    // The within-band late re-fire must emit from the resumed coroutine on the
    // async path (an inline-async backend forces process_async).
    auto backend = std::make_shared<AtwInlineAsyncBackend>();
    const OperatorId op_id{12};
    RuntimeContext ctx(op_id, "win_sum", backend.get(), nullptr);
    auto op = make_late_op(500);
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    AsyncExecutionController aec;
    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{100});
    op->process_async(StreamElement<KV>::data(std::move(b)), em, aec);
    aec.poll();
    aec.on_watermark([&] { op->on_watermark(Watermark{EventTime{1000}}, em); });
    aec.drain();
    auto e1 = collect_emits(ch);
    ASSERT_EQ(e1.size(), 1u);
    EXPECT_EQ(e1[0].pane.timing, PaneInfo::Timing::OnTime);

    Batch<KV> late;
    late.emplace(KV{1, 5}, EventTime{200});
    op->process_async(StreamElement<KV>::data(std::move(late)), em, aec);
    aec.poll();
    auto e2 = collect_emits(ch);
    ASSERT_EQ(e2.size(), 1u) << "the late pane must be emitted from the fold coroutine";
    EXPECT_EQ(e2[0].value, (KA{1, 15}));
    EXPECT_EQ(e2[0].pane.timing, PaneInfo::Timing::Late);
    EXPECT_EQ(e2[0].pane.pane_index, 1);
}

TEST(AsyncTumblingWindowOperator, SyncAsyncLatenessParityWithSyncOperator) {
    // The async operator's main output (aggregate + full PaneInfo) must be
    // byte-identical to the synchronous TumblingWindowOperator for the same
    // (record, watermark) script through on-time + within-band-late + purge.
    const auto script = [](auto& op, auto& out, auto send, auto wm) {
        send(op, out, 1, 10, 100);  // [0,1000)
        wm(op, out, 1000);          // on-time fire (1,10)
        send(op, out, 1, 5, 200);   // within band -> late pane (1,15)
        send(op, out, 1, 7, 1100);  // window [1000,2000)
        wm(op, out, 1500);          // purge [0,1000)
        wm(op, out, 2000);          // fire [1000,2000) (1,7)
    };

    // --- async op ---
    std::vector<AtwEmit> async_out;
    {
        auto backend = std::make_shared<InMemoryStateBackend>();
        const OperatorId op_id{13};
        RuntimeContext ctx(op_id, "win_sum", backend.get(), nullptr);
        auto op = make_late_op(500);
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        script(
            *op,
            em,
            [](auto& o, auto& e, std::int64_t k, std::int64_t v, std::int64_t ts) {
                Batch<KV> b;
                b.emplace(KV{k, v}, EventTime{ts});
                o.process(StreamElement<KV>::data(std::move(b)), e);
            },
            [](auto& o, auto& e, std::int64_t ts) {
                o.process(StreamElement<KV>::watermark(Watermark{EventTime{ts}}), e);
            });
        async_out = collect_emits(ch);
    }

    // --- sync reference op (in-memory TumblingWindowOperator) ---
    std::vector<AtwEmit> sync_out;
    {
        TumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t> op(
            1000ms,
            [] { return std::int64_t{0}; },
            [](std::int64_t a, std::int64_t v) { return a + v; });
        op.allowed_lateness(500ms);
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        script(
            op,
            em,
            [](auto& o, auto& e, std::int64_t k, std::int64_t v, std::int64_t ts) {
                Batch<KV> b;
                b.emplace(KV{k, v}, EventTime{ts});
                o.process(StreamElement<KV>::data(std::move(b)), e);
            },
            [](auto& o, auto& e, std::int64_t ts) {
                o.process(StreamElement<KV>::watermark(Watermark{EventTime{ts}}), e);
            });
        sync_out = collect_emits(ch);
    }

    ASSERT_EQ(async_out.size(), sync_out.size());
    EXPECT_EQ(async_out, sync_out) << "async window output must match the sync operator exactly";
}

TEST(AsyncTumblingWindowOperator, OnTimePaneIsLastWhenWatermarkJumpsPastPurge) {
    // lateness>0 but a SINGLE watermark jumps past both window_end (1000) and the
    // purge deadline (1500): the on-time pane is the window's last pane, so
    // is_last must be true (the window purges in the same sweep). Regression for
    // the is_last parity bug the adversarial review found.
    auto run = [](auto& op, auto& out) {
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{100});
        op.process(StreamElement<KV>::data(std::move(b)), out);
        op.process(StreamElement<KV>::watermark(Watermark{EventTime{1600}}), out);  // one jump
    };

    std::vector<AtwEmit> async_out;
    {
        auto backend = std::make_shared<InMemoryStateBackend>();
        const OperatorId op_id{15};
        RuntimeContext ctx(op_id, "win_sum", backend.get(), nullptr);
        auto op = make_late_op(500);
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        run(*op, em);
        async_out = collect_emits(ch);
        // No further emission after the jump (window already purged).
        op->on_watermark(Watermark{EventTime{5000}}, em);
        EXPECT_TRUE(collect_emits(ch).empty());
    }

    ASSERT_EQ(async_out.size(), 1u);
    EXPECT_EQ(async_out[0].value, (KA{1, 10}));
    EXPECT_EQ(async_out[0].pane.timing, PaneInfo::Timing::OnTime);
    EXPECT_TRUE(async_out[0].pane.is_first);
    EXPECT_TRUE(async_out[0].pane.is_last)
        << "a window purged in the same sweep as its on-time fire must mark is_last";

    // The sync operator agrees.
    std::vector<AtwEmit> sync_out;
    {
        TumblingWindowOperator<std::int64_t, std::int64_t, std::int64_t> op(
            1000ms,
            [] { return std::int64_t{0}; },
            [](std::int64_t a, std::int64_t v) { return a + v; });
        op.allowed_lateness(500ms);
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        run(op, em);
        sync_out = collect_emits(ch);
    }
    EXPECT_EQ(async_out, sync_out) << "is_last on a watermark jump must match the sync operator";
}

TEST(AsyncTumblingWindowOperator, LatenessAndPaneIndexSurviveRestore) {
    const OperatorId op_id{14};

    // Phase 1: on-time fire with lateness>0 (fired=true, pane_index bumped, the
    // purge timer pending), then checkpoint.
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "win_sum", backend1.get(), nullptr);
        auto op = make_late_op(500);
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{100});
        op->process(StreamElement<KV>::data(std::move(b)), em);
        op->on_watermark(Watermark{EventTime{1000}}, em);  // on-time pane, window kept
        op->snapshot_timers(*backend1, op_id);
        snap = backend1->snapshot(CheckpointId{1});
        auto e = collect_emits(ch);
        ASSERT_EQ(e.size(), 1u);
        EXPECT_EQ(e[0].pane.pane_index, 0);
    }

    // Phase 2: restore, then a within-band late record continues the pane index
    // monotonically; advancing past the deadline erases with no on-time re-emit.
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "win_sum", backend2.get(), nullptr);
    auto op2 = make_late_op(500);
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);
    op2->open();  // reloads current_watermark_ = 1000

    BoundedChannel<StreamElement<KA>> ch2(64);
    Emitter<KA> em2(&ch2);
    Batch<KV> late;
    late.emplace(KV{1, 5}, EventTime{200});
    op2->process(StreamElement<KV>::data(std::move(late)), em2);
    auto e2 = collect_emits(ch2);
    ASSERT_EQ(e2.size(), 1u) << "restored open window must re-fire a late pane";
    EXPECT_EQ(e2[0].value, (KA{1, 15})) << "restored accumulator continued";
    EXPECT_EQ(e2[0].pane.timing, PaneInfo::Timing::Late);
    EXPECT_EQ(e2[0].pane.pane_index, 1) << "pane_index continues monotonically across restore";

    op2->on_watermark(Watermark{EventTime{1500}}, em2);  // purge
    op2->on_watermark(Watermark{EventTime{3000}}, em2);
    EXPECT_TRUE(collect_emits(ch2).empty()) << "purged window does not re-emit";
}
