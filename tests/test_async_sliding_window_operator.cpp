// AsyncSlidingWindowOperator: event-time sliding (hopping) window aggregate on
// the async-state path. It shares all the async machinery with the tumbling
// operator (epoch-gated firing, per-key-gated fold, durable timers,
// checkpoint/restore) and adds the one thing sliding needs: a record lands in
// SEVERAL overlapping windows, each with its own accumulator + fire timer.
//
// Coverage: sync/async parity with overlapping windows; a single record fans
// into all its windows; epoch-gated firing observes all in-flight reads (a
// deferring backend); and checkpoint/restore re-fires the open windows.

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
#include "clink/operators/async_sliding_window_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
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

// Async-capable, completes get_async inline. Uniquely named (Asw*) to avoid an
// ODR clash with the like-purposed doubles in the other window test TUs.
class AswInlineAsyncBackend : public StateBackend {
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
    std::string description() const override { return "asw-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

// Genuinely defers every get_async on a worker thread, resuming on the runner
// via the wired scheduler - lets a test observe epoch gating (firing waits).
class AswDeferringBackend : public StateBackend {
public:
    AswDeferringBackend() : worker_([this] { loop_(); }) {}
    ~AswDeferringBackend() override {
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
    std::string description() const override { return "asw-deferring"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { resume_ = std::move(s); }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Defer{this, op, std::string(key)};
    }
    std::size_t deferrals() const { return deferrals_.load(std::memory_order_relaxed); }

private:
    struct Defer {
        const AswDeferringBackend* self;
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
                resume_(h);
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

// size=1000, slide=500 -> a record can fall in two overlapping windows.
std::shared_ptr<AsyncSlidingWindowOperator<std::int64_t, std::int64_t, std::int64_t>> make_op() {
    return std::make_shared<AsyncSlidingWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
        /*window_size_ms=*/1000,
        /*slide_ms=*/500,
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        int64_codec(),
        int64_codec(),
        "slide_sum");
}

std::vector<Record<KV>> recs(
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>>& xs) {
    std::vector<Record<KV>> out;
    for (const auto& [k, v, ts] : xs) {
        out.emplace_back(KV{k, v}, EventTime{ts});
    }
    return out;
}

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

std::vector<KA> collect_data(BoundedChannel<StreamElement<KA>>& ch) {
    std::vector<KA> out;
    for (const auto& e : drain_ch(ch)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.push_back(r.value());
            }
        }
    }
    std::sort(out.begin(), out.end());
    return out;
}

// size=1000, slide=500 sliding-sum op with allowed_lateness set.
std::shared_ptr<AsyncSlidingWindowOperator<std::int64_t, std::int64_t, std::int64_t>> make_late_op(
    std::int64_t lateness_ms) {
    auto op =
        std::make_shared<AsyncSlidingWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
            /*window_size_ms=*/1000,
            /*slide_ms=*/500,
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            int64_codec(),
            int64_codec(),
            "slide_sum");
    op->allowed_lateness(std::chrono::milliseconds{lateness_ms});
    return op;
}

template <typename Op>
RuntimeContext attach_late_channel(Op& op,
                                   StateBackend* backend,
                                   BoundedChannel<StreamElement<std::int64_t>>& late_ch,
                                   const std::string& tag) {
    RuntimeContext ctx(OperatorId{1}, "slide_sum", backend, nullptr);
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

std::vector<std::pair<std::int64_t, std::int64_t>> drain_side(
    BoundedChannel<StreamElement<std::int64_t>>& ch) {
    std::vector<std::pair<std::int64_t, std::int64_t>> out;
    for (const auto& e : drain_ch(ch)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.emplace_back(r.value(), r.event_time().value_or(EventTime{0}).millis());
            }
        }
    }
    return out;
}

}  // namespace

TEST(AsyncSlidingWindowOperator, SyncAndAsyncProduceIdenticalOverlappingWindows) {
    // size=1000, slide=500. @200 -> window [0,1000). @700 -> windows [0,1000)
    // AND [500,1500). So [0,1000) = 10+20 = 30, [500,1500) = 20.
    const auto input = recs({{1, 10, 200}, {1, 20, 700}});
    const std::vector<KA> expected = {{1, 20}, {1, 30}};

    const auto sync_out = run_windows(input, std::make_shared<InMemoryStateBackend>());
    const auto async_out = run_windows(input, std::make_shared<AswInlineAsyncBackend>());

    EXPECT_EQ(sync_out, expected) << "synchronous path";
    EXPECT_EQ(async_out, expected) << "async path";
    EXPECT_EQ(sync_out, async_out) << "sync and async must be identical";
}

TEST(AsyncSlidingWindowOperator, OneRecordFansIntoAllOverlappingWindows) {
    // A single record at ts=700 belongs to [0,1000) and [500,1500); both fire
    // at end-of-stream, so the same (key, value) is emitted twice.
    const auto out = run_windows(recs({{1, 5, 700}}), std::make_shared<AswInlineAsyncBackend>());
    const std::vector<KA> expected = {{1, 5}, {1, 5}};
    EXPECT_EQ(out, expected) << "the record must aggregate into both overlapping windows";
}

TEST(AsyncSlidingWindowOperator, EpochGatedFiringObservesAllInFlightReads) {
    // Declare the controller BEFORE the deferring backend so the backend's
    // worker thread is joined (in its dtor) before aec's condition_variable is
    // destroyed; otherwise the worker could signal a being-destroyed cv on
    // scope exit (a TSan data race).
    AsyncExecutionController aec;
    auto backend = std::make_shared<AswDeferringBackend>();
    const OperatorId op_id{1};
    RuntimeContext ctx(op_id, "slide_sum", backend.get(), nullptr);
    auto op = make_op();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });

    BoundedChannel<StreamElement<KA>> ch(256);
    Emitter<KA> em(&ch);

    // Both records land in window [0,1000); @700 also opens [500,1500).
    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{200});
    b.emplace(KV{1, 30}, EventTime{700});
    op->process_async(StreamElement<KV>::data(std::move(b)), em, aec);
    aec.poll();

    // Watermark 1000 closes only [0,1000); its firing waits for both deferred
    // reads, so it sees 10 + 30 = 40. [500,1500) (end 1500) does NOT fire yet.
    aec.on_watermark([&] { op->on_watermark(Watermark{EventTime{1000}}, em); });
    aec.drain();

    EXPECT_GE(backend->deferrals(), 2u);
    std::vector<KA> fired;
    for (const auto& e : drain_ch(ch)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                fired.push_back(r.value());
            }
        }
    }
    ASSERT_EQ(fired.size(), 1u) << "only window [0,1000) is due at watermark 1000";
    EXPECT_EQ(fired[0].first, 1);
    EXPECT_EQ(fired[0].second, 40) << "firing must observe both in-flight records (10 + 30)";
}

TEST(AsyncSlidingWindowOperator, OverlappingWindowsSurviveCheckpointRestore) {
    const OperatorId op_id{2};

    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "slide_sum", backend1.get(), nullptr);
        auto op = make_op();
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        // [0,1000) = 30, [500,1500) = 20; both OPEN (no firing watermark yet).
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{200});
        b.emplace(KV{1, 20}, EventTime{700});
        op->process(StreamElement<KV>::data(std::move(b)), em);
        op->snapshot_timers(*backend1, op_id);
        snap = backend1->snapshot(CheckpointId{1});
        EXPECT_TRUE(drain_ch(ch).empty());
    }

    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "slide_sum", backend2.get(), nullptr);
    auto op2 = make_op();
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);
    op2->open();

    BoundedChannel<StreamElement<KA>> ch2(64);
    Emitter<KA> em2(&ch2);
    op2->on_watermark(Watermark{EventTime{2000}}, em2);  // both windows now due

    std::vector<KA> fired;
    for (const auto& e : drain_ch(ch2)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                fired.push_back(r.value());
            }
        }
    }
    std::sort(fired.begin(), fired.end());
    const std::vector<KA> expected = {{1, 20}, {1, 30}};
    EXPECT_EQ(fired, expected) << "both restored overlapping windows must re-fire with their sums";
}

TEST(AsyncSlidingWindowOperator, LateRecordSideOutputOnlyWhenAllOverlappingWindowsPurged) {
    // size=1000, slide=500, lateness=200. A record at ts=700 lands in [0,1000)
    // (purge deadline 1200) and [500,1500) (purge deadline 1700). The late-late
    // side-output must fire ONLY when EVERY overlapping window is past purge - an
    // in-band window still aggregates the record - and then exactly once.
    auto backend = std::make_shared<InMemoryStateBackend>();
    BoundedChannel<StreamElement<std::int64_t>> late_ch(64);
    const OperatorId op_id{20};
    auto op = make_late_op(200);
    OutputTag<std::int64_t> tag("late");
    op->late_output_tag(tag);
    auto ctx = attach_late_channel(*op, backend.get(), late_ch, "late");
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    // Advance the watermark to 1300: [0,1000) is purged (1200 <= 1300) but
    // [500,1500) is still in band (1700 > 1300).
    op->on_watermark(Watermark{EventTime{1300}}, em);
    (void)collect_data(ch);

    // Record @700: one overlapping window ([500,1500)) is in band, so it is NOT
    // routed - it aggregates into that window.
    Batch<KV> b;
    b.emplace(KV{1, 5}, EventTime{700});
    op->process(StreamElement<KV>::data(std::move(b)), em);
    EXPECT_TRUE(drain_side(late_ch).empty())
        << "an in-band overlapping window must keep the record";

    // Advancing to 1800 fires + purges [500,1500) on time (sum 5).
    op->on_watermark(Watermark{EventTime{1800}}, em);
    EXPECT_EQ(collect_data(ch), (std::vector<KA>{{1, 5}}))
        << "the in-band window fired the record it kept";

    // Now EVERY window for ts=700 is past purge -> route once.
    Batch<KV> latelate;
    latelate.emplace(KV{1, 99}, EventTime{700});
    op->process(StreamElement<KV>::data(std::move(latelate)), em);
    op->on_watermark(Watermark{EventTime{3000}}, em);
    EXPECT_TRUE(collect_data(ch).empty()) << "a fully-purged record must not aggregate";

    const auto side = drain_side(late_ch);
    ASSERT_EQ(side.size(), 1u) << "routed exactly once despite two overlapping windows";
    EXPECT_EQ(side[0].first, 99);
    EXPECT_EQ(side[0].second, 700);
}
