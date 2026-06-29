// AsyncSessionWindowOperator: event-time SESSION windows on the async-state
// path. Unlike tumbling/sliding, a record does not map to fixed window starts -
// it MERGES sessions: a record that bridges the gap between two open sessions
// collapses them into one. The disaggregated state is one KeyedState row per key
// (the whole session vector), read-modify-written under the per-key gate; firing
// rides the framework TimerService (a timer at each session end, checkpointed +
// restored).
//
// Coverage: sync/async output parity including a bridge merge; a record bridging
// two sessions into one; the epoch gate (a deferring backend proves firing
// observes every in-flight merge); the late-drop policy (a pure-late fresh
// session is dropped, a late record overlapping an open session still merges);
// the async tripwire admits the event-time-only operator; and checkpoint/restore
// re-fires restored sessions AND merges a post-restore record into them.

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
#include "clink/operators/async_session_window_operator.hpp"
#include "clink/operators/session_window_operator.hpp"
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

// Async-capable, completes get_async inline. Uniquely named (Asess*) to avoid an
// ODR clash with the like-purposed doubles in the other window test TUs.
class AsessInlineAsyncBackend : public StateBackend {
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
    std::string description() const override { return "asess-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

// Genuinely defers every get_async on a worker thread, resuming on the runner
// via the wired scheduler - lets a test observe epoch gating (firing waits).
class AsessDeferringBackend : public StateBackend {
public:
    AsessDeferringBackend() : worker_([this] { loop_(); }) {}
    ~AsessDeferringBackend() override {
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
    std::string description() const override { return "asess-deferring"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { resume_ = std::move(s); }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Defer{this, op, std::string(key)};
    }
    std::size_t deferrals() const { return deferrals_.load(std::memory_order_relaxed); }

private:
    struct Defer {
        const AsessDeferringBackend* self;
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

// gap=100ms session sum: initial 0, combine a+v, merge a+b.
std::shared_ptr<AsyncSessionWindowOperator<std::int64_t, std::int64_t, std::int64_t>> make_op() {
    return std::make_shared<AsyncSessionWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
        /*gap_ms=*/100,
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
        int64_codec(),
        int64_codec(),
        "sess_sum");
}

std::vector<Record<KV>> recs(
    const std::vector<std::tuple<std::int64_t, std::int64_t, std::int64_t>>& xs) {
    std::vector<Record<KV>> out;
    for (const auto& [k, v, ts] : xs) {
        out.emplace_back(KV{k, v}, EventTime{ts});
    }
    return out;
}

// Run through the real runner; bounded source emits max watermark at EOS, so
// every open session fires. Returns the emitted (key, agg) pairs, sorted.
std::vector<KA> run_sessions(const std::vector<Record<KV>>& input,
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

std::vector<KA> collect(BoundedChannel<StreamElement<KA>>& ch) {
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

// Emitted (value, event-time-ms) in stream order (NOT sorted) - for comparing
// the async operator against the sync SessionWindowOperator step by step.
std::vector<std::pair<KA, std::int64_t>> collect_ordered(BoundedChannel<StreamElement<KA>>& ch) {
    std::vector<std::pair<KA, std::int64_t>> out;
    for (const auto& e : drain_ch(ch)) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                out.emplace_back(r.value(), r.event_time().value_or(EventTime{0}).millis());
            }
        }
    }
    return out;
}

template <typename Op>
RuntimeContext attach_late_channel(Op& op,
                                   StateBackend* backend,
                                   BoundedChannel<StreamElement<std::int64_t>>& late_ch,
                                   const std::string& tag) {
    RuntimeContext ctx(OperatorId{1}, "sess_sum", backend, nullptr);
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

TEST(AsyncSessionWindowOperator, SyncAndAsyncProduceIdenticalSessions) {
    // gap=100. Key 1: @0=10, @150=20, @90=5. @0 and @150 are 50ms apart so they
    // start as separate sessions; @90 (provisional [90,190]) bridges BOTH, so all
    // three collapse into one session [0,250] = 10+20+5 = 35. Key 2: a lone @1000
    // session = 7. The bridge result is order-independent (sum + transitive
    // overlap), so the sync and async record orders must agree.
    const auto input = recs({{1, 10, 0}, {1, 20, 150}, {1, 5, 90}, {2, 7, 1000}});
    const std::vector<KA> expected = {{1, 35}, {2, 7}};

    const auto sync_out = run_sessions(input, std::make_shared<InMemoryStateBackend>());
    const auto async_out = run_sessions(input, std::make_shared<AsessInlineAsyncBackend>());

    EXPECT_EQ(sync_out, expected) << "synchronous process() path";
    EXPECT_EQ(async_out, expected) << "async process_async() path";
    EXPECT_EQ(sync_out, async_out) << "sync and async must be identical";
}

TEST(AsyncSessionWindowOperator, NonOverlappingRecordsStayDistinctSessions) {
    // gap=100. @0 and @500 are far apart -> two sessions, fired separately.
    const auto out =
        run_sessions(recs({{1, 10, 0}, {1, 20, 500}}), std::make_shared<AsessInlineAsyncBackend>());
    const std::vector<KA> expected = {{1, 10}, {1, 20}};
    EXPECT_EQ(out, expected) << "records beyond the gap must not merge";
}

TEST(AsyncSessionWindowOperator, TripwireAdmitsEventTimeOnlySessionUnderAsync) {
    auto op = make_op();
    EXPECT_TRUE(op->supports_async());
    EXPECT_TRUE(op->fires_state_touching_timers());
    EXPECT_FALSE(op->fires_state_touching_processing_time_timers());
    EXPECT_NO_THROW(run_sessions(recs({{1, 1, 10}}), std::make_shared<AsessInlineAsyncBackend>()));
}

TEST(AsyncSessionWindowOperator, EpochGatedFiringObservesAllInFlightMerges) {
    // Declare the controller BEFORE the deferring backend so the backend's
    // worker thread is joined (in its dtor) before aec's condition_variable is
    // destroyed; otherwise the worker could signal a being-destroyed cv on
    // scope exit (a TSan data race).
    AsyncExecutionController aec;
    auto backend = std::make_shared<AsessDeferringBackend>();
    const OperatorId op_id{1};
    RuntimeContext ctx(op_id, "sess_sum", backend.get(), nullptr);
    auto op = make_op();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    backend->set_async_resume_scheduler(
        [&aec](std::coroutine_handle<> h) { aec.schedule_resume(h); });

    BoundedChannel<StreamElement<KA>> ch(256);
    Emitter<KA> em(&ch);

    // Three key-1 records whose reads all DEFER; @90 bridges @0 and @150 into one
    // session [0,250]. Firing must wait for every in-flight merge, so it sees the
    // fully merged 10+20+5 = 35, not a partial.
    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{0});
    b.emplace(KV{1, 20}, EventTime{150});
    b.emplace(KV{1, 5}, EventTime{90});
    op->process_async(StreamElement<KV>::data(std::move(b)), em, aec);
    aec.poll();

    // Watermark 300 > 250 closes the merged session. Run firing inside the AEC's
    // epoch-gated release closure (only after all three reads have drained).
    aec.on_watermark([&] { op->on_watermark(Watermark{EventTime{300}}, em); });
    aec.drain();

    EXPECT_GE(backend->deferrals(), 3u) << "all three reads genuinely deferred";
    const auto fired = collect(ch);
    const std::vector<KA> expected = {{1, 35}};
    EXPECT_EQ(fired, expected) << "firing must observe all in-flight merges (10 + 20 + 5)";
}

TEST(AsyncSessionWindowOperator, PureLateFreshSessionDropped) {
    // A record that would create a FRESH session already entirely behind the
    // watermark is dropped (at-most-once for pure-late data).
    auto backend = std::make_shared<InMemoryStateBackend>();
    const OperatorId op_id{2};
    RuntimeContext ctx(op_id, "sess_sum", backend.get(), nullptr);
    auto op = make_op();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{0});  // session [0,100]
    op->process(StreamElement<KV>::data(std::move(b)), em);
    op->on_watermark(Watermark{EventTime{200}}, em);  // fires (1,10), wm=200

    Batch<KV> late;
    late.emplace(KV{1, 99}, EventTime{50});  // provisional [50,150], end 150 <= 200, no overlap
    op->process(StreamElement<KV>::data(std::move(late)), em);
    op->on_watermark(Watermark{EventTime{1000}}, em);  // nothing new

    const auto fired = collect(ch);
    const std::vector<KA> expected = {{1, 10}};
    EXPECT_EQ(fired, expected) << "the pure-late record must be dropped, not form a late session";
}

TEST(AsyncSessionWindowOperator, LateRecordMergesIntoOpenSession) {
    // A record that arrives behind the watermark but OVERLAPS a still-open session
    // belongs to it and must merge (never dropped). gap=1000 keeps the session
    // open well past the watermark.
    auto backend = std::make_shared<InMemoryStateBackend>();
    const OperatorId op_id{3};
    RuntimeContext ctx(op_id, "sess_sum", backend.get(), nullptr);
    auto op =
        std::make_shared<AsyncSessionWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
            /*gap_ms=*/1000,
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
            int64_codec(),
            int64_codec(),
            "sess_sum");
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{0});  // session [0,1000], still open at wm 500
    op->process(StreamElement<KV>::data(std::move(b)), em);
    op->on_watermark(Watermark{EventTime{500}}, em);  // session end 1000 > 500, nothing fires

    Batch<KV> late;
    late.emplace(KV{1, 5}, EventTime{200});  // ts 200 < wm 500 but overlaps [0,1000]
    op->process(StreamElement<KV>::data(std::move(late)), em);
    op->on_watermark(Watermark{EventTime{3000}}, em);  // fires merged session

    const auto fired = collect(ch);
    const std::vector<KA> expected = {{1, 15}};
    EXPECT_EQ(fired, expected) << "a late record overlapping an open session must merge (10 + 5)";
}

TEST(AsyncSessionWindowOperator, MergeExtendedEndStaleTimerDoesNotDoubleEmit) {
    // A merge that pushes a session end forward leaves a STALE timer at the old
    // end. When both timers come due at the same watermark, the merged session
    // must fire EXACTLY ONCE: the first timer emits + purges it (it lands in the
    // emit batch, not the survivor set), so the stale timer re-reads a row that no
    // longer holds it. A second, not-yet-due session for the same key must survive
    // the first fire and fire later. This is the scenario adversarial review
    // flagged as a double-emit risk; it is safe because firing purges, not flags.
    auto backend = std::make_shared<InMemoryStateBackend>();
    const OperatorId op_id{6};
    RuntimeContext ctx(op_id, "sess_sum", backend.get(), nullptr);
    auto op = make_op();
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{0});    // session [0,100], timer @100
    b.emplace(KV{1, 20}, EventTime{60});   // merges -> [0,160], timer @160 (stale @100 stays)
    b.emplace(KV{1, 50}, EventTime{500});  // distinct session [500,600], timer @600
    op->process(StreamElement<KV>::data(std::move(b)), em);
    EXPECT_TRUE(collect(ch).empty()) << "nothing fires before a watermark";

    // Watermark 160: timers @100 (stale) and @160 are both due; [500,600] is not.
    op->on_watermark(Watermark{EventTime{160}}, em);
    EXPECT_EQ(collect(ch), (std::vector<KA>{{1, 30}}))
        << "the merged session must fire exactly once despite the stale timer";

    // Watermark 1000: the surviving session [500,600] now fires.
    op->on_watermark(Watermark{EventTime{1000}}, em);
    EXPECT_EQ(collect(ch), (std::vector<KA>{{1, 50}}))
        << "the non-due session must survive the first fire and fire later";
}

TEST(AsyncSessionWindowOperator, SessionsSurviveCheckpointRestore) {
    const OperatorId op_id{4};

    // Two distinct OPEN sessions for key 1 (no firing watermark), then
    // checkpoint both the timers and the per-key session rows.
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "sess_sum", backend1.get(), nullptr);
        auto op = make_op();
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{0});    // session [0,100]
        b.emplace(KV{1, 20}, EventTime{500});  // session [500,600]
        op->process(StreamElement<KV>::data(std::move(b)), em);
        op->snapshot_timers(*backend1, op_id);
        snap = backend1->snapshot(CheckpointId{1});
        EXPECT_TRUE(collect(ch).empty()) << "nothing fires before a watermark";
    }

    // A fresh operator + backend restores, then a watermark past both ends
    // re-fires the two restored sessions with their sums.
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "sess_sum", backend2.get(), nullptr);
    auto op2 = make_op();
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);
    op2->open();

    BoundedChannel<StreamElement<KA>> ch2(64);
    Emitter<KA> em2(&ch2);
    op2->on_watermark(Watermark{EventTime{1000}}, em2);

    const auto fired = collect(ch2);
    const std::vector<KA> expected = {{1, 10}, {1, 20}};
    EXPECT_EQ(fired, expected) << "both restored sessions must re-fire with their sums";
}

TEST(AsyncSessionWindowOperator, MergeAfterRestoreBridgesRestoredSessions) {
    const OperatorId op_id{5};

    // Two distinct OPEN sessions [0,100] and [150,250], checkpoint.
    auto backend1 = std::make_shared<InMemoryStateBackend>();
    Snapshot snap;
    {
        RuntimeContext ctx(op_id, "sess_sum", backend1.get(), nullptr);
        auto op = make_op();
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        Batch<KV> b;
        b.emplace(KV{1, 10}, EventTime{0});
        b.emplace(KV{1, 20}, EventTime{150});
        op->process(StreamElement<KV>::data(std::move(b)), em);
        op->snapshot_timers(*backend1, op_id);
        snap = backend1->snapshot(CheckpointId{1});
    }

    // Restore, then a record at @90 (provisional [90,190]) bridges the two
    // restored sessions into one [0,250]; a watermark past 250 fires it once = 35.
    auto backend2 = std::make_shared<InMemoryStateBackend>();
    backend2->restore(snap);
    RuntimeContext ctx2(op_id, "sess_sum", backend2.get(), nullptr);
    auto op2 = make_op();
    op2->set_id(op_id);
    op2->attach_runtime(&ctx2);
    op2->restore_timers(*backend2, op_id);
    op2->open();

    BoundedChannel<StreamElement<KA>> ch2(64);
    Emitter<KA> em2(&ch2);
    Batch<KV> bridge;
    bridge.emplace(KV{1, 5}, EventTime{90});
    op2->process(StreamElement<KV>::data(std::move(bridge)), em2);
    op2->on_watermark(Watermark{EventTime{300}}, em2);

    const auto fired = collect(ch2);
    const std::vector<KA> expected = {{1, 35}};
    EXPECT_EQ(fired, expected) << "post-restore record must bridge the two restored sessions (35)";
}

// ---- allowed-lateness + late panes + late-output-tag ----------------------

// gap=100, configurable lateness session-sum op.
std::shared_ptr<AsyncSessionWindowOperator<std::int64_t, std::int64_t, std::int64_t>>
make_late_session_op(std::int64_t lateness_ms) {
    auto op =
        std::make_shared<AsyncSessionWindowOperator<std::int64_t, std::int64_t, std::int64_t>>(
            /*gap_ms=*/100,
            [] { return std::int64_t{0}; },
            [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
            [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
            int64_codec(),
            int64_codec(),
            "sess_sum");
    op->allowed_lateness(std::chrono::milliseconds{lateness_ms});
    return op;
}

TEST(AsyncSessionWindowOperator, LateRecordMergesIntoFiredSessionAndReEmits) {
    // gap=100, lateness=200. A session fires on time; a within-band late record
    // overlapping it merges and re-emits the updated aggregate immediately, then
    // the session purges at session_end + lateness with no further emission.
    auto backend = std::make_shared<InMemoryStateBackend>();
    const OperatorId op_id{10};
    RuntimeContext ctx(op_id, "sess_sum", backend.get(), nullptr);
    auto op = make_late_session_op(200);
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{0});  // session [0,100]
    op->process(StreamElement<KV>::data(std::move(b)), em);
    op->on_watermark(Watermark{EventTime{150}}, em);  // fires (1,10); kept (100+200>150)
    EXPECT_EQ(collect(ch), (std::vector<KA>{{1, 10}}));

    // Within-band late record @50 (prov [50,150]) overlaps the fired session;
    // merges to [0,150] and re-emits the updated aggregate immediately.
    Batch<KV> late;
    late.emplace(KV{1, 5}, EventTime{50});
    op->process(StreamElement<KV>::data(std::move(late)), em);
    EXPECT_EQ(collect(ch), (std::vector<KA>{{1, 15}}))
        << "a late record merging into a fired session must re-emit at once";

    // Past the lateness deadline (max end 150 + 200 = 350): purge, no re-emit.
    op->on_watermark(Watermark{EventTime{400}}, em);
    EXPECT_TRUE(collect(ch).empty()) << "an already-fired session purges silently";
}

TEST(AsyncSessionWindowOperator, SessionLatenessParityWithSyncOperator) {
    // The async session operator's main output must match the synchronous
    // SessionWindowOperator for the same script through on-time fire + a
    // within-band late merge-and-re-emit.
    const auto drive = [](auto& op, auto& out) {
        const auto send = [&](std::int64_t v, std::int64_t ts) {
            Batch<KV> b;
            b.emplace(KV{1, v}, EventTime{ts});
            op.process(StreamElement<KV>::data(std::move(b)), out);
        };
        const auto wm = [&](std::int64_t ts) {
            op.process(StreamElement<KV>::watermark(Watermark{EventTime{ts}}), out);
        };
        send(10, 0);  // session [0,100]
        wm(150);      // on-time fire (1,10)
        send(5, 50);  // within band -> merge + re-emit (1,15)
        wm(400);      // purge, nothing
    };

    std::vector<std::pair<KA, std::int64_t>> async_out;
    {
        auto backend = std::make_shared<InMemoryStateBackend>();
        const OperatorId op_id{11};
        RuntimeContext ctx(op_id, "sess_sum", backend.get(), nullptr);
        auto op = make_late_session_op(200);
        op->set_id(op_id);
        op->attach_runtime(&ctx);
        op->open();
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        drive(*op, em);
        async_out = collect_ordered(ch);
    }

    std::vector<std::pair<KA, std::int64_t>> sync_out;
    {
        SessionWindowOperator<std::int64_t, std::int64_t, std::int64_t> op(
            100ms,
            [] { return std::int64_t{0}; },
            [](std::int64_t a, std::int64_t v) { return a + v; },
            [](std::int64_t a, std::int64_t b) { return a + b; });
        op.allowed_lateness(200ms);
        BoundedChannel<StreamElement<KA>> ch(64);
        Emitter<KA> em(&ch);
        drive(op, em);
        sync_out = collect_ordered(ch);
    }

    EXPECT_EQ(async_out, sync_out) << "async session output must match the sync operator exactly";
}

TEST(AsyncSessionWindowOperator, LateLateRecordRoutedToSideOutput) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    BoundedChannel<StreamElement<std::int64_t>> late_ch(64);
    const OperatorId op_id{12};
    auto op = make_late_session_op(200);
    OutputTag<std::int64_t> tag("late");
    op->late_output_tag(tag);
    auto ctx = attach_late_channel(*op, backend.get(), late_ch, "late");
    op->set_id(op_id);
    op->attach_runtime(&ctx);
    op->open();

    BoundedChannel<StreamElement<KA>> ch(64);
    Emitter<KA> em(&ch);

    Batch<KV> b;
    b.emplace(KV{1, 10}, EventTime{0});
    op->process(StreamElement<KV>::data(std::move(b)), em);
    op->on_watermark(Watermark{EventTime{1000}}, em);  // fires + purges [0,100]
    (void)collect(ch);

    // ts + gap + lateness = 50 + 100 + 200 = 350 <= watermark 1000 -> late-late.
    Batch<KV> latelate;
    latelate.emplace(KV{1, 99}, EventTime{50});
    op->process(StreamElement<KV>::data(std::move(latelate)), em);
    op->on_watermark(Watermark{EventTime{3000}}, em);
    EXPECT_TRUE(collect(ch).empty()) << "a past-deadline record must not start a fresh session";

    const auto side = drain_side(late_ch);
    ASSERT_EQ(side.size(), 1u);
    EXPECT_EQ(side[0].first, 99);
    EXPECT_EQ(side[0].second, 50) << "raw value + original event_time preserved";
}
