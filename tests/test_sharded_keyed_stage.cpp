// ShardedKeyedStage: single-writer, share-nothing execution of one keyed
// operator across S shard threads. The load-bearing claims are: records route
// by key group so a given key always reaches the same worker (counting stays
// correct - a mis-route would split a key across two private backends and the
// merged snapshot would lose count), the merged snapshot is byte-compatible
// with the mono backend both ways, restore narrows per shard by key-group
// range, and pinning never changes results.

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/core/codec.hpp"
#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/operator_metrics.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/sharded_checkpoint_store.hpp"
#include "clink/runtime/sharded_keyed_stage.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

namespace {

using namespace clink;

// Per-key running counter, keyed on the int64 record value. Mirrors the
// canonical CountingOp used elsewhere: get -> +1 -> put, emit the count.
class CountingOp final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            const auto c = state_->get(k).value_or(0) + 1;
            state_->put(k, c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }
    std::string name() const override { return "counter"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

ShardedKeyedStage<std::int64_t, std::int64_t>::OperatorFactory counting_factory() {
    return [](std::size_t /*shard*/) { return std::make_unique<CountingOp>(); };
}

// ASYNC-7: the async twin of CountingOp. Opts into the per-shard
// AsyncExecutionController and submits one coroutine per record that
// co_awaits KeyedState::get_async (inline over the shard's InMemory backend
// today). The per-key gate serialises same-key records within a shard.
class CountingOpAsync final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    // Required (pure-virtual) sync path; unused when the runner takes the
    // async branch, but keeps the operator concrete and correct either way.
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            const auto c = state_->get(k).value_or(0) + 1;
            state_->put(k, c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    void process_async(const StreamElement<std::int64_t>& el,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            auto ks = *state_;  // cheap copy of the view; outlives the Task in the closure
            aec.submit(std::to_string(k), [ks, k, &out]() mutable -> async::Task<void> {
                auto cur = co_await ks.get_async(k);
                const auto c = cur.value_or(0) + 1;
                ks.put(k, c);
                Batch<std::int64_t> b;
                b.emplace(c);
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    std::string name() const override { return "counter_async"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

ShardedKeyedStage<std::int64_t, std::int64_t>::OperatorFactory async_counting_factory() {
    return [](std::size_t /*shard*/) { return std::make_unique<CountingOpAsync>(); };
}

// key_bytes_of MUST produce the same bytes KeyedState hashes for the key group
// (key_codec_.encode(k)), so routing and state ownership agree.
KeyBytesOf<std::int64_t> int64_key_bytes() {
    return [kc = int64_codec()](const std::int64_t& v) { return kc.encode(v); };
}

// Read a key's count back from a backend via a KeyedState view (op 7, slot
// "counts"), matching what CountingOp wrote.
std::int64_t count_in(StateBackend& backend, std::int64_t key) {
    KeyedState<std::int64_t, std::int64_t> ks(
        backend, OperatorId{7}, "counts", int64_codec(), int64_codec());
    return ks.get(key).value_or(0);
}

// Drive `keys` each repeated `reps` times through a stage of `shards` workers,
// then return the merged snapshot restored into a fresh mono backend so we can
// read per-key counts. emits_out reports how many output records the downstream
// observed.
Snapshot run_counts(std::size_t shards,
                    const std::vector<std::int64_t>& keys,
                    int reps,
                    bool pin,
                    std::atomic<std::int64_t>& emits_out) {
    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.pin_threads = pin;
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        shards,
        OperatorId{7},
        counting_factory(),
        int64_key_bytes(),
        [&emits_out](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits_out.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                    std::memory_order_relaxed);
            }
            return true;
        },
        opts);
    stage.start();
    for (int r = 0; r < reps; ++r) {
        Batch<std::int64_t> b;
        for (const auto k : keys) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    stage.close_input();
    stage.await();
    return stage.snapshot(CheckpointId{1});
}

TEST(ShardedKeyedStage, RoutesAndCountsPerKey) {
    std::vector<std::int64_t> keys;
    for (std::int64_t k = 0; k < 200; ++k) {
        keys.push_back(k);
    }
    std::atomic<std::int64_t> emits{0};
    auto snap = run_counts(8, keys, 50, /*pin=*/false, emits);

    // Every input record produced one output.
    EXPECT_EQ(emits.load(), 200 * 50);

    // Restore the merged snapshot into a mono backend and check each key hit
    // exactly 50. A mis-route would split a key across shards and lose count.
    InMemoryStateBackend mono;
    mono.restore(snap);
    for (std::int64_t k = 0; k < 200; ++k) {
        EXPECT_EQ(count_in(mono, k), 50) << "key " << k << " mis-counted (routing/merge)";
    }
}

TEST(ShardedKeyedStage, SingleShardMatchesPlainKeyed) {
    std::vector<std::int64_t> keys{1, 2, 3, 4, 5};
    std::atomic<std::int64_t> emits{0};
    auto snap = run_counts(1, keys, 10, /*pin=*/false, emits);
    EXPECT_EQ(emits.load(), 5 * 10);
    InMemoryStateBackend mono;
    mono.restore(snap);
    for (const auto k : keys) {
        EXPECT_EQ(count_in(mono, k), 10);
    }
}

TEST(ShardedKeyedStage, PinnedRunMatchesUnpinned) {
    std::vector<std::int64_t> keys;
    for (std::int64_t k = 0; k < 64; ++k) {
        keys.push_back(k);
    }
    std::atomic<std::int64_t> e_pin{0};
    std::atomic<std::int64_t> e_nopin{0};
    auto snap_pin = run_counts(4, keys, 20, /*pin=*/true, e_pin);
    auto snap_nopin = run_counts(4, keys, 20, /*pin=*/false, e_nopin);
    EXPECT_EQ(e_pin.load(), e_nopin.load());

    InMemoryStateBackend a;
    a.restore(snap_pin);
    InMemoryStateBackend b;
    b.restore(snap_nopin);
    for (std::int64_t k = 0; k < 64; ++k) {
        EXPECT_EQ(count_in(a, k), 20);
        EXPECT_EQ(count_in(b, k), 20);
    }
}

// Construction-path symmetry: a snapshot taken by a MONO backend must restore
// into the sharded stage and the counts continue from there.
TEST(ShardedKeyedStage, RestoresFromMonoSnapshotAndContinues) {
    // Seed a mono backend: keys 0..49 each at count 5.
    InMemoryStateBackend seed;
    {
        KeyedState<std::int64_t, std::int64_t> ks(
            seed, OperatorId{7}, "counts", int64_codec(), int64_codec());
        for (std::int64_t k = 0; k < 50; ++k) {
            ks.put(k, 5);
        }
    }
    const Snapshot seed_snap = seed.snapshot(CheckpointId{1});

    // Restore into the stage BEFORE start(), feed each key once more, expect 6.
    std::atomic<std::int64_t> emits{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        8,
        OperatorId{7},
        counting_factory(),
        int64_key_bytes(),
        [&emits](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                std::memory_order_relaxed);
            }
            return true;
        });
    stage.restore(seed_snap);
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k = 0; k < 50; ++k) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.close_input();
    stage.await();

    InMemoryStateBackend mono;
    mono.restore(stage.snapshot(CheckpointId{2}));
    for (std::int64_t k = 0; k < 50; ++k) {
        EXPECT_EQ(count_in(mono, k), 6) << "key " << k << " did not resume from the mono snapshot";
    }
    EXPECT_EQ(emits.load(), 50);
}

// An empty stage (no records submitted) must snapshot to a valid blob that
// restores cleanly into a fresh stage and a mono backend.
TEST(ShardedKeyedStage, SnapshotAndRestoreEmptyStage) {
    std::atomic<std::int64_t> emits{0};
    auto snap = run_counts(4, /*keys=*/{}, /*reps=*/0, /*pin=*/false, emits);
    EXPECT_EQ(emits.load(), 0);

    InMemoryStateBackend mono;
    EXPECT_NO_THROW(mono.restore(snap));
    EXPECT_EQ(count_in(mono, 0), 0);

    // And the empty blob restores into a fresh stage without error.
    std::atomic<std::int64_t> e2{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        4,
        OperatorId{7},
        counting_factory(),
        int64_key_bytes(),
        [&e2](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                e2.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                             std::memory_order_relaxed);
            }
            return true;
        });
    EXPECT_NO_THROW(stage.restore(snap));
}

// Cross-shard min-merge: advance_watermark coordinates across all shards and
// forwards exactly ONE watermark downstream (not S copies), only after every
// shard has drained its data up to the watermark.
TEST(ShardedKeyedStage, AdvanceWatermarkEmitsOneDownstream) {
    constexpr std::size_t kShards = 4;
    std::atomic<std::int64_t> watermarks{0};
    std::atomic<std::int64_t> data_emits{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        OperatorId{7},
        counting_factory(),
        int64_key_bytes(),
        [&](StreamElement<std::int64_t> e) {
            if (e.is_watermark()) {
                watermarks.fetch_add(1, std::memory_order_relaxed);
            } else if (e.is_data()) {
                data_emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                     std::memory_order_relaxed);
            }
            return true;
        });
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k = 0; k < 40; ++k) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.advance_watermark(Watermark{EventTime{1000}});
    stage.advance_watermark(Watermark{EventTime{2000}});
    stage.close_input();
    stage.await();
    EXPECT_EQ(watermarks.load(), 2) << "each advance_watermark forwards exactly one watermark";
    EXPECT_EQ(data_emits.load(), 40) << "the pre-watermark data still reached downstream";
    EXPECT_TRUE(stage.worker_errors().empty());
}

// A coordinated mid-stream checkpoint returns a merged snapshot reflecting the
// state at the barrier point, and forwards the barrier downstream exactly ONCE
// (not S times). Two checkpoints prove the epoch boundaries are clean.
TEST(ShardedKeyedStage, MidStreamCheckpointIsConsistentAndForwardsOnce) {
    constexpr std::size_t kShards = 4;
    std::atomic<std::int64_t> barriers{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(kShards,
                                                        OperatorId{7},
                                                        counting_factory(),
                                                        int64_key_bytes(),
                                                        [&](StreamElement<std::int64_t> e) {
                                                            if (e.is_barrier()) {
                                                                barriers.fetch_add(
                                                                    1, std::memory_order_relaxed);
                                                            }
                                                            return true;
                                                        });
    stage.start();

    // Epoch 1: keys 0..99 once each.
    {
        Batch<std::int64_t> b;
        for (std::int64_t k = 0; k < 100; ++k) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    auto r1 = stage.checkpoint(
        CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
    ASSERT_TRUE(r1.ok);
    EXPECT_EQ(r1.id.value(), 1u);
    {
        InMemoryStateBackend mono;
        mono.restore(r1.snapshot);
        for (std::int64_t k = 0; k < 100; ++k) {
            EXPECT_EQ(count_in(mono, k), 1) << "ckpt1 key " << k;
        }
    }

    // Epoch 2: keys 0..99 once more -> counts become 2.
    {
        Batch<std::int64_t> b;
        for (std::int64_t k = 0; k < 100; ++k) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    auto r2 = stage.checkpoint(
        CheckpointBarrier{CheckpointId{2}, false, CheckpointBarrier::Mode::Aligned});
    ASSERT_TRUE(r2.ok);
    {
        InMemoryStateBackend mono;
        mono.restore(r2.snapshot);
        for (std::int64_t k = 0; k < 100; ++k) {
            EXPECT_EQ(count_in(mono, k), 2) << "ckpt2 key " << k;
        }
    }

    stage.close_input();
    stage.await();
    EXPECT_EQ(barriers.load(), 2) << "each checkpoint forwards the barrier downstream exactly once";
    EXPECT_TRUE(stage.worker_errors().empty());
}

TEST(ShardedKeyedStage, CheckpointBeforeStartThrows) {
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        2, OperatorId{7}, counting_factory(), int64_key_bytes(), [](StreamElement<std::int64_t>) {
            return true;
        });
    EXPECT_THROW(stage.checkpoint(
                     CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned}),
                 std::logic_error);
}

// ASYNC-7: an async-opting operator routes through the per-shard controller
// and still counts each key correctly across S shards (a broken per-key gate
// or a lost update would mis-count; a mis-route would split a key's count
// across two private backends and the merged snapshot would lose it).
TEST(ShardedKeyedStage, AsyncOperatorRoutesAndCountsPerKey) {
    std::vector<std::int64_t> keys;
    for (std::int64_t k = 0; k < 200; ++k) {
        keys.push_back(k);
    }
    std::atomic<std::int64_t> emits{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        8,
        OperatorId{7},
        async_counting_factory(),
        int64_key_bytes(),
        [&emits](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                std::memory_order_relaxed);
            }
            return true;
        });
    stage.start();
    for (int r = 0; r < 50; ++r) {
        Batch<std::int64_t> b;
        for (const auto k : keys) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    stage.close_input();
    stage.await();

    EXPECT_EQ(emits.load(), 200 * 50);
    EXPECT_TRUE(stage.worker_errors().empty());

    InMemoryStateBackend mono;
    mono.restore(stage.snapshot(CheckpointId{1}));
    for (std::int64_t k = 0; k < 200; ++k) {
        EXPECT_EQ(count_in(mono, k), 50) << "async key " << k << " mis-counted";
    }
}

// ASYNC-7: an in-band checkpoint of a RUNNING async stage drains each shard's
// in-flight async work before capture, so the merged snapshot is a consistent
// cut - every pre-barrier record's write is present, none torn.
TEST(ShardedKeyedStage, AsyncMidStreamCheckpointIsConsistent) {
    constexpr std::size_t kShards = 4;
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        OperatorId{7},
        async_counting_factory(),
        int64_key_bytes(),
        [](StreamElement<std::int64_t>) { return true; });
    stage.start();

    const std::vector<std::int64_t> keys{1, 2, 3, 4, 5};
    for (int r = 0; r < 10; ++r) {
        Batch<std::int64_t> b;
        for (const auto k : keys) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    auto r1 = stage.checkpoint(
        CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
    ASSERT_TRUE(r1.ok);
    {
        InMemoryStateBackend mono;
        mono.restore(r1.snapshot);
        for (const auto k : keys) {
            EXPECT_EQ(count_in(mono, k), 10) << "async ckpt key " << k << " torn";
        }
    }
    stage.close_input();
    stage.await();
    EXPECT_TRUE(stage.worker_errors().empty());
}

// A genuinely-DEFERRING per-shard backend double: get_async parks the handle
// and a per-instance background releaser thread resumes it shortly after (via
// the shard's wired scheduler), so reads actually suspend and the stage's
// blocking public API drives end to end. Single-writer per shard (the stage
// gives each shard its own instance); put/get/snapshot delegate to an inner
// InMemory store so the merge wire format is unchanged. resume_ + pending_ are
// mutex-guarded because the releaser thread and the shard worker both touch
// them.
class ShardDeferringBackend final : public StateBackend {
public:
    ShardDeferringBackend() : releaser_([this] { release_loop_(); }) {}
    ~ShardDeferringBackend() override {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_.notify_all();
        releaser_.join();
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
    std::string description() const override { return "shard-deferring"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override {
        std::lock_guard<std::mutex> lk(mu_);
        resume_ = std::move(s);
    }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Defer{this, op, std::string(key)};
    }

private:
    struct Defer {
        const ShardDeferringBackend* self;
        OperatorId op;
        std::string key;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const {
            std::lock_guard<std::mutex> lk(self->mu_);
            self->pending_.push_back(h);
            self->cv_.notify_all();
        }
        std::optional<Value> await_resume() const { return self->get(op, key); }
    };
    void release_loop_() {
        for (;;) {
            std::vector<std::coroutine_handle<>> batch;
            AsyncResumeScheduler resume;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_.wait(lk, [this] { return stop_ || !pending_.empty(); });
                if (stop_ && pending_.empty()) {
                    return;
                }
                batch.swap(pending_);
                resume = resume_;
            }
            // A tiny pause so the read genuinely overlaps other work before the
            // worker resumes it (foreign-thread post, runner-thread resume).
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            for (auto h : batch) {
                if (resume) {
                    resume(h);  // hand back to the shard's controller (runner resumes)
                }
            }
        }
    }
    InMemoryStateBackend store_;
    AsyncResumeScheduler resume_;
    mutable std::mutex mu_;
    mutable std::condition_variable cv_;
    mutable std::vector<std::coroutine_handle<>> pending_;
    bool stop_{false};
    std::thread releaser_;
};

ShardedKeyedStage<std::int64_t, std::int64_t>::ShardBackendFactory deferring_backend() {
    return [](std::size_t) -> std::unique_ptr<StateBackend> {
        return std::make_unique<ShardDeferringBackend>();
    };
}

// The deferring backend makes the per-shard reads genuinely suspend, so the
// whole async path (per-key gate + drain) runs for real, not inline. Counts
// must still be exact.
TEST(ShardedKeyedStage, AsyncDeferringBackendCountsCorrectly) {
    std::vector<std::int64_t> keys;
    for (std::int64_t k = 0; k < 200; ++k) {
        keys.push_back(k);
    }
    std::atomic<std::int64_t> emits{0};
    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.shard_backend_factory = deferring_backend();
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        8,
        OperatorId{7},
        async_counting_factory(),
        int64_key_bytes(),
        [&emits](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                std::memory_order_relaxed);
            }
            return true;
        },
        opts);
    stage.start();
    for (int r = 0; r < 50; ++r) {
        Batch<std::int64_t> b;
        for (const auto k : keys) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    stage.close_input();
    stage.await();

    EXPECT_EQ(emits.load(), 200 * 50);
    EXPECT_TRUE(stage.worker_errors().empty());
    InMemoryStateBackend mono;
    mono.restore(stage.snapshot(CheckpointId{1}));
    for (std::int64_t k = 0; k < 200; ++k) {
        EXPECT_EQ(count_in(mono, k), 50) << "deferring async key " << k << " mis-counted";
    }
}

// drain_for_barrier must quiesce a genuinely-suspending backend before capture.
TEST(ShardedKeyedStage, AsyncDeferringCheckpointConsistent) {
    constexpr std::size_t kShards = 4;
    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.shard_backend_factory = deferring_backend();
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        OperatorId{7},
        async_counting_factory(),
        int64_key_bytes(),
        [](StreamElement<std::int64_t>) { return true; },
        opts);
    stage.start();
    const std::vector<std::int64_t> keys{1, 2, 3, 4, 5};
    for (int r = 0; r < 10; ++r) {
        Batch<std::int64_t> b;
        for (const auto k : keys) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    auto r1 = stage.checkpoint(
        CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
    ASSERT_TRUE(r1.ok);
    {
        InMemoryStateBackend mono;
        mono.restore(r1.snapshot);
        for (const auto k : keys) {
            EXPECT_EQ(count_in(mono, k), 10) << "deferring ckpt key " << k << " torn";
        }
    }
    stage.close_input();
    stage.await();
    EXPECT_TRUE(stage.worker_errors().empty());
}

// Async op that fires a state-touching PROCESSING-TIME timer - previously
// REFUSED by the sharded stage's tripwire. process_async registers a timer with
// the gate key (== the record gate); on_timer emits a sentinel. Proves the
// stage now admits it (no worker error), the deadline pop fires the timer
// (sentinels reach downstream), and the per-key data counts stay correct (the
// gate kept the timer from corrupting an in-flight same-key read).
class CountingOpAsyncProcTimer final : public Operator<std::int64_t, std::int64_t> {
public:
    static constexpr std::int64_t kSentinel = 1'000'000;
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            const auto c = state_->get(k).value_or(0) + 1;
            state_->put(k, c);
            this->runtime()->timer_service()->register_processing_time_timer(0, std::to_string(k));
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    void process_async(const StreamElement<std::int64_t>& el,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            auto ks = *state_;
            const std::string gate = std::to_string(k);
            auto* rt = this->runtime();
            aec.submit(gate, [ks, k, gate, rt, &out]() mutable -> async::Task<void> {
                auto cur = co_await ks.get_async(k);
                const auto c = cur.value_or(0) + 1;
                ks.put(k, c);
                rt->timer_service()->register_processing_time_timer(0, gate);
                Batch<std::int64_t> b;
                b.emplace(c);
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    void on_processing_time_timer(std::int64_t /*ts*/,
                                  const std::string& /*key*/,
                                  Emitter<std::int64_t>& out) override {
        Batch<std::int64_t> b;
        b.emplace(kSentinel);  // sentinel proves the timer fired (no state mutation)
        out.emit_data(std::move(b));
    }
    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return true; }
    [[nodiscard]] bool fires_state_touching_processing_time_timers() const noexcept override {
        return true;
    }
    std::string name() const override { return "counter_async_proctimer"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

TEST(ShardedKeyedStage, ProctimeTimerAdmittedAndGated) {
    std::vector<std::int64_t> keys{1, 2, 3, 4, 5, 6, 7, 8};
    std::atomic<std::int64_t> data_emits{0};
    std::atomic<std::int64_t> sentinels{0};
    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.shard_backend_factory = deferring_backend();
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        4,
        OperatorId{7},
        [](std::size_t) { return std::make_unique<CountingOpAsyncProcTimer>(); },
        int64_key_bytes(),
        [&](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    if (r.value() == CountingOpAsyncProcTimer::kSentinel) {
                        sentinels.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        data_emits.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
            return true;
        },
        opts);
    stage.start();  // must NOT throw (the tripwire is gone)
    for (int r = 0; r < 20; ++r) {
        Batch<std::int64_t> b;
        for (const auto k : keys) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
    }
    stage.close_input();
    stage.await();

    EXPECT_TRUE(stage.worker_errors().empty()) << "the proctime-timer op must be admitted";
    // The per-key data counts (== 20 below) are the real GATING proof: an
    // ungated timer racing an in-flight same-key read would corrupt the running
    // count. The sentinel count proves the timer FIRED through the deadline pop,
    // but is only asserted >= 1 (not one-per-record): timers dedupe by (ts, key)
    // and are registered asynchronously from the resumed coroutine, so the
    // number of fires is timing-dependent.
    EXPECT_EQ(data_emits.load(), 8 * 20) << "data records counted exactly once";
    EXPECT_GE(sentinels.load(), 1) << "the processing-time timer fired through the deadline pop";
    InMemoryStateBackend mono;
    mono.restore(stage.snapshot(CheckpointId{1}));
    for (const auto k : keys) {
        EXPECT_EQ(count_in(mono, k), 20)
            << "key " << k << " mis-counted (timer corrupted gated state?)";
    }
}

// An async operator whose coroutine throws must fault the worker cleanly
// (record the error, close its queue) instead of leaking the record and
// hanging the next drain/EOS - the async analogue of the ThrowingOnKeyOp
// worker-death test, and the integration-level proof of the wrap_ fix.
class ThrowingOnKeyOpAsync final : public Operator<std::int64_t, std::int64_t> {
public:
    explicit ThrowingOnKeyOpAsync(std::int64_t bad_key) : bad_key_(bad_key) {}
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    void process_async(const StreamElement<std::int64_t>& el,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            auto ks = *state_;
            const auto bad = bad_key_;
            aec.submit(std::to_string(k), [ks, k, bad, &out]() mutable -> async::Task<void> {
                if (k == bad) {
                    throw std::runtime_error("boom on bad key (async)");
                }
                auto cur = co_await ks.get_async(k);
                const auto c = cur.value_or(0) + 1;
                ks.put(k, c);
                Batch<std::int64_t> b;
                b.emplace(c);
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    std::string name() const override { return "throwing_async"; }

private:
    std::int64_t bad_key_;
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

TEST(ShardedKeyedStage, AsyncWorkerThrowDoesNotHang) {
    constexpr std::size_t kShards = 4;
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        OperatorId{7},
        [](std::size_t) { return std::make_unique<ThrowingOnKeyOpAsync>(3); },
        int64_key_bytes(),
        [](StreamElement<std::int64_t>) { return true; });
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k : {1, 2, 3, 4, 5}) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.close_input();
    stage.await();  // must NOT hang: the throwing shard faults and closes its queue
    EXPECT_FALSE(stage.worker_errors().empty());
}

// Operator that throws on a chosen key. Used to prove a worker death during a
// checkpoint fails the checkpoint cleanly instead of hanging the coordinator.
class ThrowingOnKeyOp final : public Operator<std::int64_t, std::int64_t> {
public:
    explicit ThrowingOnKeyOp(std::int64_t bad_key) : bad_key_(bad_key) {}
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>& el, Emitter<std::int64_t>& out) override {
        if (!el.is_data()) {
            return;
        }
        Batch<std::int64_t> b;
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            if (k == bad_key_) {
                throw std::runtime_error("boom on bad key");
            }
            const auto c = state_->get(k).value_or(0) + 1;
            state_->put(k, c);
            b.emplace(c);
        }
        out.emit_data(std::move(b));
    }

private:
    std::int64_t bad_key_;
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

// A worker that dies (throws) does not hang checkpoint(): the coordinator wakes,
// the checkpoint reports ok=false, and the next checkpoint short-circuits.
TEST(ShardedKeyedStage, WorkerDeathDoesNotHangCheckpoint) {
    constexpr std::size_t kShards = 4;
    // bad_key=5 lives on exactly one shard; that worker throws when it sees it.
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        OperatorId{7},
        [](std::size_t) { return std::make_unique<ThrowingOnKeyOp>(5); },
        int64_key_bytes(),
        [](StreamElement<std::int64_t>) { return true; });
    stage.start();
    {
        Batch<std::int64_t> b;
        for (std::int64_t k = 0; k < 50; ++k) {
            b.emplace(k);  // includes the poison key 5
        }
        stage.submit(std::move(b));
    }
    // The poisoned worker throws; the checkpoint must complete (not hang) with
    // ok=false. Other shards may succeed, but the fan-in is not all-ok.
    auto r = stage.checkpoint(
        CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
    EXPECT_FALSE(r.ok) << "a dead worker must fail the checkpoint, not hang it";

    // A subsequent checkpoint short-circuits to failure rather than hanging.
    auto r2 = stage.checkpoint(
        CheckpointBarrier{CheckpointId{2}, false, CheckpointBarrier::Mode::Aligned});
    EXPECT_FALSE(r2.ok);

    stage.close_input();
    stage.await();
    EXPECT_FALSE(stage.worker_errors().empty());
}

// snapshot()/restore() enforce the QUIESCED precondition: calling them while
// the workers are running throws rather than producing a torn blob.
TEST(ShardedKeyedStage, SnapshotWhileRunningThrows) {
    std::atomic<std::int64_t> emits{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        2,
        OperatorId{7},
        counting_factory(),
        int64_key_bytes(),
        [&emits](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                std::memory_order_relaxed);
            }
            return true;
        });
    stage.start();
    EXPECT_THROW(stage.snapshot(CheckpointId{1}), std::logic_error);
    EXPECT_THROW(stage.restore(Snapshot{}), std::logic_error);
    stage.close_input();
    stage.await();
    // After quiescing, snapshot succeeds.
    EXPECT_NO_THROW(stage.snapshot(CheckpointId{1}));
}

// A mid-stream checkpoint snapshot is fully RESTORABLE: load it into a fresh
// stage, feed more, and the counts resume from the checkpoint (not zero).
TEST(ShardedKeyedStage, MidStreamCheckpointSnapshotRestoresIntoNewStage) {
    ShardedKeyedStage<std::int64_t, std::int64_t>::CheckpointResult r;
    {
        std::atomic<std::int64_t> emits{0};
        ShardedKeyedStage<std::int64_t, std::int64_t> stage(
            4,
            OperatorId{7},
            counting_factory(),
            int64_key_bytes(),
            [&emits](StreamElement<std::int64_t> e) {
                if (e.is_data()) {
                    emits.fetch_add(1, std::memory_order_relaxed);
                }
                return true;
            });
        stage.start();
        Batch<std::int64_t> b;
        for (std::int64_t k = 0; k < 30; ++k) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
        r = stage.checkpoint(
            CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
        stage.close_input();
        stage.await();
    }
    ASSERT_TRUE(r.ok);

    // Fresh stage, restore the mid-stream snapshot, feed each key once -> 2.
    std::atomic<std::int64_t> e2{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> resumed(4,
                                                          OperatorId{7},
                                                          counting_factory(),
                                                          int64_key_bytes(),
                                                          [&e2](StreamElement<std::int64_t> e) {
                                                              if (e.is_data()) {
                                                                  e2.fetch_add(
                                                                      1, std::memory_order_relaxed);
                                                              }
                                                              return true;
                                                          });
    resumed.restore(r.snapshot);
    resumed.start();
    Batch<std::int64_t> b2;
    for (std::int64_t k = 0; k < 30; ++k) {
        b2.emplace(k);
    }
    resumed.submit(std::move(b2));
    resumed.close_input();
    resumed.await();

    InMemoryStateBackend mono;
    mono.restore(resumed.snapshot(CheckpointId{2}));
    for (std::int64_t k = 0; k < 30; ++k) {
        EXPECT_EQ(count_in(mono, k), 2) << "key " << k << " did not resume from mid-stream ckpt";
    }
}

// A checkpoint over an empty epoch (no data submitted) succeeds and yields a
// valid empty snapshot.
TEST(ShardedKeyedStage, EmptyEpochCheckpointSucceeds) {
    std::atomic<std::int64_t> barriers{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(4,
                                                        OperatorId{7},
                                                        counting_factory(),
                                                        int64_key_bytes(),
                                                        [&barriers](StreamElement<std::int64_t> e) {
                                                            if (e.is_barrier()) {
                                                                barriers.fetch_add(
                                                                    1, std::memory_order_relaxed);
                                                            }
                                                            return true;
                                                        });
    stage.start();
    auto r = stage.checkpoint(
        CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
    stage.close_input();
    stage.await();
    EXPECT_TRUE(r.ok);
    EXPECT_EQ(barriers.load(), 1);
    InMemoryStateBackend mono;
    EXPECT_NO_THROW(mono.restore(r.snapshot));
    EXPECT_EQ(count_in(mono, 0), 0);
}

// Source for the DAG-runner tests: emits one batch of keys 0..n-1, optionally a
// checkpoint barrier, then a second batch, then EOS. No synthesized terminal
// barrier (keeps the checkpoint count deterministic).
class KeyStreamSource final : public Source<std::int64_t> {
public:
    KeyStreamSource(int n_keys, bool with_barrier) : n_(n_keys), barrier_(with_barrier) {}
    bool produce(Emitter<std::int64_t>& out) override {
        if (step_ == 0) {
            emit_batch_(out);
            step_ = barrier_ ? 1 : 3;
            return true;
        }
        if (step_ == 1) {
            out.emit_barrier(
                CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
            step_ = 2;
            return true;
        }
        if (step_ == 2) {
            emit_batch_(out);
            step_ = 3;
            return true;
        }
        return false;
    }
    bool emit_terminal_barrier_on_exit() const noexcept override { return false; }

private:
    void emit_batch_(Emitter<std::int64_t>& out) {
        Batch<std::int64_t> b;
        for (int k = 0; k < n_; ++k) {
            b.emplace(k);
        }
        out.emit_data(std::move(b));
    }
    int n_;
    bool barrier_;
    int step_{0};
};

// End-to-end through LocalExecutor: source -> add_sharded_keyed -> sink. Run 1
// streams two epochs with a checkpoint after epoch 1 and verifies the barrier
// drives a single checkpoint+ack and all data flows. Run 2 restores that
// checkpoint into a fresh stage (same uid) and streams each key once more: the
// counts must resume from the checkpoint (=> 2), proving the merged snapshot is
// a correct, restorable cut threaded through the DAG runner.
TEST(ShardedKeyedStage, DagRunnerCheckpointAckAndRestore) {
    constexpr int kKeys = 60;

    Snapshot ckpt_snap;
    int ckpt_ok_count = 0;
    // The ack callback is invoked by EVERY operator's runner thread that sees
    // the barrier (the stage runner AND the sink runner), concurrently, so these
    // counters must be atomic. ckpt_snap/ckpt_ok_count are written only by the
    // stage runner's on_checkpoint (single writer, read after join).
    std::atomic<int> acks{0};
    std::atomic<int> ok_acks{0};
    std::atomic<std::int64_t> sink_count{0};
    {
        auto src = std::make_shared<KeyStreamSource>(kKeys, /*with_barrier=*/true);
        auto sink = std::make_shared<FunctionSink<std::int64_t>>(
            [&](const std::int64_t&) { sink_count.fetch_add(1, std::memory_order_relaxed); });
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
            h0,
            4,
            counting_factory(),
            int64_key_bytes(),
            "sk-stage",
            Snapshot{},
            [&](const ShardedKeyedStage<std::int64_t, std::int64_t>::CheckpointResult& r) {
                if (r.ok) {
                    ckpt_snap = r.snapshot;
                    ++ckpt_ok_count;
                }
                return true;  // in-RAM "persist" for this test
            });
        dag.add_sink<std::int64_t>(h1, sink);
        JobConfig cfg;
        cfg.on_checkpoint_ack = [&](CheckpointId, bool ok, std::string) {
            acks.fetch_add(1, std::memory_order_relaxed);
            if (ok) {
                ok_acks.fetch_add(1, std::memory_order_relaxed);
            }
        };
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();
        EXPECT_TRUE(exec.operator_errors().empty());
    }
    EXPECT_EQ(ckpt_ok_count, 1) << "the stage took exactly one checkpoint for the in-band barrier";
    // Each operator that sees the barrier acks it (the stage AND the downstream
    // sink), so acks counts per-operator, not per-checkpoint; assert every ack
    // succeeded rather than a fixed count.
    EXPECT_GE(ok_acks.load(), 1) << "the checkpoint was acknowledged through the protocol";
    EXPECT_EQ(acks.load(), ok_acks.load()) << "every ack reported success";
    EXPECT_EQ(sink_count.load(), 2 * kKeys) << "both epochs flowed through the stage";
    ASSERT_FALSE(ckpt_snap.bytes.empty());

    // Run 2: restore + stream each key once -> counts resume at 2.
    std::vector<std::int64_t> resumed_counts;  // the sink runs single-threaded
    {
        auto src = std::make_shared<KeyStreamSource>(kKeys, /*with_barrier=*/false);
        auto sink = std::make_shared<FunctionSink<std::int64_t>>(
            [&](const std::int64_t& c) { resumed_counts.push_back(c); });
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
            h0, 4, counting_factory(), int64_key_bytes(), "sk-stage", ckpt_snap);
        dag.add_sink<std::int64_t>(h1, sink);
        LocalExecutor exec(std::move(dag));
        exec.run();
        EXPECT_TRUE(exec.operator_errors().empty());
    }
    ASSERT_EQ(resumed_counts.size(), static_cast<std::size_t>(kKeys));
    for (const auto c : resumed_counts) {
        EXPECT_EQ(c, 2) << "resumed count must continue from the restored checkpoint";
    }
}

// Source emitting one data batch then a single watermark then EOS.
class WatermarkSource final : public Source<std::int64_t> {
public:
    explicit WatermarkSource(int n_keys) : n_(n_keys) {}
    bool produce(Emitter<std::int64_t>& out) override {
        if (step_ == 0) {
            Batch<std::int64_t> b;
            for (int k = 0; k < n_; ++k) {
                b.emplace(k);
            }
            out.emit_data(std::move(b));
            step_ = 1;
            return true;
        }
        if (step_ == 1) {
            out.emit_watermark(Watermark{EventTime{5000}});
            step_ = 2;
            return true;
        }
        return false;
    }
    bool emit_terminal_barrier_on_exit() const noexcept override { return false; }

private:
    int n_;
    int step_{0};
};

// Sink counting data records and watermarks (runs single-threaded).
class CountingSink final : public Sink<std::int64_t> {
public:
    void on_data(const Batch<std::int64_t>& b) override {
        data_ += static_cast<std::int64_t>(b.size());
    }
    void on_watermark(Watermark /*wm*/) override { ++watermarks_; }
    std::int64_t data() const noexcept { return data_; }
    int watermarks() const noexcept { return watermarks_; }

private:
    std::int64_t data_{0};
    int watermarks_{0};
};

// Through the DAG runner, a single upstream watermark min-merges across the S
// shards into exactly ONE watermark at the downstream sink (not S copies).
TEST(ShardedKeyedStage, DagRunnerMergesWatermarkToOneDownstream) {
    constexpr int kKeys = 40;
    auto src = std::make_shared<WatermarkSource>(kKeys);
    auto sink = std::make_shared<CountingSink>();
    Dag dag;
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
        h0, 4, counting_factory(), int64_key_bytes(), "wm-stage");
    dag.add_sink<std::int64_t>(h1, sink);
    LocalExecutor exec(std::move(dag));
    exec.run();
    EXPECT_TRUE(exec.operator_errors().empty());
    EXPECT_EQ(sink->watermarks(), 1) << "the stage min-merges S shard watermarks into one";
    EXPECT_EQ(sink->data(), kKeys);
}

// A snapshot taken at S shards restores into a stage with a DIFFERENT shard
// count (rescale): the merged blob is key-group-tagged and shard-count-agnostic,
// and restore() splits it by each shard's key-group range. Resume each key once
// at the new parallelism and confirm counts continue at 2.
TEST(ShardedKeyedStage, SnapshotRestoresAcrossDifferentShardCounts) {
    constexpr std::int64_t kKeys = 64;
    std::vector<std::int64_t> keys;
    for (std::int64_t k = 0; k < kKeys; ++k) {
        keys.push_back(k);
    }
    std::atomic<std::int64_t> e0{0};
    const Snapshot s4 = run_counts(4, keys, /*reps=*/1, /*pin=*/false, e0);  // each key once

    for (const std::size_t shards :
         {std::size_t{8}, std::size_t{2}, std::size_t{16}, std::size_t{1}}) {
        std::atomic<std::int64_t> e{0};
        ShardedKeyedStage<std::int64_t, std::int64_t> stage(
            shards,
            OperatorId{7},
            counting_factory(),
            int64_key_bytes(),
            [&e](StreamElement<std::int64_t> el) {
                if (el.is_data()) {
                    e.fetch_add(static_cast<std::int64_t>(el.as_data().size()),
                                std::memory_order_relaxed);
                }
                return true;
            });
        stage.restore(s4);  // restore an S=4 snapshot into S=shards
        stage.start();
        Batch<std::int64_t> b;
        for (std::int64_t k = 0; k < kKeys; ++k) {
            b.emplace(k);
        }
        stage.submit(std::move(b));
        stage.close_input();
        stage.await();
        InMemoryStateBackend mono;
        mono.restore(stage.snapshot(CheckpointId{2}));
        for (std::int64_t k = 0; k < kKeys; ++k) {
            EXPECT_EQ(count_in(mono, k), 2) << "rescale to " << shards << " shards, key " << k;
        }
    }
}

// Durable, cross-process-style checkpoint: run 1 persists the stage's merged
// snapshot to disk via ShardedCheckpointStore; a fresh run 2 (independent stage
// + store, communicating only through the directory) load_latest()s it, restores
// it, and resumes counts at 2. Proves ack-after-durable persistence + restore.
TEST(ShardedKeyedStage, DurableCheckpointPersistsAndRestoresAcrossRuns) {
    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / "clink_sharded_ckpt_test";
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);
    constexpr int kKeys = 50;

    // Run 1: stream one epoch + a checkpoint, persisting the merged snapshot.
    // Acks fire from multiple runner threads, so the counters are atomic.
    std::atomic<int> acks{0};
    std::atomic<int> oks{0};
    {
        ShardedCheckpointStore store(dir, "sk");
        auto src = std::make_shared<KeyStreamSource>(kKeys, /*with_barrier=*/true);
        auto sink = std::make_shared<CountingSink>();
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
            h0,
            4,
            counting_factory(),
            int64_key_bytes(),
            "sk",
            Snapshot{},
            [&store](const ShardedKeyedStage<std::int64_t, std::int64_t>::CheckpointResult& r) {
                return store.persist(r.id, r.snapshot);
            });
        dag.add_sink<std::int64_t>(h1, sink);
        JobConfig cfg;
        cfg.on_checkpoint_ack = [&](CheckpointId, bool ok, std::string) {
            acks.fetch_add(1, std::memory_order_relaxed);
            if (ok) {
                oks.fetch_add(1, std::memory_order_relaxed);
            }
        };
        LocalExecutor exec(std::move(dag), cfg);
        exec.run();
        EXPECT_TRUE(exec.operator_errors().empty());
    }
    EXPECT_GE(oks.load(), 1) << "the durable checkpoint acked ok (ack-after-durable)";
    EXPECT_EQ(acks.load(), oks.load());

    // Run 2: a fresh store over the same dir loads the latest snapshot.
    ShardedCheckpointStore store2(dir, "sk");
    const auto restore = store2.load_latest();
    ASSERT_TRUE(restore.has_value());
    ASSERT_FALSE(restore->bytes.empty());

    std::vector<std::int64_t> resumed;  // sink runs single-threaded
    {
        auto src = std::make_shared<KeyStreamSource>(kKeys, /*with_barrier=*/false);
        auto sink = std::make_shared<FunctionSink<std::int64_t>>(
            [&](const std::int64_t& c) { resumed.push_back(c); });
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
            h0, 8, counting_factory(), int64_key_bytes(), "sk", *restore);  // note: rescaled to 8
        dag.add_sink<std::int64_t>(h1, sink);
        LocalExecutor exec(std::move(dag));
        exec.run();
        EXPECT_TRUE(exec.operator_errors().empty());
    }
    ASSERT_EQ(resumed.size(), static_cast<std::size_t>(kKeys));
    for (const auto c : resumed) {
        EXPECT_EQ(c, 2) << "resumed from the durable checkpoint (and rescaled 4 -> 8)";
    }
    std::filesystem::remove_all(dir, ec);
}

// Source: a SAFE epoch (keys 10..49) + barrier 1 (good checkpoint), then a
// POISON key (5) that kills a shard + barrier 2 (failed checkpoint), then EOS.
class SafeThenPoisonSource final : public Source<std::int64_t> {
public:
    bool produce(Emitter<std::int64_t>& out) override {
        if (step_ == 0) {
            Batch<std::int64_t> b;
            for (std::int64_t k = 10; k < 50; ++k) {
                b.emplace(k);
            }
            out.emit_data(std::move(b));
            step_ = 1;
            return true;
        }
        if (step_ == 1) {
            out.emit_barrier(
                CheckpointBarrier{CheckpointId{1}, false, CheckpointBarrier::Mode::Aligned});
            step_ = 2;
            return true;
        }
        if (step_ == 2) {
            Batch<std::int64_t> b;
            b.emplace(5);  // poison: ThrowingOnKeyOp throws on key 5
            out.emit_data(std::move(b));
            step_ = 3;
            return true;
        }
        if (step_ == 3) {
            out.emit_barrier(
                CheckpointBarrier{CheckpointId{2}, false, CheckpointBarrier::Mode::Aligned});
            step_ = 4;
            return true;
        }
        return false;
    }
    bool emit_terminal_barrier_on_exit() const noexcept override { return false; }

private:
    int step_{0};
};

// Regression for the failed-checkpoint shadowing bug: a FAILED checkpoint (a
// worker threw) must NOT be persisted, or its empty snapshot at a higher id
// would shadow the last GOOD checkpoint on load_latest() and the restore would
// silently start from zero. Run 1 persists ckpt 1 (good) then attempts ckpt 2
// (failed, a worker died); run 2 must resume from ckpt 1.
TEST(ShardedKeyedStage, FailedCheckpointDoesNotShadowLastGood) {
    const std::filesystem::path dir =
        std::filesystem::temp_directory_path() / "clink_sharded_failedckpt_test";
    std::error_code ec;
    std::filesystem::remove_all(dir, ec);

    {
        ShardedCheckpointStore store(dir, "fk");
        auto src = std::make_shared<SafeThenPoisonSource>();
        auto sink = std::make_shared<FunctionSink<std::int64_t>>([](const std::int64_t&) {});
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(src);
        auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
            h0,
            4,
            [](std::size_t) { return std::make_unique<ThrowingOnKeyOp>(5); },
            int64_key_bytes(),
            "fk",
            Snapshot{},
            [&store](const ShardedKeyedStage<std::int64_t, std::int64_t>::CheckpointResult& r) {
                return store.persist(r.id, r.snapshot);
            });
        dag.add_sink<std::int64_t>(h1, sink);
        LocalExecutor exec(std::move(dag));
        exec.run();
        // The poisoned worker threw, so the runner surfaces an operator error -
        // that is expected here; the point is what got PERSISTED.
    }

    // load_latest must return the GOOD ckpt 1 (non-empty), not the failed ckpt 2
    // (which must not have been persisted).
    ShardedCheckpointStore store2(dir, "fk");
    const auto restore = store2.load_latest();
    ASSERT_TRUE(restore.has_value()) << "the good checkpoint must be on disk";
    ASSERT_FALSE(restore->bytes.empty()) << "a failed (empty) checkpoint shadowed the good one";

    // Resume keys 10..49 once: restored from ckpt 1 (count 1) -> 2. If the failed
    // checkpoint had shadowed it, restore would be empty -> counts would be 1.
    std::vector<std::int64_t> resumed;
    {
        // Emit keys 10..49 once via a one-shot source.
        class ResumeSource final : public Source<std::int64_t> {
        public:
            bool produce(Emitter<std::int64_t>& out) override {
                if (done_) {
                    return false;
                }
                Batch<std::int64_t> b;
                for (std::int64_t k = 10; k < 50; ++k) {
                    b.emplace(k);
                }
                out.emit_data(std::move(b));
                done_ = true;
                return true;
            }
            bool emit_terminal_barrier_on_exit() const noexcept override { return false; }

        private:
            bool done_{false};
        };
        auto rsrc = std::make_shared<ResumeSource>();
        auto sink = std::make_shared<FunctionSink<std::int64_t>>(
            [&](const std::int64_t& c) { resumed.push_back(c); });
        Dag dag;
        auto h0 = dag.add_source<std::int64_t>(rsrc);
        auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
            h0, 4, counting_factory(), int64_key_bytes(), "fk", *restore);
        dag.add_sink<std::int64_t>(h1, sink);
        LocalExecutor exec(std::move(dag));
        exec.run();
        EXPECT_TRUE(exec.operator_errors().empty());
    }
    ASSERT_EQ(resumed.size(), 40u);
    for (const auto c : resumed) {
        EXPECT_EQ(c, 2) << "must resume from the GOOD checkpoint, not zero";
    }
    std::filesystem::remove_all(dir, ec);
}

// Per-shard records-in metrics make shard skew observable: the per-shard
// counters sum to the total and span more than one shard. Uses a unique op id
// so the global metrics registry (a singleton shared across tests) is not
// contaminated by other tests' counters.
TEST(ShardedKeyedStage, PerShardMetricsRecordSkew) {
    using namespace clink::metrics;
    constexpr std::size_t kShards = 4;
    constexpr OperatorId kOp{4242};  // unique to this test
    std::atomic<std::int64_t> e{0};
    // Per-shard records_in now routes through the configured registry (not the
    // hardcoded global), so point the stage at the global one this test reads.
    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.metrics = &MetricsRegistry::global();
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        kOp,
        counting_factory(),
        int64_key_bytes(),
        [&e](StreamElement<std::int64_t> el) {
            if (el.is_data()) {
                e.fetch_add(static_cast<std::int64_t>(el.as_data().size()),
                            std::memory_order_relaxed);
            }
            return true;
        },
        opts);
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k = 0; k < 200; ++k) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.close_input();
    stage.await();

    std::uint64_t total = 0;
    int nonzero = 0;
    for (std::size_t s = 0; s < kShards; ++s) {
        const std::uint64_t v =
            MetricsRegistry::global()
                .counter(op_shard_metric_name("shard_records_in_total", kOp.value(), s))
                .value();
        total += v;
        if (v > 0) {
            ++nonzero;
        }
    }
    EXPECT_EQ(total, 200u) << "every record counted in exactly one shard's metric";
    EXPECT_GT(nonzero, 1) << "records spread across multiple shards";
}

// A drain marker is coordinated across shards and forwarded downstream exactly
// ONCE (not S copies), and it does not disrupt surrounding data.
TEST(ShardedKeyedStage, DrainMarkerForwardsOnceDownstream) {
    constexpr std::size_t kShards = 4;
    std::atomic<int> drains{0};
    std::atomic<std::int64_t> data{0};
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        kShards,
        OperatorId{7},
        counting_factory(),
        int64_key_bytes(),
        [&](StreamElement<std::int64_t> el) {
            if (el.is_drain()) {
                drains.fetch_add(1, std::memory_order_relaxed);
            } else if (el.is_data()) {
                data.fetch_add(static_cast<std::int64_t>(el.as_data().size()),
                               std::memory_order_relaxed);
            }
            return true;
        });
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k = 0; k < 40; ++k) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.drain(DrainMarker{0, 8});
    stage.close_input();
    stage.await();
    EXPECT_EQ(drains.load(), 1) << "one coordinated drain downstream, not S copies";
    EXPECT_EQ(data.load(), 40) << "data around the drain still flows";
    EXPECT_TRUE(stage.worker_errors().empty());
}

// Through the DAG runner: a drain marker emitted by the source flows through
// add_sharded_keyed without disrupting data (the runner routes it to
// stage.drain). The sink cannot observe drains directly, so we assert all data
// arrived and no operator errored.
TEST(ShardedKeyedStage, DrainThroughRunnerDoesNotDisruptData) {
    constexpr int kKeys = 40;
    class DataThenDrainSource final : public Source<std::int64_t> {
    public:
        bool produce(Emitter<std::int64_t>& out) override {
            if (step_ == 0) {
                emit_batch_(out);
                step_ = 1;
                return true;
            }
            if (step_ == 1) {
                out.emit_drain(DrainMarker{0, 8});
                step_ = 2;
                return true;
            }
            if (step_ == 2) {
                emit_batch_(out);
                step_ = 3;
                return true;
            }
            return false;
        }
        bool emit_terminal_barrier_on_exit() const noexcept override { return false; }

    private:
        void emit_batch_(Emitter<std::int64_t>& out) {
            Batch<std::int64_t> b;
            for (int k = 0; k < kKeys; ++k) {
                b.emplace(k);
            }
            out.emit_data(std::move(b));
        }
        int step_{0};
    };

    auto src = std::make_shared<DataThenDrainSource>();
    std::atomic<std::int64_t> sink_count{0};
    auto sink = std::make_shared<FunctionSink<std::int64_t>>(
        [&](const std::int64_t&) { sink_count.fetch_add(1, std::memory_order_relaxed); });
    Dag dag;
    auto h0 = dag.add_source<std::int64_t>(src);
    auto h1 = dag.add_sharded_keyed<std::int64_t, std::int64_t>(
        h0, 4, counting_factory(), int64_key_bytes(), "drain-stage");
    dag.add_sink<std::int64_t>(h1, sink);
    LocalExecutor exec(std::move(dag));
    exec.run();
    EXPECT_TRUE(exec.operator_errors().empty());
    EXPECT_EQ(sink_count.load(), 2 * kKeys) << "data on both sides of the drain flowed through";
}

}  // namespace

namespace {

// Deferring shard backend that counts batched vs per-key reads (shared atomics
// survive the stage teardown so the test can read them after await()).
class CountingShardBackend final : public StateBackend {
public:
    CountingShardBackend(std::shared_ptr<std::atomic<int>> many,
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
    [[nodiscard]] std::string description() const override { return "counting-shard"; }
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

// CountingOpAsync that opts into read coalescing.
class CoalescingCountOpAsync final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "counts", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool coalesce_reads() const noexcept override { return true; }
    void process_async(const StreamElement<std::int64_t>& el,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            auto ks = *state_;
            aec.submit(std::to_string(k), [ks, k, &out]() mutable -> async::Task<void> {
                const auto c = (co_await ks.get_async(k)).value_or(0) + 1;
                ks.put(k, c);
                Batch<std::int64_t> b;
                b.emplace(c);
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    std::string name() const override { return "coalescing_counter_async"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

// Defers every read and releases the whole parked set together once `release_at`
// reads are outstanding (so all completions land in one controller poll), the
// condition under which deadline ordering is observable. The deadline-aware path
// carries the operator's order_key, which the release posts through the deadline
// scheduler. Used by the sharded deadline-resume parity test.
class ShardDeadlineParkingBackend final : public StateBackend {
public:
    explicit ShardDeadlineParkingBackend(std::size_t release_at) : release_at_(release_at) {}
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
    [[nodiscard]] std::string description() const override { return "shard-deadline-parking"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    void set_async_resume_scheduler(AsyncResumeScheduler s) override { plain_ = std::move(s); }
    void set_deadline_resume_scheduler(DeadlineResumeScheduler s) override {
        deadline_ = std::move(s);
    }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Park{this, op, std::string(key), 0};
    }
    async::Task<std::optional<Value>> get_async(OperatorId op,
                                                KeyView key,
                                                std::uint64_t order_key) const override {
        co_return co_await Park{this, op, std::string(key), order_key};
    }

private:
    struct Park {
        const ShardDeadlineParkingBackend* self;
        OperatorId op;
        std::string key;
        std::uint64_t order_key;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const { self->park_(h, order_key); }
        std::optional<Value> await_resume() const { return self->get(op, key); }
    };
    void park_(std::coroutine_handle<> h, std::uint64_t order_key) const {
        std::vector<std::pair<std::coroutine_handle<>, std::uint64_t>> to_release;
        {
            std::lock_guard<std::mutex> lk(mu_);
            pending_.push_back({h, order_key});
            if (pending_.size() < release_at_) {
                return;
            }
            to_release.swap(pending_);
        }
        for (const auto& [handle, ok] : to_release) {
            if (deadline_) {
                deadline_(handle, ok);
            } else if (plain_) {
                plain_(handle);
            }
        }
    }
    InMemoryStateBackend store_;
    std::size_t release_at_;
    AsyncResumeScheduler plain_;
    DeadlineResumeScheduler deadline_;
    mutable std::mutex mu_;
    mutable std::vector<std::pair<std::coroutine_handle<>, std::uint64_t>> pending_;
};

// Deadline-aware sharded operator: tags each key's read with a rank (the
// deadline), so urgent keys resume first. ranks: k2 < k3 < k1 -> emit [2, 3, 1].
class DeadlinePriorityShardOp final : public Operator<std::int64_t, std::int64_t> {
public:
    void open() override {
        state_.emplace(this->runtime()->template keyed_state<std::int64_t, std::int64_t>(
            "dl", int64_codec(), int64_codec()));
    }
    void process(const StreamElement<std::int64_t>&, Emitter<std::int64_t>&) override {}
    [[nodiscard]] bool supports_async() const noexcept override { return true; }
    [[nodiscard]] bool deadline_aware() const noexcept override { return true; }
    void process_async(const StreamElement<std::int64_t>& el,
                       Emitter<std::int64_t>& out,
                       AsyncExecutionController& aec) override {
        if (!el.is_data()) {
            return;
        }
        for (const auto& r : el.as_data()) {
            const auto k = r.value();
            const std::uint64_t deadline = k == 1 ? 3 : k == 2 ? 1 : 2;
            auto ks = *state_;
            aec.submit(std::to_string(k), [ks, k, deadline, &out]() mutable -> async::Task<void> {
                (void)co_await ks.get_async(k, deadline);  // deadline-tagged read
                Batch<std::int64_t> b;
                b.emplace(k);
                out.emit_data(std::move(b));
                co_return;
            });
        }
    }
    std::string name() const override { return "deadline_priority_shard"; }

private:
    std::optional<KeyedState<std::int64_t, std::int64_t>> state_;
};

}  // namespace

// With a deferring per-shard backend + an opted-in operator, the records routed
// to a shard in one batch collapse into ONE get_many_async on that shard's
// backend, never the per-key get_async. One shard so all keys land together.
TEST(ShardedKeyedStage, CoalescesShardReadsIntoOneGetMany) {
    auto many = std::make_shared<std::atomic<int>>(0);
    auto single = std::make_shared<std::atomic<int>>(0);
    std::atomic<std::int64_t> emits{0};

    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.shard_backend_factory = [many, single](std::size_t) -> std::unique_ptr<StateBackend> {
        return std::make_unique<CountingShardBackend>(many, single);
    };
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        /*shards=*/1,
        OperatorId{7},
        [](std::size_t) { return std::make_unique<CoalescingCountOpAsync>(); },
        int64_key_bytes(),
        [&emits](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                emits.fetch_add(static_cast<std::int64_t>(e.as_data().size()),
                                std::memory_order_relaxed);
            }
            return true;
        },
        opts);
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k : {10, 20, 30, 40}) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.close_input();
    stage.await();

    EXPECT_EQ(emits.load(), 4);    // every record emitted
    EXPECT_EQ(many->load(), 1);    // one batch -> one get_many on the shard
    EXPECT_EQ(single->load(), 0);  // never the per-key path
}

// ASYNC-12 parity: a deadline_aware operator on a shard whose reads all park
// then release together resumes them most-urgent-first (the shard runner flips
// its controller to Priority and wires the order_key-carrying hand-back on the
// shard's backend). ranks k2<k3<k1 -> emit order [2, 3, 1], not arrival [1,2,3].
TEST(ShardedKeyedStage, DeadlineAwareResumesUrgentShardReadsFirst) {
    ShardedKeyedStage<std::int64_t, std::int64_t>::Options opts;
    opts.shard_backend_factory = [](std::size_t) -> std::unique_ptr<StateBackend> {
        return std::make_unique<ShardDeadlineParkingBackend>(/*release_at=*/3);
    };
    std::mutex om;
    std::vector<std::int64_t> emit_order;
    ShardedKeyedStage<std::int64_t, std::int64_t> stage(
        /*shards=*/1,
        OperatorId{8},
        [](std::size_t) { return std::make_unique<DeadlinePriorityShardOp>(); },
        int64_key_bytes(),
        [&](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                std::lock_guard<std::mutex> lk(om);
                for (const auto& r : e.as_data()) {
                    emit_order.push_back(r.value());
                }
            }
            return true;
        },
        opts);
    stage.start();
    Batch<std::int64_t> b;
    for (std::int64_t k : {1, 2, 3}) {
        b.emplace(k);
    }
    stage.submit(std::move(b));
    stage.close_input();
    stage.await();

    EXPECT_EQ(emit_order, (std::vector<std::int64_t>{2, 3, 1}))
        << "shard reads must resume in ascending-deadline order";
}
