// DeadlineKeyedAggregateOperator: the first ASYNC-12 consumer - an operator
// that TAGS each record's state read with a deadline (order_key) so that, on a
// deferring backend, the most urgent records' reads resume first.
//
// Acceptance:
//   1. Sync vs async produce identical running sums (the deadline never
//      changes the result, only resume order).
//   2. UrgentReadsResumeFirst: with a deferring backend that parks every read
//      until the whole batch is outstanding then releases them together (so
//      all completions land in ONE poll), the operator's outputs come out in
//      ASCENDING deadline order - proving the order_key threaded from
//      get_async(k, deadline) all the way to schedule_resume, and that the
//      runner flipped the controller to Priority resume.
//   3. FifoWithoutDeadlineAware: the SAME deferring backend + the SAME input,
//      but a plain (non-deadline-aware) KeyedAggregateOperator, resumes in
//      ARRIVAL order - proving deadline_aware() is what changes the behaviour.

#include <algorithm>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/deadline_keyed_operator.hpp"
#include "clink/operators/keyed_aggregate_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

using KV = std::pair<std::int64_t, std::int64_t>;

std::vector<Record<KV>> dk_records(const std::vector<KV>& xs) {
    std::vector<Record<KV>> out;
    out.reserve(xs.size());
    for (const auto& x : xs) {
        out.emplace_back(x);
    }
    return out;
}

// Defers EVERY read (parks the suspended handle) and only hands the whole
// parked set back once `release_at` reads are outstanding, so all completions
// land in a single controller poll - the condition under which ResumeOrder
// matters. Both get_async overloads park; the deadline-aware one carries the
// operator's order_key, which the release posts through the deadline scheduler.
// Uniquely named to avoid ODR collision in the shared clink_core_tests binary.
class DkParkingBatchBackend : public StateBackend {
public:
    explicit DkParkingBatchBackend(std::size_t release_at) : release_at_(release_at) {}

    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        store_.restore(snap, kg_filter);
    }
    std::string description() const override { return "dk-parking-batch"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }

    void set_async_resume_scheduler(AsyncResumeScheduler s) override { plain_ = std::move(s); }
    void set_deadline_resume_scheduler(DeadlineResumeScheduler s) override {
        deadline_ = std::move(s);
    }

    // FIFO path (no deadline tag): parks with order_key 0.
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return co_await Park{this, op, std::string(key), 0};
    }
    // Deadline-aware path: parks with the operator's tag.
    async::Task<std::optional<Value>> get_async(OperatorId op,
                                                KeyView key,
                                                std::uint64_t order_key) const override {
        co_return co_await Park{this, op, std::string(key), order_key};
    }

    // The order_keys the backend was asked to resume with, in the order it
    // posted them back (= ascending-deadline order for the Priority case, since
    // the controller reorders the poll batch, but recorded here at POST time =
    // arrival order). Used only to prove the tags reached the backend.
    std::vector<std::uint64_t> posted_order_keys() const {
        std::lock_guard<std::mutex> lk(mu_);
        return posted_;
    }

private:
    struct Park {
        const DkParkingBatchBackend* self;
        OperatorId op;
        std::string key;
        std::uint64_t order_key;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const { self->park_(h, order_key); }
        std::optional<Value> await_resume() const { return self->get(op, key); }
    };

    // Runner-thread-only in practice (await_suspend runs during submit), but the
    // mutex keeps the recorded vector well-defined for the test reader.
    void park_(std::coroutine_handle<> h, std::uint64_t order_key) const {
        std::vector<std::pair<std::coroutine_handle<>, std::uint64_t>> to_release;
        {
            std::lock_guard<std::mutex> lk(mu_);
            pending_.push_back({h, order_key});
            if (pending_.size() < release_at_) {
                return;  // hold until the whole batch is outstanding
            }
            to_release.swap(pending_);
        }
        for (const auto& [handle, ok] : to_release) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                posted_.push_back(ok);
            }
            if (deadline_) {
                deadline_(handle, ok);  // order_key-carrying hand-back (Priority)
            } else if (plain_) {
                plain_(handle);  // FIFO hand-back
            }
        }
    }

    InMemoryStateBackend store_;
    std::size_t release_at_;
    AsyncResumeScheduler plain_;
    DeadlineResumeScheduler deadline_;
    mutable std::mutex mu_;
    mutable std::vector<std::pair<std::coroutine_handle<>, std::uint64_t>> pending_;
    mutable std::vector<std::uint64_t> posted_;
};

// Reports it can defer but completes inline (base default) - exercises the
// async wiring with immediate, ordered completion for the correctness test.
class DkInlineAsyncBackend : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { store_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return store_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { store_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { store_.scan(op, visit); }
    Snapshot snapshot(CheckpointId id) override { return store_.snapshot(id); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        store_.restore(snap, kg_filter);
    }
    std::string description() const override { return "dk-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        co_return get(op, key);
    }

private:
    InMemoryStateBackend store_;
};

auto make_deadline_op() {
    return std::make_shared<
        DeadlineKeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        [](const std::int64_t& /*k*/, const std::int64_t& v) {
            return static_cast<std::uint64_t>(v);  // the value IS the deadline (lower = sooner)
        },
        int64_codec(),
        int64_codec(),
        "deadline_sum");
}

std::vector<KV> run(std::shared_ptr<Operator<KV, KV>> op,
                    const std::vector<KV>& input,
                    std::shared_ptr<StateBackend> backend) {
    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(dk_records(input));
    auto sink = std::make_shared<CollectingSink<KV>>();
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, std::move(op));
    dag.add_sink<KV>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected();
}

}  // namespace

TEST(DeadlineKeyedOperator, SyncAndAsyncProduceIdenticalRunningSums) {
    const std::vector<KV> input = {{1, 10}, {2, 20}, {1, 30}, {2, 5}, {1, 7}};
    const std::vector<KV> expected = {{1, 10}, {2, 20}, {1, 40}, {2, 25}, {1, 47}};

    EXPECT_EQ(run(make_deadline_op(), input, std::make_shared<InMemoryStateBackend>()), expected)
        << "synchronous process() path";
    EXPECT_EQ(run(make_deadline_op(), input, std::make_shared<DkInlineAsyncBackend>()), expected)
        << "async process_async() path (inline completion)";
}

TEST(DeadlineKeyedOperator, UrgentReadsResumeFirst) {
    // Three DISTINCT keys (so the per-key gate lets all three reads overlap),
    // with out-of-order deadlines (= the value). All three reads park until the
    // batch is outstanding, then release together: the Priority controller must
    // resume them lowest-deadline-first, so outputs come out keys [2, 3, 1].
    const std::vector<KV> input = {{1, 50}, {2, 10}, {3, 30}};

    auto backend = std::make_shared<DkParkingBatchBackend>(/*release_at=*/3);
    auto out = run(make_deadline_op(), input, backend);

    ASSERT_EQ(out.size(), 3u);
    std::vector<std::int64_t> key_order;
    key_order.reserve(out.size());
    for (const auto& kv : out) {
        key_order.push_back(kv.first);
    }
    EXPECT_EQ(key_order, (std::vector<std::int64_t>{2, 3, 1}))
        << "outputs must resume in ascending-deadline order";

    // Each emitted aggregate is just its own value (first record per key).
    for (const auto& kv : out) {
        EXPECT_EQ(kv.first == 1 ? 50 : kv.first == 2 ? 10 : 30, kv.second);
    }

    // The operator's deadline tags actually reached the backend (not all zero).
    auto tags = backend->posted_order_keys();
    std::sort(tags.begin(), tags.end());
    EXPECT_EQ(tags, (std::vector<std::uint64_t>{10, 30, 50}));
}

TEST(DeadlineKeyedOperator, FifoWithoutDeadlineAware) {
    // Same backend + input, but a plain KeyedAggregateOperator (not
    // deadline_aware): the runner leaves the controller in FIFO, so the same
    // batched release resumes in ARRIVAL order - keys [1, 2, 3].
    const std::vector<KV> input = {{1, 50}, {2, 10}, {3, 30}};

    auto plain = std::make_shared<KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        int64_codec(),
        int64_codec(),
        "plain_sum");
    auto backend = std::make_shared<DkParkingBatchBackend>(/*release_at=*/3);
    auto out = run(plain, input, backend);

    ASSERT_EQ(out.size(), 3u);
    std::vector<std::int64_t> key_order;
    key_order.reserve(out.size());
    for (const auto& kv : out) {
        key_order.push_back(kv.first);
    }
    EXPECT_EQ(key_order, (std::vector<std::int64_t>{1, 2, 3}))
        << "without deadline_aware the resume order is arrival order";

    // The plain operator tags nothing: every read parked with order_key 0.
    EXPECT_EQ(backend->posted_order_keys(), (std::vector<std::uint64_t>{0, 0, 0}));
}
