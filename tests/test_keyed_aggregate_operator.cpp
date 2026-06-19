// KeyedAggregateOperator: the first production operator to adopt the
// async-state execution path. The acceptance, mirroring ASYNC-6: the same
// operator and input, run once on a plain backend (synchronous process())
// and once on an async-capable backend (the process_async() branch through
// the AsyncExecutionController), must produce byte-identical output.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
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

// Reports it can defer reads (so the runner takes the async branch) but
// completes get_async inline via the StateBackend base default. Exercises
// the full async wiring with deterministic, ordered completion. Uniquely
// named to avoid ODR collision with the like-purposed backend in
// test_async_operator_runner.cpp (same clink_core_tests binary).
class KaInlineAsyncBackend : public StateBackend {
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
    std::string description() const override { return "ka-inline-async"; }
    [[nodiscard]] bool supports_async_get() const noexcept override { return true; }

    // Count async reads so the test can prove the process_async() branch
    // actually ran (a sync run never touches get_async).
    async::Task<std::optional<Value>> get_async(OperatorId op, KeyView key) const override {
        get_async_calls_.fetch_add(1, std::memory_order_relaxed);
        co_return get(op, key);
    }
    std::size_t get_async_calls() const { return get_async_calls_.load(std::memory_order_relaxed); }

private:
    InMemoryStateBackend store_;
    mutable std::atomic<std::size_t> get_async_calls_{0};
};

std::vector<Record<KV>> records(const std::vector<KV>& xs) {
    std::vector<Record<KV>> out;
    out.reserve(xs.size());
    for (const auto& x : xs) {
        out.emplace_back(x);
    }
    return out;
}

std::vector<KV> run_sum(const std::vector<KV>& input, std::shared_ptr<StateBackend> backend) {
    Dag dag;
    auto src = std::make_shared<VectorSource<KV>>(records(input));
    auto op = std::make_shared<KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t>>(
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        int64_codec(),
        int64_codec(),
        "sum");
    auto sink = std::make_shared<CollectingSink<KV>>();
    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, op);
    dag.add_sink<KV>(h1, sink);

    JobConfig cfg;
    cfg.state_backend = std::move(backend);
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();
    return sink->collected();
}

}  // namespace

TEST(KeyedAggregateOperator, SyncAndAsyncProduceIdenticalRunningSums) {
    const std::vector<KV> input = {{1, 10}, {2, 20}, {1, 30}, {2, 5}, {1, 7}};
    // Running sum per key, emitted after every input, in input order.
    const std::vector<KV> expected = {{1, 10}, {2, 20}, {1, 40}, {2, 25}, {1, 47}};

    auto sync_out = run_sum(input, std::make_shared<InMemoryStateBackend>());
    auto async_backend = std::make_shared<KaInlineAsyncBackend>();
    auto async_out = run_sum(input, async_backend);

    EXPECT_EQ(sync_out, expected) << "synchronous process() path";
    EXPECT_EQ(async_out, expected) << "async process_async() path";
    EXPECT_EQ(sync_out, async_out) << "sync and async must be byte-identical";

    // Proof the async branch (process_async -> get_async) actually ran:
    // one async read per input record.
    EXPECT_EQ(async_backend->get_async_calls(), input.size());
}

TEST(KeyedAggregateOperator, AsyncIsOptInOnlyWithAsyncCapableBackend) {
    // supports_async() is true, but on a plain (non-deferring) backend the
    // runner still uses the synchronous path; result is unchanged either way.
    KeyedAggregateOperator<std::int64_t, std::int64_t, std::int64_t> op(
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        int64_codec(),
        int64_codec());
    EXPECT_TRUE(op.supports_async());
    EXPECT_FALSE(op.fires_state_touching_timers());

    const std::vector<KV> input = {{7, 1}, {7, 2}, {7, 3}};
    const std::vector<KV> expected = {{7, 1}, {7, 3}, {7, 6}};
    EXPECT_EQ(run_sum(input, std::make_shared<InMemoryStateBackend>()), expected);
}
