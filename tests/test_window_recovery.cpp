#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Source that emits a single batch then idles. We use it to reach a stable
// state-write point, snapshot the backend, then cancel without firing flush.
template <typename T>
class HoldingVectorSource final : public Source<T> {
public:
    explicit HoldingVectorSource(std::vector<Record<T>> records) : records_(std::move(records)) {}

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            Batch<T> b{records_};
            out.emit_data(std::move(b));
            emitted_ = true;
            emit_done_.store(true, std::memory_order_release);
            return true;
        }
        // Idle - let the test snapshot + cancel us.
        std::this_thread::sleep_for(2ms);
        return true;
    }

    void wait_for_emit() const {
        while (!emit_done_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(1ms);
        }
    }

    std::string name() const override { return "holding_source"; }

private:
    std::vector<Record<T>> records_;
    std::atomic<bool> emit_done_{false};
    bool emitted_{false};
};

}  // namespace

// End-to-end: state written by a persistent TumblingWindowOperator survives
// snapshot/restore across two LocalExecutor runs. The first run ingests partial
// input and is snapshotted mid-flight; the second run restores into a fresh backend
// and continues, producing aggregates that span events from both runs.
TEST(WindowRecovery, AggregationContinuesAfterRestart) {
    using KV = std::pair<std::string, int>;

    // -----------------------------------------------------------------
    // First run: emit some events, snapshot, cancel before flush fires
    // -----------------------------------------------------------------
    auto backend1 = std::make_shared<InMemoryStateBackend>();

    auto source1 = std::make_shared<HoldingVectorSource<KV>>(std::vector<Record<KV>>{
        Record<KV>{KV{"a", 1}, EventTime{100}},
        Record<KV>{KV{"a", 2}, EventTime{200}},
        Record<KV>{KV{"b", 3}, EventTime{500}},
    });
    auto window1 = std::make_shared<TumblingWindowOperator<std::string, int, std::int64_t>>(
        1000ms,
        []() -> std::int64_t { return 0; },
        [](const std::int64_t& acc, const int& v) { return acc + v; },
        string_codec(),
        int64_codec());
    auto sink1 = std::make_shared<CollectingSink<std::pair<std::string, std::int64_t>>>();

    Dag dag1;
    auto h0 = dag1.add_source<KV>(source1);
    auto h1 = dag1.add_operator<KV, std::pair<std::string, std::int64_t>>(h0, window1);
    dag1.add_sink<std::pair<std::string, std::int64_t>>(h1, sink1);

    JobConfig cfg1;
    cfg1.state_backend = backend1;
    LocalExecutor exec1(std::move(dag1), std::move(cfg1));
    exec1.start();

    source1->wait_for_emit();

    // Wait for the window operator to have actually written its 2 keys
    // (a@window-0 and b@window-0) before snapshotting. Polling avoids a
    // sleep-based race.
    const OperatorId window_op_id = window1->id();
    auto count_keys = [&] {
        std::size_t n = 0;
        backend1->scan(window_op_id, [&](auto, auto) { ++n; });
        return n;
    };
    for (int i = 0; i < 200 && count_keys() < 2; ++i) {
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_EQ(count_keys(), 2u) << "window operator did not write expected state";

    Snapshot snap = backend1->snapshot(CheckpointId{1});

    exec1.cancel();
    exec1.await_termination();

    // No window fired in the first run (no max-watermark, no flush).
    EXPECT_TRUE(sink1->collected().empty());

    // -----------------------------------------------------------------
    // Second run: fresh backend, restore from the snapshot, feed remaining
    // events, run to completion (flush fires the windows).
    // -----------------------------------------------------------------
    auto backend2 = std::make_shared<InMemoryStateBackend>();

    auto source2 = std::make_shared<VectorSource<KV>>(std::vector<Record<KV>>{
        Record<KV>{KV{"a", 10}, EventTime{900}},
        Record<KV>{KV{"b", 20}, EventTime{800}},
        Record<KV>{KV{"a", 100}, EventTime{1500}},
    });
    auto window2 = std::make_shared<TumblingWindowOperator<std::string, int, std::int64_t>>(
        1000ms,
        []() -> std::int64_t { return 0; },
        [](const std::int64_t& acc, const int& v) { return acc + v; },
        string_codec(),
        int64_codec());
    auto sink2 = std::make_shared<CollectingSink<std::pair<std::string, std::int64_t>>>();

    Dag dag2;
    auto h0_2 = dag2.add_source<KV>(source2);
    // NB: use a default-named source, but the holding source in the first run is
    // named "holding_source" - different name → different OperatorId → no
    // collision with the first run's window id. The window operator itself uses
    // the default "tumbling_window" name in both runs AND sits at the
    // same DAG position, so its OperatorId is identical across runs and
    // the snapshot's keys line up.
    auto h1_2 = dag2.add_operator<KV, std::pair<std::string, std::int64_t>>(h0_2, window2);
    dag2.add_sink<std::pair<std::string, std::int64_t>>(h1_2, sink2);

    EXPECT_EQ(window1->id(), window2->id())
        << "stable OperatorId is required for cross-run state recovery";

    JobConfig cfg2;
    cfg2.state_backend = backend2;
    cfg2.restore_from = std::move(snap);
    LocalExecutor exec2(std::move(dag2), std::move(cfg2));
    exec2.run();

    // Window [0, 1000): a = 1 + 2 + 10 = 13, b = 3 + 20 = 23
    // Window [1000, 2000): a = 100
    auto results = sink2->collected();
    std::int64_t a_first = 0;
    std::int64_t b_first = 0;
    std::int64_t a_late = 0;
    for (const auto& [k, v] : results) {
        if (k == "a" && v == 13) {
            a_first = v;
        } else if (k == "a" && v == 100) {
            a_late = v;
        } else if (k == "b" && v == 23) {
            b_first = v;
        }
    }
    EXPECT_EQ(a_first, 13);
    EXPECT_EQ(b_first, 23);
    EXPECT_EQ(a_late, 100);
    EXPECT_EQ(results.size(), 3u);
}
