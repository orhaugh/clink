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
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

template <typename T>
class HoldingVectorSource final : public Source<T> {
public:
    HoldingVectorSource(std::vector<Record<T>> records, std::string n)
        : records_(std::move(records)), name_(std::move(n)) {}

    bool produce(Emitter<T>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            if (!records_.empty()) {
                Batch<T> b{records_};
                out.emit_data(std::move(b));
            }
            emitted_ = true;
            done_.store(true, std::memory_order_release);
            return true;
        }
        std::this_thread::sleep_for(2ms);
        return true;
    }

    void wait_for_emit() const {
        while (!done_.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(1ms);
        }
    }

    std::string name() const override { return name_; }

private:
    std::vector<Record<T>> records_;
    std::string name_;
    std::atomic<bool> done_{false};
    bool emitted_{false};
};

}  // namespace

// End-to-end: an interval-join's keyed buffer state survives snapshot/restore.
// The first stage ingests only left events (no matches yet); the second stage
// starts with a fresh backend, restores, and emits right events that match the
// earlier left records purely from restored state.
TEST(JoinRecovery, BufferedLeftRecordsMatchAcrossRestart) {
    using V = std::int64_t;
    using K = std::string;

    // -------- Stage 1: two left records, zero right --------
    auto backend1 = std::make_shared<InMemoryStateBackend>();

    auto src_a1 = std::make_shared<HoldingVectorSource<V>>(
        std::vector<Record<V>>{
            Record<V>{1, EventTime{100}},
            Record<V>{2, EventTime{200}},
        },
        "left");
    auto src_b1 = std::make_shared<HoldingVectorSource<V>>(std::vector<Record<V>>{}, "right");

    Dag dag1;
    auto h_a1 = dag1.add_source<V>(src_a1);
    auto h_b1 = dag1.add_source<V>(src_b1);

    auto h_j1 = dag1.interval_join<V, V, K, std::pair<V, V>>(
        h_a1,
        h_b1,
        [](const V& v) -> K { return v == 1 ? "x" : "y"; },
        [](const V& v) -> K { return v == 10 ? "x" : "y"; },
        100ms,
        100ms,
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return std::make_pair(a.value_or(-1), b.value_or(-1));
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        string_codec());

    auto sink1 = std::make_shared<CollectingSink<std::pair<V, V>>>();
    dag1.add_sink<std::pair<V, V>>(h_j1, sink1);

    const OperatorId join_id = dag1.runners()[h_j1.runner_index].id;

    JobConfig cfg1;
    cfg1.state_backend = backend1;
    LocalExecutor exec1(std::move(dag1), std::move(cfg1));
    exec1.start();

    src_a1->wait_for_emit();
    src_b1->wait_for_emit();

    // Wait until both left-buf keys are flushed to the backend (2 entries
    // under the join operator's id with slot prefix "left_buf|").
    // Stored-key layout is [kg_byte][slot_name][|][user_key_bytes], so
    // skip byte 0 before checking the slot prefix.
    auto count_left_keys = [&]() {
        std::size_t n = 0;
        backend1->scan(join_id, [&](StateBackend::KeyView k, StateBackend::ValueView) {
            if (k.size() > 1 + 9 && k.compare(1, 9, "left_buf|") == 0) {
                ++n;
            }
        });
        return n;
    };
    for (int i = 0; i < 200 && count_left_keys() < 2; ++i) {
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_EQ(count_left_keys(), 2u) << "left buffer was not persisted in time";

    Snapshot snap = backend1->snapshot(CheckpointId{1});

    exec1.cancel();
    exec1.await_termination();
    EXPECT_TRUE(sink1->collected().empty());

    // -------- Stage 2: fresh backend, restore, supply matching right --------
    auto backend2 = std::make_shared<InMemoryStateBackend>();

    auto src_a2 = std::make_shared<VectorSource<V>>(std::vector<Record<V>>{}, "left");
    auto src_b2 = std::make_shared<VectorSource<V>>(
        std::vector<Record<V>>{
            Record<V>{10, EventTime{110}},  // joins with left x@100 (delta=10)
            Record<V>{20, EventTime{210}},  // joins with left y@200 (delta=10)
        },
        "right");

    Dag dag2;
    auto h_a2 = dag2.add_source<V>(src_a2);
    auto h_b2 = dag2.add_source<V>(src_b2);

    auto h_j2 = dag2.interval_join<V, V, K, std::pair<V, V>>(
        h_a2,
        h_b2,
        [](const V& v) -> K { return v == 1 ? "x" : "y"; },
        [](const V& v) -> K { return v == 10 ? "x" : "y"; },
        100ms,
        100ms,
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return std::make_pair(a.value_or(-1), b.value_or(-1));
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        string_codec());

    EXPECT_EQ(dag2.runners()[h_j2.runner_index].id, join_id)
        << "stable OperatorId required for join state to line up across runs";

    auto sink2 = std::make_shared<CollectingSink<std::pair<V, V>>>();
    dag2.add_sink<std::pair<V, V>>(h_j2, sink2);

    JobConfig cfg2;
    cfg2.state_backend = backend2;
    cfg2.restore_from = std::move(snap);
    LocalExecutor exec2(std::move(dag2), std::move(cfg2));
    exec2.run();

    // Both joins fire entirely from restored state (phase 2's left source
    // is empty).
    auto results = sink2->collected();
    std::sort(results.begin(), results.end());
    EXPECT_EQ(results, (std::vector<std::pair<V, V>>{{1, 10}, {2, 20}}));
}

// Same restart shape as above, but both runs execute with unaligned
// checkpoints enabled. The barrier-overtake, in-flight capture, and
// channel-replay primitives are covered at the operator level by the
// IntervalJoinUnaligned + MultiInputAlignment suites; this is the
// end-to-end complement: a real snapshot/restore cycle under unaligned
// mode must still produce each join result exactly once (no records
// lost when the barrier overtakes, none duplicated on replay).
TEST(JoinRecovery, UnalignedModeBufferedRecordsMatchExactlyOnceAcrossRestart) {
    using V = std::int64_t;
    using K = std::string;

    // -------- Stage 1: two left records, zero right, UNALIGNED -------
    auto backend1 = std::make_shared<InMemoryStateBackend>();

    auto src_a1 = std::make_shared<HoldingVectorSource<V>>(
        std::vector<Record<V>>{
            Record<V>{1, EventTime{100}},
            Record<V>{2, EventTime{200}},
        },
        "left");
    auto src_b1 = std::make_shared<HoldingVectorSource<V>>(std::vector<Record<V>>{}, "right");

    Dag dag1;
    auto h_a1 = dag1.add_source<V>(src_a1);
    auto h_b1 = dag1.add_source<V>(src_b1);

    auto h_j1 = dag1.interval_join<V, V, K, std::pair<V, V>>(
        h_a1,
        h_b1,
        [](const V& v) -> K { return v == 1 ? "x" : "y"; },
        [](const V& v) -> K { return v == 10 ? "x" : "y"; },
        100ms,
        100ms,
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return std::make_pair(a.value_or(-1), b.value_or(-1));
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        string_codec());

    auto sink1 = std::make_shared<CollectingSink<std::pair<V, V>>>();
    dag1.add_sink<std::pair<V, V>>(h_j1, sink1);

    const OperatorId join_id = dag1.runners()[h_j1.runner_index].id;

    JobConfig cfg1;
    cfg1.state_backend = backend1;
    cfg1.unaligned_checkpoints = true;
    LocalExecutor exec1(std::move(dag1), std::move(cfg1));
    exec1.start();

    src_a1->wait_for_emit();
    src_b1->wait_for_emit();

    auto count_left_keys = [&]() {
        std::size_t n = 0;
        backend1->scan(join_id, [&](StateBackend::KeyView k, StateBackend::ValueView) {
            if (k.size() > 1 + 9 && k.compare(1, 9, "left_buf|") == 0) {
                ++n;
            }
        });
        return n;
    };
    for (int i = 0; i < 200 && count_left_keys() < 2; ++i) {
        std::this_thread::sleep_for(2ms);
    }
    ASSERT_EQ(count_left_keys(), 2u) << "left buffer was not persisted in time";

    Snapshot snap = backend1->snapshot(CheckpointId{1});

    exec1.cancel();
    exec1.await_termination();
    EXPECT_TRUE(sink1->collected().empty());

    // -------- Stage 2: fresh backend, restore, matching right, UNALIGNED
    auto backend2 = std::make_shared<InMemoryStateBackend>();

    auto src_a2 = std::make_shared<VectorSource<V>>(std::vector<Record<V>>{}, "left");
    auto src_b2 = std::make_shared<VectorSource<V>>(
        std::vector<Record<V>>{
            Record<V>{10, EventTime{110}},
            Record<V>{20, EventTime{210}},
        },
        "right");

    Dag dag2;
    auto h_a2 = dag2.add_source<V>(src_a2);
    auto h_b2 = dag2.add_source<V>(src_b2);

    auto h_j2 = dag2.interval_join<V, V, K, std::pair<V, V>>(
        h_a2,
        h_b2,
        [](const V& v) -> K { return v == 1 ? "x" : "y"; },
        [](const V& v) -> K { return v == 10 ? "x" : "y"; },
        100ms,
        100ms,
        [](const std::optional<V>& a, const std::optional<V>& b) {
            return std::make_pair(a.value_or(-1), b.value_or(-1));
        },
        Dag::JoinType::Inner,
        "interval_join",
        int64_codec(),
        int64_codec(),
        string_codec());

    EXPECT_EQ(dag2.runners()[h_j2.runner_index].id, join_id)
        << "stable OperatorId required for join state to line up across runs";

    auto sink2 = std::make_shared<CollectingSink<std::pair<V, V>>>();
    dag2.add_sink<std::pair<V, V>>(h_j2, sink2);

    JobConfig cfg2;
    cfg2.state_backend = backend2;
    cfg2.restore_from = std::move(snap);
    cfg2.unaligned_checkpoints = true;
    LocalExecutor exec2(std::move(dag2), std::move(cfg2));
    exec2.run();

    auto results = sink2->collected();
    std::sort(results.begin(), results.end());
    // Exactly the two matches, each exactly once.
    EXPECT_EQ(results, (std::vector<std::pair<V, V>>{{1, 10}, {2, 20}}));
}
