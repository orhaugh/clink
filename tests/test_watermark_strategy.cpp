#include <chrono>
#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/key_by_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/operators/tumbling_window_operator.hpp"
#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/time/watermark_strategy.hpp"

using namespace clink;
using namespace std::chrono_literals;

TEST(WatermarkStrategy, MonotonicReturnsMaxSeen) {
    MonotonicWatermarkStrategy<int> s;
    EXPECT_FALSE(s.current_watermark().has_value());

    Record<int> r1{1, EventTime{100}};
    s.on_record(r1);
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{100});

    // No new record => no progression
    EXPECT_FALSE(s.current_watermark().has_value());

    // Out-of-order record does not regress watermark
    Record<int> r2{2, EventTime{50}};
    s.on_record(r2);
    EXPECT_FALSE(s.current_watermark().has_value());

    Record<int> r3{3, EventTime{200}};
    s.on_record(r3);
    auto wm2 = s.current_watermark();
    ASSERT_TRUE(wm2.has_value());
    EXPECT_EQ(wm2->timestamp(), EventTime{200});
}

TEST(WatermarkStrategy, BoundedOutOfOrdernessSubtractsBound) {
    BoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{50});
    Record<int> r{1, EventTime{1000}};
    s.on_record(r);
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{950});
}

TEST(WatermarkStrategy, BoundedDoesNotRegressOnOutOfOrderRecord) {
    BoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{50});
    s.on_record(Record<int>{1, EventTime{1000}});
    (void)s.current_watermark();  // consume

    // Older record arrives - must not regress max_ts; current_watermark
    // is still the same, so reports nullopt (not dirty).
    s.on_record(Record<int>{2, EventTime{500}});
    EXPECT_FALSE(s.current_watermark().has_value());

    // Newer record advances.
    s.on_record(Record<int>{3, EventTime{2000}});
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{1950});
}

TEST(WatermarkStrategy, BoundedToleratesRecordsBelowBound) {
    // Record with ts < bound produces a negative-millis watermark. This
    // is documented as supported (EventTime::min() is the absolute floor).
    BoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{500});
    s.on_record(Record<int>{1, EventTime{100}});
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp().millis(), -400);
}

TEST(WatermarkStrategy, MonotonicIgnoresRecordsWithoutEventTime) {
    // Record carries no event time - strategy should not advance.
    MonotonicWatermarkStrategy<int> s;
    Record<int> r{42};  // no ts
    s.on_record(r);
    EXPECT_FALSE(s.current_watermark().has_value());
}

TEST(WatermarkStrategy, MonotonicIsDirtyOnlyOnceUntilNewMax) {
    MonotonicWatermarkStrategy<int> s;
    s.on_record(Record<int>{1, EventTime{100}});
    EXPECT_TRUE(s.current_watermark().has_value());   // first read clears dirty
    EXPECT_FALSE(s.current_watermark().has_value());  // subsequent reads idempotent

    // Same max_ts - still no progression.
    s.on_record(Record<int>{2, EventTime{100}});
    EXPECT_FALSE(s.current_watermark().has_value());
}

// --- PartitionAwareBoundedOutOfOrdernessStrategy ---------------------------

// Helper: a Record carrying both an event time and a source partition.
static Record<int> part_rec(int v, std::int64_t ts, std::int32_t partition) {
    Record<int> r{v, EventTime{ts}};
    r.set_source_partition(partition);
    return r;
}

TEST(WatermarkStrategy, PartitionAwareEmitsMinAcrossPartitionsMinusBound) {
    PartitionAwareBoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{50});
    // Partition 0 races ahead to 1000, partition 1 only at 200. The watermark
    // must follow the SLOWEST partition (min = 200), not the fastest.
    s.on_record(part_rec(1, 1000, /*partition=*/0));
    s.on_record(part_rec(2, 200, /*partition=*/1));
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{150});  // min(1000,200) - 50
}

TEST(WatermarkStrategy, PartitionAwareAdvancesOnlyWhenTheSlowestPartitionAdvances) {
    PartitionAwareBoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{0});
    s.on_record(part_rec(1, 1000, 0));
    s.on_record(part_rec(2, 200, 1));
    ASSERT_EQ(s.current_watermark()->timestamp(), EventTime{200});  // min
    // Fastest partition races further - the min (slowest) is unchanged, so no
    // new watermark. This is the whole point: one fast partition cannot drag the
    // watermark past a slow one and strand the slow partition's in-window data.
    s.on_record(part_rec(3, 5000, 0));
    EXPECT_FALSE(s.current_watermark().has_value());
    // The slowest partition catches up - now the min advances.
    s.on_record(part_rec(4, 900, 1));
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{900});  // min(5000, 900)
}

TEST(WatermarkStrategy, PartitionAwareDegradesToGlobalWithoutPartition) {
    // Records with no source_partition (file / generator sources) fold into one
    // global bucket, so behaviour matches BoundedOutOfOrdernessStrategy exactly.
    PartitionAwareBoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{50});
    s.on_record(Record<int>{1, EventTime{1000}});  // no partition
    ASSERT_EQ(s.current_watermark()->timestamp(), EventTime{950});
    s.on_record(Record<int>{2, EventTime{500}});  // out of order, no regress
    EXPECT_FALSE(s.current_watermark().has_value());
    s.on_record(Record<int>{3, EventTime{2000}});
    ASSERT_EQ(s.current_watermark()->timestamp(), EventTime{1950});
}

TEST(WatermarkStrategy, PartitionAwareIdlePartitionExcludedFromMin) {
    PartitionAwareBoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{0});
    // Partition 1 lags at 200 and then goes quiet; partition 0 advances. Without
    // idleness the min is pinned to 200.
    s.on_record(part_rec(1, 1000, 0));
    s.on_record(part_rec(2, 200, 1));
    ASSERT_EQ(s.current_watermark()->timestamp(), EventTime{200});
    s.on_record(part_rec(3, 5000, 0));
    EXPECT_FALSE(s.current_watermark().has_value()) << "min still pinned to quiet partition 1";

    // The assigner marks partition 1 idle: it drops out of the min, which now
    // follows the still-active partition 0.
    s.set_idle_partitions({1});
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{5000}) << "watermark advances past the idle partition";
}

TEST(WatermarkStrategy, PartitionAwareIdlePartitionReactivatesOnRecord) {
    PartitionAwareBoundedOutOfOrdernessStrategy<int> s(std::chrono::milliseconds{0});
    s.on_record(part_rec(1, 1000, 0));
    s.on_record(part_rec(2, 200, 1));
    (void)s.current_watermark();
    s.set_idle_partitions({1});
    ASSERT_EQ(s.current_watermark()->timestamp(), EventTime{1000});  // 1 excluded, base=1000

    // Partition 1 produces again at 1500: it re-enters the min immediately. The
    // min is now min(p0=1000, p1=1500)=1000 - equal to the last emitted base, so
    // the monotonic guard yields no new watermark.
    s.on_record(part_rec(3, 1500, 1));  // re-activates partition 1
    EXPECT_FALSE(s.current_watermark().has_value());

    // Partition 0 races to 3000. The watermark advances only to 1500, proving
    // the re-activated partition 1 is back in (and constraining) the min.
    s.on_record(part_rec(4, 3000, 0));
    auto wm = s.current_watermark();
    ASSERT_TRUE(wm.has_value());
    EXPECT_EQ(wm->timestamp(), EventTime{1500}) << "re-activated p1 constrains the min";
}

TEST(WatermarkAssigner, EmitsProgressingWatermarkInline) {
    using KV = std::pair<std::string, int>;
    Dag dag;

    // Three batches, each one record. A monotonic strategy should produce
    // a watermark for each batch boundary as event time advances.
    std::vector<Record<KV>> input;
    input.emplace_back(Record<KV>{KV{"a", 1}, EventTime{100}});
    input.emplace_back(Record<KV>{KV{"a", 2}, EventTime{600}});
    input.emplace_back(Record<KV>{KV{"a", 3}, EventTime{1100}});

    auto src = std::make_shared<VectorSource<KV>>(std::move(input));
    auto assigner = std::make_shared<WatermarkAssignerOperator<KV>>(
        [](const KV&) { return EventTime{0}; },  // unused - events already have time
        std::make_unique<MonotonicWatermarkStrategy<KV>>());
    auto window = std::make_shared<TumblingWindowOperator<std::string, int, int>>(
        500ms, [] { return 0; }, [](const int& acc, const int& v) { return acc + v; });
    auto sink = std::make_shared<CollectingSink<std::pair<std::string, int>>>();

    auto h0 = dag.add_source<KV>(src);
    auto h1 = dag.add_operator<KV, KV>(h0, assigner);
    auto h2 = dag.add_operator<KV, std::pair<std::string, int>>(h1, window);
    dag.add_sink<std::pair<std::string, int>>(h2, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    // Windows: [0,500)=1, [500,1000)=2, [1000,1500)=3 - all should fire.
    int sum = 0;
    for (const auto& [k, v] : results) {
        EXPECT_EQ(k, "a");
        sum += v;
    }
    EXPECT_EQ(sum, 6);
    EXPECT_EQ(results.size(), 3u);
}
