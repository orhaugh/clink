#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"

#include "test_helpers/sanitizer_slack.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

struct Order {
    std::string user_id;
    std::string sku;
};

struct Click {
    std::string user_id;
    std::string url;
};

struct Joined {
    std::string user_id;
    std::string sku;
    std::string url;
};

}  // namespace

TEST(IntervalJoin, BasicInnerJoinOnKeyAndOverlappingInterval) {
    Dag dag;

    // Click stream
    std::vector<Record<Click>> clicks{
        Record<Click>{{"u1", "/a"}, EventTime{100}},
        Record<Click>{{"u2", "/b"}, EventTime{200}},
        Record<Click>{{"u1", "/c"}, EventTime{300}},
        Record<Click>{{"u3", "/d"}, EventTime{400}},
    };
    // Order stream
    std::vector<Record<Order>> orders{
        Record<Order>{
            {"u1", "shoe"},
            EventTime{120}},  // joins click@100 (delta=20) and click@300 (delta=-180? out)
        Record<Order>{{"u2", "shirt"}, EventTime{500}},  // delta=300 from click@200, out of bounds
        Record<Order>{{"u1", "hat"},
                      EventTime{280}},  // joins click@100 (delta=180) and click@300 (delta=20)
    };

    auto src_clicks = std::make_shared<VectorSource<Click>>(std::move(clicks), "clicks");
    auto src_orders = std::make_shared<VectorSource<Order>>(std::move(orders), "orders");

    auto h_clicks = dag.add_source<Click>(src_clicks);
    auto h_orders = dag.add_source<Order>(src_orders);

    // Window: order matches click within [-50, +200]ms (i.e. order arrives 0-200ms
    // after the click, or up to 50ms before).
    auto h_join = dag.interval_join<Click, Order, std::string, Joined>(
        h_clicks,
        h_orders,
        [](const Click& c) { return c.user_id; },
        [](const Order& o) { return o.user_id; },
        50ms,
        200ms,
        [](const std::optional<Click>& c, const std::optional<Order>& o) {
            return Joined{c->user_id, o->sku, c->url};
        });

    auto sink = std::make_shared<CollectingSink<Joined>>();
    dag.add_sink<Joined>(h_join, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    // Expected joins (click_t, order_t, delta = order - click):
    //   u1 click@100, u1 order@120 → delta=20    ✓ (in [-50, 200])
    //   u1 click@100, u1 order@280 → delta=180   ✓
    //   u1 click@300, u1 order@120 → delta=-180  ✗ (< -50)
    //   u1 click@300, u1 order@280 → delta=-20   ✓ (in [-50, 200])
    //   u2 click@200, u2 order@500 → delta=300   ✗ (> 200)
    //   u3 click@400 → no order on u3
    //
    // Three matches expected; their order in `results` depends on interleaving.
    std::vector<std::tuple<std::string, std::string, std::string>> seen;
    for (const auto& j : results) {
        seen.emplace_back(j.user_id, j.url, j.sku);
    }
    std::sort(seen.begin(), seen.end());

    std::vector<std::tuple<std::string, std::string, std::string>> expected{
        {"u1", "/a", "hat"},
        {"u1", "/a", "shoe"},
        {"u1", "/c", "hat"},
    };
    std::sort(expected.begin(), expected.end());
    EXPECT_EQ(seen, expected);
}

TEST(IntervalJoin, RecordsOutsideIntervalAreSkipped) {
    Dag dag;

    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{1000}},
    };
    // Both b records share the key (we'll use a single key by extracting 0)
    // and have timestamps far outside the [t_a-100, t_a+100] window.
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{500}},   // delta=-500, out of [-100, 100]
        Record<int>{20, EventTime{2000}},  // delta=+1000, out
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        100ms,
        100ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });

    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(sink->collected().empty());
}

TEST(IntervalJoin, WatermarkEvictsStaleBufferEntries) {
    // Stream A delivers a single record then a max-watermark; the watermark
    // should clear the per-key buffer so a late B record arriving just for
    // that key finds no match. (Without eviction, we'd still emit a join.)
    Dag dag;

    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{0}},
    };
    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");

    // Right source emits the data first, but only AFTER a deliberate delay.
    // We can't easily express "deliver after the watermark" with VectorSource,
    // so we instead use a generator source whose first call sleeps briefly
    // to allow the left's watermark to flow through and trigger eviction.
    std::atomic<int> step{0};
    auto gen = [&]() -> std::optional<Record<int>> {
        if (step.fetch_add(1) == 0) {
            // First call: wait for left's watermark to reach the join's
            // eviction threshold.
            std::this_thread::sleep_for(50ms);
            return Record<int>{2, EventTime{50}};
        }
        return std::nullopt;
    };
    auto src_b = std::make_shared<GeneratorSource<int>>(gen, "b");

    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    // Window of [-10, +10] ms. After A@0 arrives and A's watermark hits max,
    // A@0 is evictable (0 + 10 < max). The late B@50 finds no left buffer
    // entry to match.
    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        10ms,
        10ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });

    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // No joins: A was evicted before B arrived.
    EXPECT_TRUE(sink->collected().empty());
}

// ---------------------------------------------------------------------------
// Outer-join tests
// ---------------------------------------------------------------------------

namespace {

struct OuterResult {
    std::optional<int> a;
    std::optional<int> b;
    bool operator==(const OuterResult&) const = default;
};

}  // namespace

TEST(IntervalJoin, LeftOuterEmitsUnmatchedLeftRecords) {
    Dag dag;
    // Left has two records, only one of which has a matching right.
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{100}},  // matches b@110 (delta=10)
        Record<int>{2, EventTime{500}},  // no match
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{110}},
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, OuterResult>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::LeftOuter);

    auto sink = std::make_shared<CollectingSink<OuterResult>>();
    dag.add_sink<OuterResult>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    std::sort(results.begin(), results.end(), [](const auto& x, const auto& y) {
        return x.a.value_or(0) < y.a.value_or(0);
    });

    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0], (OuterResult{1, 10}));            // matched
    EXPECT_EQ(results[1], (OuterResult{2, std::nullopt}));  // unmatched left
}

TEST(IntervalJoin, RightOuterEmitsUnmatchedRightRecords) {
    Dag dag;
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{100}},
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{110}},  // matches a@100
        Record<int>{20, EventTime{500}},  // no match
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, OuterResult>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::RightOuter);

    auto sink = std::make_shared<CollectingSink<OuterResult>>();
    dag.add_sink<OuterResult>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    std::sort(results.begin(), results.end(), [](const auto& x, const auto& y) {
        return x.b.value_or(0) < y.b.value_or(0);
    });

    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0], (OuterResult{1, 10}));             // matched
    EXPECT_EQ(results[1], (OuterResult{std::nullopt, 20}));  // unmatched right
}

TEST(IntervalJoin, FullOuterEmitsUnmatchedOnBothSides) {
    Dag dag;
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{100}},  // matches b@110
        Record<int>{2, EventTime{800}},  // no match
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{110}},  // matches a@100
        Record<int>{20, EventTime{300}},  // no match
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, OuterResult>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::FullOuter);

    auto sink = std::make_shared<CollectingSink<OuterResult>>();
    dag.add_sink<OuterResult>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    // Three emissions expected: one inner match, one unmatched left, one
    // unmatched right.
    bool saw_match = false;
    bool saw_unmatched_a = false;
    bool saw_unmatched_b = false;
    for (const auto& r : results) {
        if (r.a == 1 && r.b == 10) {
            saw_match = true;
        } else if (r.a == 2 && !r.b.has_value()) {
            saw_unmatched_a = true;
        } else if (!r.a.has_value() && r.b == 20) {
            saw_unmatched_b = true;
        }
    }
    EXPECT_TRUE(saw_match);
    EXPECT_TRUE(saw_unmatched_a);
    EXPECT_TRUE(saw_unmatched_b);
    EXPECT_EQ(results.size(), 3u);
}

TEST(IntervalJoin, LeftSemiEmitsEachLeftOnceWhenAnyMatchExists) {
    Dag dag;
    // Left A=1 has multiple matching right partners; A=2 has none.
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{100}},
        Record<int>{2, EventTime{500}},
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{105}},  // matches a=1
        Record<int>{20, EventTime{120}},  // matches a=1 (delta=20)
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, OuterResult>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::LeftSemi);

    auto sink = std::make_shared<CollectingSink<OuterResult>>();
    dag.add_sink<OuterResult>(h_j, sink);
    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    // Exactly one emission: a=1 was matched (twice but emitted once); a=2
    // had no match so not emitted.
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].a, std::optional<int>{1});
    EXPECT_FALSE(results[0].b.has_value());
}

TEST(IntervalJoin, LeftAntiEmitsOnlyUnmatchedLeftRecords) {
    Dag dag;
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{100}},  // matched
        Record<int>{2, EventTime{500}},  // unmatched
        Record<int>{3, EventTime{900}},  // unmatched
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{120}},
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, OuterResult>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::LeftAnti);

    auto sink = std::make_shared<CollectingSink<OuterResult>>();
    dag.add_sink<OuterResult>(h_j, sink);
    LocalExecutor exec(std::move(dag));
    exec.run();

    auto results = sink->collected();
    std::sort(results.begin(), results.end(), [](const auto& x, const auto& y) {
        return x.a.value_or(0) < y.a.value_or(0);
    });

    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0], (OuterResult{2, std::nullopt}));
    EXPECT_EQ(results[1], (OuterResult{3, std::nullopt}));
}

// ---------------------------------------------------------------------------
// Late-arrival policy
// ---------------------------------------------------------------------------

namespace {

// Emits Watermark{1000} after a short delay (so the upstream's
// max-watermark can reach the join's input-0 first), waits for the join's
// running min to advance, then emits a late record at t=0. Closes on the
// next produce() call.
//
// The 40ms warmup is enough under a normal build for the LEFT source's
// Watermark::max() to traverse channels and reach the join's
// MultiInputAlignment as input-0's watermark. Under sanitizer
// instrumentation (5-10x slowdown) the propagation takes longer, so we
// scale the sleeps via the test slack helper - otherwise the join's
// running min stays at 0 when the late record arrives, the late check
// passes, and the drop counter never fires.
class WatermarkThenLateRightSource final : public Source<int> {
public:
    bool produce(Emitter<int>& out) override {
        if (this->cancelled() || done_) {
            return false;
        }
        std::this_thread::sleep_for(clink::test_support::scale_slack(40ms));
        out.emit_watermark(Watermark{EventTime{1000}});
        std::this_thread::sleep_for(clink::test_support::scale_slack(40ms));
        Batch<int> b;
        b.emplace(99, EventTime{0});  // 0 + lower(50) < running_wm(1000) → late
        out.emit_data(std::move(b));
        done_ = true;
        return false;
    }

    std::string name() const override { return "right"; }

private:
    bool done_{false};
};

}  // namespace

TEST(IntervalJoin, DropLatePolicySkipsRecordsBelowWatermark) {
    Dag dag;

    // Empty A source - VectorSource emits empty data + Watermark::max() and
    // closes immediately, taking input-0 out of the min computation.
    auto src_a = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{}, "left");
    auto src_b = std::make_shared<WatermarkThenLateRightSource>();

    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        200ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        },
        Dag::JoinType::Inner,
        "late_drop_join",
        std::nullopt,
        std::nullopt,
        std::nullopt,
        Dag::LateArrivalPolicy::Drop);

    const OperatorId join_id = dag.runners()[h_j.runner_index].id;

    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    MetricsRegistry metrics;
    JobConfig cfg;
    cfg.metrics = &metrics;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // No left side at all → no joins regardless of policy. The interesting
    // assertion is on the counter: the late right record must have been
    // dropped via the policy path, not via post-insertion eviction.
    EXPECT_TRUE(sink->collected().empty());
    auto& dropped =
        metrics.counter("interval_join." + std::to_string(join_id.value()) + ".late_dropped.right");
    EXPECT_EQ(dropped.value(), 1u);
}

TEST(IntervalJoin, AllowPolicyKeepsBackwardsCompatibleBehaviour) {
    // Identical pipeline shape to the Drop test, but with the default Allow
    // policy: the late record gets inserted, immediately evicted, and the
    // drop counter stays at zero.
    Dag dag;

    auto src_a = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{1, EventTime{500}}}, "a");
    auto src_b = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{99, EventTime{0}}}, "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        200ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });
    const OperatorId join_id = dag.runners()[h_j.runner_index].id;

    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    MetricsRegistry metrics;
    JobConfig cfg;
    cfg.metrics = &metrics;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto& dropped =
        metrics.counter("interval_join." + std::to_string(join_id.value()) + ".late_dropped.right");
    EXPECT_EQ(dropped.value(), 0u);
}

TEST(IntervalJoin, RightSemiAndRightAntiAreSymmetric) {
    Dag dag;
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{100}},
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{120}},  // matched
        Record<int>{20, EventTime{500}},  // unmatched
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_semi = dag.interval_join<int, int, int, OuterResult>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::RightSemi,
        "right_semi");
    auto sink_semi = std::make_shared<CollectingSink<OuterResult>>();
    dag.add_sink<OuterResult>(h_semi, sink_semi);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto semi = sink_semi->collected();
    ASSERT_EQ(semi.size(), 1u);
    EXPECT_EQ(semi[0].b, std::optional<int>{10});  // matched right emitted

    // Run a second pipeline for the anti case so the two sources are fresh.
    Dag dag2;
    std::vector<Record<int>> a2{Record<int>{1, EventTime{100}}};
    std::vector<Record<int>> b2{
        Record<int>{10, EventTime{120}},
        Record<int>{20, EventTime{500}},
    };
    auto src_a2 = std::make_shared<VectorSource<int>>(std::move(a2), "a");
    auto src_b2 = std::make_shared<VectorSource<int>>(std::move(b2), "b");
    auto h_a2 = dag2.add_source<int>(src_a2);
    auto h_b2 = dag2.add_source<int>(src_b2);
    auto h_anti = dag2.interval_join<int, int, int, OuterResult>(
        h_a2,
        h_b2,
        [](int) { return 0; },
        [](int) { return 0; },
        50ms,
        50ms,
        [](const std::optional<int>& a, const std::optional<int>& b) { return OuterResult{a, b}; },
        Dag::JoinType::RightAnti,
        "right_anti");
    auto sink_anti = std::make_shared<CollectingSink<OuterResult>>();
    dag2.add_sink<OuterResult>(h_anti, sink_anti);
    LocalExecutor exec2(std::move(dag2));
    exec2.run();

    auto anti = sink_anti->collected();
    ASSERT_EQ(anti.size(), 1u);
    EXPECT_EQ(anti[0].b, std::optional<int>{20});  // unmatched right emitted
}

// Window of zero width: only records sharing a key AND an exact event-time
// match. Pinning this avoids accidental "<=" → "<" regressions and gives
// users a way to express "exact time alignment" semantics.
TEST(IntervalJoin, ZeroWidthIntervalMatchesOnlyExactTimestamps) {
    Dag dag;
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{1000}},
        Record<int>{2, EventTime{2000}},
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{1000}},  // exact match for a=1
        Record<int>{20, EventTime{1500}},  // 500ms off - must miss
        Record<int>{30, EventTime{2000}},  // exact match for a=2
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    // Single shared key (key extractor returns 0); window = [0, 0].
    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        0ms,
        0ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });
    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    EXPECT_EQ(out, (std::vector<std::pair<int, int>>{{1, 10}, {2, 30}}));
}

// Many-to-many within a single window: every left record matches every
// right record sharing the key. Pins join cardinality semantics -
// regressions that drop duplicates would silently lose data.
TEST(IntervalJoin, ManyToManyWithinWindowProducesAllPairs) {
    Dag dag;
    std::vector<Record<int>> a_records;
    std::vector<Record<int>> b_records;
    a_records.reserve(4);
    b_records.reserve(3);
    for (int i = 0; i < 4; ++i) {
        a_records.emplace_back(Record<int>{100 + i, EventTime{1000}});
    }
    for (int i = 0; i < 3; ++i) {
        b_records.emplace_back(Record<int>{200 + i, EventTime{1000}});
    }

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        100ms,
        100ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });
    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    EXPECT_EQ(out.size(), 12u);  // 4 left × 3 right

    // Every (left, right) pair appears exactly once.
    std::sort(out.begin(), out.end());
    auto last = std::unique(out.begin(), out.end());
    EXPECT_EQ(last, out.end());  // no duplicates
}

// Records sharing a value but with different keys must not cross-match.
// Pins the per-key isolation of the buffer.
TEST(IntervalJoin, DifferentKeysAreIsolated) {
    Dag dag;
    std::vector<Record<int>> a_records{
        Record<int>{1, EventTime{1000}},  // key 0
        Record<int>{2, EventTime{1000}},  // key 1
    };
    std::vector<Record<int>> b_records{
        Record<int>{10, EventTime{1000}},  // key 0
        Record<int>{20, EventTime{1000}},  // key 1
    };

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    // Key = value % 2 - A: 1,2 → keys 1,0; B: 10,20 → keys 0,0. So a=2(k=0)
    // must match both b records (both k=0); a=1(k=1) must match nothing.
    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int v) { return v % 2; },
        [](int v) { return v % 2; },
        100ms,
        100ms,
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });
    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    std::sort(out.begin(), out.end());
    EXPECT_EQ(out, (std::vector<std::pair<int, int>>{{2, 10}, {2, 20}}));
}

// Reversed window bounds - lower=100, upper=-100 - describes an empty
// interval. The contract is: never match anything. (We don't promise a
// throw; we promise no spurious matches and no crash.)
TEST(IntervalJoin, ReversedBoundsProduceNoMatches) {
    Dag dag;
    std::vector<Record<int>> a_records{Record<int>{1, EventTime{1000}}};
    std::vector<Record<int>> b_records{Record<int>{10, EventTime{1000}}};

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_records), "a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_records), "b");
    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);

    // lower=+100ms, upper=-100ms → delta must satisfy +100 ≤ delta ≤ -100,
    // which is unsatisfiable.
    auto h_j = dag.interval_join<int, int, int, std::pair<int, int>>(
        h_a,
        h_b,
        [](int) { return 0; },
        [](int) { return 0; },
        -100ms,  // upper bound (the API takes lower_minus, upper_plus)
        -100ms,  // both negative → reversed window
        [](const std::optional<int>& a, const std::optional<int>& b) {
            return std::make_pair(*a, *b);
        });
    auto sink = std::make_shared<CollectingSink<std::pair<int, int>>>();
    dag.add_sink<std::pair<int, int>>(h_j, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_TRUE(sink->collected().empty());
}

// ---------------------------------------------------------------------------
// Unaligned-mode correctness at interval_join (the canonical // stateful multi-input operator). The
// capture path persists in-flight records from the not-yet-aligned channel into state at
// first-barrier time; the restore path pushes them back into the input channels at runner startup.
// Combined, restarts from a snapshot pick up exactly the records that were in flight at the
// unaligned barrier moment.
// ---------------------------------------------------------------------------

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

class IntsThenBarrierSource final : public Source<std::int64_t> {
public:
    IntsThenBarrierSource(std::vector<Record<std::int64_t>> records, CheckpointBarrier b)
        : records_(std::move(records)), barrier_(b) {}

    bool produce(Emitter<std::int64_t>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_data_) {
            Batch<std::int64_t> batch;
            for (const auto& r : records_) {
                batch.push(r);
            }
            out.emit_data(std::move(batch));
            emitted_data_ = true;
            return true;
        }
        if (!emitted_barrier_) {
            out.emit_barrier(barrier_);
            emitted_barrier_ = true;
            std::this_thread::sleep_for(50ms);
            return false;
        }
        return false;
    }

    bool emit_terminal_barrier_on_exit() const noexcept override { return false; }
    std::string name() const override { return "ints_then_barrier"; }

private:
    std::vector<Record<std::int64_t>> records_;
    CheckpointBarrier barrier_;
    bool emitted_data_{false};
    bool emitted_barrier_{false};
};

class IntsThenIdleSource final : public Source<std::int64_t> {
public:
    explicit IntsThenIdleSource(std::vector<Record<std::int64_t>> records)
        : records_(std::move(records)) {}

    bool produce(Emitter<std::int64_t>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            Batch<std::int64_t> batch;
            for (const auto& r : records_) {
                batch.push(r);
            }
            out.emit_data(std::move(batch));
            emitted_ = true;
        }
        std::this_thread::sleep_for(5ms);
        return true;
    }

    bool emit_terminal_barrier_on_exit() const noexcept override { return false; }
    std::string name() const override { return "ints_then_idle"; }

private:
    std::vector<Record<std::int64_t>> records_;
    bool emitted_{false};
};

}  // namespace

TEST(IntervalJoinUnaligned, BarrierFromLeftCapturesRightInflightIntoState) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    Dag dag;
    auto left_src = std::make_shared<IntsThenBarrierSource>(
        std::vector<Record<std::int64_t>>{Record<std::int64_t>{1, EventTime{100}}},
        CheckpointBarrier{CheckpointId{77}});
    auto right_src = std::make_shared<IntsThenIdleSource>(std::vector<Record<std::int64_t>>{
        Record<std::int64_t>{10, EventTime{110}},
        Record<std::int64_t>{11, EventTime{120}},
        Record<std::int64_t>{12, EventTime{130}},
    });
    auto h_left = dag.add_source<std::int64_t>(left_src);
    auto h_right = dag.add_source<std::int64_t>(right_src);

    auto h_join = dag.interval_join<std::int64_t,
                                    std::int64_t,
                                    std::int64_t,
                                    std::pair<std::int64_t, std::int64_t>>(
        h_left,
        h_right,
        [](const std::int64_t&) -> std::int64_t { return 0; },
        [](const std::int64_t&) -> std::int64_t { return 0; },
        100ms,
        100ms,
        [](const std::optional<std::int64_t>& a, const std::optional<std::int64_t>& b)
            -> std::pair<std::int64_t, std::int64_t> { return {a.value_or(-1), b.value_or(-1)}; },
        Dag::JoinType::Inner,
        "test_join",
        int64_codec(),
        int64_codec(),
        int64_codec());

    auto sink = std::make_shared<CollectingSink<std::pair<std::int64_t, std::int64_t>>>();
    dag.add_sink<std::pair<std::int64_t, std::int64_t>>(h_join, sink);

    OperatorId join_id{0};
    for (const auto& r : dag.runners()) {
        if (r.name == "test_join") {
            join_id = r.id;
            break;
        }
    }
    ASSERT_NE(join_id.value(), 0u);

    JobConfig cfg;
    cfg.state_backend = backend;
    cfg.unaligned_checkpoints = true;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();

    bool slot_seen = false;
    const auto deadline = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < deadline && !slot_seen) {
        backend->scan(join_id, [&](StateBackend::KeyView k, StateBackend::ValueView) {
            if (k == "__interval_join_right_inflight__") {
                slot_seen = true;
            }
        });
        if (!slot_seen) {
            std::this_thread::sleep_for(10ms);
        }
    }
    exec.cancel();
    exec.await_termination();

    EXPECT_TRUE(slot_seen) << "unaligned-mode interval_join should have captured right-channel "
                              "in-flight records into the state backend";
}

TEST(IntervalJoinUnaligned, RestoreReplaysCapturedInflightRecordsIntoChannels) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    // Derive the join's operator id from a probe DAG with identical shape.
    OperatorId join_id{0};
    {
        Dag probe;
        auto src_l =
            std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{});
        auto src_r =
            std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{});
        auto h_l = probe.add_source<std::int64_t>(src_l);
        auto h_r = probe.add_source<std::int64_t>(src_r);
        (void)probe.interval_join<std::int64_t,
                                  std::int64_t,
                                  std::int64_t,
                                  std::pair<std::int64_t, std::int64_t>>(
            h_l,
            h_r,
            [](const std::int64_t&) -> std::int64_t { return 0; },
            [](const std::int64_t&) -> std::int64_t { return 0; },
            100ms,
            100ms,
            [](const std::optional<std::int64_t>& a,
               const std::optional<std::int64_t>& b) -> std::pair<std::int64_t, std::int64_t> {
                return {a.value_or(-1), b.value_or(-1)};
            },
            Dag::JoinType::Inner,
            "test_join_restore",
            int64_codec(),
            int64_codec(),
            int64_codec());
        for (const auto& r : probe.runners()) {
            if (r.name == "test_join_restore") {
                join_id = r.id;
                break;
            }
        }
    }
    ASSERT_NE(join_id.value(), 0u);

    // Stash one right-side record under the inflight slot.
    {
        std::vector<Record<std::int64_t>> persist{Record<std::int64_t>{99, EventTime{150}}};
        auto bytes = Dag::serialize_records_(persist, int64_codec());
        backend->put(
            join_id,
            StateBackend::KeyView{"__interval_join_right_inflight__"},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    Dag dag;
    auto src_l = std::make_shared<VectorSource<std::int64_t>>(
        std::vector<Record<std::int64_t>>{Record<std::int64_t>{7, EventTime{100}}});
    auto src_r = std::make_shared<VectorSource<std::int64_t>>(std::vector<Record<std::int64_t>>{});
    auto h_l = dag.add_source<std::int64_t>(src_l);
    auto h_r = dag.add_source<std::int64_t>(src_r);
    auto h_join = dag.interval_join<std::int64_t,
                                    std::int64_t,
                                    std::int64_t,
                                    std::pair<std::int64_t, std::int64_t>>(
        h_l,
        h_r,
        [](const std::int64_t&) -> std::int64_t { return 0; },
        [](const std::int64_t&) -> std::int64_t { return 0; },
        100ms,
        100ms,
        [](const std::optional<std::int64_t>& a, const std::optional<std::int64_t>& b)
            -> std::pair<std::int64_t, std::int64_t> { return {a.value_or(-1), b.value_or(-1)}; },
        Dag::JoinType::Inner,
        "test_join_restore",
        int64_codec(),
        int64_codec(),
        int64_codec());
    auto sink = std::make_shared<CollectingSink<std::pair<std::int64_t, std::int64_t>>>();
    dag.add_sink<std::pair<std::int64_t, std::int64_t>>(h_join, sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    cfg.unaligned_checkpoints = true;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    auto results = sink->collected();
    ASSERT_EQ(results.size(), 1u) << "left record should join against the restored right record";
    EXPECT_EQ(results[0].first, 7);
    EXPECT_EQ(results[0].second, 99);
}
