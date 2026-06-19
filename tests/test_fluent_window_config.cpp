// Fluent window-handle tests. Two layers:
//
//   1. Graph-shape: fluent .tumbling_window / .sliding_window /
//      .session_window with .allowed_lateness + .late_output_tag
//      compiles, runs, and produces the expected inline op_type in
//      the JobGraphSpec.
//
//   2. Operator unit tests: the new
//      Keyed{Tumbling,Sliding,Session}WindowAggregateOperator
//      classes honor allowed_lateness + late_output_tag at the
//      operator level.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"

using namespace clink;
using namespace clink::api;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Graph shape via fluent API
// ---------------------------------------------------------------------------

TEST(FluentTumblingWindow, AppearsInGraphWithLatenessAndLateTagAccepted) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    OutputTag<std::int64_t> late_tag("late");
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t&) { return std::int64_t{0}; })
        .tumbling_window(1000ms)
        .allowed_lateness(500ms)
        .late_output_tag(late_tag)
        .aggregate<std::int64_t>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_tumbling_aggregate_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(FluentSlidingWindow, AppearsInGraphWithLatenessAndLateTagAccepted) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    OutputTag<std::int64_t> late_tag("late");
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t&) { return std::int64_t{0}; })
        .sliding_window(1000ms, 500ms)
        .allowed_lateness(200ms)
        .late_output_tag(late_tag)
        .aggregate<std::int64_t>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_sliding_aggregate_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(FluentSessionWindow, AppearsInGraphWithMergerAndLatenessAccepted) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t&) { return std::int64_t{0}; })
        .session_window(500ms)
        .allowed_lateness(100ms)
        .aggregate<std::int64_t>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
            [](const std::int64_t& a, const std::int64_t& b) { return a + b; });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_session_aggregate_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

// ---------------------------------------------------------------------------
// Operator unit tests - the underlying Keyed*WindowAggregateOperator
// classes the fluent API builds.
// ---------------------------------------------------------------------------

namespace {

template <typename T>
std::vector<T> drain_values(BoundedChannel<StreamElement<T>>& ch) {
    std::vector<T> out;
    while (auto e = ch.try_pop()) {
        if (!e->is_data()) {
            continue;
        }
        for (const auto& r : e->as_data()) {
            out.push_back(r.value());
        }
    }
    return out;
}

}  // namespace

TEST(KeyedTumblingWindowAggregateOperator, FiresOnTimeAndRefiresWithinLateness) {
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    api::detail::KeyedTumblingWindowAggregateOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        1000ms,
        "tumbling");
    op.set_allowed_lateness(500ms);

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    send(1, 100);
    send(2, 500);
    wm(1000);  // on-time pane fires (sum=3).
    auto first = drain_values<std::int64_t>(ch);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0], 3);

    // Late record within lateness band (purge_at=1500): re-fire.
    wm(1200);
    send(3, 200);
    auto refire = drain_values<std::int64_t>(ch);
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(refire[0], 6) << "1 + 2 + 3 = 6 on re-fire";
}

TEST(KeyedTumblingWindowAggregateOperator, RoutesPastLatenessToSideOutput) {
    BoundedChannel<StreamElement<std::int64_t>> main_ch(64);
    BoundedChannel<StreamElement<std::int64_t>> late_ch(64);
    Emitter<std::int64_t> main_em(&main_ch);

    api::detail::KeyedTumblingWindowAggregateOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        1000ms,
        "tumbling");
    op.set_allowed_lateness(500ms);
    OutputTag<std::int64_t> late_tag("late");
    op.set_late_output_tag(late_tag);

    RuntimeContext ctx(OperatorId{1}, "tumbling", nullptr, nullptr);
    SideOutputChannelMap channels;
    auto late_shared =
        std::shared_ptr<BoundedChannel<StreamElement<std::int64_t>>>(&late_ch, [](auto*) {});
    SideOutputChannelEntry entry;
    entry.channel = std::static_pointer_cast<void>(late_shared);
    entry.close_fn = [late_shared] { late_shared->close(); };
    channels.emplace("late", std::move(entry));
    ctx.set_side_output_channels(std::move(channels));
    op.attach_runtime(&ctx);

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), main_em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), main_em);
    };

    send(1, 100);
    wm(2000);  // past purge_at (1500): window purged.
    (void)drain_values<std::int64_t>(main_ch);

    send(99, 200);  // past lateness - routes to side output.
    auto late = drain_values<std::int64_t>(late_ch);
    ASSERT_EQ(late.size(), 1u);
    EXPECT_EQ(late[0], 99);
}

TEST(KeyedSlidingWindowAggregateOperator, FiresOnTimeAndRefiresWithinLateness) {
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    api::detail::KeyedSlidingWindowAggregateOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        1000ms,
        500ms,
        "sliding");
    op.set_allowed_lateness(200ms);

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    // Record at t=600 → windows [0, 1000) and [500, 1500). Choosing
    // t > slide_ms avoids the negative-start corner case in the
    // enumeration loop (which would only catch the start=0 window
    // for t < slide_ms).
    send(1, 600);
    wm(1000);  // [0, 1000) fires (end=1000 <= wm).
    auto first = drain_values<std::int64_t>(ch);
    EXPECT_EQ(first.size(), 1u);

    // Late record within lateness band on window [0, 1000):
    // purge_at = 1000 + 200 = 1200. wm=1100 < 1200, so re-fire.
    wm(1100);
    send(2, 700);
    auto refire = drain_values<std::int64_t>(ch);
    EXPECT_FALSE(refire.empty());
}

TEST(KeyedSessionWindowAggregateOperator, FiresOnTimeAndRefiresOnMergedLateRecord) {
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    api::detail::KeyedSessionWindowAggregateOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
        500ms,
        "session");
    op.set_allowed_lateness(500ms);

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    send(1, 100);
    wm(700);  // session at [100, 101], wm >= 101 + 500 = 601 → fires (agg=1).
    auto first = drain_values<std::int64_t>(ch);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0], 1);

    // Late record at t=200 within lateness band overlaps the fired
    // session (s.end=101, +gap=500 = 601 ≥ 200 → overlap). Re-fire
    // with merged agg = 1 + 2 = 3.
    send(2, 200);
    auto refire = drain_values<std::int64_t>(ch);
    ASSERT_EQ(refire.size(), 1u);
    EXPECT_EQ(refire[0], 3);
}

// ---------------------------------------------------------------------------
// .with_trigger via fluent API (graph shape)
// ---------------------------------------------------------------------------

TEST(FluentTumblingWindow, WithTriggerCompilesAndAppearsInGraph) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t&) { return std::int64_t{0}; })
        .tumbling_window(1000ms)
        .with_trigger([] { return std::make_unique<CountTrigger<std::int64_t>>(3); })
        .aggregate<std::int64_t>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_tumbling_aggregate_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(FluentSlidingWindow, WithTriggerCompilesAndAppearsInGraph) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t&) { return std::int64_t{0}; })
        .sliding_window(1000ms, 500ms)
        .with_trigger([] { return std::make_unique<CountTrigger<std::int64_t>>(2); })
        .aggregate<std::int64_t>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_sliding_aggregate_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

// ---------------------------------------------------------------------------
// Wrapper operator unit tests - exercises the fluent wrapper end-to-end
// with a CountTrigger to confirm trigger configuration flows through.
// ---------------------------------------------------------------------------

TEST(KeyedTumblingWindowFullOperator, CountTriggerFiresOnNthRecord) {
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    api::detail::KeyedTumblingWindowFullOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        [] { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& v) { return a + v; },
        10000ms,  // long window so EventTimeTrigger doesn't fire
        "tumbling_with_trigger");
    op.set_trigger(std::make_unique<CountTrigger<std::int64_t>>(3));

    RuntimeContext ctx(OperatorId{1}, "op", nullptr, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };

    send(1, 100);
    send(2, 200);
    // First two records: trigger hasn't fired yet (count=2 < 3).
    {
        std::vector<std::int64_t> emitted;
        while (auto e = ch.try_pop()) {
            if (e->is_data()) {
                for (const auto& r : e->as_data()) {
                    emitted.push_back(r.value());
                }
            }
        }
        EXPECT_TRUE(emitted.empty());
    }

    // Third record: trigger fires; count=3 → emit sum.
    send(3, 300);
    std::vector<std::int64_t> emitted;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                emitted.push_back(r.value());
            }
        }
    }
    ASSERT_EQ(emitted.size(), 1u);
    EXPECT_EQ(emitted[0], 6);  // 1 + 2 + 3
}

// ---------------------------------------------------------------------------
// Evicting fluent path
// ---------------------------------------------------------------------------

TEST(FluentEvictingTumbling, CompilesAndAppearsInGraph) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.from_elements<std::int64_t>({1, 2, 3, 4, 5});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t&) { return std::int64_t{0}; })
        .tumbling_window(1000ms)
        .evicting([] { return std::make_unique<CountEvictor<std::int64_t>>(3); })
        .allowed_lateness(200ms)
        .process<std::int64_t>(
            [](const std::vector<Record<std::int64_t>>& records, const TimeWindow&) {
                std::int64_t sum = 0;
                for (const auto& r : records) {
                    sum += r.value();
                }
                return sum;
            });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_evicting_tumbling_process_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(KeyedEvictingTumblingWindowFullOperator, EvictorFiltersRecordsBeforeProcessFn) {
    BoundedChannel<StreamElement<std::int64_t>> ch(64);
    Emitter<std::int64_t> em(&ch);

    // CountEvictor(3) keeps the most-recent 3 records.
    api::detail::KeyedEvictingTumblingWindowFullOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        1000ms,
        [](const std::vector<Record<std::int64_t>>& records, const TimeWindow&) {
            std::int64_t sum = 0;
            for (const auto& r : records) {
                sum += r.value();
            }
            return sum;
        },
        std::make_unique<CountEvictor<std::int64_t>>(3),
        "evicting_tumbling");

    RuntimeContext ctx(OperatorId{1}, "op", nullptr, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    // Five records in window [0, 1000); evictor keeps the LAST 3.
    send(1, 100);
    send(2, 200);
    send(3, 300);
    send(4, 400);
    send(5, 500);
    wm(1000);

    std::vector<std::int64_t> emitted;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                emitted.push_back(r.value());
            }
        }
    }
    ASSERT_EQ(emitted.size(), 1u);
    EXPECT_EQ(emitted[0], 3 + 4 + 5) << "evictor kept the last 3 records";
}

// ---------------------------------------------------------------------------
// Emit-form aggregate (key + window + agg -> Out) - exposes the key and
// window-end at emit time, matching the two-function aggregate overload
// other engines provide. All three window shapes (tumbling, sliding,
// session) get the new form.
// ---------------------------------------------------------------------------

namespace {

struct EmitTriple {
    std::int64_t window_end{0};
    std::int64_t key{0};
    std::int64_t sum{0};
};

}  // namespace

TEST(FluentTumblingWindowEmit, AppearsInGraphAndExposesKeyAndWindowEnd) {
    auto env = StreamExecutionEnvironment::create();
    env.registry().register_type<EmitTriple>(
        "test.EmitTriple",
        Codec<EmitTriple>{.encode = [](const EmitTriple&) { return std::vector<std::byte>{}; },
                          .decode = [](std::span<const std::byte>) -> std::optional<EmitTriple> {
                              return EmitTriple{};
                          }});
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t& v) { return v % 2; })
        .tumbling_window(1000ms)
        .aggregate<std::int64_t, EmitTriple>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
            [](std::int64_t k, const TimeWindow& w, const std::int64_t& acc) {
                return EmitTriple{w.end, k, acc};
            });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_tumbling_aggregate_emit_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(FluentSlidingWindowEmit, AppearsInGraph) {
    auto env = StreamExecutionEnvironment::create();
    env.registry().register_type<EmitTriple>(
        "test.EmitTriple",
        Codec<EmitTriple>{.encode = [](const EmitTriple&) { return std::vector<std::byte>{}; },
                          .decode = [](std::span<const std::byte>) -> std::optional<EmitTriple> {
                              return EmitTriple{};
                          }});
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t& v) { return v % 2; })
        .sliding_window(1000ms, 500ms)
        .aggregate<std::int64_t, EmitTriple>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
            [](std::int64_t k, const TimeWindow& w, const std::int64_t& acc) {
                return EmitTriple{w.end, k, acc};
            });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_sliding_aggregate_emit_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

TEST(FluentSessionWindowEmit, AppearsInGraph) {
    auto env = StreamExecutionEnvironment::create();
    env.registry().register_type<EmitTriple>(
        "test.EmitTriple",
        Codec<EmitTriple>{.encode = [](const EmitTriple&) { return std::vector<std::byte>{}; },
                          .decode = [](std::span<const std::byte>) -> std::optional<EmitTriple> {
                              return EmitTriple{};
                          }});
    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
        .key_by([](const std::int64_t& v) { return v % 2; })
        .session_window(500ms)
        .aggregate<std::int64_t, EmitTriple>(
            [] { return std::int64_t{0}; },
            [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
            [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
            [](std::int64_t k, const TimeWindow& w, const std::int64_t& acc) {
                return EmitTriple{w.end, k, acc};
            });

    bool found = false;
    for (const auto& op : env.graph().ops) {
        if (op.type.find("_inline_session_aggregate_emit_") != std::string::npos) {
            found = true;
        }
    }
    EXPECT_TRUE(found);
}

// Operator-level: the emit_fn receives the exact (key, TimeWindow, agg)
// the operator computed, and downstream sees those values folded into
// Out. Use TripleOp as a regression guard: changing the inner aggregator
// without threading key/window through correctly trips these.

TEST(KeyedTumblingWindowEmitOperator, EmitFnReceivesKeyAndWindowEnd) {
    BoundedChannel<StreamElement<EmitTriple>> ch(64);
    Emitter<EmitTriple> em(&ch);

    api::detail::KeyedTumblingWindowEmitOperator<std::int64_t, std::int64_t, EmitTriple> op(
        [](const std::int64_t& v) { return v % 2; },  // key = parity
        [] { return std::int64_t{0}; },               // initial sum
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        [](std::int64_t k, const TimeWindow& w, const std::int64_t& s) {
            return EmitTriple{w.end, k, s};
        },
        1000ms,
        "tumbling_emit");

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    // Window [0, 1000): key=1 sum = 1+3 = 4 ; key=0 sum = 2.
    send(1, 100);
    send(2, 200);
    send(3, 300);
    wm(1000);

    std::vector<EmitTriple> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    ASSERT_EQ(out.size(), 2u);
    // Order across keys is map iteration order - sort by key for the
    // assertion.
    std::sort(out.begin(), out.end(), [](const EmitTriple& a, const EmitTriple& b) {
        return a.key < b.key;
    });
    EXPECT_EQ(out[0].window_end, 1000);
    EXPECT_EQ(out[0].key, 0);
    EXPECT_EQ(out[0].sum, 2);
    EXPECT_EQ(out[1].window_end, 1000);
    EXPECT_EQ(out[1].key, 1);
    EXPECT_EQ(out[1].sum, 4);
}

TEST(KeyedSlidingWindowEmitOperator, EmitFnReceivesKeyAndWindowEnd) {
    BoundedChannel<StreamElement<EmitTriple>> ch(64);
    Emitter<EmitTriple> em(&ch);

    api::detail::KeyedSlidingWindowEmitOperator<std::int64_t, std::int64_t, EmitTriple> op(
        [](const std::int64_t&) { return std::int64_t{0}; },
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        [](std::int64_t k, const TimeWindow& w, const std::int64_t& s) {
            return EmitTriple{w.end, k, s};
        },
        1000ms,
        500ms,
        "sliding_emit");

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    // size=1000, slide=500. ts=100 falls in [0,1000) and [-500..500),
    // but only [0,1000) is non-negative. Each window's end is 500-aligned.
    send(1, 100);
    send(2, 600);
    send(3, 1100);
    wm(2000);

    std::vector<EmitTriple> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    // window ends are multiples of slide (500). All key=0.
    ASSERT_GE(out.size(), 1u);
    for (const auto& t : out) {
        EXPECT_EQ(t.key, 0);
        EXPECT_EQ(t.window_end % 500, 0);
    }
}

TEST(KeyedSessionWindowEmitOperator, EmitFnReceivesKeyAndSessionBoundary) {
    BoundedChannel<StreamElement<EmitTriple>> ch(64);
    Emitter<EmitTriple> em(&ch);

    api::detail::KeyedSessionWindowEmitOperator<std::int64_t, std::int64_t, EmitTriple> op(
        [](const std::int64_t&) { return std::int64_t{7}; },  // single key=7
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; },
        [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
        [](std::int64_t k, const TimeWindow& w, const std::int64_t& s) {
            return EmitTriple{w.end, k, s};
        },
        500ms,
        "session_emit");

    auto send = [&](std::int64_t v, std::int64_t ts) {
        Batch<std::int64_t> b;
        b.emplace(v, EventTime{ts});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), em);
    };
    auto wm = [&](std::int64_t ts) {
        op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{ts}}), em);
    };

    // First session: ts=100, 300 (within 500ms gap). end = 300 + 1 = 301.
    send(1, 100);
    send(2, 300);
    // Gap > 500ms -> second session: ts=1000. end = 1000 + 1 = 1001.
    send(3, 1000);
    // Watermark past second session_end + gap: both fire.
    wm(2000);

    std::vector<EmitTriple> out;
    while (auto e = ch.try_pop()) {
        if (e->is_data()) {
            for (const auto& r : e->as_data()) {
                out.push_back(r.value());
            }
        }
    }
    ASSERT_EQ(out.size(), 2u);
    std::sort(out.begin(), out.end(), [](const EmitTriple& a, const EmitTriple& b) {
        return a.window_end < b.window_end;
    });
    EXPECT_EQ(out[0].key, 7);
    EXPECT_EQ(out[0].window_end, 301);
    EXPECT_EQ(out[0].sum, 3);  // 1 + 2
    EXPECT_EQ(out[1].key, 7);
    EXPECT_EQ(out[1].window_end, 1001);
    EXPECT_EQ(out[1].sum, 3);  // just record 3
}
