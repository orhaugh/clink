// Unit tests for the StreamExecutionEnvironment / DataStream<T> fluent
// API. These verify the IR the env builds matches what hand-written
// JobGraphSpecs used to look like and that the planner is happy with
// the result, without any user-facing JSON in sight.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/cluster/job_graph.hpp"
#include "clink/cluster/job_planner.hpp"
#include "clink/cluster/operator_registry.hpp"
#include "clink/cluster/runner_registry.hpp"

namespace {

using namespace clink;
using namespace clink::api;

TEST(StreamEnvBuilder, SourceAndSinkAppendIntoJobGraphSpec) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(5).start(100).build());
    src.sink(FileInt64Sink::builder().path("/tmp/clink_env_test.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 2u);
    EXPECT_EQ(graph.ops[0].type, "int64_range_source");
    EXPECT_EQ(graph.ops[0].out_channel, "int64");
    EXPECT_EQ(graph.ops[0].params.at("count"), "5");
    EXPECT_EQ(graph.ops[0].params.at("start"), "100");

    EXPECT_EQ(graph.ops[1].type, "file_int64_sink");
    EXPECT_EQ(graph.ops[1].inputs, std::vector<std::string>{graph.ops[0].id});
    EXPECT_EQ(graph.ops[1].params.at("path"), "/tmp/clink_env_test.out");
}

TEST(StreamEnvBuilder, ExpectStateVersionRecordsSlotInGraph) {
    // The 4-arg expect_state_version overload must land the slot on the
    // graph's expected map (keyed by operator_id_from_uid). get() is
    // slot-blind, so assert via entries().
    auto env = StreamExecutionEnvironment::create();
    env.expect_state_version("join", "JoinLeft", 2, "left_buf");

    const auto entries = env.graph().expected_state_versions.entries();
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].op_id, clink::operator_id_from_uid("join"));
    EXPECT_EQ(entries[0].state_type, "JoinLeft");
    EXPECT_EQ(entries[0].version, 2u);
    EXPECT_EQ(entries[0].slot, "left_buf");
}

TEST(StreamEnvBuilder, TransformChainsTypedStages) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto evens = src.transform<std::int64_t>("even_filter_int64");
    auto strs = evens.transform<std::string>("int64_to_string");
    strs.sink(FileTextSink::builder().path("/tmp/clink_chain_test.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 4u);
    EXPECT_EQ(graph.ops[0].out_channel, "int64");
    EXPECT_EQ(graph.ops[1].type, "even_filter_int64");
    EXPECT_EQ(graph.ops[1].out_channel, "int64");
    EXPECT_EQ(graph.ops[2].type, "int64_to_string");
    EXPECT_EQ(graph.ops[2].out_channel, "string");
    EXPECT_EQ(graph.ops[3].type, "file_text_sink");
}

TEST(StreamEnvBuilder, BuiltGraphPlansCleanly) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).build());
    src.transform<std::int64_t>("multiply_int64", {{"factor", "10"}})
        .sink(FileInt64Sink::builder().path("/tmp/clink_plan_test.out").build());

    cluster::ensure_built_ins_registered();
    // plan_job requires registered factories; the planner finds them via
    // the runner registry (which built-ins are registered against).
    EXPECT_NO_THROW(
        { cluster::plan_job(env.graph(), cluster::OperatorRegistry::default_instance()); });
}

TEST(StreamEnvBuilder, ParallelismOnSourceFlowsThroughGraph) {
    auto env = StreamExecutionEnvironment::create();
    env.source<std::int64_t>(IntRangeSource::builder().count(10).parallelism(3).build())
        .sink(FileInt64Sink::builder().path("/tmp/clink_par_test.out").parallelism(3).build());
    const auto& graph = env.graph();
    EXPECT_EQ(graph.ops[0].parallelism, 3u);
    EXPECT_EQ(graph.ops[1].parallelism, 3u);
}

TEST(StreamEnvBuilder, EmptyGraphExecutionThrows) {
    auto env = StreamExecutionEnvironment::create();
    application::JobSubmitter submitter("127.0.0.1", 1);
    EXPECT_THROW(env.execute("noop", submitter, {}), std::runtime_error);
}

TEST(StreamEnvBuilder, DuplicateExplicitIdThrows) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(1).build(), "src");
    // Re-using "src" as the next op's id collides with the source.
    EXPECT_THROW(src.sink(FileInt64Sink::builder().path("/tmp/x").build(), "src"),
                 std::runtime_error);
}

// ClickHouse and S3 builder-descriptor tests moved to
// impls/clickhouse/tests/ and impls/s3/tests/ respectively.

TEST(StreamEnvBuilder, InlineMapMintsOpTypeAndChainsTransform) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto mapped = src.map<std::int64_t>([](const std::int64_t& v) { return v + 1; });
    mapped.sink(FileInt64Sink::builder().path("/tmp/clink_inline_map.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_EQ(graph.ops[0].type, "int64_range_source");
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_map_"));
    EXPECT_EQ(graph.ops[1].out_channel, "int64");
    EXPECT_EQ(graph.ops[2].type, "file_int64_sink");

    // The minted op-type must be reachable through the runner registry -
    // that's what the planner / TM will consult when the job runs.
    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_operator(graph.ops[1].type, "int64", "int64"), nullptr);
}

TEST(StreamEnvBuilder, InlineFlatMapRegistersWithRunnerRegistry) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto flat = src.flat_map<std::string>([](const std::int64_t& v) {
        return std::vector<std::string>{std::to_string(v), "x" + std::to_string(v)};
    });
    flat.sink(FileTextSink::builder().path("/tmp/clink_inline_flatmap.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_flat_map_"));
    EXPECT_EQ(graph.ops[1].out_channel, "string");
    EXPECT_EQ(graph.ops[2].type, "file_text_sink");

    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_operator(graph.ops[1].type, "int64", "string"), nullptr);
}

TEST(StreamEnvBuilder, InlineFilterRegistersWithRunnerRegistry) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(10).build());
    auto filtered = src.filter([](const std::int64_t& v) { return v % 2 == 0; });
    filtered.sink(FileInt64Sink::builder().path("/tmp/clink_inline_filter.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_filter_"));
    EXPECT_EQ(graph.ops[1].out_channel, "int64");

    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_operator(graph.ops[1].type, "int64", "int64"), nullptr);
}

TEST(StreamEnvBuilder, InlineMapNamesAreUniqueAcrossCalls) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(1).build());
    auto a = src.map<std::int64_t>([](const std::int64_t& v) { return v; });
    auto b = a.map<std::int64_t>([](const std::int64_t& v) { return v; });
    b.sink(FileInt64Sink::builder().path("/tmp/clink_inline_unique.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 4u);
    EXPECT_NE(graph.ops[1].type, graph.ops[2].type)
        << "two .map() calls must mint distinct op-type names so the registry doesn't reject "
           "the second registration";
}

TEST(StreamEnvBuilder, InlineKeyByRegistersExtractorAndChains) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).build());
    auto keyed = src.key_by([](const std::int64_t& v) { return v % 2; });
    keyed.template process<std::int64_t>("identity_int64")
        .sink(FileInt64Sink::builder().path("/tmp/clink_inline_key.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].key_by.starts_with("_inline_key_"));
    // Extractor must be registered for hash routing to pick it up.
    EXPECT_TRUE(cluster::KeyExtractorRegistry::default_instance().has(
        graph.ops[1].out_channel.empty() ? "int64" : "int64", graph.ops[1].key_by));
}

namespace {
// Minimal async keyed process function for the fluent-wiring smoke test.
class SmokeAsyncFn final
    : public AsyncKeyedProcessFunction<std::int64_t, std::int64_t, std::int64_t> {
public:
    async::Task<void> process_element(const std::int64_t& /*key*/,
                                      const std::int64_t& value,
                                      AsyncKeyedProcessContext<std::int64_t>& /*ctx*/,
                                      Collector<std::int64_t>& out) override {
        out.collect(value);
        co_return;
    }
};
}  // namespace

TEST(StreamEnvBuilder, InlineProcessAsyncMintsOpTypeAndCarriesKeyBy) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).build());
    src.key_by([](const std::int64_t& v) { return v % 2; })
        .process_async<std::int64_t, std::int64_t>(
            std::make_shared<SmokeAsyncFn>(),
            [](const std::int64_t& v) { return v; },
            int64_codec())
        .sink(FileInt64Sink::builder().path("/tmp/clink_inline_async_kpf.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_async_keyed_process_typed_"));
    EXPECT_FALSE(graph.ops[1].key_by.empty())
        << "async keyed process must carry the upstream key_by so the planner hash-routes inputs";
}

namespace {
class SmokeAsyncCoFn final
    : public AsyncKeyedCoProcessFunction<std::int64_t, std::int64_t, std::int64_t, std::int64_t> {
public:
    async::Task<void> process_element1(const std::int64_t& /*key*/,
                                       const std::int64_t& v,
                                       AsyncKeyedProcessContext<std::int64_t>& /*ctx*/,
                                       Collector<std::int64_t>& out) override {
        out.collect(v);
        co_return;
    }
    async::Task<void> process_element2(const std::int64_t& /*key*/,
                                       const std::int64_t& v,
                                       AsyncKeyedProcessContext<std::int64_t>& /*ctx*/,
                                       Collector<std::int64_t>& out) override {
        out.collect(v);
        co_return;
    }
};
}  // namespace

TEST(StreamEnvBuilder, InlineConnectProcessAsyncMintsOpTypeAndCarriesKeyBy) {
    auto env = StreamExecutionEnvironment::create();
    // Both streams must share the same key_by NAME for connect (the guard); a
    // named (vs inline) key_by keeps the two names identical for this
    // graph-shape test.
    auto a = env.source<std::int64_t>(IntRangeSource::builder().count(4).build())
                 .key_by(std::string("kx"));
    auto b = env.source<std::int64_t>(IntRangeSource::builder().count(4).build())
                 .key_by(std::string("kx"));
    a.connect_process_async<std::int64_t, std::int64_t, std::int64_t>(
         b,
         std::make_shared<SmokeAsyncCoFn>(),
         [](const std::int64_t& v) { return v; },
         [](const std::int64_t& v) { return v; },
         int64_codec())
        .sink(FileInt64Sink::builder().path("/tmp/clink_inline_async_co.out").build());

    const auto& graph = env.graph();
    // 2 sources + the co-op + the sink.
    ASSERT_EQ(graph.ops.size(), 4u);
    bool found = false;
    for (const auto& op : graph.ops) {
        if (op.type.starts_with("_inline_async_keyed_co_process_") && op.inputs.size() == 2u &&
            !op.key_by.empty()) {
            found = true;
        }
    }
    EXPECT_TRUE(found) << "the async co-process op must be a 2-input keyed op";
}

TEST(StreamEnvBuilder, InlineReduceMintsOpTypeAndCarriesKeyBy) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).build());
    auto reduced = src.key_by([](const std::int64_t& v) { return v % 2; })
                       .reduce([](const std::int64_t& a, const std::int64_t& b) { return a + b; });
    reduced.sink(FileInt64Sink::builder().path("/tmp/clink_inline_reduce.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_reduce_"));
    EXPECT_FALSE(graph.ops[1].key_by.empty())
        << "reduce must propagate the upstream key_by name so the planner hash-routes inputs";

    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_operator(graph.ops[1].type, "int64", "int64"), nullptr);
}

TEST(StreamEnvBuilder, InlineAssignTimestampsRegistersWatermarkAssigner) {
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v * 1000}; })
        .sink(FileInt64Sink::builder().path("/tmp/clink_inline_ts.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_ts_monotonic_"));
    EXPECT_EQ(graph.ops[1].out_channel, "int64");

    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_operator(graph.ops[1].type, "int64", "int64"), nullptr);
}

TEST(StreamEnvBuilder, InlineSlidingWindowAggregateMintsOpTypeAndCarriesKeyBy) {
    using namespace std::chrono_literals;
    auto env = StreamExecutionEnvironment::create();
    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(4).build());
    auto agg = src.key_by([](const std::int64_t& v) { return v % 2; })
                   .sliding_window(1000ms, 500ms)
                   .aggregate<std::int64_t>(
                       []() { return std::int64_t{0}; },
                       [](const std::int64_t& a, const std::int64_t& b) { return a + b; });
    agg.sink(FileInt64Sink::builder().path("/tmp/clink_inline_sliding.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 3u);
    EXPECT_TRUE(graph.ops[1].type.starts_with("_inline_sliding_aggregate_"));
    EXPECT_FALSE(graph.ops[1].key_by.empty()) << "sliding window must propagate key_by upstream";

    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_operator(graph.ops[1].type, "int64", "int64"), nullptr);
}

// Direct unit test for clink::api::detail::KeyedSlidingWindowAggregateOperator.
// Exercises the windowing semantics without going through the cluster
// path: feed StreamElement<T> data + watermark events to process(),
// capture emissions via an Emitter, assert the aggregates.
TEST(SlidingWindowAggregateOperator, AccumulatesPerKeyAndFiresOnWatermark) {
    using namespace std::chrono_literals;
    using clink::api::detail::KeyedSlidingWindowAggregateOperator;

    KeyedSlidingWindowAggregateOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t& v) { return v % 2; },                         // key by parity
        []() { return std::int64_t{0}; },                                    // initial = 0
        [](const std::int64_t& a, const std::int64_t& b) { return a + b; },  // sum
        1000ms,                                                              // window size
        1000ms,  // slide == size (tumbling)
        "test_sw");

    std::vector<std::int64_t> emitted;
    Emitter<std::int64_t> sink(
        Emitter<std::int64_t>::Forward([&emitted](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    emitted.push_back(r.value());
                }
            }
            return true;
        }));

    // Records at t=100, 500 (parity 0/0), 200, 700 (parity 1/1) - all
    // in window [0, 1000). Records at 1100, 1700 (parity 1/1), 1500
    // (parity 1) in window [1000, 2000).
    {
        Batch<std::int64_t> b;
        b.emplace(2, EventTime{100});
        b.emplace(4, EventTime{500});
        b.emplace(3, EventTime{200});
        b.emplace(5, EventTime{700});
        b.emplace(7, EventTime{1100});
        b.emplace(9, EventTime{1500});
        b.emplace(11, EventTime{1700});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), sink);
    }

    // Watermark at 1000 - fires windows ending at <= 1000, i.e. [0, 1000).
    op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{1000}}), sink);
    std::sort(emitted.begin(), emitted.end());
    // parity-0 sum in [0,1000): 2 + 4 = 6
    // parity-1 sum in [0,1000): 3 + 5 = 8
    EXPECT_EQ(emitted, (std::vector<std::int64_t>{6, 8}));

    emitted.clear();
    // Watermark at 2000 - fires [1000, 2000).
    op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{2000}}), sink);
    std::sort(emitted.begin(), emitted.end());
    // parity-1 sum in [1000,2000): 7 + 9 + 11 = 27. No parity-0 records.
    EXPECT_EQ(emitted, (std::vector<std::int64_t>{27}));
}

TEST(SlidingWindowAggregateOperator, EmitsOverlappingWindowsForTrueSlide) {
    using namespace std::chrono_literals;
    using clink::api::detail::KeyedSlidingWindowAggregateOperator;

    // size = 1000, slide = 500 -> two overlapping windows per record.
    KeyedSlidingWindowAggregateOperator<std::int64_t, std::int64_t> op(
        [](const std::int64_t&) { return 0; },  // single key
        []() { return std::int64_t{0}; },
        [](const std::int64_t& a, const std::int64_t& b) { return a + b; },
        1000ms,
        500ms,
        "test_sw_overlap");

    std::vector<std::int64_t> emitted;
    Emitter<std::int64_t> sink(
        Emitter<std::int64_t>::Forward([&emitted](StreamElement<std::int64_t> e) {
            if (e.is_data()) {
                for (const auto& r : e.as_data()) {
                    emitted.push_back(r.value());
                }
            }
            return true;
        }));

    // Single record at t=600. Windows containing 600: [500, 1500)
    // and [0, 1000). Two windows fire as the watermark advances.
    {
        Batch<std::int64_t> b;
        b.emplace(10, EventTime{600});
        op.process(StreamElement<std::int64_t>::data(std::move(b)), sink);
    }

    // Watermark at 1000 fires [0, 1000) only.
    op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{1000}}), sink);
    EXPECT_EQ(emitted, (std::vector<std::int64_t>{10}));

    emitted.clear();
    // Watermark at 1500 fires [500, 1500).
    op.process(StreamElement<std::int64_t>::watermark(Watermark{EventTime{1500}}), sink);
    EXPECT_EQ(emitted, (std::vector<std::int64_t>{10}));
}

TEST(StreamEnvBuilder, FromElementsBuildsSourceWithRegisteredFactory) {
    auto env = StreamExecutionEnvironment::create();
    env.from_elements<std::int64_t>({10, 20, 30})
        .sink(FileInt64Sink::builder().path("/tmp/clink_from_elements.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 2u);
    EXPECT_TRUE(graph.ops[0].type.starts_with("_inline_from_elements_"));
    EXPECT_EQ(graph.ops[0].out_channel, "int64");
    EXPECT_TRUE(graph.ops[0].inputs.empty()) << "from_elements is a source";

    const auto& rr = cluster::RunnerRegistry::default_instance();
    EXPECT_NE(rr.find_source(graph.ops[0].type, "int64"), nullptr);
}

TEST(StreamEnvBuilder, FromElementsAcceptsStrings) {
    auto env = StreamExecutionEnvironment::create();
    env.from_elements<std::string>({"alpha", "beta", "gamma"})
        .sink(FileTextSink::builder().path("/tmp/clink_from_elements_str.out").build());

    const auto& graph = env.graph();
    ASSERT_EQ(graph.ops.size(), 2u);
    EXPECT_EQ(graph.ops[0].out_channel, "string");
}

TEST(StreamEnvBuilder, BuilderPathsDontTouchTheFilesystem) {
    // Builders only produce descriptors; no files are opened at build
    // time. We use a path that wouldn't exist to verify no eager I/O.
    auto desc = FileTextSource::builder().path("/no/such/path/here").build();
    EXPECT_EQ(desc.op_type, "file_text_source");
    EXPECT_EQ(desc.params.at("path"), "/no/such/path/here");
}

}  // namespace
