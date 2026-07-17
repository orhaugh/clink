// Fluent-API side-output tests.
//
//   1. DataStream<T>::side_output<U>(tag) declares the tag on the
//      current op's OperatorSpec.side_outputs (channel_type = U) and
//      returns a DataStream<U> whose upstream_id is "<op>::<tag>" so
//      downstream chains pick it up via the planner's existing
//      "id::tag" parser.
//
//   2. Calling .side_output<U>(same_tag) a second time is idempotent -
//      no duplicate decl appears.
//
//   3. Re-declaring the same tag with a DIFFERENT channel type throws.
//
//   4. KeyedDataStream<T>::side_output<U>(tag) behaves the same way.
//
//   5. .late_output_tag() on a windowed stream now also populates
//      OperatorSpec.side_outputs at .aggregate() time so cluster
//      execution can wire the late channel.

#include <chrono>
#include <cstdint>
#include <string>

#include <gtest/gtest.h>

#include "clink/api/builtin_connectors.hpp"
#include "clink/api/pipeline.hpp"
#include "clink/runtime/output_tag.hpp"

using namespace clink;
using namespace clink::api;
using namespace std::chrono_literals;

namespace {

const cluster::OperatorSpec& find_op(const cluster::JobGraphSpec& g, const std::string& id) {
    for (const auto& op : g.ops) {
        if (op.id == id) {
            return op;
        }
    }
    throw std::runtime_error("test helper: op id '" + id + "' not in graph");
}

}  // namespace

TEST(FluentSideOutput, DataStreamSideOutputDeclaresTagAndReturnsTypedHandle) {
    auto env = Pipeline::create();
    OutputTag<std::string> errors{"errors"};

    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto mapped = src.map<std::int64_t>([](const std::int64_t& v) { return v + 1; });
    auto side = mapped.side_output<std::string>(errors);

    // The side-output handle reads from "<map_op_id>::errors" with
    // channel_type "string".
    EXPECT_EQ(side.id(), mapped.id() + "::errors");
    EXPECT_EQ(side.channel_type(), "string");

    // The map op now carries a SideOutputDecl for the tag.
    const auto& map_op = find_op(env.graph(), mapped.id());
    ASSERT_EQ(map_op.side_outputs.size(), 1u);
    EXPECT_EQ(map_op.side_outputs[0].tag, "errors");
    EXPECT_EQ(map_op.side_outputs[0].channel_type, "string");
}

TEST(FluentSideOutput, DownstreamChainOffSideHandleReferencesIdColonTagInput) {
    auto env = Pipeline::create();
    OutputTag<std::string> bad{"bad"};

    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto mapped = src.map<std::int64_t>([](const std::int64_t& v) { return v; });
    auto bad_stream = mapped.side_output<std::string>(bad);

    bad_stream.sink(FileTextSink::builder().path("/tmp/clink_side_bad.out").build());

    // The sink's inputs vector should reference "<map>::bad", which the
    // job_planner's parse_input_ref understands as a named side output.
    const auto& sink_op = env.graph().ops.back();
    EXPECT_EQ(sink_op.type, "file_text_sink");
    ASSERT_EQ(sink_op.inputs.size(), 1u);
    EXPECT_EQ(sink_op.inputs[0], mapped.id() + "::bad");
}

TEST(FluentSideOutput, RepeatedSameTagSameChannelIsIdempotent) {
    auto env = Pipeline::create();
    OutputTag<std::string> tag{"audit"};

    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto mapped = src.map<std::int64_t>([](const std::int64_t& v) { return v; });

    auto a = mapped.side_output<std::string>(tag);
    auto b = mapped.side_output<std::string>(tag);
    EXPECT_EQ(a.id(), b.id());

    const auto& map_op = find_op(env.graph(), mapped.id());
    EXPECT_EQ(map_op.side_outputs.size(), 1u);
}

TEST(FluentSideOutput, SameTagDifferentChannelThrows) {
    auto env = Pipeline::create();
    OutputTag<std::string> tag_str{"x"};
    OutputTag<std::int64_t> tag_int{"x"};  // same id, different element type

    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto mapped = src.map<std::int64_t>([](const std::int64_t& v) { return v; });

    (void)mapped.side_output<std::string>(tag_str);
    EXPECT_THROW(mapped.side_output<std::int64_t>(tag_int), std::runtime_error);
}

TEST(FluentSideOutput, KeyedDataStreamSideOutputDeclares) {
    auto env = Pipeline::create();
    OutputTag<std::string> reject{"reject"};

    auto src = env.source<std::int64_t>(IntRangeSource::builder().count(3).build());
    auto keyed = src.key_by([](const std::int64_t& v) { return v % 2; });
    auto reject_stream = keyed.side_output<std::string>(reject);

    EXPECT_EQ(reject_stream.id(), keyed.id() + "::reject");
    EXPECT_EQ(reject_stream.channel_type(), "string");

    const auto& key_op = find_op(env.graph(), keyed.id());
    ASSERT_EQ(key_op.side_outputs.size(), 1u);
    EXPECT_EQ(key_op.side_outputs[0].tag, "reject");
    EXPECT_EQ(key_op.side_outputs[0].channel_type, "string");
}

TEST(FluentSideOutput, TumblingWindowLateTagAppearsInSideOutputs) {
    auto env = Pipeline::create();
    OutputTag<std::int64_t> late{"late"};

    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    auto agg = src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
                   .key_by([](const std::int64_t&) { return std::int64_t{0}; })
                   .tumbling_window(1000ms)
                   .allowed_lateness(500ms)
                   .late_output_tag(late)
                   .aggregate<std::int64_t>(
                       [] { return std::int64_t{0}; },
                       [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; });

    const auto& agg_op = find_op(env.graph(), agg.id());
    ASSERT_EQ(agg_op.side_outputs.size(), 1u);
    EXPECT_EQ(agg_op.side_outputs[0].tag, "late");
    EXPECT_EQ(agg_op.side_outputs[0].channel_type, "int64");

    // The downstream-consumer pattern: pick the late tag off the
    // returned DataStream<Agg> via the idempotent side_output<T>(tag).
    auto late_stream = agg.side_output<std::int64_t>(late);
    EXPECT_EQ(late_stream.id(), agg.id() + "::late");
    EXPECT_EQ(late_stream.channel_type(), "int64");

    // Still exactly one SideOutputDecl after the idempotent re-decl.
    EXPECT_EQ(find_op(env.graph(), agg.id()).side_outputs.size(), 1u);
}

TEST(FluentSideOutput, SlidingWindowLateTagAppearsInSideOutputs) {
    auto env = Pipeline::create();
    OutputTag<std::int64_t> late{"sliding_late"};

    auto src = env.from_elements<std::int64_t>({1, 2, 3});
    auto agg = src.assign_timestamps_monotonic([](const std::int64_t& v) { return EventTime{v}; })
                   .key_by([](const std::int64_t&) { return std::int64_t{0}; })
                   .sliding_window(1000ms, 500ms)
                   .late_output_tag(late)
                   .aggregate<std::int64_t>(
                       [] { return std::int64_t{0}; },
                       [](const std::int64_t& acc, const std::int64_t& v) { return acc + v; });

    const auto& agg_op = find_op(env.graph(), agg.id());
    ASSERT_EQ(agg_op.side_outputs.size(), 1u);
    EXPECT_EQ(agg_op.side_outputs[0].tag, "sliding_late");
    EXPECT_EQ(agg_op.side_outputs[0].channel_type, "int64");
}
