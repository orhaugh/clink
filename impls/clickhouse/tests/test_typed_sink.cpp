// Verifies the typed ClickHouse sink fluent helper produces the right
// descriptor + intermediate encode-map op.

#include <cstdint>
#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/clickhouse/install.hpp"
#include "clink/clickhouse/typed_sink.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/codec.hpp"
#include "clink/plugin/plugin.hpp"

namespace {

struct DummyRecord {
    std::int64_t id{0};
    std::string name;
};

std::string to_jsoneachrow(const DummyRecord& r) {
    return std::string{"{\"id\":"} + std::to_string(r.id) + ",\"name\":\"" + r.name + "\"}";
}

}  // namespace

TEST(ClickHouseTypedSink, ComposesEncodeMapAndSink) {
    clink::cluster::ensure_built_ins_registered();
    clink::api::StreamExecutionEnvironment env;
    clink::clickhouse::install(env.registry());
    // DummyRecord is a project-internal channel type; the consumer
    // registers it with a codec before using it in the fluent map call.
    env.registry().register_type<DummyRecord>(
        "test.dummy_record",
        clink::Codec<DummyRecord>{.encode =
                                      [](const DummyRecord& r) {
                                          clink::Codec<DummyRecord>::Bytes out;
                                          for (char c : r.name) {
                                              out.push_back(static_cast<std::byte>(c));
                                          }
                                          return out;
                                      },
                                  .decode = [](clink::Codec<DummyRecord>::BytesView)
                                      -> std::optional<DummyRecord> { return DummyRecord{}; }});
    // The encode map's output channel is "string" - already a built-in.
    // The downstream sink consumes "string" which matches
    // clickhouse_sink's channel_type.

    // Use the built-in string source as the head of the chain; the
    // typed_sink helper expects a DataStream<DummyRecord> input, which
    // we don't have a built-in source for. The cleanest fixture is to
    // start the chain from a string source, .map<DummyRecord>(...) into
    // the typed shape, then pass that into clink::clickhouse::sink<T>.
    auto strings =
        env.source<std::string>(clink::api::SourceDescriptor{.op_type = "string_lines_source",
                                                             .channel_type = std::string{"string"},
                                                             .params = {{"lines", "1,2"}}});
    auto typed = strings.map<DummyRecord>([](const std::string& s) { return DummyRecord{1, s}; });
    clink::clickhouse::sink<DummyRecord>(typed,
                                         clink::clickhouse::SinkOptions{
                                             .host = "ch.internal",
                                             .port = 9000,
                                             .database = "events",
                                             .table = "my_table",
                                             .format = "JSONEachRow",
                                             .batch_rows = 1000,
                                             .batch_interval_ms = 500,
                                         },
                                         &to_jsoneachrow);

    // Expect 4 ops: source, .map<DummyRecord>, .map<string> (encoder),
    // sink. Pin the sink shape.
    const auto& g = env.graph();
    ASSERT_GE(g.ops.size(), 4u);
    const auto& sink_op = g.ops.back();
    EXPECT_EQ(sink_op.type, "clickhouse_sink");
    EXPECT_EQ(sink_op.params.at("host"), "ch.internal");
    EXPECT_EQ(sink_op.params.at("port"), "9000");
    EXPECT_EQ(sink_op.params.at("database"), "events");
    EXPECT_EQ(sink_op.params.at("table"), "my_table");
    EXPECT_EQ(sink_op.params.at("format"), "JSONEachRow");
    EXPECT_EQ(sink_op.params.at("batch_rows"), "1000");
    EXPECT_EQ(sink_op.params.at("batch_interval_ms"), "500");

    // The op upstream of the sink is the synthetic encode map.
    const auto& encoder_op = g.ops[g.ops.size() - 2];
    EXPECT_TRUE(encoder_op.type.starts_with("_inline_map_"));
    EXPECT_EQ(encoder_op.out_channel, std::string{"string"});
}
