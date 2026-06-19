// Verifies the typed S3 Parquet sink fluent helper produces the right
// descriptor and registers a per-T sink factory under a minted
// op_type.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/api/stream_execution_environment.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/core/codec.hpp"
#include "clink/plugin/plugin.hpp"
#include "clink/s3/install.hpp"
#include "clink/s3/typed_sink.hpp"

namespace {

struct DummyRecord {
    std::int64_t id{0};
    std::string name;
};

clink::Codec<DummyRecord> dummy_codec() {
    return clink::Codec<DummyRecord>{
        .encode =
            [](const DummyRecord& r) {
                clink::Codec<DummyRecord>::Bytes out;
                for (char c : r.name) {
                    out.push_back(static_cast<std::byte>(c));
                }
                return out;
            },
        .decode = [](clink::Codec<DummyRecord>::BytesView) -> std::optional<DummyRecord> {
            return DummyRecord{};
        }};
}

}  // namespace

TEST(S3TypedSink, ParquetSinkAppendsTypedDescriptorAndRegistersFactory) {
    clink::cluster::ensure_built_ins_registered();
    clink::api::StreamExecutionEnvironment env;
    clink::s3::install(env.registry());
    // DummyRecord is the user's channel type; register it with a codec
    // before the helper can route through register_sink<T>.
    env.registry().register_type<DummyRecord>("test.dummy_record", dummy_codec());

    // Build the simplest possible upstream: int64 source → map<DummyRecord>.
    auto src =
        env.source<std::int64_t>(clink::api::SourceDescriptor{.op_type = "int64_range_source",
                                                              .channel_type = std::string{"int64"},
                                                              .params = {{"count", "3"}}});
    auto typed = src.map<DummyRecord>([](std::int64_t i) { return DummyRecord{i, "row"}; });

    clink::s3::parquet_sink<DummyRecord>(typed,
                                         clink::s3::ParquetSinkOptions{
                                             .bucket = "test-bucket",
                                             .key = "2026/05/17/0001.parquet",
                                             .endpoint_override = "http://minio:9000",
                                         },
                                         dummy_codec());

    // The graph should have source + map + sink. Pin the sink shape.
    const auto& g = env.graph();
    ASSERT_GE(g.ops.size(), 3u);
    const auto& sink_op = g.ops.back();
    EXPECT_TRUE(sink_op.type.starts_with("_inline_s3_parquet_typed_sink_"));
    EXPECT_EQ(sink_op.out_channel, std::string{"test.dummy_record"});

    // The minted op_type must resolve in the runner registry to a typed
    // sink for DummyRecord. The cross-channel canonical lookup uses
    // the registered channel name.
    auto& rr = env.registry().runner_registry();
    EXPECT_NE(rr.find_sink(sink_op.type, "test.dummy_record"), nullptr);
}
