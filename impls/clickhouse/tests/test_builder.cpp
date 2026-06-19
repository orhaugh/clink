// Builder/descriptor test for the api::ClickHouseSink fluent builder.
// Verifies the descriptor produced by .build() carries the params the
// clickhouse_sink factory expects to find.

#include <gtest/gtest.h>

#include "clink/api/clickhouse_builders.hpp"

using clink::api::ClickHouseSink;

TEST(StreamEnvBuilder, ClickHouseSinkBuilderProducesExpectedDescriptor) {
    auto d = ClickHouseSink::builder()
                 .host("clickhouse.internal")
                 .port(9000)
                 .database("events")
                 .table("clicks")
                 .user("ingest")
                 .password("secret")
                 .format("jsoneachrow")
                 .batch_rows(500)
                 .batch_interval_ms(2000)
                 .build();
    EXPECT_EQ(d.op_type, "clickhouse_sink");
    EXPECT_EQ(d.channel_type, "string");
    EXPECT_EQ(d.params.at("host"), "clickhouse.internal");
    EXPECT_EQ(d.params.at("port"), "9000");
    EXPECT_EQ(d.params.at("database"), "events");
    EXPECT_EQ(d.params.at("table"), "clicks");
    EXPECT_EQ(d.params.at("user"), "ingest");
    EXPECT_EQ(d.params.at("password"), "secret");
    EXPECT_EQ(d.params.at("format"), "jsoneachrow");
    EXPECT_EQ(d.params.at("batch_rows"), "500");
    EXPECT_EQ(d.params.at("batch_interval_ms"), "2000");
}
