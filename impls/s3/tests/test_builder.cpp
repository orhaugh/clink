// Builder/descriptor test for the api::S3TextSink fluent builder.

#include <gtest/gtest.h>

#include "clink/api/s3_builders.hpp"

using clink::api::S3TextSink;

TEST(StreamEnvBuilder, S3TextSinkBuilderProducesExpectedDescriptor) {
    auto d = S3TextSink::builder()
                 .bucket("ingest-bucket")
                 .key_prefix("events/y=2026/m=05")
                 .region("us-west-2")
                 .endpoint_override("http://localstack:4566")
                 .rollover_bytes(8 * 1024 * 1024)
                 .build();
    EXPECT_EQ(d.op_type, "s3_text_sink");
    EXPECT_EQ(d.channel_type, "string");
    EXPECT_EQ(d.params.at("bucket"), "ingest-bucket");
    EXPECT_EQ(d.params.at("key_prefix"), "events/y=2026/m=05");
    EXPECT_EQ(d.params.at("region"), "us-west-2");
    EXPECT_EQ(d.params.at("endpoint_override"), "http://localstack:4566");
    EXPECT_EQ(d.params.at("rollover_bytes"), std::to_string(8 * 1024 * 1024));
}
