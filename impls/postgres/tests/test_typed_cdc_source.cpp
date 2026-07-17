// Verifies the `clink::postgres::cdc_event_source` fluent helper
// constructs the right SourceDescriptor and produces a
// DataStream<CdcEvent> whose graph entry references the existing
// postgres_cdc_event_source factory.

#include <gtest/gtest.h>

#include "clink/api/pipeline.hpp"
#include "clink/cluster/built_in_factories.hpp"
#include "clink/connectors/cdc_event.hpp"
#include "clink/postgres/cdc_event_codec.hpp"
#include "clink/postgres/install.hpp"
#include "clink/postgres/typed_cdc_source.hpp"

TEST(PostgresTypedCdcSource, ProducesExpectedDescriptor) {
    clink::cluster::ensure_built_ins_registered();
    clink::api::Pipeline env;
    clink::postgres::install(env.registry());

    auto stream = clink::postgres::cdc_event_source(env,
                                                    clink::postgres::CdcEventSourceOptions{
                                                        .conninfo = "host=localhost user=test",
                                                        .slot_name = "my_slot",
                                                        .plugin = "pgoutput",
                                                        .publication_names = "my_pub",
                                                        .create_slot = true,
                                                        .drop_slot_on_close = false,
                                                    });

    // The helper appends one source op. Inspect the env's graph to
    // pin the contract: op_type + channel_type + every required param.
    const auto& g = env.graph();
    ASSERT_EQ(g.ops.size(), 1u);
    const auto& op = g.ops.front();
    EXPECT_EQ(op.type, "postgres_cdc_event_source");
    EXPECT_EQ(op.out_channel, std::string{clink::kChannelPostgresCdcEvent});
    EXPECT_EQ(op.params.at("conninfo"), "host=localhost user=test");
    EXPECT_EQ(op.params.at("slot_name"), "my_slot");
    EXPECT_EQ(op.params.at("plugin"), "pgoutput");
    EXPECT_EQ(op.params.at("publication_names"), "my_pub");
    EXPECT_EQ(op.params.at("create_slot"), "true");
    EXPECT_EQ(op.params.at("drop_slot_on_close"), "false");

    // The returned DataStream<CdcEvent> is typed correctly - channel
    // matches the registered CdcEvent channel.
    EXPECT_EQ(stream.channel_type(), std::string{clink::kChannelPostgresCdcEvent});
}
