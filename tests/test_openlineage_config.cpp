// Tests for the OpenLineage exporter: parse_openlineage_config (pure endpoint
// parsing) and the exporter's event path. The exporter is pointed at a dead host,
// so on_event still builds the full OpenLineage JSON (build_event_json /
// write_datasets) and the worker exercises the POST-failure path (connection
// refused -> dropped); no receiver is needed to cover the JSON construction.

#include <string>

#include <gtest/gtest.h>

#include "clink/lineage/lineage_graph.hpp"
#include "clink/lineage/lineage_listener.hpp"
#include "clink/lineage/openlineage_exporter.hpp"

namespace clink::lineage {
namespace {

TEST(OpenLineageConfigParse, EmptyIsRejected) {
    OpenLineageConfig out;
    EXPECT_FALSE(parse_openlineage_config(LineageListenerConfig{}, out));
}

TEST(OpenLineageConfigParse, HttpEndpointHostPort) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config({{"endpoint", "http://receiver:9000"}}, out));
    EXPECT_EQ(out.host, "receiver");
    EXPECT_EQ(out.port, 9000);
    EXPECT_EQ(out.path, "/api/v1/lineage");  // default
    EXPECT_EQ(out.job_namespace, "clink");   // default
}

TEST(OpenLineageConfigParse, NoSchemeAndDefaultPort) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config({{"endpoint", "plainhost"}}, out));
    EXPECT_EQ(out.host, "plainhost");
    EXPECT_EQ(out.port, 80);
}

TEST(OpenLineageConfigParse, UrlKeyFallback) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config({{"url", "http://h:1234"}}, out));
    EXPECT_EQ(out.host, "h");
    EXPECT_EQ(out.port, 1234);
}

TEST(OpenLineageConfigParse, HttpsDefaultsTo443) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config({{"endpoint", "https://secure"}}, out));
    EXPECT_EQ(out.host, "secure");
    EXPECT_EQ(out.port, 443);
}

TEST(OpenLineageConfigParse, PathIsTrimmedFromAuthority) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config({{"endpoint", "http://h:9000/marquez/api"}}, out));
    EXPECT_EQ(out.host, "h");
    EXPECT_EQ(out.port, 9000);
}

TEST(OpenLineageConfigParse, Overrides) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config({{"endpoint", "http://h:1"},
                                          {"path", "/ingest"},
                                          {"namespace", "prod"},
                                          {"producer", "acme"},
                                          {"max_queue", "42"}},
                                         out));
    EXPECT_EQ(out.path, "/ingest");
    EXPECT_EQ(out.job_namespace, "prod");
    EXPECT_EQ(out.producer, "acme");
    EXPECT_EQ(out.max_queue, 42u);
}

TEST(OpenLineageConfigParse, InvalidPortAndQueueFallBackToDefaults) {
    OpenLineageConfig out;
    ASSERT_TRUE(parse_openlineage_config(
        {{"endpoint", "http://h:notaport"}, {"max_queue", "notanumber"}}, out));
    EXPECT_EQ(out.port, 80);          // bad port -> default
    EXPECT_EQ(out.max_queue, 1024u);  // bad max_queue -> default
}

// Build a graph with a source dataset (schema + facets) and a sink dataset
// (schema + facets + column lineage) so the exporter's JSON builder walks every
// facet branch.
LineageGraph sample_graph() {
    LineageGraph g;

    LineageVertex src;
    src.id = "op_0";
    src.name = "kafka_source";
    src.op_type = "kafka_source";
    LineageDataset in;
    in.ns = "kafka://broker:9092";
    in.name = "events";
    in.facets["connector"] = "kafka";
    in.schema.push_back(SchemaField{"id", "BIGINT"});
    in.schema.push_back(SchemaField{"amount", "DECIMAL"});
    src.datasets.push_back(std::move(in));
    g.sources.push_back(std::move(src));

    LineageVertex sink;
    sink.id = "op_9";
    sink.name = "postgres_sink";
    sink.op_type = "postgres_sink";
    LineageDataset out;
    out.ns = "postgres://db:5432";
    out.name = "public.totals";
    out.facets["connector"] = "postgres";
    out.schema.push_back(SchemaField{"id", "BIGINT"});
    out.schema.push_back(SchemaField{"total", "DECIMAL"});
    ColumnLineageField clf;
    clf.output = "total";
    clf.transformation = "AGGREGATION";
    clf.inputs.push_back(ColumnInputField{"kafka://broker:9092", "events", "amount"});
    out.column_lineage.push_back(std::move(clf));
    sink.datasets.push_back(std::move(out));
    g.sinks.push_back(std::move(sink));

    return g;
}

OpenLineageConfig dead_host_config() {
    OpenLineageConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 1;  // nothing listens here: the POST fails fast, which is the point
    return cfg;
}

// Firing every event kind builds the full JSON (all facet branches) and drives
// the worker's POST-failure path. We assert it does not crash and drains.
TEST(OpenLineageExporter, BuildsAndDrainsAllEventKinds) {
    OpenLineageExporter exporter(dead_host_config());

    LineageEvent start;
    start.kind = LineageEvent::Kind::JobStarted;
    start.ts_ms = 1'700'000'000'000;  // fixed ts -> exercises the iso8601 non-now path
    start.job_id = 0xABCDEF;
    start.job_name = "totals-job";
    start.graph = sample_graph();
    exporter.on_event(start);

    LineageEvent complete;
    complete.kind = LineageEvent::Kind::JobCompleted;
    complete.job_id = 0xABCDEF;
    complete.status = "ok";
    exporter.on_event(complete);

    LineageEvent cancelled = complete;
    cancelled.status = "cancelled";
    exporter.on_event(cancelled);

    LineageEvent failed = complete;
    failed.status = "failed";
    failed.error = "operator threw std::runtime_error";
    exporter.on_event(failed);

    // A JobStarted with no job_name exercises the id-fallback name branch.
    LineageEvent unnamed;
    unnamed.kind = LineageEvent::Kind::JobStarted;
    unnamed.job_id = 7;
    exporter.on_event(unnamed);

    SUCCEED();  // destructor stops + joins the worker (drains the outbox)
}

}  // namespace
}  // namespace clink::lineage
