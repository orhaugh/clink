// Tests for the OpenLineage exporter: parse_openlineage_config (pure endpoint
// parsing) and the exporter's event path. The exporter is pointed at a dead host,
// so on_event still builds the full OpenLineage JSON (build_event_json /
// write_datasets) and the worker exercises the POST-failure path (connection
// refused -> dropped); no receiver is needed to cover the JSON construction.

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/lineage/lineage_graph.hpp"
#include "clink/lineage/lineage_listener.hpp"
#include "clink/lineage/openlineage_exporter.hpp"
#include "clink/runtime/log_buffer.hpp"

#ifdef CLINK_HAS_HTTP
#include "clink/http/http_server.hpp"
#endif

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

#ifdef CLINK_HAS_HTTP
// A receiver standing in for Marquez: records every event it is sent and
// answers each with `status`.
struct Receiver {
    explicit Receiver(int status) {
        server.post("/api/v1/lineage", [this, status](const http::HttpRequest& req) {
            {
                std::lock_guard lock(mu);
                bodies.push_back(req.body);
            }
            got.notify_all();
            http::HttpResponse r;
            r.status = status;
            r.body =
                status == 201 ? "" : R"({"code":400,"message":"facet columnLineage is malformed"})";
            return r;
        });
        port = server.start("127.0.0.1", 0);
    }
    std::vector<std::string> await_events(std::size_t n) {
        std::unique_lock lock(mu);
        got.wait_for(lock, std::chrono::seconds{10}, [&] { return bodies.size() >= n; });
        return bodies;
    }
    http::HttpServer server;
    std::uint16_t port{0};
    std::mutex mu;
    std::condition_variable got;
    std::vector<std::string> bodies;
};

OpenLineageConfig config_for(std::uint16_t port) {
    OpenLineageConfig cfg;
    cfg.host = "127.0.0.1";
    cfg.port = port;
    cfg.job_namespace = "playhead";
    return cfg;
}

// What the exporter sends is a run event the OpenLineage spec accepts, naming
// only datasets that have an identity. A sink that writes nowhere a dataset
// lives (blackhole, print) has no name; Marquez took such a dataset in a START
// and then refused the run's terminal event with a 400, so the run never
// closed.
TEST(OpenLineageExporter, ARunEventCarriesTheSpecSchemaAndOnlyDatasetsWithAnIdentity) {
    Receiver marquez(201);
    ASSERT_NE(marquez.port, 0);
    {
        OpenLineageExporter exporter(config_for(marquez.port));
        LineageEvent start;
        start.kind = LineageEvent::Kind::JobStarted;
        start.ts_ms = 1'700'000'000'000;
        start.job_id = 3;
        start.job_name = "qoe";
        start.graph = sample_graph();
        LineageVertex nowhere;
        nowhere.id = "op_10";
        nowhere.name = "blackhole_sink_row";
        nowhere.op_type = "blackhole_sink_row";
        LineageDataset none;
        none.ns = "blackhole";
        nowhere.datasets.push_back(std::move(none));
        start.graph.sinks.push_back(std::move(nowhere));
        exporter.on_event(start);
        ASSERT_EQ(marquez.await_events(1).size(), 1U);
    }
    const auto event = clink::config::parse(marquez.bodies.at(0));
    EXPECT_EQ(event.at("schemaURL").as_string(),
              "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent");
    EXPECT_EQ(event.at("producer").as_string(), "https://github.com/orhaugh/clink");
    EXPECT_EQ(event.at("job").at("name").as_string(), "qoe");
    ASSERT_EQ(event.at("inputs").as_array().size(), 1U);
    const auto& outputs = event.at("outputs").as_array();
    ASSERT_EQ(outputs.size(), 1U) << "the blackhole sink's identity-less dataset must be left out";
    EXPECT_EQ(outputs.at(0).at("name").as_string(), "public.totals");
}

// A receiver that refuses an event says why, and that reason is what tells an
// operator which event or facet it objected to. The log used to carry the
// status alone.
TEST(OpenLineageExporter, ARefusedEventIsLoggedWithTheReceiversReason) {
    Receiver strict(400);
    ASSERT_NE(strict.port, 0);
    const auto since = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count() -
                       1;
    {
        OpenLineageExporter exporter(config_for(strict.port));
        LineageEvent start;
        start.kind = LineageEvent::Kind::JobStarted;
        start.job_id = 4;
        start.job_name = "alerts";
        start.graph = sample_graph();
        exporter.on_event(start);
        ASSERT_EQ(strict.await_events(1).size(), 1U);
    }  // the destructor drains the outbox, so the refusal has been logged
    bool logged = false;
    for (const auto& rec : LogBuffer::global().tail(1000, "warn", since, "lineage.openlineage")) {
        logged |= rec.message.find("status=400") != std::string::npos &&
                  rec.message.find("facet columnLineage is malformed") != std::string::npos;
    }
    EXPECT_TRUE(logged) << "the refusal was not logged with the receiver's reason";
}
#endif

}  // namespace
}  // namespace clink::lineage
