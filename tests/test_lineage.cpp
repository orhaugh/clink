// Unit tests for the data-lineage capture model and the EventBus ->
// listener dispatch path.

#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/job_graph.hpp"
#include "clink/lineage/lineage_graph.hpp"
#include "clink/lineage/lineage_listener.hpp"
#include "clink/runtime/event_bus.hpp"

using clink::cluster::JobGraphSpec;
using clink::cluster::OperatorSpec;
using namespace clink::lineage;

namespace {

OperatorSpec op(std::string id,
                std::string type,
                std::vector<std::string> inputs,
                std::map<std::string, std::string> params) {
    OperatorSpec o;
    o.id = std::move(id);
    o.type = std::move(type);
    o.inputs = std::move(inputs);
    o.params = std::move(params);
    return o;
}

const LineageVertex* find_by_id(const std::vector<LineageVertex>& vs, const std::string& id) {
    for (const auto& v : vs) {
        if (v.id == id) {
            return &v;
        }
    }
    return nullptr;
}

}  // namespace

// --- connector_family ------------------------------------------------------

TEST(LineageFamily, StripsChannelAndDirectionTokens) {
    EXPECT_EQ(connector_family("kafka_source_string"), "kafka");
    EXPECT_EQ(connector_family("kafka_2pc_sink_string"), "kafka");
    EXPECT_EQ(connector_family("s3_parquet_string_source"), "s3_parquet");
    EXPECT_EQ(connector_family("s3_text_sink"), "s3");
    EXPECT_EQ(connector_family("postgres_cdc_source"), "postgres");
    EXPECT_EQ(connector_family("file_json_upsert_sink"), "file");
    EXPECT_EQ(connector_family("parquet_row_source"), "parquet");
    EXPECT_EQ(connector_family("clickhouse_sink"), "clickhouse");
    EXPECT_EQ(connector_family("http_poll_source"), "http");
    EXPECT_EQ(connector_family("partition_file_sink"), "file");
    EXPECT_EQ(connector_family("iceberg_row_sink"), "iceberg");
}

TEST(LineageFamily, FallsBackToLeadingTokenWhenAllStripped) {
    // Every token is a modifier; the family falls back to the first token.
    EXPECT_EQ(connector_family("int64_range_source"), "int64");
}

// --- dataset_for: per-connector identity -----------------------------------

TEST(LineageDataset, Kafka) {
    const auto d = dataset_for(
        "kafka_source_string", {{"brokers", "broker:9092"}, {"topic", "orders"}}, "string");
    EXPECT_EQ(d.ns, "kafka://broker:9092");
    EXPECT_EQ(d.name, "orders");
    EXPECT_EQ(d.facets.at("connector"), "kafka");
    EXPECT_EQ(d.facets.at("schema"), "string");
}

TEST(LineageDataset, S3Parquet) {
    const auto d =
        dataset_for("s3_parquet_string_sink", {{"bucket", "lake"}, {"prefix", "events/"}}, "row");
    EXPECT_EQ(d.ns, "s3://lake");
    EXPECT_EQ(d.name, "events/");
}

TEST(LineageDataset, PostgresFromConnInfo) {
    const auto d = dataset_for(
        "postgres_cdc_source",
        {{"conninfo", "host=db.internal port=5432 dbname=shop user=cdc"}, {"table", "orders"}},
        "string");
    EXPECT_EQ(d.ns, "postgres://db.internal:5432");
    EXPECT_EQ(d.name, "shop.orders");
}

TEST(LineageDataset, LocalFile) {
    const auto d = dataset_for("file_json_sink", {{"path", "/var/out.json"}}, "row");
    EXPECT_EQ(d.ns, "file");
    EXPECT_EQ(d.name, "/var/out.json");
}

TEST(LineageDataset, GenericFallbackForUnknownConnector) {
    // A connector not enumerated still yields a usable identity.
    const auto d =
        dataset_for("acme_widget_sink", {{"host", "acme:1234"}, {"table", "things"}}, "row");
    EXPECT_EQ(d.ns, "acme_widget://acme:1234");
    EXPECT_EQ(d.name, "things");
    EXPECT_EQ(d.facets.at("connector"), "acme_widget");
}

// --- extract_lineage -------------------------------------------------------

TEST(ExtractLineage, LinearSourceToSink) {
    JobGraphSpec spec;
    spec.ops.push_back(
        op("src", "kafka_source_string", {}, {{"brokers", "b:9092"}, {"topic", "in"}}));
    spec.ops.push_back(op("map", "map_row", {"src"}, {}));
    spec.ops.push_back(op("snk", "file_json_sink", {"map"}, {{"path", "/tmp/out"}}));

    const auto g = extract_lineage(spec);
    ASSERT_EQ(g.sources.size(), 1u);
    ASSERT_EQ(g.sinks.size(), 1u);
    EXPECT_EQ(g.sources[0].id, "src");
    EXPECT_EQ(g.sinks[0].id, "snk");
    EXPECT_EQ(g.sources[0].boundedness, "unbounded");  // kafka
    ASSERT_EQ(g.edges.size(), 1u);
    EXPECT_EQ(g.edges[0].from, "src");
    EXPECT_EQ(g.edges[0].to, "snk");
}

TEST(ExtractLineage, JoinTwoSourcesReachOneSink) {
    JobGraphSpec spec;
    spec.ops.push_back(op("a", "kafka_source_string", {}, {{"topic", "a"}}));
    spec.ops.push_back(op("b", "parquet_row_source", {}, {{"path", "/data/b"}}));
    spec.ops.push_back(op("join", "equi_join_row", {"a", "b"}, {}));
    spec.ops.push_back(op("snk",
                          "clickhouse_sink",
                          {"join"},
                          {{"host", "ch"}, {"port", "9000"}, {"database", "d"}, {"table", "t"}}));

    const auto g = extract_lineage(spec);
    EXPECT_EQ(g.sources.size(), 2u);
    ASSERT_EQ(g.sinks.size(), 1u);
    // Both sources reach the single sink: two edges.
    ASSERT_EQ(g.edges.size(), 2u);
    // Parquet source is bounded; kafka unbounded.
    const auto* a = find_by_id(g.sources, "a");
    const auto* b = find_by_id(g.sources, "b");
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(a->boundedness, "unbounded");
    EXPECT_EQ(b->boundedness, "bounded");
    EXPECT_EQ(g.sinks[0].datasets[0].ns, "clickhouse://ch:9000");
    EXPECT_EQ(g.sinks[0].datasets[0].name, "d.t");
}

TEST(ExtractLineage, SideOutputInputRefResolvesToUpstream) {
    JobGraphSpec spec;
    spec.ops.push_back(op("src", "kafka_source_string", {}, {{"topic", "in"}}));
    // Consumer reads a named side output "src::late" - should still count as
    // src having downstream, so src is a source and the consumer chain leads
    // to the sink.
    spec.ops.push_back(op("snk", "file_json_sink", {"src::late"}, {{"path", "/tmp/o"}}));

    const auto g = extract_lineage(spec);
    ASSERT_EQ(g.sources.size(), 1u);
    ASSERT_EQ(g.sinks.size(), 1u);
    ASSERT_EQ(g.edges.size(), 1u);
    EXPECT_EQ(g.edges[0].from, "src");
    EXPECT_EQ(g.edges[0].to, "snk");
}

// --- JSON round-trip -------------------------------------------------------

TEST(LineageJson, RoundTrips) {
    JobGraphSpec spec;
    spec.ops.push_back(
        op("src", "kafka_source_string", {}, {{"brokers", "b:9092"}, {"topic", "in"}}));
    auto& s = spec.ops.back();
    s.uid = "my-source";
    s.display_name = "orders source";
    spec.ops.push_back(op("snk", "file_json_sink", {"src"}, {{"path", "/tmp/out"}}));

    const auto g = extract_lineage(spec);
    const auto json = g.to_json();
    const auto g2 = LineageGraph::from_json(json);

    ASSERT_EQ(g2.sources.size(), 1u);
    ASSERT_EQ(g2.sinks.size(), 1u);
    ASSERT_EQ(g2.edges.size(), 1u);
    EXPECT_EQ(g2.sources[0].id, "src");
    EXPECT_EQ(g2.sources[0].uid, "my-source");
    EXPECT_EQ(g2.sources[0].name, "orders source");
    EXPECT_EQ(g2.sources[0].boundedness, "unbounded");
    ASSERT_EQ(g2.sources[0].datasets.size(), 1u);
    EXPECT_EQ(g2.sources[0].datasets[0].ns, "kafka://b:9092");
    EXPECT_EQ(g2.sources[0].datasets[0].name, "in");
    EXPECT_EQ(g2.sources[0].datasets[0].facets.at("connector"), "kafka");
    EXPECT_EQ(g2.edges[0].from, "src");
    EXPECT_EQ(g2.edges[0].to, "snk");
}

TEST(LineageJson, FromJsonIgnoresUnknownWrapperKeys) {
    // A payload wrapping the graph with extra keys (job_id) parses cleanly.
    const std::string payload =
        R"({"job_id":7,"sources":[{"id":"s","datasets":[{"namespace":"kafka://b","name":"t","facets":{}}]}],"sinks":[],"edges":[]})";
    const auto g = LineageGraph::from_json(payload);
    ASSERT_EQ(g.sources.size(), 1u);
    EXPECT_EQ(g.sources[0].id, "s");
    EXPECT_EQ(g.sources[0].datasets[0].ns, "kafka://b");
}

// --- dispatcher: EventBus -> listener --------------------------------------

namespace {
class CapturingListener : public LineageListener {
public:
    std::vector<LineageEvent> events;
    void on_event(const LineageEvent& ev) override { events.push_back(ev); }
};
}  // namespace

TEST(LineageDispatcher, TranslatesBusEventsToListenerCalls) {
    // Use a private bus so the test is isolated from any global subscribers.
    clink::EventBus bus;

    auto listener = std::make_unique<CapturingListener>();
    auto* raw = listener.get();
    std::vector<std::unique_ptr<LineageListener>> listeners;
    listeners.push_back(std::move(listener));
    LineageDispatcher dispatcher(std::move(listeners), bus);
    EXPECT_EQ(dispatcher.listener_count(), 1u);

    // A job-lineage event -> JobStarted with the reconstructed graph.
    JobGraphSpec spec;
    spec.ops.push_back(
        op("src", "kafka_source_string", {}, {{"brokers", "b:9092"}, {"topic", "in"}}));
    spec.ops.push_back(op("snk", "file_json_sink", {"src"}, {{"path", "/tmp/o"}}));
    const auto lg = extract_lineage(spec);
    bus.publish(clink::Event{
        123,
        kEventJobLineage,
        "{\"job_id\":42,\"job_name\":\"orders-etl\",\"lineage\":" + lg.to_json() + "}"});

    // A completion event -> JobCompleted with name + status (real payload
    // shape: "errors" is a count).
    bus.publish(clink::Event{456,
                             kEventJobCompleted,
                             R"({"job_id":42,"job_name":"orders-etl","status":"ok","errors":0})"});

    // An unrelated event -> ignored.
    bus.publish(clink::Event{789, "jm.tm_registered", R"({"tm_id":"tm-1"})"});

    ASSERT_EQ(raw->events.size(), 2u);

    const auto& started = raw->events[0];
    EXPECT_EQ(started.kind, LineageEvent::Kind::JobStarted);
    EXPECT_EQ(started.job_id, 42u);
    EXPECT_EQ(started.job_name, "orders-etl");
    ASSERT_EQ(started.graph.sources.size(), 1u);
    EXPECT_EQ(started.graph.sources[0].datasets[0].name, "in");

    const auto& completed = raw->events[1];
    EXPECT_EQ(completed.kind, LineageEvent::Kind::JobCompleted);
    EXPECT_EQ(completed.job_id, 42u);
    EXPECT_EQ(completed.job_name, "orders-etl");
    EXPECT_EQ(completed.status, "ok");
}

TEST(LineageDispatcher, FailedCompletionCarriesError) {
    clink::EventBus bus;
    auto listener = std::make_unique<CapturingListener>();
    auto* raw = listener.get();
    std::vector<std::unique_ptr<LineageListener>> listeners;
    listeners.push_back(std::move(listener));
    LineageDispatcher dispatcher(std::move(listeners), bus);

    // Real failed-completion payload: "errors" is a count, "error" the first
    // failure string.
    bus.publish(
        clink::Event{1,
                     kEventJobCompleted,
                     R"({"job_id":9,"job_name":"j","status":"failed","errors":2,"error":"boom"})"});

    ASSERT_EQ(raw->events.size(), 1u);
    EXPECT_EQ(raw->events[0].status, "failed");
    EXPECT_EQ(raw->events[0].error, "boom");
}

TEST(LineageDispatcher, UnsubscribesOnDestruction) {
    clink::EventBus bus;
    auto* raw = [&] {
        auto listener = std::make_unique<CapturingListener>();
        auto* r = listener.get();
        std::vector<std::unique_ptr<LineageListener>> listeners;
        listeners.push_back(std::move(listener));
        // Dispatcher (and its listener) destroyed at end of this scope.
        LineageDispatcher d(std::move(listeners), bus);
        bus.publish(clink::Event{1, kEventJobCompleted, R"({"job_id":1,"status":"ok"})"});
        EXPECT_EQ(r->events.size(), 1u);
        return r;
    }();
    // After the dispatcher is gone, publishing must not touch freed memory.
    bus.publish(clink::Event{2, kEventJobCompleted, R"({"job_id":2,"status":"ok"})"});
    // raw is dangling now; we only assert the publish above did not crash.
    (void)raw;
}
