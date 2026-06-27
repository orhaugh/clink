// Elasticsearch LIVE integration test. SKIPPED unless CLINK_ELASTICSEARCH_TEST_
// ENDPOINT is set (e.g. http://localhost:9200 from docker/integration-services.yml).
// Proves against a real ES: a bulk index lands every doc, document_id makes
// re-delivery idempotent (count unchanged), and the per-item _bulk handler
// dead-letters a single mapping-error doc while indexing the good ones.

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/metrics/metrics_registry.hpp"

using clink::Batch;
using clink::http_connector::DlqPolicy;
using clink::http_connector::EsBulkOptions;
using clink::http_connector::HttpRequest;
using clink::http_connector::make_es_bulk_sink;

namespace {

bool es_configured() {
    return std::getenv("CLINK_ELASTICSEARCH_TEST_ENDPOINT") != nullptr;
}
std::string es_endpoint() {
    return std::getenv("CLINK_ELASTICSEARCH_TEST_ENDPOINT");
}

std::string unique_index() {
    static int n = 0;
    return "clink-it-" + std::to_string(static_cast<long>(::getpid())) + "-" + std::to_string(n++);
}

Batch<std::string> batch_of(std::vector<std::string> recs) {
    Batch<std::string> b;
    for (auto& r : recs) {
        b.emplace(std::move(r));
    }
    return b;
}

HttpRequest es_client() {
    HttpRequest::Options o;
    o.base_url = es_endpoint();
    return HttpRequest{o};
}

// Force visibility (ES is near-real-time) then read the doc count of an index.
long es_count(HttpRequest& c, const std::string& index) {
    c.post("/" + index + "/_refresh", "", "application/json");
    auto r = c.get("/" + index + "/_count");
    if (r.status < 200 || r.status >= 300) {
        return -1;
    }
    auto j = clink::config::parse(r.body);
    if (j.is_object()) {
        const auto& o = j.as_object();
        if (auto it = o.find("count"); it != o.end() && it->second.is_number()) {
            return static_cast<long>(it->second.as_number());
        }
    }
    return -1;
}

std::uint64_t counter_value(const std::string& name) {
    for (const auto& [k, v] : clink::MetricsRegistry::global().snapshot().counters) {
        if (k == name) {
            return v;
        }
    }
    return 0;
}

}  // namespace

TEST(ElasticsearchLive, BulkIndexLandsEveryDoc) {
    if (!es_configured()) {
        GTEST_SKIP() << "set CLINK_ELASTICSEARCH_TEST_ENDPOINT (docker/integration-services.yml)";
    }
    const std::string index = unique_index();
    EsBulkOptions o;
    o.url = es_endpoint();
    o.index = index;
    o.document_id = "id";
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":"1","v":"a"})",
                            R"({"id":"2","v":"b"})",
                            R"({"id":"3","v":"c"})",
                            R"({"id":"4","v":"d"})",
                            R"({"id":"5","v":"e"})"}));
    sink->flush();
    auto c = es_client();
    EXPECT_EQ(es_count(c, index), 5);
}

TEST(ElasticsearchLive, DocumentIdMakesReDeliveryIdempotent) {
    if (!es_configured()) {
        GTEST_SKIP() << "set CLINK_ELASTICSEARCH_TEST_ENDPOINT";
    }
    const std::string index = unique_index();
    EsBulkOptions o;
    o.url = es_endpoint();
    o.index = index;
    o.document_id = "id";
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const auto records =
        batch_of({R"({"id":"x","v":1})", R"({"id":"y","v":2})", R"({"id":"z","v":3})"});
    sink->on_data(records);
    sink->flush();
    sink->on_data(records);  // re-deliver the same docs (replay)
    sink->flush();
    auto c = es_client();
    EXPECT_EQ(es_count(c, index), 3) << "same document_id overwrites, not duplicates";
}

TEST(ElasticsearchLive, PerItemDlqDropsMappingErrorAndIndexesTheRest) {
    if (!es_configured()) {
        GTEST_SKIP() << "set CLINK_ELASTICSEARCH_TEST_ENDPOINT";
    }
    const std::string index = unique_index();
    auto c = es_client();
    // Strict-ish mapping: n is an integer; a non-numeric n is a 400 per item.
    auto created = c.put(
        "/" + index, R"({"mappings":{"properties":{"n":{"type":"integer"}}}})", "application/json");
    ASSERT_TRUE(created.status >= 200 && created.status < 300)
        << created.status << " " << created.body;

    EsBulkOptions o;
    o.url = es_endpoint();
    o.index = index;
    o.document_id = "id";
    o.dlq_policy = DlqPolicy::Drop;  // dead-letter the poison doc, do not crash-loop
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    const std::string dropped =
        R"(clink_connector_dropped_records_total{connector="elasticsearch_sink",direction="sink"})";
    const auto before = counter_value(dropped);
    sink->on_data(batch_of({R"({"id":"good","n":7})", R"({"id":"bad","n":"not-an-int"})"}));
    EXPECT_NO_THROW(sink->flush());  // the mapping error is dead-lettered, not crash-looped
    EXPECT_EQ(counter_value(dropped) - before, 1u);
    EXPECT_EQ(es_count(c, index), 1) << "only the good doc indexed";
}
