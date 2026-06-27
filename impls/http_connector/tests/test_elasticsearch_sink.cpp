// Elasticsearch/OpenSearch _bulk sink: NDJSON action+doc body framing, the
// optional document_id -> _id (idempotent writes), and the _bulk gotcha that a
// partial failure returns HTTP 200 with "errors":true (must be treated as a
// failure, not silently accepted).

#include <atomic>
#include <chrono>
#include <httplib.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/bulk_sink_builders.hpp"
#include "clink/metrics/metrics_registry.hpp"

using clink::Batch;
using clink::http_connector::EsBulkOptions;
using clink::http_connector::make_es_bulk_sink;

namespace {

// Stub Elasticsearch: /_bulk -> 200 {"errors":false}; /_bulk_partial -> 200
// {"errors":true} (the partial-failure shape). Captures request bodies.
class EsStub {
public:
    EsStub() {
        auto capture = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                bodies_.push_back(req.body);
                content_types_.push_back(req.get_header_value("Content-Type"));
            }
            res.status = 200;
            res.set_content(R"({"errors":false,"items":[]})", "application/json");
        };
        svr_.Post("/_bulk", capture);
        svr_.Post("/_bulk_partial", [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                bodies_.push_back(req.body);
            }
            res.status = 200;  // _bulk's footgun: 200 even though an item failed
            res.set_content(R"({"errors":true,"items":[{"index":{"status":400}}]})",
                            "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~EsStub() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    std::vector<std::string> bodies() {
        std::lock_guard<std::mutex> lk(mu_);
        return bodies_;
    }
    std::vector<std::string> content_types() {
        std::lock_guard<std::mutex> lk(mu_);
        return content_types_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> bodies_;
    std::vector<std::string> content_types_;
};

Batch<std::string> batch_of(std::vector<std::string> recs) {
    Batch<std::string> b;
    for (auto& r : recs) {
        b.emplace(std::move(r));
    }
    return b;
}

bool contains(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
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

TEST(ElasticsearchSink, BulkBodyIsActionPlusDocNdjson) {
    EsStub srv;
    EsBulkOptions o;
    o.url = srv.url();
    o.index = "logs";
    o.batch_records = 100;  // flush via flush(), not auto
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"a":1})", R"({"a":2})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Each item is an action line then the source doc, both newline-terminated.
    EXPECT_EQ(got[0],
              "{\"index\":{\"_index\":\"logs\"}}\n{\"a\":1}\n"
              "{\"index\":{\"_index\":\"logs\"}}\n{\"a\":2}\n");
    // _bulk requires the NDJSON content type.
    ASSERT_FALSE(srv.content_types().empty());
    EXPECT_EQ(srv.content_types()[0], "application/x-ndjson");
}

TEST(ElasticsearchSink, DocumentIdGoesIntoActionLine) {
    EsStub srv;
    EsBulkOptions o;
    o.url = srv.url();
    o.index = "logs";
    o.document_id = "id";  // idempotent: re-delivery overwrites the same _id
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":"k1","a":1})", R"({"id":42,"a":2})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Key order in the action metadata is the JSON serializer's (sorted), so
    // assert on the presence of both fields rather than exact ordering.
    EXPECT_TRUE(contains(got[0], R"("_index":"logs")")) << got[0];
    EXPECT_TRUE(contains(got[0], R"("_id":"k1")")) << got[0];         // string id
    EXPECT_TRUE(contains(got[0], R"("_id":"42")")) << got[0];         // numeric id stringified
    EXPECT_TRUE(contains(got[0], R"({"id":"k1","a":1})")) << got[0];  // source doc verbatim
}

TEST(ElasticsearchSink, LargeNumericDocumentIdDoesNotCollapseOrTrapUb) {
    // A numeric document_id beyond int64 range must NOT be cast to int64 (UB,
    // and on saturation every such id would collapse onto INT64_MAX, silently
    // overwriting distinct documents). Two distinct > 9.2e18 ids must stay
    // distinct in the action metadata.
    EsStub srv;
    EsBulkOptions o;
    o.url = srv.url();
    o.index = "logs";
    o.document_id = "id";
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"id":1e19,"a":1})", R"({"id":2e19,"a":2})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Both ids are present and DIFFERENT (no INT64_MAX collapse).
    EXPECT_FALSE(contains(got[0], R"("_id":"9223372036854775807")")) << got[0];
    const auto first = got[0].find(R"("_id":)");
    ASSERT_NE(first, std::string::npos) << got[0];
    const auto second = got[0].find(R"("_id":)", first + 1);
    ASSERT_NE(second, std::string::npos) << got[0];
    EXPECT_NE(got[0].substr(first, 30), got[0].substr(second, 30)) << got[0];
}

TEST(ElasticsearchSink, ErrorsFalseResponseSucceeds) {
    EsStub srv;
    EsBulkOptions o;
    o.url = srv.url();
    o.index = "logs";
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"a":1})"}));
    EXPECT_NO_THROW(sink->flush());
    EXPECT_EQ(srv.bodies().size(), 1u);
}

TEST(ElasticsearchSink, PartialFailureErrorsTrueIsTreatedAsFailure) {
    EsStub srv;
    EsBulkOptions o;
    o.url = srv.url();
    o.index = "logs";
    o.path = "/_bulk_partial";  // returns 200 with "errors":true
    o.max_retries = 1;          // fast
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"a":1})"}));
    try {
        sink->flush();
        FAIL() << "expected throw on errors:true partial failure";
    } catch (const std::runtime_error& e) {
        // 2xx-but-rejected keeps a body snippet for diagnosis.
        EXPECT_TRUE(contains(e.what(), "errors") || contains(e.what(), "HTTP 200")) << e.what();
    }
}

TEST(ElasticsearchSink, EmitsConnectorMetricsOnFlush) {
    EsStub srv;
    EsBulkOptions o;
    o.url = srv.url();
    o.index = "logs";
    o.batch_records = 100;
    auto sink = make_es_bulk_sink(o);
    sink->open();
    // Delta-based so the test is robust to the shared global registry.
    const std::string records =
        R"(clink_connector_records_total{connector="elasticsearch_sink",direction="sink"})";
    const auto before = counter_value(records);
    sink->on_data(batch_of({R"({"a":1})", R"({"a":2})", R"({"a":3})"}));
    sink->flush();
    EXPECT_EQ(counter_value(records) - before, 3u);
}

TEST(ElasticsearchSink, RequiresUrlAndIndex) {
    EsBulkOptions o;
    o.index = "logs";  // no url
    EXPECT_THROW(make_es_bulk_sink(o), std::runtime_error);
    EsBulkOptions o2;
    o2.url = "http://x:1";  // no index
    EXPECT_THROW(make_es_bulk_sink(o2), std::runtime_error);
}
