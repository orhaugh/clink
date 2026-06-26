// Splunk HEC sink: the {"event":...} envelope, the `Authorization: Splunk
// <token>` header, application/json content type, and NDJSON batching of
// concatenated event objects. HEC (unlike ES _bulk) signals failure via the
// HTTP status, so a plain 2xx check suffices.

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

using clink::Batch;
using clink::http_connector::make_splunk_hec_sink;
using clink::http_connector::SplunkHecOptions;

namespace {

class HecStub {
public:
    HecStub() {
        auto capture = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                bodies_.push_back(req.body);
                auths_.push_back(req.get_header_value("Authorization"));
                content_types_.push_back(req.get_header_value("Content-Type"));
                paths_.push_back(req.path);
            }
            res.status = 200;
            res.set_content(R"({"text":"Success","code":0})", "application/json");
        };
        svr_.Post("/services/collector/event", capture);
        svr_.Post("/services/collector", capture);
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~HecStub() {
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
    std::vector<std::string> auths() {
        std::lock_guard<std::mutex> lk(mu_);
        return auths_;
    }
    std::vector<std::string> content_types() {
        std::lock_guard<std::mutex> lk(mu_);
        return content_types_;
    }
    std::vector<std::string> paths() {
        std::lock_guard<std::mutex> lk(mu_);
        return paths_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> bodies_;
    std::vector<std::string> auths_;
    std::vector<std::string> content_types_;
    std::vector<std::string> paths_;
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

}  // namespace

TEST(SplunkHecSink, WrapsRecordsInEventEnvelopeNdjson) {
    HecStub srv;
    SplunkHecOptions o;
    o.url = srv.url();
    o.token = "TOKEN-1";
    o.batch_records = 100;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"a":1})", R"({"a":2})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Each record is embedded verbatim as the "event" value; events are
    // newline-separated (NDJSON), each line terminated.
    EXPECT_EQ(got[0], "{\"event\":{\"a\":1}}\n{\"event\":{\"a\":2}}\n");
    // Canonical HEC endpoint + auth keyword + content type.
    ASSERT_FALSE(srv.paths().empty());
    EXPECT_EQ(srv.paths()[0], "/services/collector/event");
    EXPECT_EQ(srv.auths()[0], "Splunk TOKEN-1");
    EXPECT_EQ(srv.content_types()[0], "application/json");
}

TEST(SplunkHecSink, EventMetadataGoesInTheEnvelope) {
    HecStub srv;
    SplunkHecOptions o;
    o.url = srv.url();
    o.token = "T";
    o.sourcetype = "_json";
    o.index = "main";
    o.batch_records = 100;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"a":1})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_TRUE(contains(got[0], R"("event":{"a":1})")) << got[0];
    EXPECT_TRUE(contains(got[0], R"("sourcetype":"_json")")) << got[0];
    EXPECT_TRUE(contains(got[0], R"("index":"main")")) << got[0];
}

TEST(SplunkHecSink, RequiresUrlAndToken) {
    SplunkHecOptions o;
    o.token = "T";  // no url
    EXPECT_THROW(make_splunk_hec_sink(o), std::runtime_error);
    SplunkHecOptions o2;
    o2.url = "http://x:1";  // no token
    EXPECT_THROW(make_splunk_hec_sink(o2), std::runtime_error);
}
