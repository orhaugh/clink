// HTTP end-to-end round-trip: http_sink POSTs records to a server, then
// http_poll_source GETs them back. Uses an in-process httplib server (a faithful
// real HTTP peer with a keep-alive client), so it runs in CI with no external
// dependency - it exercises the sink and the poll source together over the wire.

#include <atomic>
#include <chrono>
#include <httplib.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/http_connector/http_poll_source.hpp"
#include "clink/operators/operator_base.hpp"

using clink::Batch;
using clink::Emitter;
using clink::StreamElement;
using clink::http_connector::BatchedHttpBulkSink;
using clink::http_connector::BulkFraming;
using clink::http_connector::HttpPollOptions;
using clink::http_connector::make_http_poll_source;

namespace {

// A tiny store-and-forward HTTP service: POST /ingest stores each NDJSON line;
// GET /poll returns the stored records as a JSON array.
class StoreForwardServer {
public:
    StoreForwardServer() {
        svr_.Post("/ingest", [this](const httplib::Request& req, httplib::Response& res) {
            std::lock_guard<std::mutex> lk(mu_);
            std::size_t start = 0;
            while (start < req.body.size()) {
                auto nl = req.body.find('\n', start);
                const std::size_t end = nl == std::string::npos ? req.body.size() : nl;
                if (end > start) {
                    records_.push_back(req.body.substr(start, end - start));
                }
                if (nl == std::string::npos) {
                    break;
                }
                start = nl + 1;
            }
            res.status = 200;
        });
        svr_.Get("/poll", [this](const httplib::Request&, httplib::Response& res) {
            std::string body = "[";
            {
                std::lock_guard<std::mutex> lk(mu_);
                for (std::size_t i = 0; i < records_.size(); ++i) {
                    if (i) {
                        body.push_back(',');
                    }
                    body += records_[i];
                }
            }
            body.push_back(']');
            res.set_content(body, "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~StoreForwardServer() {
        svr_.stop();
        if (thread_.joinable()) {
            thread_.join();
        }
    }
    std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    std::size_t stored() {
        std::lock_guard<std::mutex> lk(mu_);
        return records_.size();
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> records_;
};

struct Captured {
    std::vector<std::string> values;
};
Emitter<std::string> capturing(Captured& sink) {
    return Emitter<std::string>{[&sink](StreamElement<std::string> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data()) {
                sink.values.push_back(r.value());
            }
        }
        return true;
    }};
}
bool contains(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

}  // namespace

TEST(HttpRoundTrip, SinkThenPollSourceDeliversEveryRecord) {
    StoreForwardServer srv;

    // Sink three records (NDJSON) to /ingest.
    BatchedHttpBulkSink<std::string>::Options sopts;
    sopts.http.base_url = srv.url();
    sopts.path = "/ingest";
    sopts.framing = BulkFraming::Ndjson;
    sopts.max_records = 100;
    sopts.name = "rt_http_sink";
    BatchedHttpBulkSink<std::string> sink(
        sopts, [](std::string& out, const std::string& rec) { out += rec; });
    sink.open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"id":1})"});
    b.emplace(std::string{R"({"id":2})"});
    b.emplace(std::string{R"({"id":3})"});
    sink.on_data(b);
    sink.flush();
    sink.close();
    ASSERT_EQ(srv.stored(), 3u);

    // Poll them back from /poll.
    HttpPollOptions po;
    po.url = srv.url();
    po.path = "/poll";
    po.interval = std::chrono::milliseconds{0};
    auto src = make_http_poll_source(std::move(po));
    Captured cap;
    auto em = capturing(cap);
    src->produce(em);
    ASSERT_EQ(cap.values.size(), 3u);
    EXPECT_TRUE(contains(cap.values[0], "\"id\":1")) << cap.values[0];
    EXPECT_TRUE(contains(cap.values[1], "\"id\":2")) << cap.values[1];
    EXPECT_TRUE(contains(cap.values[2], "\"id\":3")) << cap.values[2];
}
