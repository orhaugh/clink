// BatchedHttpBulkSink against a local httplib server stub: framing, flush
// triggers (count threshold, checkpoint barrier), retry on transient failure,
// and throw-on-permanent-failure (at-least-once: never silently drop).

#include <atomic>
#include <chrono>
#include <httplib.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/http_connector/batched_http_bulk_sink.hpp"
#include "clink/metrics/metrics_registry.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::http_connector::BatchedHttpBulkSink;
using clink::http_connector::BulkFraming;
using clink::http_connector::DlqPolicy;

namespace {

std::uint64_t counter_value(const std::string& name) {
    for (const auto& [k, v] : clink::MetricsRegistry::global().snapshot().counters) {
        if (k == name) {
            return v;
        }
    }
    return 0;
}

// A local HTTP server that captures POST bodies and can be told to fail the
// first N requests with 503 (for the retry path).
class StubServer {
public:
    explicit StubServer(int fail_first = 0) : fail_remaining_(fail_first) {
        svr_.Post("/ingest", [this](const httplib::Request& req, httplib::Response& res) {
            if (fail_remaining_.fetch_sub(1) > 0) {
                res.status = 503;
                return;
            }
            {
                std::lock_guard<std::mutex> lk(mu_);
                bodies_.push_back(req.body);
            }
            res.status = 200;
        });
        svr_.Post("/always500",
                  [](const httplib::Request&, httplib::Response& res) { res.status = 500; });
        svr_.Post("/always400",  // permanent (poison) request
                  [](const httplib::Request&, httplib::Response& res) { res.status = 400; });
        svr_.Post("/redirect",  // 3xx: operator-fixable misconfig, must NOT be dropped
                  [](const httplib::Request&, httplib::Response& res) { res.status = 301; });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~StubServer() {
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

private:
    httplib::Server svr_;
    std::atomic<int> fail_remaining_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> bodies_;
};

BatchedHttpBulkSink<std::string>::Options base_opts(const std::string& url, std::size_t max_recs) {
    BatchedHttpBulkSink<std::string>::Options o;
    o.http.base_url = url;
    o.path = "/ingest";
    o.max_records = max_recs;
    o.retry_base_backoff = std::chrono::milliseconds{5};  // keep the retry test fast
    o.name = "test_http_sink";
    return o;
}

// Render: the record is already a JSON object string; append verbatim.
auto verbatim() {
    return [](std::string& out, const std::string& rec) { out += rec; };
}

Batch<std::string> batch_of(std::vector<std::string> recs) {
    Batch<std::string> b;
    for (auto& r : recs) {
        b.emplace(std::move(r));
    }
    return b;
}

}  // namespace

TEST(HttpBulkSink, JsonArrayFramingFlushesOnBarrier) {
    StubServer srv;
    auto opts = base_opts(srv.url(), /*max_records=*/100);  // above batch size -> no auto-flush
    opts.framing = BulkFraming::JsonArray;
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    sink.on_data(batch_of({R"({"a":1})", R"({"a":2})", R"({"a":3})"}));
    EXPECT_TRUE(srv.bodies().empty());  // nothing sent until a flush trigger
    sink.on_barrier(CheckpointBarrier{CheckpointId{1}});
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0], R"([{"a":1},{"a":2},{"a":3}])");
}

TEST(HttpBulkSink, NdjsonFraming) {
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.framing = BulkFraming::Ndjson;
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    sink.on_data(batch_of({R"({"a":1})", R"({"a":2})"}));
    sink.flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(got[0], "{\"a\":1}\n{\"a\":2}\n");
}

TEST(HttpBulkSink, AutoFlushesAtMaxRecords) {
    StubServer srv;
    auto opts = base_opts(srv.url(), /*max_records=*/2);
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    sink.on_data(batch_of({R"({"a":1})", R"({"a":2})", R"({"a":3})"}));  // 2 then 1 buffered
    EXPECT_EQ(srv.bodies().size(), 1u);                                  // first 2 auto-flushed
    sink.flush();                                                        // the trailing 1
    EXPECT_EQ(srv.bodies().size(), 2u);
}

TEST(HttpBulkSink, RetriesTransientFailureThenSucceeds) {
    StubServer srv(/*fail_first=*/2);  // first 2 POSTs -> 503, then 200
    auto opts = base_opts(srv.url(), 100);
    opts.max_retries = 4;
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    sink.on_data(batch_of({R"({"a":1})"}));
    EXPECT_NO_THROW(sink.flush());
    ASSERT_EQ(srv.bodies().size(), 1u);
    EXPECT_EQ(srv.bodies()[0], R"([{"a":1}])");
}

TEST(HttpBulkSink, ThrowsOnPermanentFailureNeverDrops) {
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.path = "/always500";
    opts.max_retries = 1;  // 2 attempts total, fast
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    sink.on_data(batch_of({R"({"a":1})"}));
    EXPECT_THROW(sink.flush(), std::runtime_error);
}

TEST(HttpBulkSink, ClampsMaxRetries) {
    StubServer srv;
    // High: max_retries=1000 must clamp to 20 (21 attempts) so the backoff
    // shift can't overflow and the sink can't schedule a runaway sleep. 0
    // backoff keeps the test fast.
    {
        auto opts = base_opts(srv.url(), 100);
        opts.path = "/always500";
        opts.max_retries = 1000;
        opts.retry_base_backoff = std::chrono::milliseconds{0};
        BatchedHttpBulkSink<std::string> sink(opts, verbatim());
        sink.open();
        sink.on_data(batch_of({R"({"a":1})"}));
        try {
            sink.flush();
            FAIL() << "expected throw on permanent failure";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("21 attempts"), std::string::npos) << e.what();
        }
    }
    // Negative: clamps to 0 (1 attempt) - still throws (no silent no-delivery).
    {
        auto opts = base_opts(srv.url(), 100);
        opts.path = "/always500";
        opts.max_retries = -5;
        opts.retry_base_backoff = std::chrono::milliseconds{0};
        BatchedHttpBulkSink<std::string> sink(opts, verbatim());
        sink.open();
        sink.on_data(batch_of({R"({"a":1})"}));
        try {
            sink.flush();
            FAIL() << "expected throw";
        } catch (const std::runtime_error& e) {
            EXPECT_NE(std::string(e.what()).find("1 attempts"), std::string::npos) << e.what();
        }
    }
}

TEST(HttpBulkSink, RejectsHttpsWhenTlsUnsupported) {
    if (clink::http_connector::HttpRequest::tls_supported()) {
        GTEST_SKIP() << "build has TLS; the reject-https guard does not apply";
    }
    auto opts = base_opts("https://example.invalid", 100);
    EXPECT_THROW(BatchedHttpBulkSink<std::string>(opts, verbatim()), std::runtime_error);
}

TEST(HttpFailureClassify, TransientVsPermanent) {
    using clink::http_connector::classify_http_status;
    using clink::http_connector::HttpFailureClass;
    // Transient: transport error, rate limit, 5xx.
    EXPECT_EQ(classify_http_status(0), HttpFailureClass::Transient);
    EXPECT_EQ(classify_http_status(429), HttpFailureClass::Transient);
    EXPECT_EQ(classify_http_status(500), HttpFailureClass::Transient);
    EXPECT_EQ(classify_http_status(503), HttpFailureClass::Transient);
    // Permanent: 4xx (except 429) - a poison request that replays identically.
    EXPECT_EQ(classify_http_status(400), HttpFailureClass::Permanent);
    EXPECT_EQ(classify_http_status(403), HttpFailureClass::Permanent);
    EXPECT_EQ(classify_http_status(404), HttpFailureClass::Permanent);
}

TEST(HttpDlq, DropPolicyDropsPermanentFailureInsteadOfThrowing) {
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.path = "/always400";  // permanent (poison) request
    opts.max_retries = 1;
    opts.dlq_policy = DlqPolicy::Drop;
    opts.name = "dlq_http_sink";
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"a":1})"});
    b.emplace(std::string{R"({"a":2})"});
    sink.on_data(b);
    const std::string dropped =
        R"(clink_connector_dropped_records_total{connector="dlq_http_sink",direction="sink"})";
    const auto before = counter_value(dropped);
    EXPECT_NO_THROW(sink.flush());  // poison batch dropped, not crash-looped
    EXPECT_EQ(counter_value(dropped) - before, 2u);
}

TEST(HttpDlq, DropPolicyStillThrowsOnTransientExhaustion) {
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.path = "/always500";  // transient (outage) - must replay, not drop
    opts.max_retries = 1;
    opts.dlq_policy = DlqPolicy::Drop;
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"a":1})"});
    sink.on_data(b);
    EXPECT_THROW(sink.flush(), std::runtime_error);  // transient -> replay, never silent drop
}

TEST(HttpDlq, DropPolicyDoesNotDrop3xxRedirect) {
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.path = "/redirect";  // 301 - a misconfig, not poison data
    opts.max_retries = 1;
    opts.dlq_policy = DlqPolicy::Drop;
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"a":1})"});
    sink.on_data(b);
    EXPECT_THROW(sink.flush(), std::runtime_error);  // surfaces, not silently dropped
}

TEST(HttpDlq, DefaultFailPolicyThrowsOnPermanent) {
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.path = "/always400";
    opts.max_retries = 1;
    // default dlq_policy == Fail
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"a":1})"});
    sink.on_data(b);
    EXPECT_THROW(sink.flush(), std::runtime_error);
}

TEST(HttpBulkSink, ResponseHandlerOutOfRangeIndexIsIgnoredNotUB) {
    // The response_handler is a public extension seam; a misbehaving custom
    // handler returning an out-of-range index must be skipped, not indexed OOB.
    StubServer srv;
    auto opts = base_opts(srv.url(), 100);
    opts.response_handler = [](const clink::http_connector::HttpResponse&, std::size_t) {
        clink::http_connector::BulkResult r;
        r.ok = false;
        r.failed_transient = {99};  // out of range for a 1-record batch
        return r;
    };
    opts.max_retries = 0;
    BatchedHttpBulkSink<std::string> sink(opts, verbatim());
    sink.open();
    Batch<std::string> b;
    b.emplace(std::string{R"({"a":1})"});
    sink.on_data(b);
    EXPECT_NO_THROW(sink.flush());  // OOB index skipped -> no survivors -> success, no UB
}
