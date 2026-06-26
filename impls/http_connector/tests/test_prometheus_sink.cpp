// Prometheus Pushgateway sink: the /metrics/job/<job> push path, the text
// exposition body (# TYPE + one sample line per series, trailing newline), label
// derivation + escaping, and the load-bearing dedup (the gateway rejects two
// lines with the same metric name + identical labels, so the sink collapses
// them last-write-wins).

#include <atomic>
#include <chrono>
#include <httplib.h>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/prometheus_push_sink.hpp"

using clink::Batch;
using clink::http_connector::make_prometheus_pushgateway_sink;
using clink::http_connector::PromPushOptions;

namespace {

class PgwStub {
public:
    explicit PgwStub(int status = 200) : status_(status) {
        auto capture = [this](const httplib::Request& req, httplib::Response& res) {
            {
                std::lock_guard<std::mutex> lk(mu_);
                bodies_.push_back(req.body);
                paths_.push_back(req.path);
                content_types_.push_back(req.get_header_value("Content-Type"));
            }
            res.status = status_.load();
        };
        svr_.Post(R"(/metrics/.*)", capture);
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~PgwStub() {
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
    std::vector<std::string> paths() {
        std::lock_guard<std::mutex> lk(mu_);
        return paths_;
    }
    std::vector<std::string> content_types() {
        std::lock_guard<std::mutex> lk(mu_);
        return content_types_;
    }

private:
    httplib::Server svr_;
    std::atomic<int> status_;
    int port_{0};
    std::thread thread_;
    std::mutex mu_;
    std::vector<std::string> bodies_;
    std::vector<std::string> paths_;
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

int count_occurrences(const std::string& hay, const std::string& needle) {
    int n = 0;
    for (std::size_t p = hay.find(needle); p != std::string::npos; p = hay.find(needle, p + 1)) {
        ++n;
    }
    return n;
}

}  // namespace

TEST(PrometheusSink, PushPathAndExpositionBody) {
    PgwStub srv;
    PromPushOptions o;
    o.url = srv.url();
    o.job = "myjob";
    o.metric_name = "temperature";
    o.value_field = "v";
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"v":21.5,"sensor":"a"})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    // Job is in the URL path, not the body.
    ASSERT_FALSE(srv.paths().empty());
    EXPECT_EQ(srv.paths()[0], "/metrics/job/myjob");
    EXPECT_TRUE(contains(srv.content_types()[0], "text/plain")) << srv.content_types()[0];
    // # TYPE once, then a sample line, body ends with a newline.
    EXPECT_TRUE(contains(got[0], "# TYPE temperature gauge\n")) << got[0];
    EXPECT_TRUE(contains(got[0], "temperature{sensor=\"a\"} 21.5\n")) << got[0];
    EXPECT_EQ(got[0].back(), '\n');
}

TEST(PrometheusSink, DeduplicatesIdenticalSeriesLastWriteWins) {
    // Two records map to the SAME series (same labels) - the gateway would 400
    // on duplicate lines, so the sink must collapse to one line, last value.
    PgwStub srv;
    PromPushOptions o;
    o.url = srv.url();
    o.job = "j";
    o.metric_name = "g";
    o.value_field = "value";
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"value":1,"host":"a"})", R"({"value":2,"host":"a"})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_EQ(count_occurrences(got[0], "g{host=\"a\"}"), 1) << got[0];
    EXPECT_TRUE(contains(got[0], "g{host=\"a\"} 2\n")) << got[0];   // last value won
    EXPECT_FALSE(contains(got[0], "g{host=\"a\"} 1\n")) << got[0];  // first value dropped
}

TEST(PrometheusSink, LabelValuesAreEscaped) {
    PgwStub srv;
    PromPushOptions o;
    o.url = srv.url();
    o.job = "j";
    o.metric_name = "g";
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    // msg = he said "hi"\nbye  ->  backslash-escape the quote and newline.
    sink->on_data(batch_of({R"({"value":1,"msg":"he said \"hi\"\nbye"})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_TRUE(contains(got[0], R"(msg="he said \"hi\"\nbye")")) << got[0];
}

TEST(PrometheusSink, DropsRecordsWithNoNumericValue) {
    PgwStub srv;
    PromPushOptions o;
    o.url = srv.url();
    o.job = "j";
    o.value_field = "value";
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    // First has no value field, second's value is a non-numeric string -> both
    // dropped; nothing to push, so flush() makes no request.
    sink->on_data(batch_of({R"({"host":"a"})", R"({"value":"abc"})"}));
    sink->flush();
    EXPECT_TRUE(srv.bodies().empty());
    // A string that parses as a number IS accepted.
    sink->on_data(batch_of({R"({"value":"3.5"})"}));
    sink->flush();
    auto got = srv.bodies();
    ASSERT_EQ(got.size(), 1u);
    EXPECT_TRUE(contains(got[0], "3.5")) << got[0];
}

TEST(PrometheusSink, GroupingLabelsGoInThePath) {
    PgwStub srv;
    PromPushOptions o;
    o.url = srv.url();
    o.job = "j";
    o.grouping = "instance=host1,region=eu";
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"value":1})"}));
    sink->flush();
    ASSERT_FALSE(srv.paths().empty());
    EXPECT_EQ(srv.paths()[0], "/metrics/job/j/instance/host1/region/eu");
}

TEST(PrometheusSink, PathLabelWithSlashIsBase64Encoded) {
    PgwStub srv;
    PromPushOptions o;
    o.url = srv.url();
    o.job = "j";
    o.grouping = "path=/var/tmp";  // '/' is unsafe in a path segment
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"value":1})"}));
    sink->flush();
    ASSERT_FALSE(srv.paths().empty());
    // base64url(no pad) of "/var/tmp" == "L3Zhci90bXA" (the pushgateway README's
    // own example).
    EXPECT_EQ(srv.paths()[0], "/metrics/job/j/path@base64/L3Zhci90bXA");
}

TEST(PrometheusSink, AcceptsHttp202AsSuccess) {
    PgwStub srv(202);  // pushgateway returns 202 under --push.disable-consistency-check
    PromPushOptions o;
    o.url = srv.url();
    o.job = "j";
    o.max_retries = 1;
    o.batch_records = 100;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch_of({R"({"value":1})"}));
    EXPECT_NO_THROW(sink->flush());
}

TEST(PrometheusSink, RequiresUrlAndJob) {
    PromPushOptions o;
    o.job = "j";  // no url
    EXPECT_THROW(make_prometheus_pushgateway_sink(o), std::runtime_error);
    PromPushOptions o2;
    o2.url = "http://x:1";  // no job
    EXPECT_THROW(make_prometheus_pushgateway_sink(o2), std::runtime_error);
}
