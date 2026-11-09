// The generic HTTP sink's contract, built through its factory as a job builds
// it, against an endpoint the test runs: one that, as most JSON APIs do,
// refuses a body it cannot read, so a record the body's framing cannot carry
// costs every record of the request. It needs no service; the endpoint is the
// sink's counterpart. The cases that need a request held in flight connect
// through a TCP proxy (tests/integration/tcp_fault_proxy.hpp).

#include <atomic>
#include <chrono>
#include <cstdint>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;

namespace {

// POST /json: the body must be one JSON document, else 400. POST /ndjson: every
// line must be one JSON document, else 400. POST /status/<n>: answers n. The
// first fail_first requests to /json are answered 503.
class Endpoint {
public:
    Endpoint() {
        svr_.Post("/json", [this](const httplib::Request& req, httplib::Response& res) {
            ++requests_;
            if (fail_first_.fetch_sub(1) > 0) {
                res.status = 503;
                return;
            }
            try {
                (void)clink::config::parse(req.body);
            } catch (const std::exception&) {
                res.status = 400;
                res.set_content(R"({"error":"the body is not JSON"})", "application/json");
                return;
            }
            const std::lock_guard<std::mutex> lock(mu_);
            bodies_.push_back(req.body);
        });
        svr_.Post("/ndjson", [this](const httplib::Request& req, httplib::Response& res) {
            ++requests_;
            std::size_t at = 0;
            while (at < req.body.size()) {
                const auto nl = req.body.find('\n', at);
                const auto line = req.body.substr(at, nl - at);
                try {
                    (void)clink::config::parse(line);
                } catch (const std::exception&) {
                    res.status = 400;
                    return;
                }
                at = nl == std::string::npos ? req.body.size() : nl + 1;
            }
            const std::lock_guard<std::mutex> lock(mu_);
            bodies_.push_back(req.body);
        });
        svr_.Post(R"(/status/(\d+))", [this](const httplib::Request& req, httplib::Response& res) {
            ++requests_;
            res.status = std::stoi(req.matches[1]);
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(std::chrono::milliseconds{2});
        }
    }
    ~Endpoint() {
        svr_.stop();
        thread_.join();
    }
    Endpoint(const Endpoint&) = delete;
    Endpoint& operator=(const Endpoint&) = delete;

    [[nodiscard]] std::uint16_t port() const { return static_cast<std::uint16_t>(port_); }
    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    void fail_first(int n) { fail_first_.store(n); }
    [[nodiscard]] int requests() const { return requests_.load(); }
    std::vector<std::string> bodies() {
        const std::lock_guard<std::mutex> lock(mu_);
        return bodies_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    std::atomic<int> fail_first_{0};
    std::atomic<int> requests_{0};
    std::mutex mu_;
    std::vector<std::string> bodies_;
};

struct Capture : clink::DeadLetterQueue {
    void report(const clink::BadRecord& r) override {
        const std::lock_guard<std::mutex> lock(mu);
        records.push_back(r);
    }
    std::mutex mu;
    std::vector<clink::BadRecord> records;
};

// A sink as the factory builds it, attached to a runtime with a dead-letter
// queue and a cancel flag.
struct Built {
    explicit Built(std::map<std::string, std::string> params) {
        const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
            "http_sink", std::string{clink::cluster::kChannelString});
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = std::move(params);
        sink = std::static_pointer_cast<clink::Sink<std::string>>(f->build(ctx));
        rt = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{8080}, "http_sink", nullptr, nullptr);
        rt->set_dead_letter_queue(&dlq);
        rt->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        sink->attach_runtime(rt.get());
        sink->open();
    }
    ~Built() {
        sink->close();
        sink->attach_runtime(nullptr);
    }
    Built(const Built&) = delete;
    Built& operator=(const Built&) = delete;

    std::shared_ptr<clink::Sink<std::string>> sink;
    Capture dlq;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> rt;
};

Batch<std::string> batch(const std::vector<std::string>& records) {
    Batch<std::string> b;
    for (const auto& r : records) {
        b.emplace(std::string{r});
    }
    return b;
}

template <typename F>
std::string thrown(F&& f) {
    try {
        std::forward<F>(f)();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

const CheckpointBarrier kBarrier{CheckpointId{1}};

}  // namespace

TEST(HttpSinkContract, TheBodyCarriesEachRecordAsWritten) {
    Endpoint ep;
    const std::vector<std::string> records = {R"({"id":1,"amount":12345678901234567890.5})",
                                              "[1,2,{\"a\":null}]",
                                              "\"text\"",
                                              "-0.0",
                                              "{\n  \"pretty\": true\n}"};
    {
        Built b({{"url", ep.url()}, {"path", "/json"}});
        b.sink->on_data(batch(records));
        b.sink->on_barrier(kBarrier);
    }
    std::string want = "[";
    for (std::size_t i = 0; i < records.size(); ++i) {
        want += (i == 0 ? "" : ",") + records[i];
    }
    want += "]";
    EXPECT_EQ(ep.bodies(), (std::vector<std::string>{want}));

    Endpoint nd;
    {
        Built b({{"url", nd.url()}, {"path", "/ndjson"}, {"bulk_format", "ndjson"}});
        b.sink->on_data(batch({R"({"a":1})", R"({"b":2})"}));
        b.sink->on_barrier(kBarrier);
    }
    EXPECT_EQ(nd.bodies(), (std::vector<std::string>{"{\"a\":1}\n{\"b\":2}\n"}));
}

// One record that is not JSON, or an empty one, once made the whole json_array
// body something other than JSON: the endpoint refused the request, and under
// dlq=drop every record of it went to the dead-letter queue. Refused before it
// is sent, it is the only one.
TEST(HttpSinkContract, ARecordThatIsNotJsonIsDeadLetteredAlone) {
    Endpoint ep;
    Built b({{"url", ep.url()}, {"path", "/json"}, {"dlq", "drop"}});
    b.sink->on_data(batch({R"({"a":1})", "not json", "", R"({"b":2})"}));
    EXPECT_NO_THROW(b.sink->on_barrier(kBarrier));
    EXPECT_EQ(ep.bodies(), (std::vector<std::string>{R"([{"a":1},{"b":2}])"}));
    ASSERT_EQ(b.dlq.records.size(), 2U);
    EXPECT_EQ(b.dlq.records[0].payload, "not json");
    EXPECT_EQ(b.dlq.records[1].payload, "");
    EXPECT_NE(b.dlq.records[0].error.find("must be one JSON value"), std::string::npos)
        << b.dlq.records[0].error;
}

TEST(HttpSinkContract, ARecordThatIsNotJsonFailsTheSinkWithNothingOfItSent) {
    Endpoint ep;
    Built b({{"url", ep.url()}, {"path", "/json"}});
    const auto why = thrown([&] {
        b.sink->on_data(batch({R"({"a":1})", "not json"}));
        b.sink->on_barrier(kBarrier);
    });
    EXPECT_NE(why.find("must be one JSON value"), std::string::npos) << why;
    EXPECT_NE(thrown([&] { b.sink->on_barrier(kBarrier); }), "");
    EXPECT_EQ(ep.requests(), 0);
}

// Under ndjson a record holding a newline once went out as two lines.
TEST(HttpSinkContract, ARecordHoldingANewlineIsRefusedUnderNdjson) {
    Endpoint ep;
    Built b({{"url", ep.url()}, {"path", "/ndjson"}, {"bulk_format", "ndjson"}, {"dlq", "drop"}});
    b.sink->on_data(batch({R"({"a":1})", "{\"b\":\n2}", R"({"c":3})"}));
    EXPECT_NO_THROW(b.sink->on_barrier(kBarrier));
    EXPECT_EQ(ep.bodies(), (std::vector<std::string>{"{\"a\":1}\n{\"c\":3}\n"}));
    ASSERT_EQ(b.dlq.records.size(), 1U);
    EXPECT_NE(b.dlq.records[0].error.find("holds a newline"), std::string::npos)
        << b.dlq.records[0].error;
}

// A 404 is the endpoint's state, not the records': retried, then the flush
// fails keeping its batch, and nothing is dead-lettered even under dlq=drop.
TEST(HttpSinkContract, AMissingPathFailsTheFlushAndDeadLettersNothing) {
    Endpoint ep;
    Built b({{"url", ep.url()}, {"path", "/status/404"}, {"dlq", "drop"}, {"max_retries", "1"}});
    b.sink->on_data(batch({R"({"a":1})"}));
    const auto why = thrown([&] { b.sink->on_barrier(kBarrier); });
    EXPECT_NE(why.find("HTTP 404"), std::string::npos) << why;
    EXPECT_TRUE(b.dlq.records.empty());
    EXPECT_NE(thrown([&] { b.sink->on_barrier(kBarrier); }), "");
}

// A 400 refuses the request's records: dead-lettered under dlq=drop, which is
// what dlq=drop is for.
TEST(HttpSinkContract, ARefusalOfTheRecordsIsDeadLetteredUnderDrop) {
    Endpoint ep;
    Built b({{"url", ep.url()}, {"path", "/status/400"}, {"dlq", "drop"}});
    b.sink->on_data(batch({R"({"a":1})", R"({"b":2})"}));
    EXPECT_NO_THROW(b.sink->on_barrier(kBarrier));
    EXPECT_EQ(b.dlq.records.size(), 2U);
}

TEST(HttpSinkContract, APassingRefusalIsRetried) {
    Endpoint ep;
    ep.fail_first(2);
    Built b({{"url", ep.url()}, {"path", "/json"}});
    b.sink->on_data(batch({R"({"a":1})"}));
    EXPECT_NO_THROW(b.sink->on_barrier(kBarrier));
    EXPECT_EQ(ep.bodies(), (std::vector<std::string>{R"([{"a":1}])"}));
    EXPECT_EQ(ep.requests(), 3);
}

// A cancelled task ends a post in flight at once, the batch still pending.
TEST(HttpSinkContract, ACancelEndsAPostInFlight) {
    Endpoint ep;
    clink::itest::TcpFaultProxy proxy(ep.port(), "127.0.0.1");
    Built b({{"url", "http://127.0.0.1:" + std::to_string(proxy.port())},
             {"path", "/json"},
             {"request_timeout_ms", "60000"}});
    proxy.set_blackhole(true);
    b.sink->on_data(batch({R"({"a":1})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds{300});
        b.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = thrown([&] { b.sink->on_barrier(kBarrier); });
    const auto took = std::chrono::steady_clock::now() - t0;
    canceller.join();
    EXPECT_NE(why.find("cancelled"), std::string::npos) << why;
    EXPECT_LT(took, std::chrono::seconds{5});
    proxy.set_blackhole(false);
}
