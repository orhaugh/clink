// The Prometheus Pushgateway sink without a gateway of its own: the sample
// rules (series identity, names, label values, the value, grouping conflicts,
// bounds), the push path, the reading of push responses, and the sink itself
// against a stub gateway that scripts each answer (refusals, throttling, lost
// replies, cancellation), in both push_series modes, plus the factory's strict
// options.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/prometheus_push_sink.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::http_connector::BoundedResponse;
using clink::http_connector::DlqPolicy;
using clink::http_connector::make_prometheus_pushgateway_sink;
using clink::http_connector::PromPushOptions;
using clink::http_connector::PromPushSeries;
using clink::http_connector::PromPushVerdict;
using clink::http_connector::PromSampleRenderer;
using clink::http_connector::pushgateway_push_result;
using clink::http_connector::pushgateway_redact;
using clink::http_connector::pushgateway_value_text;
using clink::http_connector::UnrenderableRecord;

namespace {

using namespace std::chrono_literals;

// A JSON \u escape, built at run time so that no tool on the way to the
// compiler can turn the escape into the character it names.
std::string esc(const char* hex) {
    return std::string{"\\"} + "u" + hex;
}

PromPushOptions base() {
    PromPushOptions o;
    o.url = "http://127.0.0.1:1";
    o.job = "j";
    o.metric_name = "m";
    o.value_field = "v";
    return o;
}

PromSampleRenderer::Sample sample(const PromPushOptions& o, const std::string& rec) {
    return PromSampleRenderer(o).render(rec);
}

std::string labels(const PromPushOptions& o, const std::string& rec) {
    return sample(o, rec).labels;
}

// The renderer's refusal of `rec`, or "" when it renders.
std::string refusal(const PromPushOptions& o, const std::string& rec) {
    try {
        (void)PromSampleRenderer(o).render(rec);
    } catch (const UnrenderableRecord& e) {
        return e.what();
    }
    return "";
}

template <typename F>
std::string error_of(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

bool has(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

Batch<std::string> batch(const std::vector<std::string>& recs) {
    Batch<std::string> b;
    for (const auto& r : recs) {
        b.emplace(r);
    }
    return b;
}

std::uint64_t counter(const char* metric) {
    return clink::MetricsRegistry::global()
        .counter(clink::metrics::connector_metric_name(metric, "prometheus_sink", "sink"))
        .value();
}

BoundedResponse reply(int status, std::string body = {}) {
    BoundedResponse r;
    r.status = status;
    r.body = std::move(body);
    r.complete = true;
    r.request_written = true;
    return r;
}

// The gateway's own 400 for a push that repeats a series, as 1.10.0 words it:
// it quotes the series' label values.
const char* const kDuplicate =
    R"(pushed metrics are invalid or inconsistent with existing metrics: collected metric "m" )"
    R"({ label:{name:"device" value:"s3cret-device"} label:{name:"job" value:"j"} )"
    R"(untyped:{value:2}} was collected before with the same name and label values)";

// A stub Pushgateway: answers each push as scripted (default 200), keeps every
// push, and serves /api/v1/metrics from a body the case sets.
class GatewayStub {
public:
    struct Answer {
        int status{200};
        std::string body;
        std::chrono::milliseconds delay{0};  // before answering
    };

    GatewayStub() {
        svr_.Post(R"(/metrics/.*)", [this](const httplib::Request& req, httplib::Response& res) {
            Answer a;
            {
                const std::lock_guard<std::mutex> lock(mu_);
                bodies_.push_back(req.body);
                paths_.push_back(req.path);
                types_.push_back(req.get_header_value("Content-Type"));
                auth_.push_back(req.get_header_value("Authorization"));
                const auto call = static_cast<int>(bodies_.size());
                if (const auto it = script_.find(call); it != script_.end()) {
                    a = it->second;
                } else if (always_) {
                    a = *always_;
                }
            }
            if (a.delay.count() > 0) {
                std::this_thread::sleep_for(a.delay);
            }
            res.status = a.status;
            if (!a.body.empty()) {
                res.set_content(a.body, "text/plain");
            }
        });
        svr_.Get("/api/v1/metrics", [this](const httplib::Request&, httplib::Response& res) {
            const std::lock_guard<std::mutex> lock(mu_);
            ++reads_;
            res.status = query_status_;
            res.set_content(query_body_, "application/json");
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~GatewayStub() {
        svr_.stop();
        thread_.join();
    }
    GatewayStub(const GatewayStub&) = delete;
    GatewayStub& operator=(const GatewayStub&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    void on_call(int n, Answer a) {
        const std::lock_guard<std::mutex> lock(mu_);
        script_[n] = std::move(a);
    }
    void always(Answer a) {
        const std::lock_guard<std::mutex> lock(mu_);
        always_ = std::move(a);
    }
    void serve_query(std::string body, int status = 200) {
        const std::lock_guard<std::mutex> lock(mu_);
        query_body_ = std::move(body);
        query_status_ = status;
    }
    [[nodiscard]] std::vector<std::string> bodies() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return bodies_;
    }
    [[nodiscard]] std::vector<std::string> paths() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return paths_;
    }
    [[nodiscard]] std::vector<std::string> types() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return types_;
    }
    [[nodiscard]] std::vector<std::string> auth() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return auth_;
    }
    [[nodiscard]] int reads() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return reads_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::map<int, Answer> script_;
    std::optional<Answer> always_;
    std::vector<std::string> bodies_;
    std::vector<std::string> paths_;
    std::vector<std::string> types_;
    std::vector<std::string> auth_;
    std::string query_body_{R"({"status":"success","data":[]})"};
    int query_status_{200};
    int reads_{0};
};

PromPushOptions against(const GatewayStub& stub, PromPushSeries mode = PromPushSeries::Batch) {
    auto o = base();
    o.url = stub.url();
    o.max_retries = 2;
    o.push_series = mode;
    return o;
}

struct Capture : clink::DeadLetterQueue {
    void report(const clink::BadRecord& r) override {
        const std::lock_guard<std::mutex> lock(mu);
        records.push_back(r);
    }
    std::mutex mu;
    std::vector<clink::BadRecord> records;
};

// A sink attached to a runtime context with a dead-letter capture, a cancel
// signal the case controls, and the checkpoint the run restored from.
struct Attached {
    explicit Attached(clink::Sink<std::string>& s, std::uint64_t restored_from = 0) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{9091}, "prometheus_sink", nullptr, nullptr);
        ctx->set_dead_letter_queue(&dlq);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
        ctx->set_restore_from_checkpoint_id(restored_from);
        sink.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    clink::Sink<std::string>& sink;
    Capture dlq;
    std::shared_ptr<std::atomic<bool>> cancel = std::make_shared<std::atomic<bool>>(false);
    std::unique_ptr<clink::RuntimeContext> ctx;
};

const char* const kType = "# TYPE m gauge\n";

}  // namespace

// ---- the sample rules -------------------------------------------------------------------

TEST(PromRender, EveryFieldButTheValueIsALabelInNameOrder) {
    const auto o = base();
    const auto s = sample(o, R"({"v":1.5,"zone":"b","area":"a"})");
    EXPECT_EQ(s.labels, R"({area="a",zone="b"})");
    EXPECT_EQ(s.value, 1.5);
    EXPECT_EQ(PromSampleRenderer(o).line(s.labels, s.value), "m{area=\"a\",zone=\"b\"} 1.5\n");
    EXPECT_EQ(labels(o, R"({"v":2})"), "");
}

TEST(PromRender, NamesThatAreNotLabelNamesAreRefusedNotRewritten) {
    const auto o = base();
    for (const std::string& name : std::vector<std::string>{
             "device-id", "device.id", "1x", "__x", "x:y", "caf" + esc("00e9")}) {
        const auto why = refusal(o, R"({"v":1,")" + name + R"(":"a"})");
        EXPECT_TRUE(has(why, "not a Prometheus label name")) << name << ": " << why;
    }
    // The name the old sink rewrote "device-id" into is a distinct field, kept.
    EXPECT_EQ(labels(o, R"({"v":1,"device_id":"a"})"), R"({device_id="a"})");
    EXPECT_EQ(labels(o, R"({"v":1,"_x":"a"})"), R"({_x="a"})");
}

TEST(PromRender, LabelValuesKeepEveryCharacterAndEscapeOnlyBackslashQuoteAndLineFeed) {
    const auto o = base();
    EXPECT_EQ(labels(o, R"({"v":1,"a":"q\"b\\s\nn"})"), R"({a="q\"b\\s\nn"})");
    EXPECT_EQ(labels(o, R"({"v":1,"a":"x\ty\rz"})"), "{a=\"x\ty\rz\"}");
    const std::string nul_bel =
        labels(o, R"({"v":1,"a":"x)" + esc("0000") + "y" + esc("0007") + R"("})");
    EXPECT_EQ(nul_bel, std::string("{a=\"x\0y\a\"}", 10));
    EXPECT_EQ(labels(o,
                     R"({"v":1,"a":"caf)" + esc("00e9") + " " + esc("20ac") + " " + esc("d83d") +
                         esc("de00") + R"("})"),
              "{a=\"caf\xC3\xA9 \xE2\x82\xAC \xF0\x9F\x98\x80\"}");
    // Text that is not UTF-8 never reaches a label: the JSON reader refuses it.
    EXPECT_TRUE(has(refusal(o, "{\"v\":1,\"a\":\"x\xFFy\"}"), "not valid JSON"));
}

TEST(PromRender, AnEmptyStringAndNullBothMeanTheLabelIsAbsent) {
    const auto o = base();
    EXPECT_EQ(labels(o, R"({"v":1,"a":""})"), "");
    EXPECT_EQ(labels(o, R"({"v":1,"a":null})"), "");
    EXPECT_EQ(labels(o, R"({"v":1,"a":"","b":"x"})"), R"({b="x"})");
}

TEST(PromRender, NumbersAreLabelledWithTheirJsonTextExactly) {
    const auto o = base();
    EXPECT_EQ(labels(o, R"({"v":1,"id":9007199254740993})"), R"({id="9007199254740993"})");
    EXPECT_EQ(labels(o, R"({"v":1,"id":9007199254740992})"), R"({id="9007199254740992"})");
    EXPECT_EQ(labels(o, R"({"v":1,"id":18446744073709551615})"), R"({id="18446744073709551615"})");
    EXPECT_EQ(labels(o, R"({"v":1,"id":18446744073709551614})"), R"({id="18446744073709551614"})");
    EXPECT_EQ(labels(o, R"({"v":1,"id":-42})"), R"({id="-42"})");
    EXPECT_EQ(labels(o, R"({"v":1,"id":1.50})"), R"({id="1.50"})");
    EXPECT_EQ(labels(o, R"({"v":1,"id":1e3})"), R"({id="1e3"})");
    EXPECT_EQ(labels(o, R"({"v":1,"ok":true,"no":false})"), R"({no="false",ok="true"})");
    // A label value is text: the string "42" and the number 42 name one series,
    // and 1 and 1.0 two.
    EXPECT_EQ(labels(o, R"({"v":1,"id":"42"})"), labels(o, R"({"v":1,"id":42})"));
    EXPECT_NE(labels(o, R"({"v":1,"id":1})"), labels(o, R"({"v":1,"id":1.0})"));
    EXPECT_TRUE(has(refusal(o, R"({"v":1,"a":{"b":1}})"), "an object in its 'a' field"));
    EXPECT_TRUE(has(refusal(o, R"({"v":1,"a":[1]})"), "an array in its 'a' field"));
}

TEST(PromRender, TheValueIsANumberABooleanADecimalStringOrANonFiniteSpelling) {
    const auto o = base();
    const auto value = [&](const std::string& v) { return sample(o, R"({"v":)" + v + "}").value; };
    EXPECT_EQ(value("1.5"), 1.5);
    EXPECT_EQ(value("-2"), -2.0);
    EXPECT_EQ(value("true"), 1.0);
    EXPECT_EQ(value("false"), 0.0);
    EXPECT_EQ(value(R"("3.25")"), 3.25);
    EXPECT_EQ(value(R"("-0.5e1")"), -5.0);
    EXPECT_EQ(value("9007199254740992"), 9007199254740992.0);
    EXPECT_EQ(value("-9007199254740992"), -9007199254740992.0);
    EXPECT_TRUE(std::isnan(value(R"("NaN")")));
    EXPECT_EQ(value(R"("Infinity")"), HUGE_VAL);
    EXPECT_EQ(value(R"("+Inf")"), HUGE_VAL);
    EXPECT_EQ(value(R"("-Infinity")"), -HUGE_VAL);
    EXPECT_EQ(value(R"("-Inf")"), -HUGE_VAL);
    EXPECT_EQ(pushgateway_value_text(value("0.1")), "0.1");
    EXPECT_EQ(pushgateway_value_text(value(R"("NaN")")), "NaN");
    EXPECT_EQ(pushgateway_value_text(value(R"("-Inf")")), "-Inf");
    EXPECT_EQ(pushgateway_value_text(value("1e308")), "1e+308");
    for (const std::string v : {"9007199254740993",
                                "-9007199254740993",
                                R"("9007199254740993")",
                                "18446744073709551615",
                                R"("99999999999999999999")"}) {
        EXPECT_TRUE(has(refusal(o, R"({"v":)" + v + "}"), "past 2^53")) << v;
    }
    for (const std::string v : {R"(" 1")",
                                R"("1,5")",
                                R"("0x10")",
                                R"("")",
                                R"("nan")",
                                R"("1_000")",
                                R"("+1")",
                                R"("01")"}) {
        EXPECT_TRUE(has(refusal(o, R"({"v":)" + v + "}"), "not a decimal number")) << v;
    }
    EXPECT_TRUE(has(refusal(o, R"({"v":"1e400"})"), "outside the range of a double"));
    EXPECT_TRUE(has(refusal(o, R"({"v":1e400})"), "not valid JSON"));
    EXPECT_TRUE(has(refusal(o, R"({"v":null})"), "a null 'v' (value_field)"));
    EXPECT_TRUE(has(refusal(o, R"({"x":1})"), "has no 'v' (value_field)"));
    EXPECT_TRUE(has(refusal(o, R"({"v":{"a":1}})"), "an object in its 'v' (value_field)"));
    EXPECT_TRUE(has(refusal(o, "[1]"), "not a JSON object"));
    EXPECT_TRUE(has(refusal(o, "{nope"), "not valid JSON"));
}

TEST(PromRender, AGroupingLabelInARecordMustBeAbsentOrTheGroupsValue) {
    auto o = base();
    o.grouping = "instance=h1";
    EXPECT_EQ(labels(o, R"({"v":1,"instance":"h1","job":"j","a":"x"})"), R"({a="x"})");
    EXPECT_EQ(labels(o, R"({"v":1,"instance":"","job":null})"), "");
    EXPECT_TRUE(
        has(refusal(o, R"({"v":1,"instance":"h2"})"), "differs from the group's 'instance'"));
    EXPECT_TRUE(has(refusal(o, R"({"v":1,"job":"other"})"), "differs from the group's 'job'"));
    // instance is an ordinary label when the group does not name it.
    EXPECT_EQ(labels(base(), R"({"v":1,"instance":"i1"})"), R"({instance="i1"})");
}

TEST(PromRender, ASampleLongerThanMaxSampleBytesIsRefused) {
    auto o = base();
    o.max_sample_bytes = 64;
    EXPECT_EQ(refusal(o, R"({"v":1,"a":"short"})"), "");
    const auto why = refusal(o, R"({"v":1,"a":")" + std::string(80, 'x') + R"("})");
    EXPECT_TRUE(has(why, "more than max_sample_bytes (64)")) << why;
    EXPECT_FALSE(has(why, "xxxx")) << why;
}

TEST(PromRender, RefusalsNameTheFieldButNeverItsValue) {
    auto o = base();
    o.grouping = "region=eu";
    for (const std::string rec : {R"({"v":"s3cret-value"})",
                                  R"({"v":9007199254740993,"tag":"s3cret-value"})",
                                  R"({"v":1,"region":"s3cret-value"})",
                                  R"({"v":1,"tag":["s3cret-value"]})"}) {
        const auto why = refusal(o, rec);
        EXPECT_FALSE(why.empty()) << rec;
        EXPECT_FALSE(has(why, "s3cret")) << why;
        EXPECT_FALSE(has(why, "9007199254740993")) << why;
    }
}

TEST(PromRender, ThePathCarriesTheGroupAndEncodesWhatAPathSegmentCannot) {
    const auto path = [](const std::string& job, const std::string& grouping) {
        auto o = base();
        o.job = job;
        o.grouping = grouping;
        return PromSampleRenderer(o).path();
    };
    EXPECT_EQ(path("j", ""), "/metrics/job/j");
    EXPECT_EQ(path("j", "instance=host-1, region = eu"),
              "/metrics/job/j/instance/host-1/region/eu");
    EXPECT_EQ(path("j", "path=/var/tmp"), "/metrics/job/j/path@base64/L3Zhci90bXA");
    EXPECT_EQ(path("j", "e="), "/metrics/job/j/e@base64/=");
    EXPECT_EQ(path("a/b", ""), "/metrics/job@base64/YS9i");
    EXPECT_EQ(path("j", "v=~~~"), "/metrics/job/j/v@base64/fn5-");  // the URL-safe alphabet
    EXPECT_EQ(path("j", "v=a b"), "/metrics/job/j/v@base64/YSBi");
    EXPECT_EQ(path("j", "v=a+b%2F"), "/metrics/job/j/v@base64/YStiJTJG");
    EXPECT_EQ(path("j", "city=caf\xC3\xA9"), "/metrics/job/j/city@base64/Y2Fmw6k");
    auto o = base();
    o.grouping = "region=eu,empty=";
    const PromSampleRenderer r(o);
    const auto& group = r.group();
    EXPECT_EQ(group.size(), 3U);
    EXPECT_EQ(group.at("job"), "j");
    EXPECT_EQ(group.at("empty"), "");
}

TEST(PromRender, TheOptionsThatShapeASampleAreCheckedWhenTheSinkIsBuilt) {
    const auto built = [](const PromPushOptions& o) {
        return error_of([&] { (void)make_prometheus_pushgateway_sink(o); });
    };
    auto o = base();
    EXPECT_EQ(built(o), "");
    for (const std::string name : {"my-metric", "1m", "", "m{a}"}) {
        o = base();
        o.metric_name = name;
        EXPECT_TRUE(has(built(o), "must be a Prometheus metric name")) << name;
    }
    o = base();
    o.metric_name = "m:rate_5m";
    EXPECT_EQ(built(o), "");
    o.metric_name = "push_time_seconds";
    EXPECT_TRUE(has(built(o), "the gateway writes that metric itself"));
    o.metric_name = "labels";
    EXPECT_EQ(built(o), "");
    o.push_series = PromPushSeries::All;
    EXPECT_TRUE(has(built(o), "gateway's query API"));
    o = base();
    o.job = "";
    EXPECT_TRUE(has(built(o), "'job' is required"));
    o = base();
    o.value_field = "";
    EXPECT_TRUE(has(built(o), "'value_field' must not be empty"));
    const std::vector<std::pair<std::string, std::string>> groupings = {
        {"region", "without '='"},
        {"a=1,,b=2", "an empty entry"},
        {"a=1,", "an empty entry"},
        {"job=x", "may not name 'job'"},
        {"a=1,a=2", "names 'a' twice"},
        {"__a=1", "not a Prometheus label name"},
        {"device-id=1", "not a Prometheus label name"},
    };
    for (const auto& [g, why] : groupings) {
        o = base();
        o.grouping = g;
        EXPECT_TRUE(has(built(o), why)) << g << ": " << built(o);
    }
    o = base();
    o.help = "bad \xFF help";
    EXPECT_TRUE(has(built(o), "'help' is not valid UTF-8"));
    o = base();
    o.help = "back\\slash\nnext";
    EXPECT_EQ(PromSampleRenderer(o).preamble(), "# HELP m back\\\\slash\\nnext\n# TYPE m gauge\n");
}

TEST(PromRender, TheSinksRangesAreCheckedWhenItIsBuilt) {
    const auto built = [](const PromPushOptions& o) {
        return error_of([&] { (void)make_prometheus_pushgateway_sink(o); });
    };
    auto o = base();
    o.url = "";
    EXPECT_TRUE(has(built(o), "'url' is required"));
    o = base();
    o.url = "http://user:pa55word@gateway:9091";
    const auto creds = built(o);
    EXPECT_TRUE(has(creds, "'url' carries credentials")) << creds;
    EXPECT_FALSE(has(creds, "pa55word")) << creds;
    const auto range = [&](auto set, const std::string& key) {
        auto c = base();
        set(c);
        const auto why = built(c);
        EXPECT_TRUE(has(why, "'" + key + "' must be from")) << key << ": " << why;
    };
    range([](PromPushOptions& c) { c.batch_records = 0; }, "batch_records");
    range([](PromPushOptions& c) { c.max_series = 0; }, "max_series");
    range([](PromPushOptions& c) { c.batch_bytes = 100; }, "batch_bytes");
    range([](PromPushOptions& c) { c.max_sample_bytes = 8 * 1024 * 1024; }, "max_sample_bytes");
    range([](PromPushOptions& c) { c.max_retries = 21; }, "max_retries");
    range([](PromPushOptions& c) { c.max_age = -1ms; }, "linger_ms");
    range([](PromPushOptions& c) { c.connect_timeout_ms = 0; }, "connect_timeout_ms");
    range([](PromPushOptions& c) { c.request_timeout_ms = 600001; }, "request_timeout_ms");
    o = base();
    o.batch_bytes = 1024;
    o.max_sample_bytes = 1024;
    EXPECT_TRUE(has(built(o), "must hold the # HELP and # TYPE lines"));
}

// ---- push responses ---------------------------------------------------------------------

TEST(PromPushResult, A200ConfirmsAndA202ConfirmsAPushTheGatewayDidNotCheck) {
    EXPECT_EQ(pushgateway_push_result(reply(200)).kind, PromPushVerdict::Kind::Pushed);
    const auto q = pushgateway_push_result(reply(202));
    EXPECT_EQ(q.kind, PromPushVerdict::Kind::Pushed);
    EXPECT_TRUE(has(q.detail, "without the gateway's consistency check"));
    for (const int s : {201, 204}) {
        const auto v = pushgateway_push_result(reply(s));
        EXPECT_EQ(v.kind, PromPushVerdict::Kind::Retry) << s;
        EXPECT_TRUE(has(v.detail, "is not a confirmation")) << v.detail;
    }
}

TEST(PromPushResult, A400RefusesThePushWholeAndHidesTheValuesItQuotes) {
    const auto v = pushgateway_push_result(reply(400, kDuplicate));
    EXPECT_EQ(v.kind, PromPushVerdict::Kind::Refused);
    EXPECT_TRUE(has(v.detail, "HTTP 400"));
    EXPECT_TRUE(has(v.detail, "was collected before with the same name and label values"))
        << v.detail;
    EXPECT_FALSE(has(v.detail, "s3cret")) << v.detail;
    EXPECT_EQ(pushgateway_redact("text format parsing error in line 1: invalid label value "
                                 "\"x\\xffy\"\nsecond line"),
              "text format parsing error in line 1: invalid label value \"...\"");
}

TEST(PromPushResult, CredentialsPathsThrottlingAndOutagesAreRetriedThenFail) {
    for (const int s : {401, 403, 404, 405, 408, 413, 429, 500, 503}) {
        const auto v = pushgateway_push_result(reply(s, "denied"));
        EXPECT_EQ(v.kind, PromPushVerdict::Kind::Retry) << s;
        EXPECT_TRUE(has(v.detail, "HTTP " + std::to_string(s))) << v.detail;
    }
    EXPECT_TRUE(has(pushgateway_push_result(reply(401)).detail, "credentials were refused"));
    EXPECT_TRUE(has(pushgateway_push_result(reply(413)).detail, "lower batch_bytes"));
    BoundedResponse lost;
    lost.failure = BoundedResponse::Failure::Read;
    lost.request_written = true;
    lost.error = "read timeout";
    const auto v = pushgateway_push_result(lost);
    EXPECT_EQ(v.kind, PromPushVerdict::Kind::Retry);
    EXPECT_TRUE(has(v.detail, "may have been applied")) << v.detail;
    BoundedResponse refused;
    refused.failure = BoundedResponse::Failure::Connect;
    refused.error = "connection refused";
    EXPECT_TRUE(has(pushgateway_push_result(refused).detail, "connection was not made"));
    auto truncated = reply(200);
    truncated.complete = false;
    truncated.failure = BoundedResponse::Failure::TooLarge;
    EXPECT_EQ(pushgateway_push_result(truncated).kind, PromPushVerdict::Kind::Retry);
}

// ---- the sink against a stub gateway ----------------------------------------------------

TEST(PromSink, APushIsAPostOfThePreambleAndOneLinePerSeriesInNameOrder) {
    GatewayStub gw;
    auto o = against(gw);
    o.grouping = "instance=h1";
    o.help = "Temperature";
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":2,"room":"b"})", R"({"v":1,"room":"a"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.paths()[0], "/metrics/job/j/instance/h1");
    EXPECT_EQ(gw.types()[0], "text/plain; version=0.0.4; charset=utf-8");
    EXPECT_EQ(gw.bodies()[0],
              "# HELP m Temperature\n# TYPE m gauge\nm{room=\"a\"} 1\nm{room=\"b\"} 2\n");
}

TEST(PromSink, RepeatedUpdatesToASeriesPushItsLatestValueOnce) {
    GatewayStub gw;
    auto sink = make_prometheus_pushgateway_sink(against(gw));
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})",
                         R"({"v":2,"k":"a"})",
                         R"({"v":3,"k":"a"})",
                         R"({"v":4,"k":"a","e":""})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"a\"} 4\n");
}

TEST(PromSink, InBatchModeEachTriggerPushesOnlyTheSeriesSinceThePreviousPush) {
    GatewayStub gw;
    auto o = against(gw);
    o.batch_records = 2;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    // batch_records: the second series pushes both.
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})", R"({"v":3,"k":"c"})"}));
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"a\"} 1\nm{k=\"b\"} 2\n");
    // A barrier pushes what came since: c alone, which replaces a and b.
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    ASSERT_EQ(gw.bodies().size(), 2U);
    EXPECT_EQ(gw.bodies()[1], std::string{kType} + "m{k=\"c\"} 3\n");
    // Nothing since: a barrier and end of input push nothing.
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    sink->flush();
    EXPECT_EQ(gw.bodies().size(), 2U);
    sink->on_data(batch({R"({"v":4,"k":"d"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 3U);
    EXPECT_EQ(gw.bodies()[2], std::string{kType} + "m{k=\"d\"} 4\n");
}

TEST(PromSink, InBatchModeTheSinkPushesBeforeItsBufferPassesBatchBytes) {
    GatewayStub gw;
    auto o = against(gw);
    o.batch_bytes = 1024;
    o.max_sample_bytes = 512;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    std::vector<std::string> recs;
    for (int i = 0; i < 6; ++i) {
        recs.push_back(R"({"v":1,"k":")" + std::to_string(i) + std::string(300, 'x') + R"("})");
    }
    sink->on_data(batch(recs));
    sink->flush();
    ASSERT_GE(gw.bodies().size(), 2U);
    std::size_t lines = 0;
    for (const auto& b : gw.bodies()) {
        EXPECT_LE(b.size(), 1024U);
        lines += static_cast<std::size_t>(std::count(b.begin(), b.end(), '\n')) - 1;
    }
    EXPECT_EQ(lines, 6U) << "every record in exactly one push";
}

TEST(PromSink, LingerPushesUpdatesOnceTheOldestIsThatOld) {
    GatewayStub gw;
    auto o = against(gw);
    o.max_age = 50ms;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    EXPECT_TRUE(gw.bodies().empty());
    std::this_thread::sleep_for(80ms);
    sink->on_data(batch({R"({"v":2,"k":"b"})"}));
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"a\"} 1\nm{k=\"b\"} 2\n");
}

TEST(PromSink, InAllModeEveryPushCarriesEverySeriesTheSinkHolds) {
    GatewayStub gw;
    auto o = against(gw, PromPushSeries::All);
    o.max_age = 50ms;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})"}));
    EXPECT_TRUE(gw.bodies().empty()) << "no count trigger in all mode";
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    std::this_thread::sleep_for(80ms);
    sink->on_data(batch({R"({"v":3,"k":"c"})"}));
    std::this_thread::sleep_for(80ms);
    sink->on_data(batch({R"({"v":4,"k":"a"})"}));          // linger
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});  // unchanged: pushed again
    sink->flush();                                         // nothing new: no push
    const std::string all = std::string{kType} + "m{k=\"a\"} 4\nm{k=\"b\"} 2\nm{k=\"c\"} 3\n";
    ASSERT_EQ(gw.bodies().size(), 3U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"a\"} 1\nm{k=\"b\"} 2\n");
    EXPECT_EQ(gw.bodies()[1], all);
    EXPECT_EQ(gw.bodies()[2], all);
    sink->on_data(batch({R"({"v":5,"k":"d"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 4U);
    EXPECT_EQ(gw.bodies()[3],
              std::string{kType} + "m{k=\"a\"} 4\nm{k=\"b\"} 2\nm{k=\"c\"} 3\nm{k=\"d\"} 5\n");
}

TEST(PromSink, InAllModeASeriesPastTheBoundsFailsTheSinkUntilItReopens) {
    GatewayStub gw;
    auto o = against(gw, PromPushSeries::All);
    o.max_series = 2;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"b"})", R"({"v":3,"k":"a"})"}));
    const auto why = error_of([&] { sink->on_data(batch({R"({"v":4,"k":"c"})"})); });
    EXPECT_TRUE(has(why, "past max_series")) << why;
    EXPECT_TRUE(has(error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); }),
                    "refuses to push or acknowledge a barrier"));
    EXPECT_TRUE(gw.bodies().empty());
    sink->open();
    sink->on_data(batch({R"({"v":5,"k":"c"})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"c\"} 5\n");

    auto b = against(gw, PromPushSeries::All);
    b.batch_bytes = 1024;
    b.max_sample_bytes = 512;
    auto bytes = make_prometheus_pushgateway_sink(b);
    bytes->open();
    const auto big = [](int i) {
        return R"({"v":1,"k":")" + std::to_string(i) + std::string(300, 'x') + R"("})";
    };
    bytes->on_data(batch({big(1), big(2), big(3)}));
    EXPECT_TRUE(has(error_of([&] { bytes->on_data(batch({big(4)})); }), "past batch_bytes"));
}

TEST(PromSink, ARefusedRecordUnderFailStopsTheSinkUntilItReopens) {
    GatewayStub gw;
    auto sink = make_prometheus_pushgateway_sink(against(gw));
    sink->open();
    const auto why = error_of(
        [&] { sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":1,"bad-name":"x"})"})); });
    EXPECT_TRUE(has(why, "not a Prometheus label name")) << why;
    const auto barrier = error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); });
    EXPECT_TRUE(has(barrier, "threw a record away")) << barrier;
    EXPECT_TRUE(has(error_of([&] { sink->flush(); }), "threw a record away"));
    EXPECT_TRUE(gw.bodies().empty()) << "nothing pushed past the refusal";
    sink->open();
    sink->on_data(batch({R"({"v":2,"k":"b"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"b\"} 2\n");
}

TEST(PromSink, ARefusedRecordUnderDropIsDeadLetteredAndNeverReplacesAValidSample) {
    GatewayStub gw;
    auto o = against(gw);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink);
    sink->open();
    const auto dropped = counter("dropped_records_total");
    sink->on_data(batch({R"({"v":1,"k":"a"})",
                         R"({"v":"s3cret","k":"a"})",
                         R"({"v":2,"k":"a","nested":{"x":1}})",
                         R"({"v":3,"k":"b"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 1U);
    EXPECT_EQ(gw.bodies()[0], std::string{kType} + "m{k=\"a\"} 1\nm{k=\"b\"} 3\n");
    ASSERT_EQ(at.dlq.records.size(), 2U);
    EXPECT_EQ(at.dlq.records[0].payload, R"({"v":"s3cret","k":"a"})");
    EXPECT_TRUE(has(at.dlq.records[0].error, "'v' (value_field)")) << at.dlq.records[0].error;
    EXPECT_FALSE(has(at.dlq.records[0].error, "s3cret")) << at.dlq.records[0].error;
    EXPECT_EQ(at.dlq.records[0].connector, "prometheus_sink");
    EXPECT_EQ(at.dlq.records[0].location, gw.url() + "/metrics/job/j");
    EXPECT_EQ(counter("dropped_records_total") - dropped, 2U);
}

TEST(PromSink, A400FailsAtOnceKeepsTheSeriesAndHidesTheirValues) {
    GatewayStub gw;
    gw.on_call(1, {400, kDuplicate, 0ms});
    auto sink = make_prometheus_pushgateway_sink(against(gw));
    sink->open();
    sink->on_data(batch({R"({"v":1,"device":"s3cret-device"})"}));
    const auto why = error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); });
    EXPECT_TRUE(has(why, "refused the push")) << why;
    EXPECT_TRUE(has(why, "nothing of it was applied")) << why;
    EXPECT_FALSE(has(why, "s3cret")) << why;
    EXPECT_EQ(gw.bodies().size(), 1U) << "a refusal is not retried";
    // What it carried stays pending: the next barrier sends it again.
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    ASSERT_EQ(gw.bodies().size(), 2U);
    EXPECT_EQ(gw.bodies()[1], gw.bodies()[0]);
}

TEST(PromSink, AnUnconfirmingSuccessThrottlingAndOutagesAreRetried) {
    GatewayStub gw;
    gw.on_call(1, {204, "", 0ms});
    gw.on_call(2, {429, "slow down", 0ms});
    gw.on_call(3, {503, "", 0ms});
    auto o = against(gw);
    o.max_retries = 3;
    auto sink = make_prometheus_pushgateway_sink(o);
    sink->open();
    const auto written = counter("records_total");
    sink->on_data(batch({R"({"v":1,"k":"a"})", R"({"v":2,"k":"a"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 4U);
    for (const auto& b : gw.bodies()) {
        EXPECT_EQ(b, gw.bodies()[0]);
    }
    EXPECT_EQ(counter("records_total") - written, 2U) << "both records, once, when confirmed";
}

TEST(PromSink, RefusedCredentialsAreRetriedThenFailAndNoRecordIsDeadLettered) {
    GatewayStub gw;
    gw.always({401, "unauthorized", 0ms});
    auto o = against(gw);
    o.dlq_policy = DlqPolicy::Drop;
    o.headers = "Authorization: Basic c2VjcmV0LXBhc3M=";
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink);
    sink->open();
    const auto written = counter("records_total");
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "failed after 3 attempt(s)")) << why;
    EXPECT_TRUE(has(why, "credentials were refused")) << why;
    EXPECT_FALSE(has(why, "c2VjcmV0")) << why;
    EXPECT_EQ(gw.bodies().size(), 3U);
    EXPECT_EQ(gw.auth()[0], "Basic c2VjcmV0LXBhc3M=");
    EXPECT_TRUE(at.dlq.records.empty());
    EXPECT_EQ(counter("records_total"), written) << "an unconfirmed push writes nothing";
}

TEST(PromSink, AReplyLostAfterTheGatewayReceivedThePushIsSentAgain) {
    GatewayStub gw;
    gw.on_call(1, {200, "", 1500ms});  // held past request_timeout_ms
    auto o = against(gw);
    o.request_timeout_ms = 300;
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), 2U);
    EXPECT_EQ(gw.bodies()[1], gw.bodies()[0]) << "the resend carries the same series";
}

TEST(PromSink, RetriesEndOnCancelAndWhenSpentAndWhatTheyCarriedStaysPending) {
    GatewayStub gw;
    gw.always({503, "", 0ms});
    auto o = against(gw);
    o.max_retries = 20;
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(300ms);
        at.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { sink->flush(); });
    canceller.join();
    EXPECT_TRUE(has(why, "cancelled while retrying")) << why;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 5s);
    at.cancel->store(false);
    gw.always({200, "", 0ms});
    const auto before = gw.bodies().size();
    sink->flush();
    ASSERT_EQ(gw.bodies().size(), before + 1);
    EXPECT_EQ(gw.bodies().back(), std::string{kType} + "m{k=\"a\"} 1\n");
}

TEST(PromSink, ACancelEndsAPushInFlight) {
    GatewayStub gw;
    gw.on_call(1, {200, "", 3000ms});
    auto o = against(gw);
    o.request_timeout_ms = 60000;
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(200ms);
        at.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { sink->flush(); });
    canceller.join();
    EXPECT_TRUE(has(why, "cancelled while a push")) << why;
    EXPECT_TRUE(has(why, "stays pending")) << why;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 2500ms);
}

TEST(PromSink, ARestoredRunInAllModeAdoptsTheSeriesTheGroupHolds) {
    GatewayStub gw;
    gw.serve_query(
        R"({"status":"success","data":[)"
        R"({"labels":{"job":"other"},"m":{"type":"GAUGE","metrics":[)"
        R"({"labels":{"job":"other","instance":"","k":"zz"},"value":"9"}]}},)"
        R"({"labels":{"job":"j","region":"eu"},)"
        R"("last_push_successful":true,)"
        R"("n":{"type":"COUNTER","metrics":[{"labels":{"job":"j","region":"eu"},"value":"1"}]},)"
        R"("m":{"type":"GAUGE","help":"h","metrics":[)"
        R"({"labels":{"job":"j","region":"eu","instance":"","k":"a"},"value":"1"},)"
        R"({"labels":{"job":"j","region":"eu","instance":"","k":"b"},"value":"9.007199254740992e+15"},)"
        R"({"labels":{"job":"j","region":"eu","instance":"","k":"c"},"value":"NaN"}]}}]})");
    auto o = against(gw, PromPushSeries::All);
    o.grouping = "region=eu";
    {
        auto sink = make_prometheus_pushgateway_sink(o);
        Attached at(*sink, /*restored_from=*/7);
        sink->open();
        sink->on_data(batch({R"({"v":5,"k":"a"})", R"({"v":6,"k":"d"})"}));
        sink->on_barrier(CheckpointBarrier{CheckpointId{8}});
        ASSERT_EQ(gw.reads(), 1);
        ASSERT_EQ(gw.bodies().size(), 1U);
        EXPECT_EQ(gw.bodies()[0],
                  std::string{kType} +
                      "m{k=\"a\"} 5\nm{k=\"b\"} 9007199254740992\n"
                      "m{k=\"c\"} NaN\nm{k=\"d\"} 6\n");
        sink->on_barrier(CheckpointBarrier{CheckpointId{9}});
        EXPECT_EQ(gw.reads(), 1) << "adopted once per run";
    }
    {
        // A fresh start replaces what the group holds rather than adopting it.
        auto sink = make_prometheus_pushgateway_sink(o);
        Attached at(*sink, /*restored_from=*/0);
        sink->open();
        sink->on_data(batch({R"({"v":1,"k":"x"})"}));
        sink->flush();
        EXPECT_EQ(gw.reads(), 1);
        EXPECT_EQ(gw.bodies().back(), std::string{kType} + "m{k=\"x\"} 1\n");
    }
    {
        // Batch mode never reads the group.
        auto b = against(gw);
        b.grouping = "region=eu";
        auto sink = make_prometheus_pushgateway_sink(b);
        Attached at(*sink, /*restored_from=*/7);
        sink->open();
        sink->on_data(batch({R"({"v":1,"k":"y"})"}));
        sink->flush();
        EXPECT_EQ(gw.reads(), 1);
    }
}

TEST(PromSink, ARestoredRunRefusesAGroupItCannotAdopt) {
    GatewayStub gw;
    auto o = against(gw, PromPushSeries::All);
    o.max_retries = 0;
    const auto adopt = [&](const std::string& query) {
        gw.serve_query(query);
        auto sink = make_prometheus_pushgateway_sink(o);
        Attached at(*sink, /*restored_from=*/3);
        sink->open();
        sink->on_data(batch({R"({"v":1,"k":"a"})"}));
        return error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{4}}); });
    };
    EXPECT_TRUE(has(adopt(R"({"status":"success","data":[{"labels":{"job":"j"},)"
                          R"("m":{"type":"COUNTER","metrics":[]}}]})"),
                    "holds m as COUNTER"));
    EXPECT_TRUE(has(adopt("not json"), "not JSON the sink can read"));
    gw.serve_query("", 503);
    auto sink = make_prometheus_pushgateway_sink(o);
    Attached at(*sink, /*restored_from=*/3);
    sink->open();
    sink->on_data(batch({R"({"v":1,"k":"a"})"}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "read of the gateway's series")) << why;
    EXPECT_TRUE(gw.bodies().empty()) << "no push before the group is adopted";
}

TEST(PromSink, TheFactoryReadsEveryOptionWholeAndRunsOnOneSubtask) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "prometheus_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    const auto build = [&](std::map<std::string, std::string> params,
                           std::uint32_t parallelism = 1) {
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = std::move(params);
        ctx.params.emplace("url", "http://127.0.0.1:1");
        ctx.params.emplace("job", "j");
        ctx.parallelism = parallelism;
        return error_of([&] { (void)f->build(ctx); });
    };
    EXPECT_EQ(build({}), "");
    EXPECT_EQ(build({{"push_series", "all"}, {"max_series", "5"}, {"dlq", "drop"}}), "");
    EXPECT_TRUE(has(build({{"batch_records", "12abc"}}), "must be an integer"));
    EXPECT_TRUE(has(build({{"batch_records", "-1"}}), "must be from 1 to 100000"));
    EXPECT_TRUE(has(build({{"max_retries", "99999999999"}}), "must be from 0 to 20"));
    EXPECT_TRUE(has(build({{"push_series", "every"}}), "must be one of 'batch', 'all'"));
    EXPECT_TRUE(has(build({{"dlq", "skip"}}), "must be one of 'fail', 'drop'"));
    EXPECT_TRUE(has(build({{"verify_tls", "no"}}), "must be one of 'true', 'false'"));
    EXPECT_TRUE(has(build({{"push_series", "all"}, {"batch_records", "5"}}),
                    "'batch_records' applies to push_series='batch'"));
    EXPECT_TRUE(has(build({{"max_series", "5"}}), "'max_series' applies to push_series='all'"));
    EXPECT_TRUE(has(build({}, 2), "runs on one subtask")) << build({}, 2);
    // A small batch_bytes alone still holds a sample: max_sample_bytes follows it.
    EXPECT_EQ(build({{"batch_bytes", "1024"}}), "");
    EXPECT_TRUE(has(build({{"batch_bytes", "1024"}, {"max_sample_bytes", "2048"}}),
                    "'max_sample_bytes' must be from 64 to 1024"));
}
