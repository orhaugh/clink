// The InfluxDB sink without a server of its own: the line-protocol renderer
// (point identity, escaping, refusals, timestamps, numbers), the reading of
// write responses, and the sink itself against a stub /api/v2/write that
// scripts each answer (partial writes, refusals, throttling, lost replies,
// cancellation), plus the factory's strict options.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <httplib.h>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/influxdb_sink.hpp"
#include "clink/metrics/connector_metrics.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::http_connector::BulkResult;
using clink::http_connector::DlqPolicy;
using clink::http_connector::HttpResponse;
using clink::http_connector::influx_write_result;
using clink::http_connector::InfluxDbOptions;
using clink::http_connector::InfluxLineRenderer;
using clink::http_connector::make_influxdb_sink;
using clink::http_connector::UnrenderableRecord;

namespace {

using namespace std::chrono_literals;

// A JSON \u escape, built at run time so that no tool on the way to the
// compiler can turn the escape into the character it names.
std::string esc(const char* hex) {
    return std::string{"\\"} + "u" + hex;
}

InfluxDbOptions base(const std::string& tags = "", const std::string& ts = "") {
    InfluxDbOptions o;
    o.url = "http://127.0.0.1:1";
    o.org = "o";
    o.bucket = "b";
    o.token = "t0ken-s3cret";
    o.measurement = "m";
    o.tag_keys = tags;
    o.timestamp_field = ts;
    return o;
}

std::string line(const InfluxDbOptions& o, const std::string& rec) {
    std::string out;
    InfluxLineRenderer(o).render(out, rec);
    return out;
}

// The renderer's refusal of `rec`, or "" when it renders.
std::string refusal(const InfluxDbOptions& o, const std::string& rec) {
    std::string out = "untouched";
    try {
        InfluxLineRenderer(o).render(out, rec);
    } catch (const UnrenderableRecord& e) {
        EXPECT_EQ(out, "untouched") << "a refusal left part of a line behind";
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

Batch<std::string> batch(const std::vector<std::string>& recs) {
    Batch<std::string> b;
    for (const auto& r : recs) {
        b.emplace(r);
    }
    return b;
}

std::uint64_t counter(const char* metric) {
    return clink::MetricsRegistry::global()
        .counter(clink::metrics::connector_metric_name(metric, "influxdb_sink", "sink"))
        .value();
}

// A stub /api/v2/write: answers each call as scripted (default 204), keeps
// every body, and can hold a call past the client's timeout.
class InfluxStub {
public:
    struct Answer {
        int status{204};
        std::string body;
        std::chrono::milliseconds delay{0};  // before answering
    };

    InfluxStub() {
        svr_.Post("/api/v2/write", [this](const httplib::Request& req, httplib::Response& res) {
            Answer a;
            {
                const std::lock_guard<std::mutex> lock(mu_);
                bodies_.push_back(req.body);
                queries_.push_back(req.target);
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
                res.set_content(a.body, "application/json");
            }
        });
        port_ = svr_.bind_to_any_port("127.0.0.1");
        thread_ = std::thread([this] { svr_.listen_after_bind(); });
        while (!svr_.is_running()) {
            std::this_thread::sleep_for(2ms);
        }
    }
    ~InfluxStub() {
        svr_.stop();
        thread_.join();
    }
    InfluxStub(const InfluxStub&) = delete;
    InfluxStub& operator=(const InfluxStub&) = delete;

    [[nodiscard]] std::string url() const { return "http://127.0.0.1:" + std::to_string(port_); }
    void on_call(int n, Answer a) {
        const std::lock_guard<std::mutex> lock(mu_);
        script_[n] = std::move(a);
    }
    void always(Answer a) {
        const std::lock_guard<std::mutex> lock(mu_);
        always_ = std::move(a);
    }
    [[nodiscard]] std::vector<std::string> bodies() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return bodies_;
    }
    [[nodiscard]] std::vector<std::string> queries() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return queries_;
    }
    [[nodiscard]] std::vector<std::string> auth() const {
        const std::lock_guard<std::mutex> lock(mu_);
        return auth_;
    }

private:
    httplib::Server svr_;
    int port_{0};
    std::thread thread_;
    mutable std::mutex mu_;
    std::map<int, Answer> script_;
    std::optional<Answer> always_;
    std::vector<std::string> bodies_;
    std::vector<std::string> queries_;
    std::vector<std::string> auth_;
};

InfluxDbOptions against(const InfluxStub& stub) {
    auto o = base("host", "ts");
    o.url = stub.url();
    o.max_retries = 2;
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

// A sink attached to a runtime context with a dead-letter capture and a cancel
// signal the case controls.
struct Attached {
    explicit Attached(clink::Sink<std::string>& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{8086}, "influxdb_sink", nullptr, nullptr);
        ctx->set_dead_letter_queue(&dlq);
        ctx->set_cancel_signal(clink::CancelSignal{cancel, nullptr});
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

const char* const kConflict =
    R"({"code":"unprocessable entity","message":"failure writing points to database: partial )"
    R"(write: field type conflict: input field \"v\" on measurement \"m\" is type string, )"
    R"(already exists as type float dropped=1"})";

}  // namespace

// ---- the renderer ----------------------------------------------------------------------

TEST(InfluxRender, TagsInConfiguredOrderThenFieldsByNameThenTheTimestamp) {
    const auto o = base("region,host", "ts");
    EXPECT_EQ(line(o, R"({"host":"h1","region":"eu","v":1.5,"a":"x","ts":1700000000000000000})"),
              R"(m,region=eu,host=h1 a="x",v=1.5 1700000000000000000)");
}

TEST(InfluxRender, NamesAndTagValuesAreEscapedAsLineProtocolRequires) {
    auto o = base("t k=1", "");
    o.measurement = "cpu load,x=1";
    EXPECT_EQ(line(o,
                   R"({"t k=1":"v,a l=x","f k,=y":1,"tab":"a\tb","bs":"a\\b","uni":"caf)" +
                       esc("00e9") + R"("})"),
              "cpu\\ load\\,x=1,t\\ k\\=1=v\\,a\\ l\\=x bs=\"a\\\\b\",f\\ k\\,\\=y=1,tab=\"a\tb\","
              "uni=\"caf\xc3\xa9\"");
    // A backslash before a character line protocol escapes stays a literal one.
    auto p = base("t", "");
    EXPECT_EQ(line(p, R"({"t":"a\\,b","f":1})"), "m,t=a\\\\,b f=1");
}

TEST(InfluxRender, StringValuesKeepLineBreaksTabsQuotesAndBackslashesInsideTheirQuotes) {
    const auto o = base();
    EXPECT_EQ(line(o, R"({"s":"a\nb\r\tc \"q\" \\ end\\"})"),
              "m s=\"a\nb\r\tc \\\"q\\\" \\\\ end\\\\\"");
}

TEST(InfluxRender, WhatLineProtocolCannotCarryIsRefusedNotSplit) {
    const auto o = base("t", "");
    // A line break in a tag would end the point and start another.
    EXPECT_NE(refusal(o, R"({"t":"a\nevil,host=x cpu=99","f":1})").find("line break"),
              std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"a\rb","f":1})").find("line break"), std::string::npos);
    EXPECT_NE(refusal(o, "{\"t\":\"a" + esc("0000") + "b\",\"f\":1}").find("NUL"),
              std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"ends\\","f":1})").find("backslash"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","f\nkey":1})").find("field name"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","key\\":1})").find("backslash"), std::string::npos);
    // InfluxDB drops a NUL from a string field, so the value would change.
    EXPECT_NE(refusal(o, "{\"t\":\"x\",\"s\":\"a" + esc("0000") + "b\"}").find("NUL"),
              std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","time":1})").find("'time'"), std::string::npos);
}

TEST(InfluxRender, RecordsThatAreNotOneObjectWithAFieldAreRefused) {
    const auto o = base("t", "");
    EXPECT_NE(refusal(o, "not json").find("not valid JSON"), std::string::npos);
    EXPECT_NE(refusal(o, "[1,2]").find("not a JSON object"), std::string::npos);
    EXPECT_NE(refusal(o, R"("x")").find("not a JSON object"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x"})").find("no field to write"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","n":null})").find("no field to write"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","f":{"a":1}})").find("JSON object"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","f":[1]})").find("JSON array"), std::string::npos);
    // A null field is left out: InfluxDB has no null.
    EXPECT_EQ(line(o, R"({"t":"x","f":1,"n":null})"), "m,t=x f=1");
}

TEST(InfluxRender, EveryConfiguredTagMustBePresentAndNonEmpty) {
    const auto o = base("t,id", "");
    EXPECT_NE(refusal(o, R"({"t":"x","f":1})").find("no 'id' field"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"x","id":null,"f":1})").find("null 'id' tag"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":"","id":1,"f":1})").find("is empty"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"t":{"a":1},"id":1,"f":1})").find("JSON object"), std::string::npos);
    // An integer tag past 2^53 is written exactly: a double would merge two series.
    EXPECT_EQ(line(o, R"({"t":true,"id":9007199254740993,"f":1})"),
              "m,t=true,id=9007199254740993 f=1");
}

TEST(InfluxRender, TimestampsAreRequiredExactAndWithinRange) {
    const auto o = base("", "ts");
    EXPECT_NE(refusal(o, R"({"f":1})").find("no 'ts'"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"f":1,"ts":null})").find("null 'ts'"), std::string::npos);
    for (const char* bad : {R"("1700000000")", "1.5", "1.0", "1e3", "true"}) {
        EXPECT_NE(refusal(o, std::string{R"({"f":1,"ts":)"} + bad + "}").find("not a JSON integer"),
                  std::string::npos)
            << bad;
    }
    // Neighbouring nanoseconds above 2^53 stay two points.
    EXPECT_EQ(line(o, R"({"f":1,"ts":9007199254740993})"), "m f=1 9007199254740993");
    EXPECT_EQ(line(o, R"({"f":1,"ts":9007199254740994})"), "m f=1 9007199254740994");
    EXPECT_EQ(line(o, R"({"f":1,"ts":9223372036854775806})"), "m f=1 9223372036854775806");
    EXPECT_EQ(line(o, R"({"f":1,"ts":-9223372036854775806})"), "m f=1 -9223372036854775806");
    EXPECT_NE(refusal(o, R"({"f":1,"ts":9223372036854775807})").find("outside InfluxDB's time"),
              std::string::npos);
    EXPECT_NE(refusal(o, R"({"f":1,"ts":-9223372036854775807})").find("outside InfluxDB's time"),
              std::string::npos);
    EXPECT_NE(refusal(o, R"({"f":1,"ts":9223372036854775808})").find("not a JSON integer"),
              std::string::npos);
    auto s = base("", "ts");
    s.precision = "s";
    EXPECT_EQ(line(s, R"({"f":1,"ts":9223372036})"), "m f=1 9223372036");
    EXPECT_NE(refusal(s, R"({"f":1,"ts":9223372037})").find("at precision s"), std::string::npos);
    EXPECT_EQ(line(s, R"({"f":1,"ts":-9223372036})"), "m f=1 -9223372036");
    // Without timestamp_field the server stamps the point.
    EXPECT_EQ(line(base(), R"({"f":1,"ts":5})"), "m f=1,ts=5");
}

TEST(InfluxRender, NumbersAreFloatFieldsUnlessTyped) {
    auto o = base();
    EXPECT_EQ(line(o, R"({"a":42,"b":1.5,"c":9007199254740992,"d":-9007199254740992,"e":1e+300})"),
              "m a=42,b=1.5,c=9007199254740992,d=-9007199254740992,e=1e+300");
    EXPECT_NE(refusal(o, R"({"a":9007199254740993})").find("past 2^53"), std::string::npos);
    EXPECT_NE(refusal(o, R"({"a":9223372036854775808})").find("outside 64 bits"),
              std::string::npos);
    EXPECT_EQ(line(o, R"({"a":1e19})"), "m a=1e+19");  // written as a float: a float
    o.number_fields = "typed";
    EXPECT_EQ(line(o, R"({"a":42,"b":1.5,"c":9007199254740993,"d":-9223372036854775808})"),
              "m a=42i,b=1.5,c=9007199254740993i,d=-9223372036854775808i");
    EXPECT_NE(refusal(o, R"({"a":9223372036854775808})").find("outside 64 bits"),
              std::string::npos);
    EXPECT_EQ(line(o, R"({"t":true,"f":false})"), "m f=f,t=t");
}

TEST(InfluxRender, RefusalsNameTheFieldButNeverItsValue) {
    const auto o = base("t", "ts");
    const std::string secret = "card-4111-1111";
    for (const auto& rec : {"{\"t\":\"" + secret + "\\n\",\"f\":1,\"ts\":1}",
                            "{\"t\":\"x\",\"f\":\"" + secret + "\",\"ts\":\"" + secret + "\"}",
                            "{\"t\":\"x\",\"f\":{\"k\":\"" + secret + "\"},\"ts\":1}"}) {
        const auto why = refusal(o, rec);
        EXPECT_FALSE(why.empty()) << rec;
        EXPECT_EQ(why.find(secret), std::string::npos) << why;
    }
}

TEST(InfluxRender, TheOptionsAreCheckedWhenTheSinkIsBuilt) {
    const auto refused = [](auto mutate) {
        auto o = base();
        mutate(o);
        return error_of([&o] { (void)make_influxdb_sink(o); });
    };
    using O = InfluxDbOptions;
    EXPECT_NE(refused([](O& o) { o.tag_keys = "a,a"; }).find("names 'a' twice"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.tag_keys = "a,,b"; }).find("is empty"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.tag_keys = "time"; }).find("reserves"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.tag_keys = "_field"; }).find("reserves"), std::string::npos);
    EXPECT_NE(refused([](O& o) {
                  o.tag_keys = "ts";
                  o.timestamp_field = "ts";
              }).find("timestamp field"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.measurement = "#m"; }).find("comment"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.measurement = "a\nb"; }).find("line break"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.precision = "m"; }).find("'precision'"), std::string::npos);
    EXPECT_NE(refused([](O& o) { o.number_fields = "int"; }).find("'number_fields'"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.max_retries = 21; }).find("'max_retries' must be 0 to 20"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.batch_records = 0; }).find("'batch_records'"),
              std::string::npos);
    EXPECT_NE(refused([](O& o) { o.token.clear(); }).find("'token' is required"),
              std::string::npos);
    EXPECT_EQ(refused([](O&) {}), "");
}

// ---- write responses -------------------------------------------------------------------

TEST(InfluxWriteResult, OnlyA204ConfirmsTheWrite) {
    HttpResponse ok;
    ok.status = 204;
    EXPECT_TRUE(influx_write_result(ok, 3).ok);
    HttpResponse odd;
    odd.status = 200;
    odd.body = "<html>proxy</html>";
    const auto r = influx_write_result(odd, 3);
    EXPECT_FALSE(r.ok);
    EXPECT_EQ(r.failed_transient.size(), 3u);
    EXPECT_NE(r.detail.find("does not confirm"), std::string::npos);
}

TEST(InfluxWriteResult, A400RefusesTheWholeRequestAndHidesTheLineItQuotes) {
    HttpResponse res;
    res.status = 400;
    res.body =
        R"({"code":"invalid","message":"unable to parse 'm,t=x f=card-4111 1': bad timestamp"})";
    const auto r = influx_write_result(res, 2);
    EXPECT_EQ(r.failed_permanent.size(), 2u);
    EXPECT_NE(r.detail.find("wrote none of it"), std::string::npos);
    EXPECT_NE(r.detail.find("unable to parse '...': bad timestamp"), std::string::npos) << r.detail;
    EXPECT_EQ(r.detail.find("card-4111"), std::string::npos);
}

TEST(InfluxWriteResult, A422IsAPartialWriteThatNamesNoPoint) {
    HttpResponse res;
    res.status = 422;
    res.body = kConflict;
    const auto r = influx_write_result(res, 5);
    EXPECT_FALSE(r.ok);
    EXPECT_TRUE(r.failed_permanent.empty());
    EXPECT_TRUE(r.failed_transient.empty());
    EXPECT_EQ(r.refused_unattributed, 1u);
    EXPECT_NE(r.detail.find("field type conflict"), std::string::npos);
    res.body = R"({"code":"unprocessable entity","message":"partial write"})";
    EXPECT_EQ(influx_write_result(res, 5).refused_unattributed, 1u);  // at least one
}

TEST(InfluxWriteResult, CredentialsBucketsAndSizeAreTheEndpointsProblemNotTheRecords) {
    for (const auto& [status, needle] :
         std::vector<std::pair<int, std::string>>{{401, "token was refused"},
                                                  {403, "token was refused"},
                                                  {404, "no such organisation or bucket"},
                                                  {413, "lower batch_bytes"},
                                                  {503, ""},
                                                  {429, ""},
                                                  {500, ""},
                                                  {0, ""}}) {
        HttpResponse res;
        res.status = status;
        const auto r = influx_write_result(res, 4);
        EXPECT_EQ(r.failed_transient.size(), 4u) << status;
        EXPECT_TRUE(r.failed_permanent.empty()) << status;
        EXPECT_NE(r.detail.find(needle), std::string::npos) << status << ": " << r.detail;
    }
}

// ---- the sink against a stub server ----------------------------------------------------

TEST(InfluxSink, TheRequestNamesTheBucketPrecisionAndToken) {
    InfluxStub stub;
    auto o = against(stub);
    o.org = "my org";
    o.bucket = "b/1";
    o.precision = "ms";
    auto sink = make_influxdb_sink(o);
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})"}));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    ASSERT_EQ(stub.queries().size(), 1u);
    EXPECT_EQ(stub.queries()[0], "/api/v2/write?org=my%20org&bucket=b%2F1&precision=ms");
    EXPECT_EQ(stub.auth()[0], "Token t0ken-s3cret");
    EXPECT_EQ(stub.bodies()[0], "m,host=h v=1 5\n");
}

TEST(InfluxSink, AnUnexpectedSuccessIsResentAndNeverCountedAsWritten) {
    InfluxStub stub;
    stub.on_call(1, {200, "{}"});
    auto sink = make_influxdb_sink(against(stub));
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})"}));
    sink->flush();
    ASSERT_EQ(stub.bodies().size(), 2u);
    EXPECT_EQ(stub.bodies()[0], stub.bodies()[1]);
}

TEST(InfluxSink, ARefusedRecordUnderFailStopsTheSinkUntilItReopens) {
    InfluxStub stub;
    auto sink = make_influxdb_sink(against(stub));
    sink->open();
    const auto why = error_of(
        [&] { sink->on_data(batch({R"({"host":"h","v":1,"ts":5})", R"({"host":"h","v":2})"})); });
    EXPECT_NE(why.find("no 'ts'"), std::string::npos) << why;
    // The refused record is in no batch: a barrier now would cover it as written.
    EXPECT_NE(error_of([&] {
                  sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
              }).find("refused a record earlier"),
              std::string::npos);
    EXPECT_TRUE(stub.bodies().empty());
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":3,"ts":6})"}));
    sink->flush();
    EXPECT_EQ(stub.bodies().size(), 1u);
}

TEST(InfluxSink, ARefusedRecordUnderDropIsDeadLetteredAndItsNeighboursWritten) {
    InfluxStub stub;
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_influxdb_sink(o);
    Attached a(*sink);
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})",
                         R"({"host":"h","v":2,"ts":1.5})",
                         R"({"host":"h\nx","v":3,"ts":7})",
                         R"({"host":"h","v":4,"ts":8})"}));
    sink->flush();
    ASSERT_EQ(stub.bodies().size(), 1u);
    EXPECT_EQ(stub.bodies()[0], "m,host=h v=1 5\nm,host=h v=4 8\n");
    ASSERT_EQ(a.dlq.records.size(), 2u);
    EXPECT_NE(a.dlq.records[0].error.find("not a JSON integer"), std::string::npos);
    EXPECT_NE(a.dlq.records[1].error.find("line break"), std::string::npos);
}

TEST(InfluxSink, A400UnderDropDeadLettersTheRequestWhichNothingOfWasWritten) {
    InfluxStub stub;
    stub.on_call(1, {400, R"({"code":"invalid","message":"unable to parse 'x': bad"})"});
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_influxdb_sink(o);
    Attached a(*sink);
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})", R"({"host":"h","v":2,"ts":6})"}));
    sink->flush();
    EXPECT_EQ(stub.bodies().size(), 1u);  // not resent: the server would refuse it again
    EXPECT_EQ(a.dlq.records.size(), 2u);
}

TEST(InfluxSink, APartialWriteFailsTheSinkUnderFailAndKeepsTheBatch) {
    InfluxStub stub;
    stub.on_call(1, {422, kConflict});
    auto sink = make_influxdb_sink(against(stub));
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})",
                         R"({"host":"h","v":"x","ts":6})",
                         R"({"host":"h","v":3,"ts":7})"}));
    const auto why = error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); });
    EXPECT_NE(why.find("1 of the 3 record(s)"), std::string::npos) << why;
    EXPECT_NE(why.find("does not say which"), std::string::npos) << why;
    EXPECT_EQ(why.find("t0ken"), std::string::npos);
    // No later barrier passes without the batch being sent again and confirmed.
    sink->on_barrier(CheckpointBarrier{CheckpointId{2}});
    ASSERT_EQ(stub.bodies().size(), 2u);
    EXPECT_EQ(stub.bodies()[0], stub.bodies()[1]);
}

TEST(InfluxSink, APartialWriteUnderDropIsOneDeadLetterThatSaysSo) {
    InfluxStub stub;
    stub.on_call(1, {422, kConflict});
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_influxdb_sink(o);
    Attached a(*sink);
    sink->open();
    const auto dropped0 = counter("dropped_records_total");
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})",
                         R"({"host":"h","v":"x","ts":6})",
                         R"({"host":"h","v":3,"ts":7})"}));
    sink->flush();
    EXPECT_EQ(stub.bodies().size(), 1u);  // not resent
    ASSERT_EQ(a.dlq.records.size(), 1u);
    EXPECT_NE(a.dlq.records[0].error.find("1 of the 3 record(s)"), std::string::npos);
    EXPECT_NE(a.dlq.records[0].error.find("does not say which"), std::string::npos);
    EXPECT_EQ(a.dlq.records[0].payload, stub.bodies()[0]);
    EXPECT_EQ(counter("dropped_records_total") - dropped0, 1u);
}

TEST(InfluxSink, ARefusedTokenIsRetriedThenFailsAndIsNeverDeadLettered) {
    InfluxStub stub;
    stub.always({401, R"({"code":"unauthorized","message":"unauthorized access"})"});
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    o.max_retries = 1;
    auto sink = make_influxdb_sink(o);
    Attached a(*sink);
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})"}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_NE(why.find("token was refused"), std::string::npos) << why;
    EXPECT_EQ(why.find("t0ken-s3cret"), std::string::npos) << why;
    EXPECT_TRUE(a.dlq.records.empty());
    EXPECT_EQ(stub.bodies().size(), 2u);
}

TEST(InfluxSink, AReplyLostAfterTheServerReceivedTheBatchIsSentAgain) {
    InfluxStub stub;
    stub.on_call(1, {204, "", 800ms});  // longer than the request timeout below
    auto o = against(stub);
    o.request_timeout_ms = 200;
    auto sink = make_influxdb_sink(o);
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})"}));
    sink->flush();
    ASSERT_EQ(stub.bodies().size(), 2u);  // the first reached the server
    EXPECT_EQ(stub.bodies()[0], stub.bodies()[1]);
}

TEST(InfluxSink, RetriesEndOnCancelAndWhenSpent) {
    InfluxStub stub;
    stub.always({503, ""});
    {
        auto o = against(stub);
        o.max_retries = 1;
        auto sink = make_influxdb_sink(o);
        sink->open();
        sink->on_data(batch({R"({"host":"h","v":1,"ts":5})"}));
        EXPECT_NE(error_of([&] { sink->flush(); }).find("failed after 2 attempts"),
                  std::string::npos);
    }
    auto o = against(stub);
    o.max_retries = 20;
    auto sink = make_influxdb_sink(o);
    Attached a(*sink);
    sink->open();
    sink->on_data(batch({R"({"host":"h","v":1,"ts":5})"}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(150ms);
        a.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { sink->flush(); });
    canceller.join();
    EXPECT_NE(why.find("cancelled"), std::string::npos) << why;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s);
}

TEST(InfluxSink, TheFactoryReadsEveryOptionWhole) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "influxdb_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    clink::cluster::OperatorBuildContext ctx;
    ctx.params = {{"url", "http://127.0.0.1:1"},
                  {"org", "o"},
                  {"bucket", "b"},
                  {"token", "t"},
                  {"measurement", "m"},
                  {"tags", "host"},
                  {"timestamp_field", "ts"}};
    EXPECT_NO_THROW((void)f->build(ctx));
    for (const auto& [key, value, rule] :
         std::vector<std::tuple<std::string, std::string, std::string>>{
             {"precision", "m", "'precision' must be one of 'ns', 'us', 'ms', 's'"},
             {"number_fields", "int", "'number_fields' must be one of 'float', 'typed'"},
             {"verify_tls", "yes", "'verify_tls' must be one of 'true', 'false'"},
             {"dlq", "drp", "'dlq' must be one of 'fail', 'drop'"},
             {"batch_records", "0", "'batch_records' must be at least 1"},
             {"batch_records", "12abc", "'batch_records' must be an integer"},
             {"max_retries", "21", "'max_retries' must be 0 to 20"},
             {"linger_ms", "abc", "'linger_ms' must be an integer"},
             {"request_timeout_ms", "0", "'request_timeout_ms' must be 1 to 600000"},
             {"tags", "host,host", "names 'host' twice"},
             {"tags", "ts", "timestamp field"}}) {
        auto c = ctx;
        c.params[key] = value;
        const auto why = error_of([&] { (void)f->build(c); });
        EXPECT_NE(why.find(rule), std::string::npos) << key << "=" << value << ": " << why;
    }
}
