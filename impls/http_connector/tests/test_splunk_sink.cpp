// The Splunk HEC sink without a Splunk server: the event rules (one JSON object
// per record, written exactly, no way to add envelope members or events), the
// reading of HEC's replies and acknowledgement replies, and the sink itself
// against a stub HEC (hec_stub.hpp) that scripts each answer: refused and
// partly accepted requests, a busy server, lost replies, refused tokens,
// replies that are not HEC's, cancellation, and indexer acknowledgement
// (delayed, never arriving, unreadable, bounded). Plus the factory's strict
// options. The stub is a protocol model; none of this is evidence about a
// Splunk server.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cluster/operator_registry.hpp"
#include "clink/config/json.hpp"
#include "clink/core/record.hpp"
#include "clink/http_connector/splunk_hec_sink.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "hec_stub.hpp"

using clink::Batch;
using clink::http_connector::BoundedResponse;
using clink::http_connector::DlqPolicy;
using clink::http_connector::HecAckVerdict;
using clink::http_connector::HecEventRenderer;
using clink::http_connector::HecEventVerdict;
using clink::http_connector::make_splunk_hec_sink;
using clink::http_connector::splunk_hec_ack_result;
using clink::http_connector::splunk_hec_describe;
using clink::http_connector::splunk_hec_event_result;
using clink::http_connector::SplunkHecAck;
using clink::http_connector::SplunkHecOptions;
using clink::http_connector::SplunkHecSink;
using clink::http_connector::UnrenderableRecord;
using clink::http_connector::hec_stub::HecStub;

namespace {

using namespace std::chrono_literals;

SplunkHecOptions base() {
    SplunkHecOptions o;
    o.url = "http://127.0.0.1:1";
    o.token = "hec-token";
    return o;
}

std::string envelope(const SplunkHecOptions& o, const std::string& rec) {
    std::string out;
    HecEventRenderer(o).render(out, rec);
    return out;
}

std::string refusal(const SplunkHecOptions& o, const std::string& rec) {
    std::string out = "kept";
    try {
        HecEventRenderer(o).render(out, rec);
    } catch (const UnrenderableRecord& e) {
        EXPECT_EQ(out, "kept") << "a refused record left something behind";
        return e.what();
    }
    return {};
}

template <typename F>
std::string error_of(F&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        return e.what();
    }
    return {};
}

bool has(const std::string& hay, const std::string& needle) {
    return hay.find(needle) != std::string::npos;
}

BoundedResponse reply(int status, std::string body, std::string type = "application/json") {
    BoundedResponse r;
    r.status = status;
    r.body = std::move(body);
    r.complete = true;
    r.headers["content-type"] = std::move(type);
    return r;
}

BoundedResponse hec(int status, int code, const std::string& text, const std::string& more = {}) {
    return reply(status,
                 R"({"text":")" + text + R"(","code":)" + std::to_string(code) + more + "}");
}

Batch<std::string> batch(std::vector<std::string> recs) {
    Batch<std::string> b;
    for (auto& r : recs) {
        b.emplace(std::move(r));
    }
    return b;
}

std::string rec(int n) {
    return R"({"id":)" + std::to_string(n) + R"(,"msg":"m)" + std::to_string(n) + R"("})";
}

struct Capture : clink::DeadLetterQueue {
    void report(const clink::BadRecord& r) override {
        const std::lock_guard<std::mutex> lock(mu);
        records.push_back(r);
    }
    std::mutex mu;
    std::vector<clink::BadRecord> records;
};

// A sink attached to a runtime context with a dead-letter capture and a
// cancel signal the case controls.
struct Attached {
    explicit Attached(clink::Sink<std::string>& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{8088}, "splunk_hec_sink", nullptr, nullptr);
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

SplunkHecOptions against(const HecStub& stub) {
    auto o = base();
    o.url = stub.url();
    o.max_retries = 2;
    o.request_timeout_ms = 2000;
    return o;
}

SplunkHecOptions indexed(const HecStub& stub) {
    auto o = against(stub);
    o.ack = SplunkHecAck::Indexer;
    o.ack_poll_interval = 20ms;
    return o;
}

std::size_t count(const std::vector<std::string>& v, const std::string& x) {
    return static_cast<std::size_t>(std::count(v.begin(), v.end(), x));
}

}  // namespace

// ---- the event rules --------------------------------------------------------------------

TEST(HecRender, OneRecordIsOneEnvelopeWithItsTokensAsWritten) {
    const auto o = base();
    EXPECT_EQ(envelope(o, R"({"b":1,"a":"x"})"), R"({"event":{"b":1,"a":"x"}})");
    // Whitespace between tokens goes, raw line breaks with it; key order stays.
    EXPECT_EQ(envelope(o, " {\n \"z\" : [ 1 , 2 ] ,\r\n\t\"a\" : { } }\n "),
              R"({"event":{"z":[1,2],"a":{}}})");
    EXPECT_EQ(envelope(o, "{}"), R"({"event":{}})");
}

TEST(HecRender, NumbersKeepEveryDigitAsWritten) {
    const auto o = base();
    const std::string r =
        R"({"big":12345678901234567890123,"u64":18446744073709551616,"two53":9007199254740993,)"
        R"("d":1.50,"e":1E+2,"neg":-0,"tiny":4.9e-324,"huge":1e400,"f":-0.000})";
    EXPECT_EQ(envelope(o, r), R"({"event":)" + r + "}");
}

TEST(HecRender, StringsKeepTheirEscapesUnicodeAndControlCharacters) {
    const auto o = base();
    const std::string r =
        R"({"q":"a\"b\\c\/d","c":"\n\t\b\f\r\u0000\u001F","u":"\u2028\ud83d\ude00",)"
        "\"raw\":\"\xC3\xA9\xF0\x9F\x98\x80\",\"del\":\"\x7F\"}";
    EXPECT_EQ(envelope(o, r), R"({"event":)" + r + "}");
    // A line break inside a value is only ever the escape; the envelope is one line.
    EXPECT_EQ(envelope(o, "{\"m\":\"a\\nb\"}\n").find('\n'), std::string::npos);
}

TEST(HecRender, MetadataIsTheSinksAloneAndRecordKeysStayInTheEvent) {
    auto o = base();
    o.host = "web\"01";
    o.index = "main";
    o.source = "c:\\logs";
    o.sourcetype = "_json";
    EXPECT_EQ(HecEventRenderer(o).metadata(),
              R"(,"host":"web\"01","index":"main","source":"c:\\logs","sourcetype":"_json")");
    // A record's own keys named like metadata are event content, not metadata.
    const std::string r =
        R"({"index":"secret","host":"h","time":1,"fields":{"x":"y"},"event":"inner"})";
    const std::string env = envelope(o, r);
    const auto v = clink::config::parse(env);
    ASSERT_TRUE(v.is_object());
    EXPECT_EQ(v.as_object().size(), 5U);
    EXPECT_EQ(v.at("index").as_string(), "main");
    EXPECT_EQ(v.at("host").as_string(), "web\"01");
    EXPECT_FALSE(v.contains("time"));
    EXPECT_FALSE(v.contains("fields"));
    EXPECT_EQ(v.at("event").at("index").as_string(), "secret");
    EXPECT_EQ(env, R"({"event":)" + r + HecEventRenderer(o).metadata() + "}");
}

TEST(HecRender, ARecordCannotAddEnvelopeMembersOrEvents) {
    const auto o = base();
    for (const std::string& r : std::vector<std::string>{
             R"({"a":1},"index":"other")",
             R"({"a":1}} {"event":"x")",
             R"({"a":1}{"event":"x"})",
             "{\"a\":1}\n{\"event\":\"x\"}",
             R"({"a":1} ,)",
             R"({"a":1} x)",
         }) {
        EXPECT_TRUE(has(refusal(o, r), "has more after its object")) << r << ": " << refusal(o, r);
    }
    EXPECT_TRUE(has(refusal(o, R"("x"},"index":"y")"), "is a JSON string, not an object"));
    EXPECT_TRUE(has(refusal(o, R"({"a":"x"},"index":"y)"), "has more after its object"));
}

TEST(HecRender, RecordsThatAreNotOneJsonObjectAreRefused) {
    const auto o = base();
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"[1]", "is a JSON array"},
        {R"("s")", "is a JSON string"},
        {"12", "is a JSON number"},
        {"-1", "is a JSON number"},
        {"true", "is a JSON boolean"},
        {"null", "is JSON null"},
        {"", "is empty"},
        {" \n ", "is empty"},
        {"x", "is not JSON"},
        {"{", "ends inside an object"},
        {R"({"a":})", "is not a JSON value"},
        {R"({"a":1,})", "not a string"},
        {"{'a':1}", "not a string"},
        {R"({a:1})", "not a string"},
        {R"({"a" 1})", "lacks ':'"},
        {R"({"a":1 "b":2})", "lacks ',' or '}'"},
        {R"({"a":[1 2]})", "lacks ',' or ']'"},
        {R"({"a":01})", "lacks ',' or '}'"},
        {R"({"a":1.})", "malformed number"},
        {R"({"a":.5})", "is not a JSON value"},
        {R"({"a":+1})", "is not a JSON value"},
        {R"({"a":1e})", "malformed number"},
        {R"({"a":-})", "malformed number"},
        {R"({"a":NaN})", "is not a JSON value"},
        {R"({"a":tru})", "is not a JSON value"},
        {R"({"a":"x)", "ends inside a string"},
        {R"({"a":[1,2)", "ends inside an array"},
    };
    for (const auto& [r, why] : cases) {
        const auto got = refusal(o, r);
        EXPECT_TRUE(has(got, why)) << "'" << r << "': " << got;
        EXPECT_TRUE(has(got, "each record must be exactly one JSON object")) << got;
    }
}

TEST(HecRender, StringsMustBeValidUtf8WithNoRawControlCharacter) {
    const auto o = base();
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"{\"a\":\"\xFF\"}", "not valid UTF-8"},
        {"{\"a\":\"\xC0\xAF\"}", "not valid UTF-8"},          // overlong '/'
        {"{\"a\":\"\xED\xA0\x80\"}", "not valid UTF-8"},      // an encoded surrogate
        {"{\"a\":\"\xE2\x82\"}", "not valid UTF-8"},          // cut short
        {"{\"a\":\"\xF4\x90\x80\x80\"}", "not valid UTF-8"},  // past U+10FFFF
        {"{\"\xFF\":1}", "not valid UTF-8"},                  // in a key
        {"{\"a\":\"x\x01y\"}", "raw control character"},
        {"{\"a\":\"x\ny\"}", "raw control character"},
        {"{\"a\":\"x\ty\"}", "raw control character"},
        {R"({"a":"\x41"})", "invalid escape"},
        {R"({"a":"\u12"})", "malformed \\u escape"},
        {R"({"a":"\uZZZZ"})", "malformed \\u escape"},
        {R"({"a":"\ud800"})", "unpaired surrogate"},
        {R"({"a":"\udc00"})", "unpaired surrogate"},
        {R"({"a":"\ud800\u0041"})", "unpaired surrogate"},
        {R"({"a":"\ud800x"})", "unpaired surrogate"},
    };
    for (const auto& [r, why] : cases) {
        const auto got = refusal(o, r);
        EXPECT_TRUE(has(got, why)) << r << ": " << got;
    }
    // Every byte offset points into the record, and no content is quoted.
    EXPECT_TRUE(has(refusal(o, "{\"secret\":\"pa\xFFss\"}"), "(byte 13)"));
    EXPECT_FALSE(has(refusal(o, "{\"secret\":\"pa\xFFss\"}"), "secret"));
}

TEST(HecRender, ARepeatedKeyIsRefusedAfterUnescaping) {
    const auto o = base();
    EXPECT_TRUE(has(refusal(o, R"({"a":1,"a":2})"), "repeats a key"));
    EXPECT_TRUE(has(refusal(o, R"({"a":1,"\u0061":2})"), "repeats a key"));
    EXPECT_TRUE(
        has(refusal(o, R"({"x":{"k":1,"k":1}})"), "repeats a key in the object that starts"));
    EXPECT_TRUE(has(refusal(o, R"({"x":[{"k":1,"k":1}]})"), "(byte 6)"));
    EXPECT_EQ(refusal(o, R"({"a":1,"A":2})"), "");
    EXPECT_EQ(refusal(o, R"({"a":{"a":{"a":1}},"b":[{"a":1},{"a":2}]})"), "");
}

TEST(HecRender, NestingPastOneHundredLevelsIsRefused) {
    const auto o = base();
    const auto nested = [](int levels) {
        std::string s;
        for (int i = 1; i < levels; ++i) {
            s += i % 2 == 0 ? "[" : "{\"a\":";
        }
        s += "1";
        for (int i = levels - 1; i >= 1; --i) {
            s += i % 2 == 0 ? "]" : "}";
        }
        return "{\"top\":" + s + "}";
    };
    EXPECT_EQ(refusal(o, nested(100)), "");  // the top object and 99 inside it
    EXPECT_TRUE(has(refusal(o, nested(101)), "nests more than 100 levels deep"));
    EXPECT_TRUE(has(refusal(o, "{\"a\":" + std::string(5000, '[')), "nests more than 100"));
}

TEST(HecRender, AnEnvelopeLongerThanARequestMayCarryIsRefused) {
    auto o = base();
    o.batch_bytes = 1024;
    // {"event":{"x":"<n bytes>"}} is 18 + n bytes.
    EXPECT_EQ(refusal(o, R"({"x":")" + std::string(1006, 'a') + R"("})"), "");
    const auto why = refusal(o, R"({"x":")" + std::string(1007, 'a') + R"("})");
    EXPECT_TRUE(has(why, "envelope is 1025 bytes, more than one request may carry")) << why;
    EXPECT_TRUE(has(why, "batch_bytes, 1024")) << why;
}

TEST(HecRender, TheOptionsAreCheckedWithoutShowingTheToken) {
    const auto why = [](const std::function<void(SplunkHecOptions&)>& change) {
        auto o = base();
        o.token = "s3cret-token";
        change(o);
        return error_of([&] { SplunkHecSink sink(o); });
    };
    EXPECT_EQ(why([](SplunkHecOptions&) {}), "");
    EXPECT_EQ(why([](SplunkHecOptions& o) { o.url = "http://splunk:8088/"; }), "");
    EXPECT_EQ(why([](SplunkHecOptions& o) { o.path = "/services/collector"; }), "");
    EXPECT_EQ(why([](SplunkHecOptions& o) { o.path = "/services/collector/event/1.0"; }), "");
    EXPECT_EQ(why([](SplunkHecOptions& o) { o.path = "/proxy/services/collector/event"; }), "");
    const std::vector<std::pair<std::function<void(SplunkHecOptions&)>, std::string>> cases = {
        {[](SplunkHecOptions& o) { o.url.clear(); }, "'url' is required"},
        {[](SplunkHecOptions& o) { o.url = "ftp://h"; }, "must start with http"},
        {[](SplunkHecOptions& o) { o.url = "http://"; }, "names no host"},
        {[](SplunkHecOptions& o) { o.url = "http://u:pw@h:8088"; }, "carries credentials"},
        {[](SplunkHecOptions& o) { o.url = "http://h:8088?token=x"; }, "query or fragment"},
        {[](SplunkHecOptions& o) { o.url = "http://h:8088/services"; }, "carries a path"},
        {[](SplunkHecOptions& o) { o.token.clear(); }, "'token' is required"},
        {[](SplunkHecOptions& o) { o.token = "a b"; }, "printable ASCII"},
        {[](SplunkHecOptions& o) { o.token = "a\r\nX-Evil: 1"; }, "printable ASCII"},
        {[](SplunkHecOptions& o) { o.path = "/services/collector/raw"; }, "not raw text"},
        {[](SplunkHecOptions& o) { o.path = "/services/collector/health"; }, "health"},
        {[](SplunkHecOptions& o) { o.path = "/services/collector/ack"; }, "acknowledgement"},
        {[](SplunkHecOptions& o) { o.path = "services/collector/event"; }, "must start with '/'"},
        {[](SplunkHecOptions& o) { o.path = "/services/collector/event?channel=x"; }, "query"},
        {[](SplunkHecOptions& o) { o.headers = "authorization: Bearer x"; }, "sets Authorization"},
        {[](SplunkHecOptions& o) { o.headers = "AUTHORIZATION: Splunk y"; }, "sets Authorization"},
        {[](SplunkHecOptions& o) { o.headers = "x-splunk-request-channel: abc"; },
         "X-Splunk-Request-Channel"},
        {[](SplunkHecOptions& o) { o.headers = "Content-Type: text/plain"; }, "sets Content-Type"},
        {[](SplunkHecOptions& o) { o.headers = "X-A: b\x01"; }, "control character"},
        {[](SplunkHecOptions& o) { o.index = "m\xFF"; }, "'index' must be valid UTF-8"},
        {[](SplunkHecOptions& o) { o.host = "a\nb"; }, "'host' must be valid UTF-8"},
        {[](SplunkHecOptions& o) { o.batch_records = 0; }, "'batch_records' must be from 1"},
        {[](SplunkHecOptions& o) { o.batch_bytes = 1023; }, "'batch_bytes' must be from 1024"},
        {[](SplunkHecOptions& o) { o.max_retries = 21; }, "'max_retries' must be from 0 to 20"},
        {[](SplunkHecOptions& o) { o.max_pending_acks = 0; }, "'max_pending_acks'"},
        {[](SplunkHecOptions& o) { o.ack_poll_interval = 9ms; }, "'ack_poll_ms'"},
        {[](SplunkHecOptions& o) { o.ack_timeout = 999ms; }, "'ack_timeout_ms'"},
        {[](SplunkHecOptions& o) {
             o.batch_bytes = 1024;
             o.source = std::string(1020, 's');
         },
         "cannot hold the metadata"},
    };
    for (const auto& [change, expected] : cases) {
        const auto got = why(change);
        EXPECT_TRUE(has(got, expected)) << expected << " / " << got;
        EXPECT_FALSE(has(got, "s3cret")) << got;
    }
}

// ---- replies ----------------------------------------------------------------------------

TEST(HecReply, AnAcceptanceIsA200WithCode0Or24Or25) {
    using Kind = HecEventVerdict::Kind;
    const auto kind = [](const BoundedResponse& r) {
        return splunk_hec_event_result(r, 3, SplunkHecAck::None).kind;
    };
    EXPECT_EQ(kind(hec(200, 0, "Success")), Kind::Accepted);
    EXPECT_TRUE(
        splunk_hec_event_result(hec(200, 0, "Success"), 3, SplunkHecAck::None).detail.empty());
    const auto near = splunk_hec_event_result(
        hec(200, 24, "HEC queue is approaching its capacity limit"), 3, SplunkHecAck::None);
    EXPECT_EQ(near.kind, Kind::Accepted);
    EXPECT_TRUE(has(near.detail, "code 24")) << near.detail;
    EXPECT_EQ(kind(hec(200, 25, "HEC ACK is approaching its capacity limit")), Kind::Accepted);
    // Every other 200 is not an acceptance.
    EXPECT_EQ(kind(hec(200, 17, "HEC is healthy")), Kind::Failed);
    EXPECT_EQ(kind(hec(200, 6, "Invalid data format")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, "<html><body>OK</body></html>", "text/html")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, "")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, R"({"text":"Success"})")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, R"({"text":"Success","code":"0"})")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, R"({"text":"Success","code":0.5})")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, "[]")), Kind::Failed);
    EXPECT_EQ(kind(reply(200, R"({"text":"Success","code":0)")), Kind::Failed);
    EXPECT_TRUE(has(
        splunk_hec_event_result(reply(200, "<html/>", "text/html"), 3, SplunkHecAck::None).detail,
        "not counted as accepted"));
    // Other 2xx and 3xx are not HEC's answers either.
    EXPECT_EQ(kind(reply(204, "")), Kind::Failed);
    EXPECT_EQ(kind(reply(202, R"({"text":"Success","code":0})")), Kind::Failed);
    EXPECT_EQ(kind(reply(301, "", "text/html")), Kind::Failed);
}

TEST(HecReply, IndexerModeNeedsExactlyOneAckIdAndTheOtherModeNone) {
    using Kind = HecEventVerdict::Kind;
    const auto get = [](const std::string& more, SplunkHecAck ack = SplunkHecAck::Indexer) {
        return splunk_hec_event_result(hec(200, 0, "Success", more), 2, ack);
    };
    EXPECT_EQ(get(R"(,"ackId":7)").ack_id, 7U);
    EXPECT_EQ(get(R"(,"ackID":"7")").ack_id, 7U);  // as Splunk's documentation writes it
    EXPECT_EQ(get(R"(,"ackId":7,"ackID":"7")").ack_id, 7U);
    EXPECT_EQ(get(R"(,"ackId":0)").kind, Kind::Accepted);
    for (const std::string& bad : {R"(,"ackId":7,"ackID":"8")",
                                   R"(,"ackId":-1)",
                                   R"(,"ackId":"x")",
                                   R"(,"ackId":1.5)",
                                   R"(,"ackId":null)",
                                   R"(,"ackID":"")"}) {
        EXPECT_EQ(get(bad).kind, Kind::Failed) << bad;
    }
    const auto none = get("");
    EXPECT_EQ(none.kind, Kind::Failed);
    EXPECT_TRUE(has(none.detail, "does not use indexer acknowledgement")) << none.detail;
    const auto unexpected = get(R"(,"ackId":3)", SplunkHecAck::None);
    EXPECT_EQ(unexpected.kind, Kind::Failed);
    EXPECT_TRUE(has(unexpected.detail, "set ack='indexer'")) << unexpected.detail;
}

TEST(HecReply, EveryFailureClassIsToldApartAndNoneIsARecordRefusal) {
    using Kind = HecEventVerdict::Kind;
    const auto get = [](const BoundedResponse& r) {
        return splunk_hec_event_result(r, 4, SplunkHecAck::None);
    };
    for (const auto& r : {hec(401, 2, "Token is required"),
                          hec(401, 3, "Invalid authorization"),
                          hec(403, 1, "Token disabled"),
                          hec(403, 4, "Invalid token"),
                          hec(400, 21, "Invalid token"),
                          hec(400, 22, "Token disabled"),
                          reply(403, "<html>denied</html>", "text/html")}) {
        const auto v = get(r);
        EXPECT_EQ(v.kind, Kind::Failed) << r.body;
        EXPECT_TRUE(has(v.detail, "refused the token")) << v.detail;
    }
    const std::vector<std::pair<BoundedResponse, std::string>> failed = {
        {hec(400, 7, "Incorrect index"), "the token's allowed indexes"},
        {hec(400, 10, "Data channel is missing"), "set ack='indexer'"},
        {hec(400, 14, "ACK is disabled"), "set ack='none'"},
        {hec(400, 11, "Invalid data channel"), "did not accept the channel"},
        {hec(400, 5, "No data"), "code 5"},
        {hec(400, 15, "Error in handling indexed fields"), "code 15"},
        {hec(400, 16, "Query string authorization is not enabled"), "code 16"},
        {hec(400, 0, "Success"), "not 400"},
        {hec(400, 9, "Server is busy"), "not 400"},
        {reply(400, "<html>bad</html>", "text/html"), "not an HEC reply"},
        {reply(413, "too big", "text/plain"), "lower batch_bytes"},
        {reply(404, "", "text/plain"), "no HEC event endpoint"},
        {reply(405, "", "text/plain"), "HTTP 405"},
    };
    for (const auto& [r, why] : failed) {
        const auto v = get(r);
        EXPECT_EQ(v.kind, Kind::Failed) << r.body;
        EXPECT_TRUE(has(v.detail, why)) << why << " / " << v.detail;
    }
    for (const auto& r : {hec(500, 8, "Internal server error"),
                          hec(503, 9, "Server is busy"),
                          hec(503, 18, "HEC is unhealthy, queues are full"),
                          hec(503, 19, "HEC is unhealthy, ack service unavailable"),
                          hec(503, 20, "HEC is unhealthy"),
                          hec(503, 23, "Server is shutting down"),
                          hec(429, 26, "HEC queue is at capacity"),
                          hec(429, 27, "HEC ACK channel is at capacity"),
                          reply(502, "<html>bad gateway</html>", "text/html"),
                          reply(429, "", "text/plain")}) {
        const auto v = get(r);
        EXPECT_EQ(v.kind, Kind::Retry) << r.body;
        EXPECT_TRUE(has(v.detail, "may have accepted")) << v.detail;
    }
    BoundedResponse unsent;
    unsent.failure = BoundedResponse::Failure::Connect;
    unsent.error = "Connection refused";
    EXPECT_EQ(get(unsent).kind, Kind::Retry);
    EXPECT_TRUE(has(get(unsent).detail, "received none of the events")) << get(unsent).detail;
    BoundedResponse lost;
    lost.failure = BoundedResponse::Failure::Read;
    lost.request_written = true;
    lost.status = 200;
    EXPECT_EQ(get(lost).kind, Kind::Retry);
    EXPECT_TRUE(has(get(lost).detail, "may have accepted some or all")) << get(lost).detail;
    BoundedResponse timed_out;
    timed_out.interrupted = true;
    timed_out.request_written = true;
    EXPECT_TRUE(has(get(timed_out).detail, "no reply within request_timeout_ms"));
}

TEST(HecReply, ARefusedEventMustBeOneTheRequestCarried) {
    using Kind = HecEventVerdict::Kind;
    const auto get = [](const std::string& more, int code = 6) {
        return splunk_hec_event_result(
            hec(400, code, "Invalid data format", more), 5, SplunkHecAck::None);
    };
    const auto v = get(R"(,"invalid-event-number":2)");
    EXPECT_EQ(v.kind, Kind::RefusedEvent);
    EXPECT_EQ(v.event, 2U);
    EXPECT_EQ(get(R"(,"invalid-event-number":0)", 12).kind, Kind::RefusedEvent);
    EXPECT_EQ(get(R"(,"invalid-event-number":4)", 13).event, 4U);
    for (const std::string& bad : {std::string{},
                                   std::string{R"(,"invalid-event-number":5)"},
                                   std::string{R"(,"invalid-event-number":-1)"},
                                   std::string{R"(,"invalid-event-number":"2")"},
                                   std::string{R"(,"invalid-event-number":1.0)"}}) {
        const auto w = get(bad);
        EXPECT_EQ(w.kind, Kind::Failed) << bad;
        EXPECT_TRUE(has(w.detail, "naming no event of the 5")) << w.detail;
    }
    // Code 7 names an event too, but the index is the sink's, not the event's.
    EXPECT_EQ(get(R"(,"invalid-event-number":1)", 7).kind, Kind::Failed);
}

TEST(HecReply, AcknowledgementRepliesAreReadStrictly) {
    using Kind = HecAckVerdict::Kind;
    const std::vector<std::uint64_t> asked{0, 1, 2};
    const auto read = splunk_hec_ack_result(reply(200, R"({"acks":{"0":true,"1":false}})"), asked);
    EXPECT_EQ(read.kind, Kind::Read);  // 2 left out: no status yet
    EXPECT_EQ(read.indexed, std::vector<std::uint64_t>{0});
    EXPECT_EQ(splunk_hec_ack_result(reply(200, R"({"acks":{}})"), asked).kind, Kind::Read);
    const std::vector<std::pair<std::string, std::string>> malformed = {
        {R"({"acks":{"3":true}})", "which was not asked about"},
        {R"({"acks":{"0":"true"}})", "neither true nor false"},
        {R"({"acks":{"x":true}})", "not a number"},
        {R"({"acks":[0]})", "no \"acks\" object"},
        {R"({"text":"Success","code":0})", "no \"acks\" object"},
        {"<html/>", "not a JSON object"},
    };
    for (const auto& [body, why] : malformed) {
        const auto v = splunk_hec_ack_result(reply(200, body), asked);
        EXPECT_EQ(v.kind, Kind::Failed) << body;
        EXPECT_TRUE(has(v.detail, why)) << v.detail;
        EXPECT_TRUE(v.indexed.empty());
    }
    EXPECT_EQ(splunk_hec_ack_result(hec(503, 9, "Server is busy"), asked).kind, Kind::Retry);
    EXPECT_EQ(splunk_hec_ack_result(hec(429, 27, "at capacity"), asked).kind, Kind::Retry);
    BoundedResponse lost;
    lost.failure = BoundedResponse::Failure::Read;
    EXPECT_EQ(splunk_hec_ack_result(lost, asked).kind, Kind::Retry);
    const auto disabled = splunk_hec_ack_result(hec(400, 14, "ACK is disabled"), asked);
    EXPECT_EQ(disabled.kind, Kind::Failed);
    EXPECT_TRUE(has(disabled.detail, "set ack='none'")) << disabled.detail;
    EXPECT_TRUE(has(splunk_hec_ack_result(hec(403, 4, "Invalid token"), asked).detail,
                    "refused the token"));
}

TEST(HecReply, ADescriptionShowsNoBodyThatIsNotHecs) {
    const auto html = splunk_hec_describe(
        reply(502, "<html>upstream said: password=hunter2</html>", "text/html; charset=utf-8"));
    EXPECT_TRUE(has(html, "HTTP 502 with a text/html; charset=utf-8 body of 44 bytes")) << html;
    EXPECT_FALSE(has(html, "hunter2")) << html;
    const auto own = splunk_hec_describe(hec(403, 4, "Invalid token\\u0001"));
    EXPECT_EQ(own, "HTTP 403, HEC code 4 (Invalid token?)");
    EXPECT_EQ(splunk_hec_describe(reply(503, R"({"code":9})")),
              "HTTP 503, HEC code 9 (Server is busy)");
}

// ---- the sink ---------------------------------------------------------------------------

TEST(HecSink, WritesOneEnvelopePerLineWithTheTokenAndTheMetadata) {
    HecStub stub;
    auto o = against(stub);
    o.index = "main";
    o.sourcetype = "_json";
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    sink->on_data(batch({rec(1), " { \"id\" : 2 }\n"}));
    sink->flush();
    const auto reqs = stub.requests();
    ASSERT_EQ(reqs.size(), 1U);
    const std::string meta = R"(,"index":"main","sourcetype":"_json"})";
    EXPECT_EQ(reqs[0].body, R"({"event":)" + rec(1) + meta + "\n" + R"({"event":{"id":2})" + meta);
    EXPECT_EQ(reqs[0].path, "/services/collector/event");
    EXPECT_EQ(reqs[0].auth, "Splunk hec-token");
    EXPECT_EQ(reqs[0].content_type, "application/json");
    EXPECT_TRUE(reqs[0].channel.empty()) << "ack='none' sends no channel";
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), R"({"id":2})"}));
    for (const auto& e : stub.accepted()) {
        EXPECT_EQ(e.meta.at("index"), R"("main")");
        EXPECT_EQ(e.meta.at("sourcetype"), R"("_json")");
        EXPECT_EQ(e.meta.size(), 2U);
    }
}

TEST(HecSink, ARequestCarriesAtMostBatchRecordsEventsAndBatchBytesBytes) {
    HecStub stub;
    auto o = against(stub);
    o.batch_records = 3;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    std::vector<std::string> recs;
    for (int i = 1; i <= 7; ++i) {
        recs.push_back(rec(i));
    }
    sink->on_data(batch(recs));
    EXPECT_EQ(stub.requests().size(), 2U) << "two full requests before the barrier";
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    ASSERT_EQ(stub.requests().size(), 3U);
    EXPECT_EQ(stub.events().size(), 7U);

    // Bytes: each envelope {"event":{"x":"<200 a>"}} is 218 bytes; 1024 holds
    // four with their three line breaks (875), not five (1094).
    HecStub stub2;
    auto o2 = against(stub2);
    o2.batch_bytes = 1024;
    auto sink2 = make_splunk_hec_sink(o2);
    sink2->open();
    std::vector<std::string> big(9, R"({"x":")" + std::string(200, 'a') + R"("})");
    sink2->on_data(batch(big));
    sink2->flush();
    const auto reqs = stub2.requests();
    ASSERT_EQ(reqs.size(), 3U);
    for (const auto& r : reqs) {
        EXPECT_LE(r.body.size(), 1024U);
    }
    EXPECT_EQ(reqs[0].body.size(), 875U);
    EXPECT_EQ(stub2.events().size(), 9U);
}

TEST(HecSink, LingerSendsEventsThatWaitedLongEnough) {
    HecStub stub;
    auto o = against(stub);
    o.max_age = 50ms;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    sink->on_data(batch({rec(1)}));
    EXPECT_TRUE(stub.requests().empty());
    std::this_thread::sleep_for(80ms);
    sink->on_data(batch({rec(2)}));
    ASSERT_EQ(stub.requests().size(), 1U);
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(2)}));
}

TEST(HecSink, ARefusedRecordFailsTheSinkUntilItIsOpenedAgain) {
    HecStub stub;
    auto sink = make_splunk_hec_sink(against(stub));
    Attached at(*sink);
    sink->open();
    const auto why = error_of([&] { sink->on_data(batch({rec(1), R"({"a":1},"index":"x")"})); });
    EXPECT_TRUE(has(why, "has more after its object")) << why;
    for (const auto& next : {std::function<void()>{[&] { sink->on_data(batch({rec(2)})); }},
                             std::function<void()>{[&] {
                                 sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
                             }},
                             std::function<void()>{[&] { sink->flush(); }}}) {
        EXPECT_TRUE(has(error_of(next), "refuses to write or acknowledge a barrier"))
            << error_of(next);
    }
    EXPECT_TRUE(stub.requests().empty());
    EXPECT_TRUE(at.dlq.records.empty()) << "dlq=fail reports nothing";
    sink->open();
    sink->on_data(batch({rec(3)}));
    sink->flush();
    EXPECT_EQ(stub.events(), std::vector<std::string>{rec(3)});
}

TEST(HecSink, ARefusedRecordIsDeadLetteredUnderDrop) {
    HecStub stub;
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_splunk_hec_sink(o);
    Attached at(*sink);
    sink->open();
    const std::string bad = R"({"a":1,"a":2})";
    sink->on_data(batch({rec(1), bad, rec(2)}));
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(2)}));
    ASSERT_EQ(at.dlq.records.size(), 1U);
    EXPECT_EQ(at.dlq.records[0].payload, bad);
    EXPECT_TRUE(has(at.dlq.records[0].error, "repeats a key")) << at.dlq.records[0].error;
    EXPECT_EQ(at.dlq.records[0].connector, "splunk_hec_sink");
}

TEST(HecSink, AnEventHecRefusesFailsTheSinkWithEveryEventKeptUnderFail) {
    HecStub stub;
    stub.script({HecStub::refuse_event(1)});
    auto sink = make_splunk_hec_sink(against(stub));
    sink->open();
    sink->on_data(batch({rec(1), rec(2), rec(3)}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "HEC refused event 2 of the 3")) << why;
    EXPECT_TRUE(has(why, "every event of the request stays pending")) << why;
    // The stub, as HEC is reported to, kept the event before the refused one.
    EXPECT_EQ(stub.events(), std::vector<std::string>{rec(1)});
    sink->flush();  // the stub takes them all now
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(1), rec(2), rec(3)}));
}

TEST(HecSink, AnEventHecRefusesIsDeadLetteredAndTheOthersSentAgainUnderDrop) {
    HecStub stub;
    stub.script({HecStub::refuse_event(1)});
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    o.max_retries = 0;  // a refusal is no retry
    auto sink = make_splunk_hec_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({rec(1), rec(2), rec(3)}));
    sink->flush();
    // rec(1) was in before the refusal and goes again: nothing confirmed it.
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(1), rec(3)}));
    ASSERT_EQ(at.dlq.records.size(), 1U);
    EXPECT_EQ(at.dlq.records[0].payload, R"({"event":)" + rec(2) + "}");
    EXPECT_TRUE(has(at.dlq.records[0].error, "HEC refused event 2 of the 3"))
        << at.dlq.records[0].error;
    // An event the stub itself cannot read is refused the same way.
    HecStub stub2;
    stub2.script({HecStub::refuse_event(0, 13)});
    auto o2 = against(stub2);
    o2.dlq_policy = DlqPolicy::Drop;
    auto sink2 = make_splunk_hec_sink(o2);
    Attached at2(*sink2);
    sink2->open();
    sink2->on_data(batch({rec(1)}));
    sink2->flush();
    EXPECT_TRUE(stub2.events().empty());
    EXPECT_EQ(at2.dlq.records.size(), 1U);
}

TEST(HecSink, ABusyServersPartialAcceptanceIsSentAgainWhole) {
    HecStub stub;
    stub.script({HecStub::busy_after(2)});
    auto sink = make_splunk_hec_sink(against(stub));
    sink->open();
    sink->on_data(batch({rec(1), rec(2), rec(3), rec(4)}));
    sink->flush();
    const auto ev = stub.events();
    EXPECT_EQ(stub.requests().size(), 2U);
    EXPECT_EQ(count(ev, rec(1)), 2U);
    EXPECT_EQ(count(ev, rec(2)), 2U);
    EXPECT_EQ(count(ev, rec(3)), 1U);
    EXPECT_EQ(count(ev, rec(4)), 1U);
}

TEST(HecSink, AReplyLostAfterAcceptanceIsSentAgain) {
    HecStub stub;
    stub.script({HecStub::accept_then_stall(1500ms)});
    auto o = against(stub);
    o.request_timeout_ms = 300;
    auto sink = make_splunk_hec_sink(o);
    sink->open();
    sink->on_data(batch({rec(1), rec(2)}));
    sink->flush();
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(2), rec(1), rec(2)}));
}

TEST(HecSink, SpentRetriesFailWithTheEventsKeptForTheNextBarrier) {
    HecStub stub;
    stub.script({HecStub::hec(503, 9, "Server is busy"),
                 HecStub::hec(503, 9, "Server is busy"),
                 HecStub::hec(500, 8, "Internal server error")});
    auto sink = make_splunk_hec_sink(against(stub));  // max_retries 2
    sink->open();
    sink->on_data(batch({rec(1)}));
    const auto why =
        error_of([&] { sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}); });
    EXPECT_TRUE(has(why, "not accepted after 3 attempt(s)")) << why;
    EXPECT_TRUE(has(why, "HEC code 8")) << why;
    EXPECT_TRUE(stub.events().empty());
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{2}});
    EXPECT_EQ(stub.events(), std::vector<std::string>{rec(1)});
    EXPECT_EQ(stub.requests().size(), 4U);
}

TEST(HecSink, ThrottlingIsRetried) {
    HecStub stub;
    stub.script({HecStub::hec(429, 26, "HEC queue is at capacity")});
    auto sink = make_splunk_hec_sink(against(stub));
    sink->open();
    sink->on_data(batch({rec(1)}));
    sink->flush();
    EXPECT_EQ(stub.requests().size(), 2U);
    EXPECT_EQ(stub.events(), std::vector<std::string>{rec(1)});
}

TEST(HecSink, ARefusedTokenFailsAtOnceAndIsNeverDeadLettered) {
    HecStub::Options so;
    so.token = "the-right-one";
    HecStub stub(so);
    auto o = against(stub);
    o.token = "s3cret-wrong-token";
    o.dlq_policy = DlqPolicy::Drop;
    auto sink = make_splunk_hec_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({rec(1)}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "refused the token")) << why;
    EXPECT_FALSE(has(why, "s3cret")) << why;
    EXPECT_EQ(stub.requests().size(), 1U) << "not retried";
    EXPECT_TRUE(at.dlq.records.empty());
    // HEC's 400s for a token are not record refusals either.
    HecStub stub2;
    stub2.script({HecStub::hec(400, 21, "Invalid token")});
    auto o2 = against(stub2);
    o2.dlq_policy = DlqPolicy::Drop;
    auto sink2 = make_splunk_hec_sink(o2);
    Attached at2(*sink2);
    sink2->open();
    sink2->on_data(batch({rec(1)}));
    EXPECT_TRUE(has(error_of([&] { sink2->flush(); }), "refused the token"));
    EXPECT_TRUE(at2.dlq.records.empty());
}

TEST(HecSink, AnIndexHecRefusesFailsAndIsNeverDeadLettered) {
    HecStub stub;
    stub.script({HecStub::hec(400, 7, "Incorrect index")});
    auto o = against(stub);
    o.dlq_policy = DlqPolicy::Drop;
    o.index = "nope";
    auto sink = make_splunk_hec_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({rec(1)}));
    EXPECT_TRUE(has(error_of([&] { sink->flush(); }), "the token's allowed indexes"));
    EXPECT_TRUE(at.dlq.records.empty());
    sink->flush();  // kept, and sent once the index takes it
    EXPECT_EQ(stub.events(), std::vector<std::string>{rec(1)});
}

TEST(HecSink, AReplyThatIsNotHecsIsNotTakenAsAcceptance) {
    HecStub stub;
    stub.script({HecStub::accept_then_reply(200, "<html><body>Welcome</body></html>")});
    stub.script({HecStub::accept_then_reply(200, R"({"text":"HEC is healthy","code":17})")});
    auto sink = make_splunk_hec_sink(against(stub));
    sink->open();
    sink->on_data(batch({rec(1)}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "not counted as accepted")) << why;
    EXPECT_FALSE(has(why, "Welcome")) << why;
    EXPECT_TRUE(has(error_of([&] { sink->flush(); }), "code 17"));
    sink->flush();
    // The stub took it each time; the sink could not know.
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(1), rec(1)}));
}

TEST(HecSink, ATokenThatRequiresAcknowledgementIsToldToUseIt) {
    HecStub::Options so;
    so.require_ack = true;
    HecStub stub(so);
    auto sink = make_splunk_hec_sink(against(stub));
    sink->open();
    sink->on_data(batch({rec(1)}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "set ack='indexer'")) << why;
    EXPECT_EQ(stub.requests().size(), 1U);
}

TEST(HecSink, ACancelEndsTheRetryPauseAndTheRequestInFlight) {
    HecStub stub;
    stub.script(std::vector<HecStub::Step>(20, HecStub::hec(503, 9, "busy")));
    auto o = against(stub);
    o.max_retries = 20;
    auto sink = make_splunk_hec_sink(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({rec(1)}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(500ms);
        at.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why = error_of([&] { sink->flush(); });
    canceller.join();
    EXPECT_TRUE(has(why, "cancelled while retrying")) << why;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s);
    at.cancel->store(false);

    stub.clear_script();
    stub.script({HecStub::accept_then_stall(5000ms)});
    std::thread canceller2([&] {
        std::this_thread::sleep_for(300ms);
        at.cancel->store(true);
    });
    const auto t1 = std::chrono::steady_clock::now();
    const auto why2 = error_of([&] { sink->flush(); });
    canceller2.join();
    EXPECT_TRUE(has(why2, "cancelled while a request")) << why2;
    EXPECT_LT(std::chrono::steady_clock::now() - t1, 2s);
    at.cancel->store(false);
    sink->flush();  // still pending, so sent again
    EXPECT_EQ(count(stub.events(), rec(1)), 2U);
}

TEST(HecSink, IndexerModeSendsOneChannelAndWaitsForEveryAcknowledgement) {
    HecStub::Options so;
    so.require_ack = true;
    so.ack_after = 2;  // each ackId reads true at its third query
    HecStub stub(so);
    auto o = indexed(stub);
    o.batch_records = 2;
    auto sink = std::make_shared<SplunkHecSink>(o);
    sink->open();
    const std::string channel = sink->channel();
    EXPECT_TRUE(clink::http_connector::hec_stub::guid_shaped(channel)) << channel;
    EXPECT_EQ(channel[14], '4') << "a version 4 GUID";
    sink->on_data(batch({rec(1), rec(2), rec(3)}));
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    EXPECT_EQ(stub.events().size(), 3U);
    auto acked = stub.acknowledged();
    std::sort(acked.begin(), acked.end());
    EXPECT_EQ(acked, (std::vector<std::uint64_t>{0, 1}))
        << "both requests confirmed by the barrier";
    EXPECT_GE(stub.ack_queries().size(), 3U);
    for (const auto& r : stub.requests()) {
        EXPECT_EQ(r.channel, channel);
    }
    for (const auto& q : stub.ack_queries()) {
        EXPECT_EQ(q.channel, channel);
        EXPECT_EQ(q.auth, "Splunk hec-token");
        EXPECT_EQ(q.path, "/services/collector/ack");
    }
    // A barrier with nothing new waits for nothing.
    const auto queries = stub.ack_queries().size();
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{2}});
    EXPECT_EQ(stub.ack_queries().size(), queries);
}

TEST(HecSink, AnAcknowledgementThatNeverArrivesFailsTheSinkUntilItIsOpenedAgain) {
    HecStub::Options so;
    so.require_ack = true;
    so.ack_never = true;
    HecStub stub(so);
    auto o = indexed(stub);
    o.ack_timeout = 1000ms;
    auto sink = std::make_shared<SplunkHecSink>(o);
    sink->open();
    const std::string first = sink->channel();
    sink->on_data(batch({rec(1)}));
    const auto t0 = std::chrono::steady_clock::now();
    const auto why =
        error_of([&] { sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}); });
    EXPECT_GE(std::chrono::steady_clock::now() - t0, 1000ms);
    EXPECT_TRUE(has(why, "did not acknowledge ackId 0 (1 event(s)) on channel " + first)) << why;
    EXPECT_TRUE(has(why, "replay")) << why;
    EXPECT_TRUE(
        has(error_of([&] { sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{2}}); }),
            "refuses to write or acknowledge a barrier"));
    EXPECT_TRUE(has(error_of([&] { sink->on_data(batch({rec(2)})); }), "refuses"));
    stub.set_ack_never(false);
    sink->open();
    EXPECT_NE(sink->channel(), first) << "a new run, a new channel";
    sink->on_data(batch({rec(3)}));
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{3}});
    EXPECT_EQ(stub.events(), (std::vector<std::string>{rec(1), rec(3)}));
    EXPECT_EQ(stub.accepted().back().channel, sink->channel());
}

TEST(HecSink, AcknowledgementQueriesThatFailAreAskedAgainOrFailTheSink) {
    HecStub::Options so;
    so.require_ack = true;
    HecStub stub(so);
    stub.script_acks({HecStub::hec(503, 9, "Server is busy"),
                      HecStub::reply(502, "<html>bad gateway</html>", "text/html")});
    auto sink = std::make_shared<SplunkHecSink>(indexed(stub));
    sink->open();
    sink->on_data(batch({rec(1)}));
    sink->on_barrier(
        clink::CheckpointBarrier{clink::CheckpointId{1}});  // asked three times, the third answered
    EXPECT_EQ(stub.ack_queries().size(), 3U);
    EXPECT_EQ(stub.acknowledged().size(), 1U);

    stub.script_acks({HecStub::reply(200, R"({"acks":{"99":true}})")});
    sink->on_data(batch({rec(2)}));
    const auto why =
        error_of([&] { sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{2}}); });
    EXPECT_TRUE(has(why, "could not be read")) << why;
    EXPECT_TRUE(has(why, "not asked about")) << why;
    EXPECT_TRUE(has(error_of([&] { sink->flush(); }), "refuses"));
}

TEST(HecSink, AnAcceptanceWithoutAnAckIdFailsIndexerMode) {
    HecStub stub;  // a token without indexer acknowledgement
    auto sink = std::make_shared<SplunkHecSink>(indexed(stub));
    sink->open();
    sink->on_data(batch({rec(1)}));
    const auto why = error_of([&] { sink->flush(); });
    EXPECT_TRUE(has(why, "does not use indexer acknowledgement")) << why;
    EXPECT_FALSE(stub.requests().front().channel.empty()) << "the channel was sent";
}

TEST(HecSink, AcknowledgementsAwaitedAtOnceAreBounded) {
    HecStub::Options so;
    so.require_ack = true;
    so.ack_after = 1;
    HecStub stub(so);
    auto o = indexed(stub);
    o.batch_records = 1;
    o.max_pending_acks = 2;
    auto sink = std::make_shared<SplunkHecSink>(o);
    sink->open();
    sink->on_data(batch({rec(1), rec(2), rec(3), rec(4), rec(5)}));
    // Sends three to five each waited for an acknowledgement, before any barrier.
    EXPECT_GE(stub.acknowledged().size(), 3U);
    EXPECT_FALSE(stub.ack_queries().empty());
    sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}});
    EXPECT_EQ(stub.acknowledged().size(), 5U);
}

TEST(HecSink, ACancelEndsTheWaitForAcknowledgements) {
    HecStub::Options so;
    so.require_ack = true;
    so.ack_never = true;
    HecStub stub(so);
    auto o = indexed(stub);
    o.ack_timeout = 60000ms;
    auto sink = std::make_shared<SplunkHecSink>(o);
    Attached at(*sink);
    sink->open();
    sink->on_data(batch({rec(1)}));
    std::thread canceller([&] {
        std::this_thread::sleep_for(300ms);
        at.cancel->store(true);
    });
    const auto t0 = std::chrono::steady_clock::now();
    const auto why =
        error_of([&] { sink->on_barrier(clink::CheckpointBarrier{clink::CheckpointId{1}}); });
    canceller.join();
    // Most likely in the pause between queries, else in a query.
    EXPECT_TRUE(has(why, "cancelled while")) << why;
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 3s);
}

// ---- the factory ------------------------------------------------------------------------

TEST(HecFactory, TheRegisteredFactoryReadsEveryOptionWholeAndStrictly) {
    const auto* f = clink::cluster::OperatorRegistry::default_instance().find_sink(
        "splunk_hec_sink", std::string{clink::cluster::kChannelString});
    ASSERT_NE(f, nullptr);
    const auto build = [&](std::map<std::string, std::string> params) {
        clink::cluster::OperatorBuildContext ctx;
        ctx.params = std::move(params);
        ctx.params.emplace("url", "http://127.0.0.1:1");
        ctx.params.emplace("token", "t");
        return error_of([&] { (void)f->build(ctx); });
    };
    EXPECT_EQ(build({}), "");
    EXPECT_EQ(build({{"ack", "indexer"},
                     {"max_pending_acks", "5"},
                     {"ack_poll_ms", "100"},
                     {"ack_timeout_ms", "5000"},
                     {"dlq", "drop"},
                     {"linger_ms", "10"}}),
              "");
    EXPECT_TRUE(has(build({{"batch_records", "12abc"}}), "must be an integer"));
    EXPECT_TRUE(has(build({{"batch_records", "-1"}}), "must be from 1 to 100000"));
    EXPECT_TRUE(has(build({{"batch_bytes", "1023"}}), "must be from 1024"));
    EXPECT_TRUE(has(build({{"max_retries", "99999999999"}}), "must be from 0 to 20"));
    EXPECT_TRUE(has(build({{"ack", "yes"}}), "must be one of 'none', 'indexer'"));
    EXPECT_TRUE(has(build({{"dlq", "skip"}}), "must be one of 'fail', 'drop'"));
    EXPECT_TRUE(has(build({{"verify_tls", "no"}}), "must be one of 'true', 'false'"));
    EXPECT_TRUE(
        has(build({{"ack_timeout_ms", "5000"}}), "'ack_timeout_ms' applies to ack='indexer'"));
    EXPECT_TRUE(has(build({{"max_pending_acks", "5"}}), "applies to ack='indexer'"));
    EXPECT_TRUE(has(build({{"headers", "Authorization: Basic x"}}), "sets Authorization"));
    EXPECT_TRUE(has(build({{"path", "/services/collector/raw"}}), "not raw text"));
}
