// The InfluxDB sink against a live InfluxDB v2. Skipped unless
// CLINK_INFLUXDB_TEST_ENDPOINT names one (scripts/influxdb-live.sh sets it, with
// the org, bucket, token and the version it started). Each case writes to a
// measurement of its own through the production sink and reads every point
// back with Flux (influx_live.hpp), comparing tags, times, field values and
// field types rather than counting rows.
//
// What they hold the sink to:
//   - the server is the version the gate started;
//   - timestamps are written exactly: neighbours above 2^53 stay two points,
//     InfluxDB's range ends are written, and a record without a usable
//     timestamp is refused, never given the server's clock;
//   - every name and string the sink accepts comes back as it was sent, and
//     what it refuses (line breaks in names, NUL) never reaches the server;
//   - refused records beside good ones: dead-lettered with the good ones
//     written under dlq=drop, the sink stopped with nothing written under fail;
//   - float and typed numbers, and a field-type conflict's partial write;
//   - a repeated identity merges fields, identical or not;
//   - a reply lost after the server applied a batch: one point with a
//     timestamp, two without;
//   - a refused token and a missing bucket fail the sink, the token never
//     appearing in the error.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/influxdb_sink.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/runtime/runtime_context.hpp"

#include "influx_live.hpp"
#include "tests/integration/tcp_fault_proxy.hpp"

using clink::Batch;
using clink::CheckpointBarrier;
using clink::CheckpointId;
using clink::http_connector::DlqPolicy;
using clink::http_connector::HttpRequest;
using clink::http_connector::InfluxDbOptions;
using clink::http_connector::make_influxdb_sink;
namespace live = clink::http_connector::influx_live;

namespace {

using namespace std::chrono_literals;

#define REQUIRE_LIVE()                                                                  \
    do {                                                                                \
        if (!live::configured()) {                                                      \
            GTEST_SKIP() << "set CLINK_INFLUXDB_TEST_ENDPOINT (+ _ORG/_BUCKET/_TOKEN)"; \
        }                                                                               \
    } while (0)

// A JSON \u escape, built at run time so that no tool on the way to the
// compiler can turn the escape into the character it names.
std::string esc(const char* hex) {
    return std::string{"\\"} + "u" + hex;
}

std::string measurement(const char* base) {
    static int n = 0;
    return std::string{"clink_it_"} + base + "_" + std::to_string(static_cast<long>(::getpid())) +
           "_" + std::to_string(n++);
}

InfluxDbOptions options(const std::string& m, const std::string& tags, const std::string& ts) {
    InfluxDbOptions o;
    o.url = live::endpoint();
    o.org = live::org();
    o.bucket = live::bucket();
    o.token = live::token();
    o.measurement = m;
    o.tag_keys = tags;
    o.timestamp_field = ts;
    o.max_retries = 1;
    return o;
}

Batch<std::string> batch(const std::vector<std::string>& recs) {
    Batch<std::string> b;
    for (const auto& r : recs) {
        b.emplace(r);
    }
    return b;
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

struct Capture : clink::DeadLetterQueue {
    void report(const clink::BadRecord& r) override {
        const std::lock_guard<std::mutex> lock(mu);
        records.push_back(r);
    }
    std::mutex mu;
    std::vector<clink::BadRecord> records;
};

struct Attached {
    explicit Attached(clink::Sink<std::string>& s) : sink(s) {
        ctx = std::make_unique<clink::RuntimeContext>(
            clink::OperatorId{8087}, "influxdb_sink", nullptr, nullptr);
        ctx->set_dead_letter_queue(&dlq);
        sink.attach_runtime(ctx.get());
    }
    ~Attached() { sink.attach_runtime(nullptr); }
    Attached(const Attached&) = delete;
    Attached& operator=(const Attached&) = delete;
    clink::Sink<std::string>& sink;
    Capture dlq;
    std::unique_ptr<clink::RuntimeContext> ctx;
};

void write_all(const InfluxDbOptions& o, const std::vector<std::string>& recs) {
    auto sink = make_influxdb_sink(o);
    sink->open();
    sink->on_data(batch(recs));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    sink->close();
}

live::Field fld(std::string value, std::string type) {
    return live::Field{std::move(value), std::move(type)};
}

}  // namespace

TEST(InfluxDbLive, TheServerIsTheVersionTheGateStarted) {
    REQUIRE_LIVE();
    const std::string want = live::env_or("CLINK_INFLUXDB_TEST_VERSION");
    if (want.empty()) {
        GTEST_SKIP() << "set CLINK_INFLUXDB_TEST_VERSION to the version the server should be";
    }
    HttpRequest::Options o;
    o.base_url = live::endpoint();
    HttpRequest c{o};
    const auto res = c.get("/health");
    ASSERT_EQ(res.status, 200);
    const auto j = clink::config::parse(res.body);
    const std::string got = j.as_object().at("version").as_string();
    EXPECT_EQ(got, "v" + want) << "the server reports " << got;
    std::printf("[influxdb-live] version=%s\n", got.c_str());
}

TEST(InfluxDbLive, TimestampsAreWrittenExactlyToTheEndsOfTheRange) {
    REQUIRE_LIVE();
    const auto m = measurement("ts");
    // InfluxDB's last nanosecond is accepted (the write succeeds), but a Flux
    // range cannot reach it, its stop being exclusive; the point just below is read.
    write_all(options(m, "id", "ts"),
              {R"({"id":"a","v":1,"ts":9007199254740993})",
               R"({"id":"a","v":2,"ts":9007199254740994})",
               R"({"id":"a","v":3,"ts":9223372036854775806})",
               R"({"id":"a","v":5,"ts":9223372036854775000})",
               R"({"id":"a","v":4,"ts":-9223372036854775806})"});
    const auto p = live::points(m);
    EXPECT_EQ(p.at("id=a@1970-04-15T05:59:59.254740993Z").fields.at("v"), fld("1", "double"));
    EXPECT_EQ(p.at("id=a@1970-04-15T05:59:59.254740994Z").fields.at("v"), fld("2", "double"));
    EXPECT_EQ(p.at("id=a@2262-04-11T23:47:16.854775Z").fields.at("v"), fld("5", "double"));
    EXPECT_EQ(p.at("id=a@1677-09-21T00:12:43.145224194Z").fields.at("v"), fld("4", "double"));
}

TEST(InfluxDbLive, NeighbouringNanosecondsAbove2To53StayTwoPoints) {
    REQUIRE_LIVE();
    const auto m = measurement("nanos");
    write_all(options(m, "id", "ts"),
              {R"({"id":"a","v":1,"ts":1700000000000000001})",
               R"({"id":"a","v":2,"ts":1700000000000000002})"});
    const auto p = live::points(m);
    ASSERT_EQ(p.size(), 2u);
    EXPECT_EQ(p.at("id=a@2023-11-14T22:13:20.000000001Z").fields.at("v"), fld("1", "double"));
    EXPECT_EQ(p.at("id=a@2023-11-14T22:13:20.000000002Z").fields.at("v"), fld("2", "double"));
}

TEST(InfluxDbLive, EveryNameAndStringTheSinkAcceptsComesBackAsSent) {
    REQUIRE_LIVE();
    const auto m = measurement("esc") + " a,b=c";
    write_all(options(m, "t k=1", "ts"),
              {R"({"t k=1":"v,a l=x\\y\ttab","f k,=y":"x","s":"line\nbreak\ttab \"quoted\" )"
               R"(back\\slash caf)" +
               esc("00e9") + R"(","cr":"a\rb","ts":1})"});
    const auto p = live::points(m);
    ASSERT_EQ(p.size(), 1u);
    const auto& pt = p.begin()->second;
    EXPECT_EQ(pt.tags.at("t k=1"), "v,a l=x\\y\ttab");
    EXPECT_EQ(pt.fields.at("f k,=y"), fld("x", "string"));
    EXPECT_EQ(pt.fields.at("s"),
              fld("line\nbreak\ttab \"quoted\" back\\slash caf\xc3\xa9", "string"));
    // The annotated CSV drops a carriage return, so it is mapped to text first.
    const auto cr =
        live::flux_values("import \"strings\"\nfrom(bucket: \"" + live::bucket() +
                          "\") |> range(start: 0) |> filter(fn: (r) => r._measurement == \"" + m +
                          "\" and r._field == \"cr\") |> map(fn: (r) => ({r with _value: "
                          "strings.replaceAll(v: r._value, t: \"\\r\", u: \"<CR>\")}))");
    ASSERT_EQ(cr.size(), 1u);
    EXPECT_EQ(cr[0], "a<CR>b");
}

TEST(InfluxDbLive, RefusedRecordsBesideGoodOnesUnderEachPolicy) {
    REQUIRE_LIVE();
    const std::vector<std::string> recs = {
        R"({"id":"a","v":1,"ts":1})",
        R"({"id":"a","v":2})",                                      // no timestamp
        R"({"id":"a","v":3,"ts":2.5})",                             // fractional timestamp
        R"({"id":"x\ninjected v=99","v":4,"ts":3})",                // a line break in a tag
        "{\"id\":\"a\",\"s\":\"a" + esc("0000") + "b\",\"ts\":4}",  // NUL in a string
        R"({"id":"a","v":6,"ts":5})"};
    {
        const auto m = measurement("drop");
        auto o = options(m, "id", "ts");
        o.dlq_policy = DlqPolicy::Drop;
        auto sink = make_influxdb_sink(o);
        Attached a(*sink);
        sink->open();
        sink->on_data(batch(recs));
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
        const auto p = live::points(m);
        ASSERT_EQ(p.size(), 2u);
        EXPECT_EQ(p.at("id=a@1970-01-01T00:00:00.000000001Z").fields.at("v"), fld("1", "double"));
        EXPECT_EQ(p.at("id=a@1970-01-01T00:00:00.000000005Z").fields.at("v"), fld("6", "double"));
        EXPECT_EQ(a.dlq.records.size(), 4u);
    }
    {
        const auto m = measurement("fail");
        auto sink = make_influxdb_sink(options(m, "id", "ts"));
        sink->open();
        EXPECT_NE(error_of([&] { sink->on_data(batch(recs)); }).find("no 'ts'"), std::string::npos);
        EXPECT_NE(error_of([&] {
                      sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
                  }).find("refused a record earlier"),
                  std::string::npos);
        EXPECT_TRUE(live::points(m).empty());
    }
}

TEST(InfluxDbLive, FloatAndTypedNumbersAndATypeConflictsPartialWrite) {
    REQUIRE_LIVE();
    const auto m = measurement("num");
    auto f = options(m, "id", "ts");
    write_all(f, {R"({"id":"a","v":9007199254740992,"ts":1})"});
    auto t = options(m, "id", "ts");
    t.number_fields = "typed";
    write_all(t, {R"({"id":"a","c":9007199254740993,"ts":2})"});
    auto p = live::points(m);
    EXPECT_EQ(p.at("id=a@1970-01-01T00:00:00.000000001Z").fields.at("v"),
              fld("9007199254740992", "double"));
    EXPECT_EQ(p.at("id=a@1970-01-01T00:00:00.000000002Z").fields.at("c"),
              fld("9007199254740993", "long"));
    // v exists as a float; a string v in a batch is refused by the server, which
    // writes the rest and says only how many it dropped.
    const std::vector<std::string> mixed = {R"({"id":"b","v":1,"ts":10})",
                                            R"({"id":"b","v":"text","ts":11})",
                                            R"({"id":"b","v":3,"ts":12})"};
    {
        auto sink = make_influxdb_sink(options(m, "id", "ts"));
        sink->open();
        sink->on_data(batch(mixed));
        const auto why = error_of([&] { sink->on_barrier(CheckpointBarrier{CheckpointId{1}}); });
        EXPECT_NE(why.find("1 of the 3 record(s)"), std::string::npos) << why;
        EXPECT_NE(why.find("field type conflict"), std::string::npos) << why;
    }
    p = live::points(m);
    EXPECT_EQ(p.count("id=b@1970-01-01T00:00:00.00000001Z"), 1u);
    EXPECT_EQ(p.count("id=b@1970-01-01T00:00:00.000000011Z"), 0u);
    EXPECT_EQ(p.count("id=b@1970-01-01T00:00:00.000000012Z"), 1u);
    auto d = options(m, "id", "ts");
    d.dlq_policy = DlqPolicy::Drop;
    auto sink = make_influxdb_sink(d);
    Attached a(*sink);
    sink->open();
    sink->on_data(batch(mixed));
    sink->on_barrier(CheckpointBarrier{CheckpointId{1}});
    ASSERT_EQ(a.dlq.records.size(), 1u);
    EXPECT_NE(a.dlq.records[0].error.find("does not say which"), std::string::npos);
}

TEST(InfluxDbLive, ARepeatedIdentityMergesItsFields) {
    REQUIRE_LIVE();
    const auto m = measurement("dup");
    auto o = options(m, "id", "ts");
    write_all(o, {R"({"id":"a","x":1,"y":2,"ts":100})"});
    write_all(o, {R"({"id":"a","x":1,"y":2,"ts":100})"});  // identical: one point
    write_all(o, {R"({"id":"b","x":1,"y":2,"ts":100})"});
    write_all(o, {R"({"id":"b","x":5,"ts":100})"});  // fewer fields: y stays
    const auto p = live::points(m);
    ASSERT_EQ(p.size(), 2u);
    const auto& a = p.at("id=a@1970-01-01T00:00:00.0000001Z");
    EXPECT_EQ(a.fields.at("x"), fld("1", "double"));
    EXPECT_EQ(a.fields.at("y"), fld("2", "double"));
    const auto& b = p.at("id=b@1970-01-01T00:00:00.0000001Z");
    EXPECT_EQ(b.fields.at("x"), fld("5", "double"));
    EXPECT_EQ(b.fields.at("y"), fld("2", "double")) << "a field the later write lacks stays";
}

TEST(InfluxDbLive, AReplyLostAfterTheServerAppliedABatchIsOnePointOnlyWithATimestamp) {
    REQUIRE_LIVE();
    const auto port = static_cast<std::uint16_t>(
        std::stoi(live::endpoint().substr(live::endpoint().rfind(':') + 1)));
    for (const bool timestamped : {true, false}) {
        const auto m = measurement(timestamped ? "lost_ts" : "lost_server");
        clink::itest::TcpFaultProxy proxy(port);
        proxy.set_request_marker("POST /api/v2/write");
        auto o = options(m, "id", timestamped ? "ts" : "");
        o.url = "http://127.0.0.1:" + std::to_string(proxy.port());
        o.request_timeout_ms = 1000;
        o.max_retries = 3;
        auto sink = make_influxdb_sink(o);
        sink->open();
        proxy.hold_at_upload(1);
        sink->on_data(batch({R"({"id":"a","v":1,"ts":7})"}));
        sink->on_barrier(CheckpointBarrier{CheckpointId{1}});  // resent after the timeout
        proxy.drop_held();
        sink->close();
        const auto p = live::points(m);
        if (timestamped) {
            ASSERT_EQ(p.size(), 1u) << "the resent point lands on the same series and time";
        } else {
            EXPECT_EQ(p.size(), 2u) << "the server stamped the first write and the resend";
        }
        std::printf("[influxdb-live] reply lost, %s: %zu point(s)\n",
                    timestamped ? "timestamped" : "server time",
                    p.size());
    }
}

TEST(InfluxDbLive, ARefusedTokenOrAMissingBucketFailsTheSinkWithoutTheToken) {
    REQUIRE_LIVE();
    const auto m = measurement("auth");
    {
        auto o = options(m, "id", "ts");
        o.token = "wrong-t0ken-s3cret";
        o.max_retries = 0;
        auto sink = make_influxdb_sink(o);
        sink->open();
        sink->on_data(batch({R"({"id":"a","v":1,"ts":1})"}));
        const auto why = error_of([&] { sink->flush(); });
        EXPECT_NE(why.find("HTTP 401"), std::string::npos) << why;
        EXPECT_NE(why.find("token was refused"), std::string::npos) << why;
        EXPECT_EQ(why.find("wrong-t0ken-s3cret"), std::string::npos) << why;
    }
    {
        auto o = options(m, "id", "ts");
        o.bucket = "no_such_bucket";
        o.max_retries = 0;
        auto sink = make_influxdb_sink(o);
        sink->open();
        sink->on_data(batch({R"({"id":"a","v":1,"ts":1})"}));
        const auto why = error_of([&] { sink->flush(); });
        EXPECT_NE(why.find("HTTP 404"), std::string::npos) << why;
        EXPECT_NE(why.find("no such organisation or bucket"), std::string::npos) << why;
    }
    EXPECT_TRUE(live::points(m).empty());
}
