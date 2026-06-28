// Offline unit tests for the InfluxDB v2 line-protocol sink. The line-protocol rendering
// (render_influx_line) is deterministic and needs no server, so it carries the bulk of the
// coverage; the live end-to-end test lives in test_influxdb_live.cpp (env-gated).

#include <string>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "clink/http_connector/influxdb_sink.hpp"

using clink::http_connector::InfluxDbOptions;
using clink::http_connector::make_influxdb_sink;
using clink::http_connector::render_influx_line;

namespace {

// Convenience: render one record with the given measurement / tags / timestamp field.
std::string line(const std::string& measurement,
                 const std::vector<std::string>& tags,
                 const std::string& ts_field,
                 const std::string& rec) {
    std::unordered_set<std::string> tag_set(tags.begin(), tags.end());
    std::string out;
    render_influx_line(out, measurement, tags, tag_set, ts_field, rec);
    return out;
}

}  // namespace

TEST(InfluxRender, FieldsOnlySortedByKey) {
    // JsonObject is an ordered std::map, so field order is deterministic (humidity < temp).
    // Numbers render as locale-independent shortest floats.
    EXPECT_EQ(line("env", {}, "", R"({"temp":21.5,"humidity":60})"), "env humidity=60,temp=21.5");
}

TEST(InfluxRender, TagsInConfiguredOrderThenFields) {
    EXPECT_EQ(line("env", {"host", "region"}, "", R"({"host":"a","region":"eu","v":1})"),
              "env,host=a,region=eu v=1");
}

TEST(InfluxRender, StringFieldQuotedAndEscaped) {
    EXPECT_EQ(line("logs", {}, "", R"({"msg":"he said \"hi\"\\x"})"),
              R"(logs msg="he said \"hi\"\\x")");
}

TEST(InfluxRender, BoolFieldsTF) {
    EXPECT_EQ(line("flags", {}, "", R"({"bad":false,"ok":true})"), "flags bad=f,ok=t");
}

TEST(InfluxRender, TimestampFieldAppendedAndExcludedFromFields) {
    EXPECT_EQ(line("env", {}, "ts", R"({"ts":1234567890,"v":2})"), "env v=2 1234567890");
}

TEST(InfluxRender, NullFieldsAndMissingTagsSkipped) {
    EXPECT_EQ(line("env", {"host"}, "", R"({"v":1,"x":null})"), "env v=1");
}

TEST(InfluxRender, MeasurementAndTagSpecialCharsEscaped) {
    // Space/comma in measurement; space/comma/equals in tag key+value all backslash-escaped.
    EXPECT_EQ(line("my env", {"a,b"}, "", R"({"a,b":"x y=z","v":1})"),
              R"(my\ env,a\,b=x\ y\=z v=1)");
}

TEST(InfluxRender, NestedAndArrayFieldsSkipped) {
    EXPECT_EQ(line("env", {}, "", R"({"v":1,"obj":{"a":1},"arr":[1,2]})"), "env v=1");
}

TEST(InfluxRender, NonObjectJsonYieldsBareMeasurement) {
    // Out-of-contract: InfluxDB will 400 this (-> DLQ/throw), never a silent accept.
    EXPECT_EQ(line("env", {}, "", "[1,2,3]"), "env ");
}

TEST(InfluxRender, NonJsonYieldsBareMeasurement) {
    EXPECT_EQ(line("env", {}, "", "not json at all"), "env ");
}

TEST(InfluxRender, IntegerValuedNumberRendersAsFloatLiteral) {
    // Shortest round-trip form: 2400 stays "2400" (a float literal in line protocol - no 'i').
    EXPECT_EQ(line("env", {}, "", R"({"n":2400})"), "env n=2400");
}

TEST(InfluxSink, RequiredOptionsValidated) {
    InfluxDbOptions base;
    base.url = "http://localhost:8086";
    base.org = "o";
    base.bucket = "b";
    base.token = "t";
    base.measurement = "m";
    EXPECT_NO_THROW(make_influxdb_sink(base));

    auto missing = [&](auto mutate) {
        InfluxDbOptions o = base;
        mutate(o);
        return o;
    };
    EXPECT_THROW(make_influxdb_sink(missing([](auto& o) { o.url.clear(); })), std::runtime_error);
    EXPECT_THROW(make_influxdb_sink(missing([](auto& o) { o.org.clear(); })), std::runtime_error);
    EXPECT_THROW(make_influxdb_sink(missing([](auto& o) { o.bucket.clear(); })),
                 std::runtime_error);
    EXPECT_THROW(make_influxdb_sink(missing([](auto& o) { o.token.clear(); })), std::runtime_error);
    EXPECT_THROW(make_influxdb_sink(missing([](auto& o) { o.measurement.clear(); })),
                 std::runtime_error);
    EXPECT_THROW(make_influxdb_sink(missing([](auto& o) { o.precision = "minutes"; })),
                 std::runtime_error);
}

TEST(InfluxSink, FlushAndCloseBeforeOpenAreSafe) {
    InfluxDbOptions o;
    o.url = "http://localhost:8086";
    o.org = "o";
    o.bucket = "b";
    o.token = "t";
    o.measurement = "m";
    auto sink = make_influxdb_sink(o);
    EXPECT_NO_THROW(sink->flush());  // nothing buffered: no POST
    EXPECT_NO_THROW(sink->close());
}
