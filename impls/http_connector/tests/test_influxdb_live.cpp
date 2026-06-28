// InfluxDB v2 LIVE integration test. SKIPPED unless CLINK_INFLUXDB_TEST_ENDPOINT is set
// (e.g. http://localhost:8086). Org/bucket/token come from CLINK_INFLUXDB_TEST_{ORG,BUCKET,
// TOKEN} (defaults match the docker influxdb:2 used in the README). Proves against a real
// InfluxDB: a line-protocol batch lands every point, and a Flux query-back returns the written
// tags + field values.

#include <cstdlib>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/http_connector/http_request.hpp"
#include "clink/http_connector/influxdb_sink.hpp"

using clink::Batch;
using clink::http_connector::HttpRequest;
using clink::http_connector::InfluxDbOptions;
using clink::http_connector::make_influxdb_sink;

namespace {

bool influx_configured() {
    return std::getenv("CLINK_INFLUXDB_TEST_ENDPOINT") != nullptr;
}
std::string env_or(const char* k, const std::string& dflt) {
    const char* v = std::getenv(k);
    return v != nullptr ? std::string(v) : dflt;
}
std::string influx_endpoint() {
    return std::getenv("CLINK_INFLUXDB_TEST_ENDPOINT");
}
std::string influx_org() {
    return env_or("CLINK_INFLUXDB_TEST_ORG", "clink");
}
std::string influx_bucket() {
    return env_or("CLINK_INFLUXDB_TEST_BUCKET", "clink");
}
std::string influx_token() {
    return env_or("CLINK_INFLUXDB_TEST_TOKEN", "clink-token");
}

std::string unique_measurement() {
    static int n = 0;
    return "clink_it_" + std::to_string(static_cast<long>(::getpid())) + "_" + std::to_string(n++);
}

Batch<std::string> batch_of(std::vector<std::string> recs) {
    Batch<std::string> b;
    for (auto& r : recs) {
        b.emplace(std::move(r));
    }
    return b;
}

// Flux query-back: returns the annotated-CSV body for all points of `measurement` in the
// last hour, or an empty string on a non-2xx response.
std::string flux_query(const std::string& measurement) {
    HttpRequest::Options o;
    o.base_url = influx_endpoint();
    o.headers["Authorization"] = "Token " + influx_token();
    o.headers["Accept"] = "application/csv";
    HttpRequest c{o};
    const std::string flux = "from(bucket:\"" + influx_bucket() +
                             "\") |> range(start:-1h) |> filter(fn:(r) => r._measurement == \"" +
                             measurement + "\")";
    auto r = c.post("/api/v2/query?org=" + influx_org(), flux, "application/vnd.flux");
    if (r.status < 200 || r.status >= 300) {
        return {};
    }
    return r.body;
}

}  // namespace

TEST(InfluxDbLive, WritePointsAndQueryBack) {
    if (!influx_configured()) {
        GTEST_SKIP() << "set CLINK_INFLUXDB_TEST_ENDPOINT (+ _ORG/_BUCKET/_TOKEN)";
    }
    const std::string meas = unique_measurement();
    InfluxDbOptions o;
    o.url = influx_endpoint();
    o.org = influx_org();
    o.bucket = influx_bucket();
    o.token = influx_token();
    o.measurement = meas;
    o.tag_keys = "host";
    o.timestamp_field = "ts";
    o.precision = "s";
    o.batch_records = 100;
    // Current epoch seconds so the points land inside the query's -1h range below.
    const auto now_s = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    auto sink = make_influxdb_sink(o);
    sink->open();
    sink->on_data(batch_of({
        R"({"host":"node-a","temp":21.5,"ts":)" + std::to_string(now_s) + "}",
        R"({"host":"node-b","temp":19,"ts":)" + std::to_string(now_s - 1) + "}",
    }));
    sink->flush();  // POSTs; throws on delivery failure
    sink->close();

    const std::string csv = flux_query(meas);
    ASSERT_FALSE(csv.empty()) << "Flux query returned no rows for " << meas;
    EXPECT_NE(csv.find("node-a"), std::string::npos);
    EXPECT_NE(csv.find("node-b"), std::string::npos);
    EXPECT_NE(csv.find("21.5"), std::string::npos);
}
