// OTLP/HTTP export: the encoders, the span buffer, and the exporter driven
// against a real in-process HTTP server standing in for a collector.
//
// The encoder tests parse the produced JSON with the same parser the rest
// of the engine uses rather than grepping substrings, so a structural
// mistake (unbalanced nesting, a number where the protobuf-JSON mapping
// wants a string) fails here and not in a collector at 2am.

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/json.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/otlp_export.hpp"
#include "clink/test/test_cluster.hpp"

#ifdef CLINK_HAS_HTTP
#include <condition_variable>
#include <mutex>

#include "clink/http/http_server.hpp"
#endif

namespace {

using clink::MetricsRegistry;
using clink::metrics::OtlpSpan;
using clink::metrics::SpanBuffer;

TEST(OtlpExport, MetricsJsonCarriesEveryKindInProtobufJsonShape) {
    MetricsRegistry::Snapshot snap;
    snap.counters.emplace_back("clink_test_events_total", 5);
    snap.gauges.emplace_back("clink_test_backlog", -2);
    MetricsRegistry::HistogramEntry h;
    h.name = "clink_test_latency_ms";
    h.data.upper_bounds = {10.0, 20.0};
    h.data.bucket_counts = {1, 2, 4};  // last = +Inf overflow
    h.data.sum = 42.5;
    h.data.count = 7;
    snap.histograms.push_back(std::move(h));

    const auto json = clink::metrics::otlp_metrics_json(snap, "clink-test", 123456789);
    const auto root = clink::config::parse(json);

    const auto& rm = root.at("resourceMetrics").as_array().at(0);
    // service.name is how a collector attributes the data to this process.
    const auto& attr = rm.at("resource").at("attributes").as_array().at(0);
    EXPECT_EQ(attr.at("key").as_string(), "service.name");
    EXPECT_EQ(attr.at("value").at("stringValue").as_string(), "clink-test");

    const auto& metrics = rm.at("scopeMetrics").as_array().at(0).at("metrics").as_array();
    ASSERT_EQ(metrics.size(), 3u);

    // Counter: monotonic cumulative sum; 64-bit values are STRINGS in the
    // protobuf-JSON mapping, which is exactly the mistake this test pins.
    const auto& counter = metrics.at(0);
    EXPECT_EQ(counter.at("name").as_string(), "clink_test_events_total");
    const auto& sum = counter.at("sum");
    EXPECT_TRUE(sum.at("isMonotonic").as_bool());
    EXPECT_EQ(static_cast<int>(sum.at("aggregationTemporality").as_number()), 2);
    const auto& dp = sum.at("dataPoints").as_array().at(0);
    EXPECT_EQ(dp.at("asInt").as_string(), "5");
    EXPECT_EQ(dp.at("timeUnixNano").as_string(), "123456789");

    const auto& gauge = metrics.at(1);
    EXPECT_EQ(gauge.at("name").as_string(), "clink_test_backlog");
    EXPECT_EQ(gauge.at("gauge").at("dataPoints").as_array().at(0).at("asInt").as_string(), "-2");

    const auto& hist = metrics.at(2).at("histogram").at("dataPoints").as_array().at(0);
    EXPECT_EQ(hist.at("count").as_string(), "7");
    EXPECT_DOUBLE_EQ(hist.at("sum").as_number(), 42.5);
    ASSERT_EQ(hist.at("bucketCounts").as_array().size(), 3u);
    ASSERT_EQ(hist.at("explicitBounds").as_array().size(), 2u);
    EXPECT_EQ(hist.at("bucketCounts").as_array().at(2).as_string(), "4");
    EXPECT_DOUBLE_EQ(hist.at("explicitBounds").as_array().at(1).as_number(), 20.0);
}

TEST(OtlpExport, SpansJsonCarriesIdentityTimesAttributesAndStatus) {
    OtlpSpan span;
    span.name = "clink.checkpoint";
    span.start_unix_nano = 1000;
    span.end_unix_nano = 2000;
    span.attributes = {{"clink.job_id", "7"}};
    span.ok = false;
    span.trace_id = std::string(32, 'a');
    span.span_id = std::string(16, 'b');

    const auto json = clink::metrics::otlp_spans_json({span}, "clink-test");
    const auto root = clink::config::parse(json);
    const auto& s = root.at("resourceSpans")
                        .as_array()
                        .at(0)
                        .at("scopeSpans")
                        .as_array()
                        .at(0)
                        .at("spans")
                        .as_array()
                        .at(0);
    EXPECT_EQ(s.at("traceId").as_string(), std::string(32, 'a'));
    EXPECT_EQ(s.at("spanId").as_string(), std::string(16, 'b'));
    EXPECT_EQ(s.at("name").as_string(), "clink.checkpoint");
    EXPECT_EQ(s.at("startTimeUnixNano").as_string(), "1000");
    EXPECT_EQ(s.at("endTimeUnixNano").as_string(), "2000");
    EXPECT_EQ(s.at("attributes").as_array().at(0).at("key").as_string(), "clink.job_id");
    EXPECT_EQ(static_cast<int>(s.at("status").at("code").as_number()), 2)
        << "ok=false must map to STATUS_CODE_ERROR";
}

TEST(OtlpExport, SpanBufferIsANoOpUntilAnExporterEnablesIt) {
    auto& buf = SpanBuffer::global();
    ASSERT_FALSE(buf.enabled()) << "the default must be off: no exporter, no cost, no growth";
    buf.record(OtlpSpan{.name = "dropped"});
    EXPECT_TRUE(buf.drain().empty());

    buf.set_enabled(true);
    buf.record(OtlpSpan{.name = "kept"});
    auto drained = buf.drain();
    ASSERT_EQ(drained.size(), 1u);
    EXPECT_EQ(drained[0].name, "kept");
    // Identity is generated at record time so the encoder stays pure.
    EXPECT_EQ(drained[0].trace_id.size(), 32u);
    EXPECT_EQ(drained[0].span_id.size(), 16u);
    buf.set_enabled(false);
}

TEST(OtlpExport, SpanBufferDropsOldestAtCapacityAndCountsTheDrop) {
    auto& buf = SpanBuffer::global();
    buf.set_enabled(true);
    const auto dropped_before =
        MetricsRegistry::global().counter("clink_otlp_spans_dropped_total").value();
    for (std::size_t i = 0; i < SpanBuffer::kCapacity + 3; ++i) {
        buf.record(OtlpSpan{.name = "s" + std::to_string(i)});
    }
    const auto drained = buf.drain();
    EXPECT_EQ(drained.size(), SpanBuffer::kCapacity)
        << "a slow collector must never grow engine memory past the cap";
    EXPECT_EQ(drained.front().name, "s3") << "drop-oldest, keep-newest";
    EXPECT_EQ(MetricsRegistry::global().counter("clink_otlp_spans_dropped_total").value(),
              dropped_before + 3);
    buf.set_enabled(false);
}

#ifdef CLINK_HAS_HTTP

// End to end against a stand-in collector: the exporter POSTs real OTLP
// JSON to /v1/metrics and /v1/traces. export_once() is driven directly so
// nothing here waits on a clock.
TEST(OtlpExport, ExporterPostsMetricsAndSpansToACollector) {
    std::mutex mu;
    std::string metrics_body;
    std::string traces_body;

    clink::http::HttpServer server;
    server.post("/v1/metrics", [&](const clink::http::HttpRequest& req) {
        std::lock_guard lock(mu);
        metrics_body = req.body;
        return clink::http::HttpResponse{};
    });
    server.post("/v1/traces", [&](const clink::http::HttpRequest& req) {
        std::lock_guard lock(mu);
        traces_body = req.body;
        return clink::http::HttpResponse{};
    });
    const auto port = server.start("127.0.0.1", 0);
    ASSERT_NE(port, 0);

    MetricsRegistry::global().counter("clink_otlp_e2e_probe_total").increment();
    const auto ok_before = MetricsRegistry::global().counter("clink_otlp_exports_total").value();

    {
        clink::metrics::OtlpHttpExporter exporter({.host = "127.0.0.1",
                                                   .port = port,
                                                   .service_name = "clink-e2e",
                                                   // Long interval: the loop never fires on its
                                                   // own inside this test; export_once() drives.
                                                   .interval_ms = 3600000});
        EXPECT_TRUE(SpanBuffer::global().enabled())
            << "constructing the exporter must arm the span sites";
        SpanBuffer::global().record(
            OtlpSpan{.name = "clink.checkpoint", .start_unix_nano = 1, .end_unix_nano = 2});
        exporter.export_once();
    }
    EXPECT_FALSE(SpanBuffer::global().enabled())
        << "destroying the exporter must disarm the span sites";

    std::lock_guard lock(mu);
    ASSERT_FALSE(metrics_body.empty());
    ASSERT_FALSE(traces_body.empty());
    const auto metrics = clink::config::parse(metrics_body);
    EXPECT_NE(metrics_body.find("clink_otlp_e2e_probe_total"), std::string::npos);
    EXPECT_EQ(metrics.at("resourceMetrics")
                  .as_array()
                  .at(0)
                  .at("resource")
                  .at("attributes")
                  .as_array()
                  .at(0)
                  .at("value")
                  .at("stringValue")
                  .as_string(),
              "clink-e2e");
    const auto traces = clink::config::parse(traces_body);
    const auto& spans = traces.at("resourceSpans")
                            .as_array()
                            .at(0)
                            .at("scopeSpans")
                            .as_array()
                            .at(0)
                            .at("spans")
                            .as_array();
    ASSERT_EQ(spans.size(), 1u);
    EXPECT_EQ(spans.at(0).at("name").as_string(), "clink.checkpoint");
    EXPECT_EQ(MetricsRegistry::global().counter("clink_otlp_exports_total").value(), ok_before + 1);
    server.stop();
}

TEST(OtlpExport, AnUnreachableCollectorCountsAFailureAndTakesNothingDown) {
    const auto fail_before =
        MetricsRegistry::global().counter("clink_otlp_export_failures_total").value();
    // Port 1 on localhost: connection refused, immediately.
    clink::metrics::OtlpHttpExporter exporter(
        {.host = "127.0.0.1", .port = 1, .service_name = "clink-e2e", .interval_ms = 3600000});
    exporter.export_once();
    EXPECT_EQ(MetricsRegistry::global().counter("clink_otlp_export_failures_total").value(),
              fail_before + 1);
}

#endif  // CLINK_HAS_HTTP

// The clink.submit span site, behaviourally: a real in-process cluster runs
// a job to completion with the buffer enabled, and the drained spans carry
// exactly one clink.submit with the job id and task count. This is the site
// the tracker's "spans cover the checkpoint lifecycle only" gap named; the
// recovery and rescale sites follow the same guarded pattern at their
// (integration-tested) transitions.
TEST(OtlpExport, ASubmittedJobRecordsALifecycleSpan) {
    auto& buf = SpanBuffer::global();
    buf.set_enabled(true);
    (void)buf.drain();  // clear anything an earlier test left behind

    clink::test::TestCluster mini({.workers = 1, .slots_per_worker = 4});
    clink::cluster::JobGraphSpec g;
    clink::cluster::OperatorSpec src;
    src.type = "int64_range_source";
    src.id = "src";
    src.parallelism = 1;
    src.out_channel = std::string{clink::cluster::kChannelInt64};
    src.params = {{"count", "5"}};
    g.ops.push_back(src);
    clink::cluster::OperatorSpec snk;
    snk.type = "collecting_int64_sink";
    snk.id = "snk";
    snk.inputs = {"src"};
    snk.parallelism = 1;
    snk.out_channel = std::string{clink::cluster::kChannelInt64};
    g.ops.push_back(snk);
    const auto job_id = mini.execute(g);

    const auto spans = buf.drain();
    buf.set_enabled(false);
    std::size_t submits = 0;
    for (const auto& s : spans) {
        if (s.name != "clink.submit") {
            continue;
        }
        ++submits;
        EXPECT_GE(s.end_unix_nano, s.start_unix_nano);
        bool saw_job_id = false;
        bool saw_tasks = false;
        for (const auto& [k, v] : s.attributes) {
            if (k == "clink.job_id") {
                saw_job_id = true;
                EXPECT_EQ(v, std::to_string(job_id));
            }
            if (k == "clink.tasks") {
                saw_tasks = true;
                EXPECT_EQ(v, "2");
            }
        }
        EXPECT_TRUE(saw_job_id) << "the submit span carries no job id";
        EXPECT_TRUE(saw_tasks) << "the submit span carries no task count";
    }
    EXPECT_EQ(submits, 1u) << "exactly one clink.submit span per submit";
}

}  // namespace
