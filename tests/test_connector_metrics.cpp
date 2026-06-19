// Connector metric helpers: name-shape + helper accumulation.
// The full sink/source paths are integration-tested via the per-impl
// test binaries that talk to real brokers; the registry-side
// accumulation is what we pin here.

#include <string>

#include <gtest/gtest.h>

#include "clink/metrics/connector_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

// commit_latency is a histogram now (OBS-1b).
std::uint64_t hist_count(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().count;
}
double hist_sum(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().sum;
}

std::int64_t gauge_value(const std::string& name) {
    auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.gauges) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

}  // namespace

TEST(ConnectorMetrics, NamesAreTaggedByConnectorAndDirection) {
    const auto name_src = clink::metrics::connector_metric_name("records_total", "kafka", "source");
    const auto name_snk = clink::metrics::connector_metric_name("records_total", "kafka", "sink");
    EXPECT_NE(name_src, name_snk);
    EXPECT_NE(name_src.find("connector=\"kafka\""), std::string::npos);
    EXPECT_NE(name_src.find("direction=\"source\""), std::string::npos);
    EXPECT_NE(name_snk.find("direction=\"sink\""), std::string::npos);
}

TEST(ConnectorMetrics, RecordsAndBytesAccumulateByDirection) {
    using namespace clink::metrics;
    const auto src_records = connector_metric_name("records_total", "kafka", "source");
    const auto snk_records = connector_metric_name("records_total", "kafka", "sink");
    const auto src_bytes = connector_metric_name("bytes_total", "kafka", "source");
    const auto snk_bytes = connector_metric_name("bytes_total", "kafka", "sink");

    const auto sr0 = counter_value(src_records);
    const auto skr0 = counter_value(snk_records);
    const auto sb0 = counter_value(src_bytes);
    const auto skb0 = counter_value(snk_bytes);

    connector::records_in_inc("kafka", 3);
    connector::records_out_inc("kafka", 7);
    connector::bytes_in_inc("kafka", 1024);
    connector::bytes_out_inc("kafka", 4096);

    EXPECT_EQ(counter_value(src_records) - sr0, 3u);
    EXPECT_EQ(counter_value(snk_records) - skr0, 7u);
    EXPECT_EQ(counter_value(src_bytes) - sb0, 1024u);
    EXPECT_EQ(counter_value(snk_bytes) - skb0, 4096u);
}

TEST(ConnectorMetrics, ErrorsAndCommitLatencyAccumulate) {
    using namespace clink::metrics;
    const auto err = connector_metric_name("errors_total", "postgres", "sink");
    const auto lat = connector_metric_name("commit_latency_ns", "postgres", "sink");

    const auto e0 = counter_value(err);
    const auto ls0 = hist_sum(lat);
    const auto lc0 = hist_count(lat);

    connector::error_inc("postgres");
    connector::commit_latency_observe("postgres", 1500);
    connector::commit_latency_observe("postgres", 2500);

    EXPECT_EQ(counter_value(err) - e0, 1u);
    EXPECT_EQ(hist_sum(lat) - ls0, 4000.0);
    EXPECT_EQ(hist_count(lat) - lc0, 2u);
}

TEST(ConnectorMetrics, ConsumerLagSetsSourceGauge) {
    using namespace clink::metrics;
    const auto lag = connector_metric_name("consumer_lag", "postgres_cdc", "source");

    connector::consumer_lag_set("postgres_cdc", 12345);
    EXPECT_EQ(gauge_value(lag), 12345);

    connector::consumer_lag_set("postgres_cdc", 0);
    EXPECT_EQ(gauge_value(lag), 0);
}
