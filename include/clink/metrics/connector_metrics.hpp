#pragma once

// Connector (source/sink) observability.
//
// Coverage:
//   - clink_connector_records_in_total / out_total
//   - clink_connector_bytes_in_total  / out_total
//   - clink_connector_errors_total
//   - clink_connector_commit_latency_ns_sum / count
//   - clink_connector_consumer_lag (gauge, source-side only)
//
// Tagged by connector name + direction so a process running both a
// Kafka source and a Kafka sink shows up as two distinct entries.
// Source connectors typically populate the `in_*` counters + the
// consumer_lag gauge; sink connectors populate the `out_*` +
// commit_latency counters. Both populate errors_total.
//
// The registry has no first-class tags so the tag is inlined in the
// metric name in Prometheus-compatible `{key="value"}` form. Tools
// scraping /metrics see e.g.
//
//   clink_connector_records_out_total{connector="kafka",direction="sink"} 12345
//
// Helpers below pick the right metric name and direction so callers
// only have to know their connector identity.

#include <cstdint>
#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

inline std::string connector_metric_name(const char* metric,
                                         const std::string& connector,
                                         const char* direction) {
    std::string out = "clink_connector_";
    out += metric;
    out += "{connector=\"";
    out += connector;
    out += "\",direction=\"";
    out += direction;
    out += "\"}";
    return out;
}

namespace connector {

inline void records_in_inc(const std::string& connector, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(connector_metric_name("records_total", connector, "source"))
        .increment(n);
}
inline void records_out_inc(const std::string& connector, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(connector_metric_name("records_total", connector, "sink"))
        .increment(n);
}
inline void bytes_in_inc(const std::string& connector, std::uint64_t n) {
    MetricsRegistry::global()
        .counter(connector_metric_name("bytes_total", connector, "source"))
        .increment(n);
}
inline void bytes_out_inc(const std::string& connector, std::uint64_t n) {
    MetricsRegistry::global()
        .counter(connector_metric_name("bytes_total", connector, "sink"))
        .increment(n);
}
inline void error_inc(const std::string& connector, const char* direction = "sink") {
    MetricsRegistry::global()
        .counter(connector_metric_name("errors_total", connector, direction))
        .increment();
}
// Sink commit latency histogram (OBS-1b). Keeps the commit_latency_ns_sum /
// _count exposition names, adds _bucket for p50/p95/p99.
inline void commit_latency_observe(const std::string& connector, std::uint64_t duration_ns) {
    MetricsRegistry::global()
        .histogram(connector_metric_name("commit_latency_ns", connector, "sink"))
        .observe(static_cast<double>(duration_ns));
}
inline void consumer_lag_set(const std::string& connector, std::int64_t lag) {
    MetricsRegistry::global()
        .gauge(connector_metric_name("consumer_lag", connector, "source"))
        .set(lag);
}
// Records a sink routed to the dead-letter path (dropped after a PERMANENT
// failure under DlqPolicy::Drop) rather than retried. dropped_records counts the
// records; permanent_failures counts the drop events (batches/items).
inline void dropped_records_inc(const std::string& connector, std::uint64_t n = 1) {
    MetricsRegistry::global()
        .counter(connector_metric_name("dropped_records_total", connector, "sink"))
        .increment(n);
}
inline void permanent_failures_inc(const std::string& connector) {
    MetricsRegistry::global()
        .counter(connector_metric_name("permanent_failures_total", connector, "sink"))
        .increment();
}

}  // namespace connector

}  // namespace clink::metrics
