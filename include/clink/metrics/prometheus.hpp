#pragma once

// Render a MetricsRegistry::Snapshot as a Prometheus text exposition body.
//
// See https://prometheus.io/docs/instrumenting/exposition_formats/ for the
// Content-Type used by the /metrics endpoint and the line format:
//
//   # HELP <name> <text>
//   # TYPE <name> counter|gauge|histogram
//   <name> <value>
//
// Histograms render the native Prometheus form: cumulative <name>_bucket{le}
// lines (ascending, closed by le="+Inf" = total count), then <name>_sum and
// <name>_count.
//
// We don't carry HELP strings (the metric names in process_metrics.hpp are
// self-describing enough for Prometheus / Grafana to consume directly). TYPE
// lines come from which container the metric lives in inside the snapshot.

#include <string>

#include "clink/metrics/metrics_registry.hpp"

namespace clink::metrics {

std::string render_prometheus(const MetricsRegistry::Snapshot& snap);

inline constexpr const char* kPrometheusContentType = "text/plain; version=0.0.4";

}  // namespace clink::metrics
