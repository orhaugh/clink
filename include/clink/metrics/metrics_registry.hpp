#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "clink/metrics/counter.hpp"
#include "clink/metrics/gauge.hpp"
#include "clink/metrics/histogram.hpp"

namespace clink {

// MetricsRegistry is a small in-process registry of named counters and gauges.
//
// It's intentionally not a full metrics library - its job is to give every
// engine component a single place to register and look up metrics. The OpenTel
// boundary in otel_boundary.hpp can scrape this registry on a tick.
//
// Names are flat strings for now. Tagged metrics ("operator", "stage", ...)
// can be added without breaking this interface; existing call sites would
// keep working.
class MetricsRegistry {
public:
    Counter& counter(const std::string& name);
    Gauge& gauge(const std::string& name);

    // Look up (or lazily create) a histogram. The no-bounds overload uses
    // default_latency_buckets_ns(); the bounds overload lets a caller pick its
    // own le boundaries. Bounds are fixed at first creation: a later call with
    // the same name returns the existing histogram and ignores new bounds.
    Histogram& histogram(const std::string& name);
    Histogram& histogram(const std::string& name, std::vector<double> upper_bounds);

    struct HistogramEntry {
        std::string name;
        Histogram::Snapshot data;
    };

    struct Snapshot {
        std::vector<std::pair<std::string, std::uint64_t>> counters;
        std::vector<std::pair<std::string, std::int64_t>> gauges;
        std::vector<HistogramEntry> histograms;
    };

    Snapshot snapshot() const;

    // Replace the global registry (used by tests). Otherwise prefer the global
    // accessor below.
    static MetricsRegistry& global();

private:
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::unique_ptr<Counter>> counters_;
    std::unordered_map<std::string, std::unique_ptr<Gauge>> gauges_;
    std::unordered_map<std::string, std::unique_ptr<Histogram>> histograms_;
};

}  // namespace clink
