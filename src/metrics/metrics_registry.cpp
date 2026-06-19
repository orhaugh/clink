#include "clink/metrics/metrics_registry.hpp"

#include <utility>

namespace clink {

Counter& MetricsRegistry::counter(const std::string& name) {
    std::lock_guard lock(mu_);
    auto it = counters_.find(name);
    if (it == counters_.end()) {
        it = counters_.emplace(name, std::make_unique<Counter>()).first;
    }
    return *it->second;
}

Gauge& MetricsRegistry::gauge(const std::string& name) {
    std::lock_guard lock(mu_);
    auto it = gauges_.find(name);
    if (it == gauges_.end()) {
        it = gauges_.emplace(name, std::make_unique<Gauge>()).first;
    }
    return *it->second;
}

Histogram& MetricsRegistry::histogram(const std::string& name) {
    return histogram(name, default_latency_buckets_ns());
}

Histogram& MetricsRegistry::histogram(const std::string& name, std::vector<double> upper_bounds) {
    std::lock_guard lock(mu_);
    auto it = histograms_.find(name);
    if (it == histograms_.end()) {
        it = histograms_.emplace(name, std::make_unique<Histogram>(std::move(upper_bounds))).first;
    }
    return *it->second;
}

MetricsRegistry::Snapshot MetricsRegistry::snapshot() const {
    std::lock_guard lock(mu_);
    Snapshot snap;
    snap.counters.reserve(counters_.size());
    snap.gauges.reserve(gauges_.size());
    snap.histograms.reserve(histograms_.size());
    for (const auto& [name, c] : counters_) {
        snap.counters.emplace_back(name, c->value());
    }
    for (const auto& [name, g] : gauges_) {
        snap.gauges.emplace_back(name, g->value());
    }
    for (const auto& [name, h] : histograms_) {
        snap.histograms.push_back(HistogramEntry{name, h->snapshot()});
    }
    return snap;
}

MetricsRegistry& MetricsRegistry::global() {
    static MetricsRegistry instance;
    return instance;
}

}  // namespace clink
