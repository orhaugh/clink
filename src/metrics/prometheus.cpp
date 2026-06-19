#include "clink/metrics/prometheus.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>

namespace clink::metrics {

namespace {

template <typename Pair>
void sort_by_name(std::vector<Pair>& v) {
    std::sort(v.begin(), v.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
}

// Render a double for a Prometheus value / le label: integral values print as
// plain integers (the common case for ns boundaries and sums), otherwise fall
// back to the default float formatting (Prometheus accepts scientific notation).
std::string format_double(double v) {
    if (std::isfinite(v) && v == std::floor(v) && std::abs(v) < 1e18) {
        std::ostringstream o;
        o << static_cast<long long>(v);
        return o.str();
    }
    std::ostringstream o;
    o << v;
    return o.str();
}

}  // namespace

std::string render_prometheus(const MetricsRegistry::Snapshot& snap) {
    // Sort to give a stable line order. Prometheus doesn't require it, but it
    // makes diffing /metrics output between two runs (or in tests) sane.
    auto counters = snap.counters;
    auto gauges = snap.gauges;
    auto histograms = snap.histograms;
    sort_by_name(counters);
    sort_by_name(gauges);
    std::sort(histograms.begin(), histograms.end(), [](const auto& a, const auto& b) {
        return a.name < b.name;
    });

    std::ostringstream out;
    for (const auto& [name, value] : counters) {
        out << "# TYPE " << name << " counter\n" << name << ' ' << value << '\n';
    }
    for (const auto& [name, value] : gauges) {
        out << "# TYPE " << name << " gauge\n" << name << ' ' << value << '\n';
    }
    for (const auto& h : histograms) {
        // Prometheus histogram: cumulative _bucket{le} lines (ascending), a
        // closing +Inf bucket equal to the total count, then _sum and _count.
        out << "# TYPE " << h.name << " histogram\n";
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < h.data.upper_bounds.size(); ++i) {
            cumulative += (i < h.data.bucket_counts.size()) ? h.data.bucket_counts[i] : 0;
            out << h.name << "_bucket{le=\"" << format_double(h.data.upper_bounds[i]) << "\"} "
                << cumulative << '\n';
        }
        out << h.name << "_bucket{le=\"+Inf\"} " << h.data.count << '\n';
        out << h.name << "_sum " << format_double(h.data.sum) << '\n';
        out << h.name << "_count " << h.data.count << '\n';
    }
    return out.str();
}

}  // namespace clink::metrics
