#include "clink/metrics/prometheus.hpp"

#include <algorithm>
#include <cmath>
#include <sstream>
#include <string>
#include <unordered_set>

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

    // A metric may carry inlined labels in its name (e.g.
    // `clink_disk_total_bytes{volume="checkpoint"}`); the # TYPE line must name
    // the bare metric without labels, and be emitted once per base name even
    // when several label sets share it.
    const auto base_name = [](const std::string& n) {
        const auto pos = n.find('{');
        return pos == std::string::npos ? n : n.substr(0, pos);
    };
    std::unordered_set<std::string> typed;

    std::ostringstream out;
    for (const auto& [name, value] : counters) {
        if (typed.insert(base_name(name)).second) {
            out << "# TYPE " << base_name(name) << " counter\n";
        }
        out << name << ' ' << value << '\n';
    }
    for (const auto& [name, value] : gauges) {
        if (typed.insert(base_name(name)).second) {
            out << "# TYPE " << base_name(name) << " gauge\n";
        }
        out << name << ' ' << value << '\n';
    }
    for (const auto& h : histograms) {
        // Prometheus histogram: cumulative _bucket{le} lines (ascending), a
        // closing +Inf bucket equal to the total count, then _sum and _count.
        // The suffixes go on the base name and `le` joins the inlined label
        // set: `name{op_id="7"}` renders `name_bucket{op_id="7",le="..."}`,
        // `name_sum{op_id="7"}` and `name_count{op_id="7"}`. Appending the
        // suffix to the whole name put it after the labels, which is not valid
        // exposition, and a scraper dropped every labelled histogram.
        const std::string base = base_name(h.name);
        const auto brace = h.name.find('{');
        const std::string labels = brace == std::string::npos
                                       ? std::string{}
                                       : h.name.substr(brace + 1, h.name.size() - brace - 2);
        const std::string with_labels = labels.empty() ? std::string{} : "{" + labels + "}";
        const std::string le_prefix = labels.empty() ? std::string{"{"} : "{" + labels + ",";
        if (typed.insert(base).second) {
            out << "# TYPE " << base << " histogram\n";
        }
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < h.data.upper_bounds.size(); ++i) {
            cumulative += (i < h.data.bucket_counts.size()) ? h.data.bucket_counts[i] : 0;
            out << base << "_bucket" << le_prefix << "le=\""
                << format_double(h.data.upper_bounds[i]) << "\"} " << cumulative << '\n';
        }
        out << base << "_bucket" << le_prefix << "le=\"+Inf\"} " << h.data.count << '\n';
        out << base << "_sum" << with_labels << ' ' << format_double(h.data.sum) << '\n';
        out << base << "_count" << with_labels << ' ' << h.data.count << '\n';
    }
    return out.str();
}

}  // namespace clink::metrics
