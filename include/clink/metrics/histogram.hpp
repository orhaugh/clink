#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace clink {

// Fixed-bucket histogram with Prometheus "le" (less-than-or-equal) semantics
// (OBS-1).
//
// A counter/gauge pair can only ever give you a mean; a histogram gives you the
// distribution - p50/p95/p99 tails, which is what makes the no-JVM latency story
// visible (no GC noise in the high quantiles). observe() is lock-free: each
// bucket count, the running sum, and the total count are atomics. Bucket upper
// bounds are supplied at construction (must be ascending and finite); an
// implicit +Inf bucket at the end catches everything larger, exactly like a
// Prometheus histogram.
class Histogram {
public:
    explicit Histogram(std::vector<double> upper_bounds)
        : upper_bounds_(std::move(upper_bounds)), buckets_(upper_bounds_.size() + 1) {}

    Histogram(const Histogram&) = delete;
    Histogram& operator=(const Histogram&) = delete;
    Histogram(Histogram&&) = delete;
    Histogram& operator=(Histogram&&) = delete;

    // Record one observation. Finds the first bucket whose upper bound is >=
    // value (the +Inf bucket if none), and bumps that bucket, the count, and
    // the running sum. A linear scan is fine for the small bucket counts
    // (~10-15) histograms use in practice.
    void observe(double value) noexcept {
        std::size_t idx = upper_bounds_.size();  // default: the +Inf bucket
        for (std::size_t i = 0; i < upper_bounds_.size(); ++i) {
            if (value <= upper_bounds_[i]) {
                idx = i;
                break;
            }
        }
        buckets_[idx].fetch_add(1, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(value, std::memory_order_relaxed);
    }

    struct Snapshot {
        // Finite le boundaries, ascending. The implicit +Inf bucket is not
        // listed here but is the last entry of bucket_counts.
        std::vector<double> upper_bounds;
        // Per-bucket (NON-cumulative) counts; size == upper_bounds.size() + 1,
        // the last element being the +Inf overflow bucket.
        std::vector<std::uint64_t> bucket_counts;
        double sum{0.0};
        std::uint64_t count{0};

        // Quantile in [0,1] by Prometheus-style linear interpolation within the
        // bucket that contains it. Returns 0 if no observations.
        [[nodiscard]] double quantile(double q) const;
    };

    [[nodiscard]] Snapshot snapshot() const {
        Snapshot s;
        s.upper_bounds = upper_bounds_;
        s.bucket_counts.reserve(buckets_.size());
        for (const auto& b : buckets_) {
            s.bucket_counts.push_back(b.load(std::memory_order_relaxed));
        }
        s.sum = sum_.load(std::memory_order_relaxed);
        s.count = count_.load(std::memory_order_relaxed);
        return s;
    }

    [[nodiscard]] double quantile(double q) const { return snapshot().quantile(q); }

    [[nodiscard]] const std::vector<double>& upper_bounds() const noexcept { return upper_bounds_; }

private:
    std::vector<double> upper_bounds_;
    std::vector<std::atomic<std::uint64_t>> buckets_;  // size == upper_bounds_ + 1
    std::atomic<std::uint64_t> count_{0};
    std::atomic<double> sum_{0.0};  // C++20 atomic<double>::fetch_add
};

// Default bucket boundaries for latency histograms, in nanoseconds: 1us .. 1s
// on a roughly 1-5-10 ladder. Good resolution across the range a stream
// operator's per-call and per-checkpoint latencies actually span.
inline std::vector<double> default_latency_buckets_ns() {
    return {1e3, 5e3, 1e4, 5e4, 1e5, 5e5, 1e6, 5e6, 1e7, 5e7, 1e8, 5e8, 1e9};
}

// Out-of-line so the header stays light; defined here as inline to keep this a
// header-only metric primitive like Counter/Gauge.
inline double Histogram::Snapshot::quantile(double q) const {
    if (count == 0) {
        return 0.0;
    }
    if (q <= 0.0) {
        return 0.0;
    }
    if (q >= 1.0) {
        q = 1.0;
    }
    const double rank = q * static_cast<double>(count);
    std::uint64_t cumulative = 0;
    for (std::size_t i = 0; i < bucket_counts.size(); ++i) {
        const std::uint64_t prev_cumulative = cumulative;
        cumulative += bucket_counts[i];
        if (static_cast<double>(cumulative) < rank) {
            continue;
        }
        // The target rank falls in bucket i. Interpolate linearly between this
        // bucket's lower and upper bound by how far into the bucket the rank is.
        const double lower = (i == 0) ? 0.0 : upper_bounds[i - 1];
        if (i >= upper_bounds.size()) {
            // +Inf overflow bucket: no finite upper bound to interpolate to;
            // report the largest finite boundary (the conventional clamp).
            return upper_bounds.empty() ? lower : upper_bounds.back();
        }
        const double upper = upper_bounds[i];
        const double in_bucket = static_cast<double>(bucket_counts[i]);
        if (in_bucket <= 0.0) {
            return upper;
        }
        const double frac = (rank - static_cast<double>(prev_cumulative)) / in_bucket;
        return lower + (upper - lower) * frac;
    }
    return upper_bounds.empty() ? 0.0 : upper_bounds.back();
}

}  // namespace clink
