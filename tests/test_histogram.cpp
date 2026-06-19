// OBS-1: Histogram metric primitive + registry integration + Prometheus
// histogram exposition.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/histogram.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/prometheus.hpp"

using namespace clink;

namespace {

bool contains(const std::string& haystack, const std::string& needle) {
    return haystack.find(needle) != std::string::npos;
}

}  // namespace

TEST(Histogram, BucketsSumAndCount) {
    Histogram h({10.0, 20.0, 30.0});  // 3 finite buckets + an +Inf bucket
    h.observe(5);                     // bucket 0 (<=10)
    h.observe(10);                    // bucket 0 (<=10)
    h.observe(15);                    // bucket 1 (<=20)
    h.observe(25);                    // bucket 2 (<=30)
    h.observe(100);                   // +Inf

    const auto s = h.snapshot();
    ASSERT_EQ(s.bucket_counts.size(), 4u);
    EXPECT_EQ(s.bucket_counts[0], 2u);
    EXPECT_EQ(s.bucket_counts[1], 1u);
    EXPECT_EQ(s.bucket_counts[2], 1u);
    EXPECT_EQ(s.bucket_counts[3], 1u);  // +Inf
    EXPECT_EQ(s.count, 5u);
    EXPECT_DOUBLE_EQ(s.sum, 155.0);
}

TEST(Histogram, QuantileInterpolatesWithinBucket) {
    // 100 observations spread one-per-integer across [0,100) using fine buckets.
    Histogram h({25.0, 50.0, 75.0, 100.0});
    for (int i = 0; i < 100; ++i) {
        h.observe(static_cast<double>(i));
    }
    const auto s = h.snapshot();
    // Each finite bucket holds 25 observations; medians land mid-bucket.
    EXPECT_NEAR(s.quantile(0.5), 50.0, 1.0);
    EXPECT_NEAR(s.quantile(0.95), 95.0, 2.0);
    EXPECT_NEAR(s.quantile(0.99), 99.0, 2.0);
}

TEST(Histogram, QuantileEmptyIsZero) {
    Histogram h(default_latency_buckets_ns());
    EXPECT_DOUBLE_EQ(h.quantile(0.5), 0.0);
}

TEST(Histogram, OverflowBucketQuantileClampsToLargestBound) {
    Histogram h({10.0, 20.0});
    h.observe(5);
    h.observe(1000);  // +Inf
    h.observe(2000);  // +Inf
    // p99 lands in the +Inf bucket; clamp to the largest finite bound (20).
    EXPECT_DOUBLE_EQ(h.quantile(0.99), 20.0);
}

TEST(Histogram, RegistryLazyCreateAndSnapshot) {
    MetricsRegistry reg;
    reg.histogram("clink_test_latency_ns").observe(1500);    // bucket "5000"
    reg.histogram("clink_test_latency_ns").observe(750000);  // bucket "1000000"

    const auto snap = reg.snapshot();
    ASSERT_EQ(snap.histograms.size(), 1u);
    EXPECT_EQ(snap.histograms[0].name, "clink_test_latency_ns");
    EXPECT_EQ(snap.histograms[0].data.count, 2u);
    EXPECT_DOUBLE_EQ(snap.histograms[0].data.sum, 751500.0);
}

TEST(Histogram, RegistryCustomBoundsFixedAtFirstCreate) {
    MetricsRegistry reg;
    reg.histogram("h", {1.0, 2.0, 3.0}).observe(2.5);
    // Second call with different bounds returns the existing histogram.
    auto& again = reg.histogram("h", {100.0});
    again.observe(2.5);
    EXPECT_EQ(again.upper_bounds().size(), 3u);  // original bounds kept
    EXPECT_EQ(again.snapshot().count, 2u);
}

TEST(Histogram, PrometheusExposition) {
    MetricsRegistry reg;
    auto& h = reg.histogram("clink_op_latency_ns", {1000.0, 1000000.0});
    h.observe(500);       // <=1000
    h.observe(2000);      // <=1000000
    h.observe(50000000);  // +Inf

    const auto body = metrics::render_prometheus(reg.snapshot());
    EXPECT_TRUE(contains(body, "# TYPE clink_op_latency_ns histogram"));
    // Cumulative buckets: le=1000 -> 1, le=1000000 -> 2, le=+Inf -> 3.
    EXPECT_TRUE(contains(body, "clink_op_latency_ns_bucket{le=\"1000\"} 1"));
    EXPECT_TRUE(contains(body, "clink_op_latency_ns_bucket{le=\"1000000\"} 2"));
    EXPECT_TRUE(contains(body, "clink_op_latency_ns_bucket{le=\"+Inf\"} 3"));
    EXPECT_TRUE(contains(body, "clink_op_latency_ns_count 3"));
    EXPECT_TRUE(contains(body, "clink_op_latency_ns_sum 50002500"));
}

TEST(Histogram, DefaultLatencyBucketsAscending) {
    const auto b = default_latency_buckets_ns();
    ASSERT_FALSE(b.empty());
    for (std::size_t i = 1; i < b.size(); ++i) {
        EXPECT_LT(b[i - 1], b[i]) << "buckets must be strictly ascending at " << i;
    }
}
