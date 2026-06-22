// Tests for the system-metrics sampler + the labeled-metric Prometheus
// exposition (one # TYPE line per base name, even with multiple label sets).

#include <optional>
#include <string>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/prometheus.hpp"
#include "clink/metrics/system_metrics.hpp"

namespace {

std::size_t count_substr(const std::string& hay, const std::string& needle) {
    std::size_t n = 0;
    std::size_t pos = 0;
    while ((pos = hay.find(needle, pos)) != std::string::npos) {
        ++n;
        pos += needle.size();
    }
    return n;
}

}  // namespace

// The exposition must emit a single "# TYPE <base> ..." line for a metric even
// when several label sets share the base name, and render every labeled sample.
TEST(SystemMetrics, RenderEmitsOneTypeLinePerLabeledBase) {
    clink::MetricsRegistry reg;
    reg.gauge(R"(clink_disk_total_bytes{volume="workdir"})").set(1000);
    reg.gauge(R"(clink_disk_total_bytes{volume="checkpoint"})").set(2000);

    const auto body = clink::metrics::render_prometheus(reg.snapshot());

    EXPECT_EQ(count_substr(body, "# TYPE clink_disk_total_bytes gauge"), 1U);
    EXPECT_NE(body.find(R"(clink_disk_total_bytes{volume="workdir"} 1000)"), std::string::npos);
    EXPECT_NE(body.find(R"(clink_disk_total_bytes{volume="checkpoint"} 2000)"), std::string::npos);
}

// Volumes that resolve to the same filesystem are reported once.
TEST(SystemMetrics, SameDeviceVolumesAreDeduplicated) {
    clink::metrics::configure_disk_volumes({{"workdir", "."}, {"checkpoint", "."}});
    clink::metrics::sample_system_metrics();
    const auto snap = clink::MetricsRegistry::global().snapshot();

    std::size_t disk_total_series = 0;
    for (const auto& [name, value] : snap.gauges) {
        (void)value;
        if (name.rfind("clink_disk_total_bytes", 0) == 0) {
            ++disk_total_series;
        }
    }
    // Both volumes point at the same filesystem, so only one survives.
    EXPECT_EQ(disk_total_series, 1U);
}

// The sampler populates the process + disk gauges with live values.
TEST(SystemMetrics, SamplerPopulatesProcessAndDiskGauges) {
    clink::metrics::configure_disk_volumes({{"workdir", "."}});
    clink::metrics::sample_system_metrics();
    const auto snap = clink::MetricsRegistry::global().snapshot();

    const auto find_gauge = [&](std::string_view needle) -> std::optional<std::int64_t> {
        for (const auto& [name, value] : snap.gauges) {
            if (name.find(needle) != std::string::npos) {
                return value;
            }
        }
        return std::nullopt;
    };

    const auto rss = find_gauge("clink_process_resident_memory_bytes");
    ASSERT_TRUE(rss.has_value());
    EXPECT_GT(*rss, 0);

    const auto disk = find_gauge(R"(clink_disk_total_bytes{volume="workdir"})");
    ASSERT_TRUE(disk.has_value());
    EXPECT_GT(*disk, 0);
}
