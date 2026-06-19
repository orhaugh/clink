// State-backend metrics: snapshot/restore counters and duration sums.

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/state_metrics.hpp"
#include "clink/state/file_backed_state_backend.hpp"
#include "clink/state/in_memory_state_backend.hpp"

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

// snapshot/restore durations are histograms now (OBS-1b).
std::uint64_t hist_count(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().count;
}

}  // namespace

TEST(StateMetrics, InMemorySnapshotIncrementsCounters) {
    InMemoryStateBackend be;
    be.put(OperatorId{42}, "k1", "v1");
    be.put(OperatorId{42}, "k2", "v22");

    const auto total_before =
        counter_value(clink::metrics::state_metric_name("snapshot_total", "in_memory"));
    const auto bytes_before =
        counter_value(clink::metrics::state_metric_name("snapshot_bytes_sum", "in_memory"));
    const auto dur_count_before =
        hist_count(clink::metrics::state_metric_name("snapshot_duration_ns", "in_memory"));

    auto snap = be.snapshot(CheckpointId{1});
    EXPECT_FALSE(snap.bytes.empty());

    EXPECT_EQ(counter_value(clink::metrics::state_metric_name("snapshot_total", "in_memory")) -
                  total_before,
              1u);
    EXPECT_GT(counter_value(clink::metrics::state_metric_name("snapshot_bytes_sum", "in_memory")) -
                  bytes_before,
              0u);
    EXPECT_EQ(hist_count(clink::metrics::state_metric_name("snapshot_duration_ns", "in_memory")) -
                  dur_count_before,
              1u);
}

TEST(StateMetrics, InMemoryRestoreIncrementsCounters) {
    InMemoryStateBackend src;
    src.put(OperatorId{1}, "k", "v");
    auto snap = src.snapshot(CheckpointId{1});

    InMemoryStateBackend dst;
    const auto total_before =
        counter_value(clink::metrics::state_metric_name("restore_total", "in_memory"));
    const auto dur_count_before =
        hist_count(clink::metrics::state_metric_name("restore_duration_ns", "in_memory"));

    dst.restore(snap);

    EXPECT_EQ(counter_value(clink::metrics::state_metric_name("restore_total", "in_memory")) -
                  total_before,
              1u);
    EXPECT_EQ(hist_count(clink::metrics::state_metric_name("restore_duration_ns", "in_memory")) -
                  dur_count_before,
              1u);
}

TEST(StateMetrics, FileBackedTagsBackendDistinctly) {
    const auto dir = std::filesystem::temp_directory_path() / "clink_state_metrics_filebacked_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    FileBackedStateBackend be(dir.string());
    be.put(OperatorId{1}, "a", "b");

    const auto fb_before =
        counter_value(clink::metrics::state_metric_name("snapshot_total", "file_backed"));
    const auto im_before =
        counter_value(clink::metrics::state_metric_name("snapshot_total", "in_memory"));

    auto snap = be.snapshot(CheckpointId{7});

    EXPECT_EQ(counter_value(clink::metrics::state_metric_name("snapshot_total", "file_backed")) -
                  fb_before,
              1u);
    EXPECT_EQ(
        counter_value(clink::metrics::state_metric_name("snapshot_total", "in_memory")) - im_before,
        1u);  // inner backend also fired

    std::filesystem::remove_all(dir);
}
