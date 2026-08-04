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

// --- last_snapshot_bytes: the number a per-job state size is built from ---
//
// Every backend already computed this and threw it away. These pin that it is
// reported, that it TRACKS state rather than being a constant, and that a backend
// which has never snapshotted says so instead of saying zero - because for a
// capacity number, "unmeasured" reading as "empty" is the wrong way to be wrong.

TEST(StateSize, ABackendThatHasNeverSnapshottedReportsNothingRatherThanZero) {
    InMemoryStateBackend be;
    EXPECT_FALSE(be.last_snapshot_bytes().has_value())
        << "a backend with no snapshot yet reported a size, so an unmeasured job would be "
           "indistinguishable from an empty one";

    be.put(OperatorId{1}, "k", "v");
    EXPECT_FALSE(be.last_snapshot_bytes().has_value())
        << "state was written but no snapshot taken; the reported size is the SNAPSHOT size, so "
           "it must still be absent";
}

TEST(StateSize, TheReportedSizeGrowsWithTheStateItSnapshots) {
    InMemoryStateBackend be;
    be.put(OperatorId{1}, "k1", "v1");
    (void)be.snapshot(CheckpointId{1});
    const auto small = be.last_snapshot_bytes();
    ASSERT_TRUE(small.has_value());
    EXPECT_GT(*small, 0u);

    // An order of magnitude more state must produce a materially larger figure.
    // Asserting only "non-zero" would pass on a constant, which is the failure
    // mode that matters here: a size metric that never moves is worse than none,
    // because a capacity trend drawn from it looks flat.
    for (int i = 0; i < 200; ++i) {
        be.put(OperatorId{1}, "key-" + std::to_string(i), std::string(64, 'x'));
    }
    (void)be.snapshot(CheckpointId{2});
    const auto large = be.last_snapshot_bytes();
    ASSERT_TRUE(large.has_value());
    EXPECT_GT(*large, *small * 2)
        << "200 extra keys of 64 bytes did not at least double the reported snapshot size ("
        << *small << " -> " << *large << "), so the figure does not track the state";
}

TEST(StateSize, FileBackedReportsItsSnapshotSizeToo) {
    // The durable backend is the one a capacity question is usually about, and it
    // reaches persist() through a different path (capture-then-persist) from
    // InMemory's snapshot().
    const auto dir =
        std::filesystem::temp_directory_path() / ("clink_state_size_" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    {
        FileBackedStateBackend be(dir);
        EXPECT_FALSE(be.last_snapshot_bytes().has_value());
        be.put(OperatorId{7}, "k", std::string(128, 'z'));
        (void)be.snapshot(CheckpointId{1});
        const auto bytes = be.last_snapshot_bytes();
        ASSERT_TRUE(bytes.has_value())
            << "the file-backed backend did not report a snapshot size, so a durable job's state "
               "size would be unmeasured";
        EXPECT_GT(*bytes, 128u) << "the reported size is below the value it stored";
    }
    std::filesystem::remove_all(dir);
}
