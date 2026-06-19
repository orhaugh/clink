// OBS-3: disaggregated-state observability. Exercises the disagg metric emit
// helpers and confirms RemoteReadBackend reports hot hits and remote-load
// latency through them. (LocalObjectCache + CAS-store wiring is covered in the
// s3 test suite.)

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "clink/metrics/disagg_metrics.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/state/remote_read_backend.hpp"

using namespace clink;

namespace {

std::uint64_t counter_value(const std::string& name) {
    for (const auto& [n, v] : MetricsRegistry::global().snapshot().counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

std::int64_t gauge_value(const std::string& name) {
    for (const auto& [n, v] : MetricsRegistry::global().snapshot().gauges) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

std::uint64_t hist_count(const std::string& base) {
    return MetricsRegistry::global().histogram(base).snapshot().count;
}

std::string_view sv(const std::string& s) {
    return std::string_view{s};
}

}  // namespace

TEST(DisaggMetrics, EmitHelpersUpdateRegistry) {
    using namespace clink::metrics;
    const auto hits0 = counter_value(kObjectCacheHits);
    const auto miss0 = counter_value(kObjectCacheMisses);
    const auto loads0 = counter_value(kRemoteLoads);
    const auto lat0 = hist_count(kRemoteLoadLatencyNs);

    disagg::object_cache_hit();
    disagg::object_cache_hit();
    disagg::object_cache_miss();
    disagg::object_cache_entries_set(42);
    disagg::remote_load_observe(1234);
    disagg::checkpoint_written(7, 4096);

    EXPECT_EQ(counter_value(kObjectCacheHits) - hits0, 2u);
    EXPECT_EQ(counter_value(kObjectCacheMisses) - miss0, 1u);
    EXPECT_EQ(gauge_value(kObjectCacheEntries), 42);
    EXPECT_EQ(counter_value(kRemoteLoads) - loads0, 1u);
    EXPECT_EQ(hist_count(kRemoteLoadLatencyNs) - lat0, 1u);
    EXPECT_EQ(gauge_value(kCheckpointObjects), 7);
    EXPECT_EQ(gauge_value(kCheckpointObjectBytes), 4096);
}

TEST(DisaggMetrics, RemoteReadBackendReportsHotHitsAndLoads) {
    using namespace clink::metrics;
    const auto hot0 = counter_value(kRemoteHotHits);
    const auto loads0 = counter_value(kRemoteLoads);
    const auto lat0 = hist_count(kRemoteLoadLatencyNs);

    RemoteReadBackend backend([](OperatorId, std::string k) -> std::optional<StateBackend::Value> {
        const std::string v = "remote-" + k;
        return StateBackend::Value(reinterpret_cast<const std::byte*>(v.data()),
                                   reinterpret_cast<const std::byte*>(v.data()) + v.size());
    });

    // Cold read: a remote load (counter + latency histogram observation).
    auto v = backend.get(OperatorId{1}, sv("x"));
    ASSERT_TRUE(v.has_value());
    EXPECT_EQ(counter_value(kRemoteLoads) - loads0, 1u);
    EXPECT_EQ(hist_count(kRemoteLoadLatencyNs) - lat0, 1u);

    // Second read of the same key: served hot, no new remote load.
    auto v2 = backend.get(OperatorId{1}, sv("x"));
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(counter_value(kRemoteHotHits) - hot0, 1u);
    EXPECT_EQ(counter_value(kRemoteLoads) - loads0, 1u);  // unchanged
}
