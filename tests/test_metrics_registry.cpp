// Unit tests for the metrics layer (Counter, Gauge, MetricsRegistry,
// OtelBoundary). These types are simple but every operator and runtime
// path eventually wants to publish a metric, so a regression here would
// silently break observability in production.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/metrics/counter.hpp"
#include "clink/metrics/gauge.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/otel_boundary.hpp"

using namespace clink;
using namespace std::chrono_literals;

// ----- Counter -----

TEST(Counter, IncrementsByOneByDefault) {
    Counter c;
    c.increment();
    c.increment();
    c.increment();
    EXPECT_EQ(c.value(), 3u);
}

TEST(Counter, IncrementsByCustomAmount) {
    Counter c;
    c.increment(100);
    c.increment(7);
    EXPECT_EQ(c.value(), 107u);
}

TEST(Counter, IsThreadSafe) {
    // 8 threads each increment 10000 times; final must equal exactly
    // 80000 - proves the atomic increment isn't lossy under contention.
    Counter c;
    std::vector<std::thread> threads;
    threads.reserve(8);
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&c] {
            for (int j = 0; j < 10000; ++j) {
                c.increment();
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(c.value(), 80000u);
}

// ----- Gauge -----

TEST(Gauge, SetAddSub) {
    Gauge g;
    g.set(100);
    EXPECT_EQ(g.value(), 100);
    g.add(25);
    EXPECT_EQ(g.value(), 125);
    g.sub(50);
    EXPECT_EQ(g.value(), 75);
}

TEST(Gauge, AcceptsNegativeValues) {
    Gauge g;
    g.set(-1);
    EXPECT_EQ(g.value(), -1);
    g.sub(99);
    EXPECT_EQ(g.value(), -100);
}

TEST(Gauge, AddIsThreadSafe) {
    Gauge g;
    std::vector<std::thread> threads;
    threads.reserve(4);
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&g] {
            for (int j = 0; j < 10000; ++j) {
                g.add(1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(g.value(), 40000);
}

// ----- MetricsRegistry -----

TEST(MetricsRegistry, IdempotentLookupReturnsSameInstance) {
    MetricsRegistry reg;
    Counter& a = reg.counter("requests");
    Counter& b = reg.counter("requests");
    EXPECT_EQ(&a, &b);

    a.increment(5);
    EXPECT_EQ(b.value(), 5u);
}

TEST(MetricsRegistry, CountersAndGaugesAreSeparateNamespaces) {
    MetricsRegistry reg;
    Counter& c = reg.counter("x");
    Gauge& g = reg.gauge("x");

    c.increment(7);
    g.set(-3);

    EXPECT_EQ(c.value(), 7u);
    EXPECT_EQ(g.value(), -3);
}

TEST(MetricsRegistry, SnapshotReflectsCurrentValues) {
    MetricsRegistry reg;
    reg.counter("hits").increment(5);
    reg.counter("misses").increment(2);
    reg.gauge("queue_depth").set(42);

    auto snap = reg.snapshot();
    ASSERT_EQ(snap.counters.size(), 2u);
    ASSERT_EQ(snap.gauges.size(), 1u);

    // Order is unordered_map iteration order; tests must not rely on it.
    auto find_counter = [&](const std::string& name) -> std::uint64_t {
        for (const auto& [n, v] : snap.counters) {
            if (n == name) {
                return v;
            }
        }
        return 0;
    };
    EXPECT_EQ(find_counter("hits"), 5u);
    EXPECT_EQ(find_counter("misses"), 2u);
    EXPECT_EQ(snap.gauges[0].first, "queue_depth");
    EXPECT_EQ(snap.gauges[0].second, 42);
}

TEST(MetricsRegistry, SnapshotSeesUpdatesAfterFirstSnapshot) {
    MetricsRegistry reg;
    reg.counter("ops").increment(1);
    auto first = reg.snapshot();
    EXPECT_EQ(first.counters.front().second, 1u);

    reg.counter("ops").increment(10);
    auto second = reg.snapshot();
    EXPECT_EQ(second.counters.front().second, 11u);
}

TEST(MetricsRegistry, EmptyRegistrySnapshotIsEmpty) {
    MetricsRegistry reg;
    auto snap = reg.snapshot();
    EXPECT_TRUE(snap.counters.empty());
    EXPECT_TRUE(snap.gauges.empty());
}

TEST(MetricsRegistry, ConcurrentLookupsDoNotCorruptMap) {
    // Multiple threads each request a counter by name; verify they
    // converge on the same instance and the total increment is exact.
    MetricsRegistry reg;
    std::vector<std::thread> threads;
    threads.reserve(4);
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&reg] {
            for (int j = 0; j < 1000; ++j) {
                reg.counter("contended").increment();
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(reg.counter("contended").value(), 4000u);
}

TEST(MetricsRegistry, GlobalIsSingleton) {
    auto& a = MetricsRegistry::global();
    auto& b = MetricsRegistry::global();
    EXPECT_EQ(&a, &b);
}

// ----- OtelBoundary -----

TEST(OtelBoundary, EndpointAndIntervalRoundTrip) {
    MetricsRegistry reg;
    OtelBoundary boundary(reg);

    EXPECT_EQ(boundary.endpoint(), "");
    EXPECT_EQ(boundary.export_interval(), 10s);

    boundary.set_endpoint("https://otel.example/v1/metrics");
    boundary.set_export_interval(250ms);

    EXPECT_EQ(boundary.endpoint(), "https://otel.example/v1/metrics");
    EXPECT_EQ(boundary.export_interval(), 250ms);
}

TEST(OtelBoundary, ExportOnceCallsSinkWithCurrentSnapshot) {
    MetricsRegistry reg;
    reg.counter("x").increment(5);
    reg.gauge("y").set(-7);

    OtelBoundary boundary(reg);
    std::vector<MetricsRegistry::Snapshot> received;
    boundary.set_export_fn(
        [&received](const MetricsRegistry::Snapshot& s) { received.push_back(s); });

    boundary.export_once();
    ASSERT_EQ(received.size(), 1u);
    ASSERT_EQ(received[0].counters.size(), 1u);
    ASSERT_EQ(received[0].gauges.size(), 1u);
    EXPECT_EQ(received[0].counters[0].first, "x");
    EXPECT_EQ(received[0].counters[0].second, 5u);
    EXPECT_EQ(received[0].gauges[0].first, "y");
    EXPECT_EQ(received[0].gauges[0].second, -7);
}

TEST(OtelBoundary, ExportOnceIsNoOpWithoutSink) {
    MetricsRegistry reg;
    reg.counter("noop").increment();

    OtelBoundary boundary(reg);
    // No sink installed - export_once must be a safe no-op (no crash,
    // no side effects).
    boundary.export_once();
    EXPECT_EQ(reg.counter("noop").value(), 1u);  // unchanged by export
}

TEST(OtelBoundary, MultipleExportsReflectChangedState) {
    MetricsRegistry reg;
    reg.counter("requests").increment();

    OtelBoundary boundary(reg);
    std::vector<MetricsRegistry::Snapshot> received;
    boundary.set_export_fn(
        [&received](const MetricsRegistry::Snapshot& s) { received.push_back(s); });

    boundary.export_once();
    reg.counter("requests").increment(99);
    boundary.export_once();

    ASSERT_EQ(received.size(), 2u);
    EXPECT_EQ(received[0].counters[0].second, 1u);
    EXPECT_EQ(received[1].counters[0].second, 100u);
}
