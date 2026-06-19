// Direct tests for RuntimeContext. Most uses go through operator
// open() so accessors aren't widely exercised by other tests; these
// fill the gap.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

TEST(RuntimeContext, AccessorsReturnConstructorValues) {
    InMemoryStateBackend backend;
    MetricsRegistry metrics;
    RuntimeContext ctx(OperatorId{42}, "stage", &backend, &metrics);

    EXPECT_EQ(ctx.operator_id(), OperatorId{42});
    EXPECT_EQ(ctx.operator_name(), "stage");
    EXPECT_EQ(ctx.state_backend(), &backend);
    EXPECT_EQ(ctx.metrics(), &metrics);
    EXPECT_TRUE(ctx.has_state_backend());
}

TEST(RuntimeContext, NullBackendReportsAbsence) {
    MetricsRegistry metrics;
    RuntimeContext ctx(OperatorId{1}, "stage", nullptr, &metrics);
    EXPECT_FALSE(ctx.has_state_backend());
    EXPECT_EQ(ctx.state_backend(), nullptr);
}

TEST(RuntimeContext, KeyedStateThrowsWhenBackendMissing) {
    RuntimeContext ctx(OperatorId{1}, "stage", nullptr, nullptr);
    auto build = [&] {
        // Wrap in a lambda so the comma in the template arg list
        // doesn't confuse the EXPECT_THROW macro.
        return ctx.keyed_state<std::string, std::int64_t>("slot", string_codec(), int64_codec());
    };
    EXPECT_THROW(build(), std::runtime_error);
}

TEST(RuntimeContext, BroadcastStateThrowsWhenBackendMissing) {
    RuntimeContext ctx(OperatorId{1}, "stage", nullptr, nullptr);
    EXPECT_THROW(ctx.broadcast_state<std::int64_t>("slot", int64_codec()), std::runtime_error);
}

TEST(RuntimeContext, KeyedStateIsScopedToOperatorId) {
    InMemoryStateBackend backend;
    RuntimeContext ctx_a(OperatorId{1}, "stage", &backend, nullptr);
    RuntimeContext ctx_b(OperatorId{2}, "stage", &backend, nullptr);

    auto kv_a =
        ctx_a.keyed_state<std::string, std::int64_t>("shared", string_codec(), int64_codec());
    auto kv_b =
        ctx_b.keyed_state<std::string, std::int64_t>("shared", string_codec(), int64_codec());

    kv_a.put("x", 1);
    kv_b.put("x", 99);
    EXPECT_EQ(*kv_a.get("x"), 1);
    EXPECT_EQ(*kv_b.get("x"), 99);
}

TEST(RuntimeContext, KeyedStateWithTtlConfigPlumbsThrough) {
    // The TtlConfig overload should wire the per-slot expiry into the
    // returned KeyedState. Verify the TTL config round-trips and that
    // get() lazy-purges after the configured window.
    InMemoryStateBackend backend;
    RuntimeContext ctx(OperatorId{1}, "stage", &backend, nullptr);
    TtlConfig ttl{};
    ttl.ttl = std::chrono::milliseconds{50};
    ttl.refresh_on_write = true;
    auto kv =
        ctx.keyed_state<std::string, std::int64_t>("ttl-slot", string_codec(), int64_codec(), ttl);

    EXPECT_TRUE(kv.ttl_config().enabled());
    EXPECT_EQ(kv.ttl_config().ttl.count(), 50);

    kv.put("alive", 1);
    ASSERT_TRUE(kv.get("alive").has_value());
    EXPECT_EQ(*kv.get("alive"), 1);

    std::this_thread::sleep_for(std::chrono::milliseconds{75});
    EXPECT_FALSE(kv.get("alive").has_value())
        << "TTL'd entry should expire and be lazy-purged on read";
}

TEST(RuntimeContext, BroadcastStateRoundTrip) {
    InMemoryStateBackend backend;
    RuntimeContext ctx(OperatorId{1}, "stage", &backend, nullptr);

    auto bs = ctx.broadcast_state<std::int64_t>("rules", int64_codec());
    bs.put(42);
    EXPECT_EQ(*bs.get(), 42);
}

// --- Phase 29d-3: drain-target signal --------------------------------

TEST(RuntimeContext, DrainTargetReturnsZeroWhenSignalUnset) {
    RuntimeContext ctx(OperatorId{1}, "src", nullptr, nullptr);
    EXPECT_EQ(ctx.drain_target(), 0u);
}

TEST(RuntimeContext, DrainTargetReadsThroughSharedSignal) {
    RuntimeContext ctx(OperatorId{1}, "src", nullptr, nullptr);
    auto signal = std::make_shared<std::atomic<std::uint32_t>>(0);
    ctx.set_drain_target_signal(signal);
    EXPECT_EQ(ctx.drain_target(), 0u);

    // Simulate the cluster's BeginRescale dispatch setting the signal.
    signal->store(8, std::memory_order_release);
    EXPECT_EQ(ctx.drain_target(), 8u);

    // Subsequent updates are visible through the same context.
    signal->store(4, std::memory_order_release);
    EXPECT_EQ(ctx.drain_target(), 4u);
}

TEST(RuntimeContext, DrainTargetSignalCanBeCleared) {
    RuntimeContext ctx(OperatorId{1}, "src", nullptr, nullptr);
    auto signal = std::make_shared<std::atomic<std::uint32_t>>(5);
    ctx.set_drain_target_signal(signal);
    EXPECT_EQ(ctx.drain_target(), 5u);

    // Resetting the signal to nullptr returns to "no rescale" semantics.
    ctx.set_drain_target_signal(nullptr);
    EXPECT_EQ(ctx.drain_target(), 0u);
}
