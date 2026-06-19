// Watermark alignment tests - // WatermarkStrategy.withWatermarkAlignment(group, maxDrift).
//
// Two layers:
//   * AlignmentGroup directly - verify the drift check + wait/notify
//     semantics in isolation.
//   * WatermarkAssignerOperator with .with_watermark_alignment(...) -
//     verify that a fast assigner blocks at the top of process()
//     when ahead of group min by > max_drift, and resumes when a
//     peer catches up.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/watermark_assigner_operator.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/time/alignment_group.hpp"

using namespace clink;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// AlignmentGroup direct
// ---------------------------------------------------------------------------

TEST(AlignmentGroup, SoloMemberWithinDriftReturnsImmediately) {
    auto g = std::make_shared<AlignmentGroup>();
    auto id = g->join();
    // No peers; "min" of just our own member equals our own wm.
    // Drift = 0; max_drift = 100ms; should return immediately.
    const bool aligned = g->wait_until_within_drift(id, Watermark{EventTime{500}}, 100, 10ms);
    EXPECT_TRUE(aligned);
}

TEST(AlignmentGroup, BlocksUntilSlowMemberAdvances) {
    auto g = std::make_shared<AlignmentGroup>();
    auto fast = g->join();
    auto slow = g->join();
    g->update_watermark(fast, Watermark{EventTime{1000}});
    g->update_watermark(slow, Watermark{EventTime{100}});

    std::atomic<bool> aligned{false};
    std::thread waiter(
        [&] { aligned = g->wait_until_within_drift(fast, Watermark{EventTime{1000}}, 100, 2s); });

    // Slow member catches up after a short delay.
    std::this_thread::sleep_for(50ms);
    g->update_watermark(slow, Watermark{EventTime{950}});  // drift = 1000 - 950 = 50 < 100
    waiter.join();
    EXPECT_TRUE(aligned);
}

TEST(AlignmentGroup, TimesOutWhenSlowMemberNeverCatchesUp) {
    auto g = std::make_shared<AlignmentGroup>();
    auto fast = g->join();
    auto slow = g->join();
    g->update_watermark(fast, Watermark{EventTime{1000}});
    g->update_watermark(slow, Watermark{EventTime{100}});

    // max_drift = 100; drift = 900 > 100. Slow member never moves.
    // wait should hit the deadline and return false.
    const bool aligned = g->wait_until_within_drift(fast, Watermark{EventTime{1000}}, 100, 50ms);
    EXPECT_FALSE(aligned);
}

TEST(AlignmentGroup, ShutdownWakesWaitersImmediately) {
    auto g = std::make_shared<AlignmentGroup>();
    auto fast = g->join();
    auto slow = g->join();
    g->update_watermark(fast, Watermark{EventTime{1000}});
    g->update_watermark(slow, Watermark{EventTime{0}});

    std::atomic<bool> aligned{true};
    std::thread waiter(
        [&] { aligned = g->wait_until_within_drift(fast, Watermark{EventTime{1000}}, 100, 2s); });
    std::this_thread::sleep_for(50ms);
    g->shutdown();
    waiter.join();
    EXPECT_FALSE(aligned);  // shutdown is treated as "not aligned"
}

// ---------------------------------------------------------------------------
// WatermarkAssignerOperator with alignment
// ---------------------------------------------------------------------------

TEST(WatermarkAssignerAlignment, FastAssignerBlocksOnNextProcessUntilPeerCatchesUp) {
    // Two assigners share a group with max_drift = 50ms.
    auto group = std::make_shared<AlignmentGroup>();

    BoundedChannel<StreamElement<int>> out_fast(16);
    BoundedChannel<StreamElement<int>> out_slow(16);
    Emitter<int> em_fast(&out_fast);
    Emitter<int> em_slow(&out_slow);

    WatermarkAssignerOperator<int> fast([](const int& v) { return EventTime{v}; },
                                        std::make_unique<MonotonicWatermarkStrategy<int>>());
    fast.with_watermark_alignment(group, 50ms, /*max_wait=*/2s);

    WatermarkAssignerOperator<int> slow([](const int& v) { return EventTime{v}; },
                                        std::make_unique<MonotonicWatermarkStrategy<int>>());
    slow.with_watermark_alignment(group, 50ms, /*max_wait=*/2s);

    RuntimeContext fast_ctx(OperatorId{1}, "fast", nullptr, nullptr);
    RuntimeContext slow_ctx(OperatorId{2}, "slow", nullptr, nullptr);
    fast.attach_runtime(&fast_ctx);
    slow.attach_runtime(&slow_ctx);
    fast.open();
    slow.open();

    // Send a record on the fast side to advance its watermark to 1000.
    {
        Batch<int> b;
        b.emplace(1000, EventTime{1000});
        fast.process(StreamElement<int>::data(std::move(b)), em_fast);
    }
    // Slow side is still at min; fast is at 1000. Drift = 1000 -
    // min(1000, min)=1000 > 50ms. The NEXT fast process() should
    // block at the top until slow catches up.

    std::atomic<bool> done{false};
    std::thread fast_thread([&] {
        Batch<int> b;
        b.emplace(1500, EventTime{1500});
        fast.process(StreamElement<int>::data(std::move(b)), em_fast);
        done = true;
    });

    // Briefly verify fast didn't proceed.
    std::this_thread::sleep_for(100ms);
    EXPECT_FALSE(done) << "fast assigner should be blocked on alignment";

    // Slow catches up.
    {
        Batch<int> b;
        b.emplace(1480, EventTime{1480});  // drift becomes 1500 - 1480 = 20 < 50.
        slow.process(StreamElement<int>::data(std::move(b)), em_slow);
    }

    fast_thread.join();
    EXPECT_TRUE(done) << "fast assigner resumed after peer caught up";
}

TEST(WatermarkAssignerAlignment, NoGroupConfiguredDoesNotBlock) {
    // Sanity: with no alignment group set, the assigner behaves
    // exactly as before - no blocking, regardless of how fast it
    // advances.
    BoundedChannel<StreamElement<int>> out_ch(16);
    Emitter<int> em(&out_ch);

    WatermarkAssignerOperator<int> op([](const int& v) { return EventTime{v}; },
                                      std::make_unique<MonotonicWatermarkStrategy<int>>());
    // No with_watermark_alignment call.

    RuntimeContext ctx(OperatorId{1}, "assigner", nullptr, nullptr);
    op.attach_runtime(&ctx);
    op.open();

    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < 5; ++i) {
        Batch<int> b;
        b.emplace(i * 1000, EventTime{i * 1000});
        op.process(StreamElement<int>::data(std::move(b)), em);
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, 200ms) << "no-alignment path must not introduce delays";
}

TEST(WatermarkAssignerAlignment, RegistryReturnsSharedGroup) {
    auto& reg = AlignmentGroupRegistry::default_instance();
    auto a = reg.get_or_create("test-group-xyz");
    auto b = reg.get_or_create("test-group-xyz");
    EXPECT_EQ(a.get(), b.get()) << "same name resolves to same group";

    reg.erase("test-group-xyz");
    auto c = reg.get_or_create("test-group-xyz");
    EXPECT_NE(a.get(), c.get()) << "after erase, get_or_create creates a fresh group";
    reg.erase("test-group-xyz");
}
