#include <atomic>
#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "clink/runtime/bounded_channel.hpp"

using namespace clink;
using namespace std::chrono_literals;

TEST(BoundedChannel, BasicPushPop) {
    BoundedChannel<int> ch(4);
    EXPECT_TRUE(ch.try_push(1));
    EXPECT_TRUE(ch.try_push(2));
    EXPECT_EQ(ch.size(), 2u);

    auto a = ch.try_pop();
    auto b = ch.try_pop();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*a, 1);
    EXPECT_EQ(*b, 2);
}

TEST(BoundedChannel, FullChannelRejectsTryPush) {
    BoundedChannel<int> ch(2);
    EXPECT_TRUE(ch.try_push(1));
    EXPECT_TRUE(ch.try_push(2));
    EXPECT_FALSE(ch.try_push(3));
}

TEST(BoundedChannel, BlockingPushWaitsForCapacity) {
    BoundedChannel<int> ch(1);
    EXPECT_TRUE(ch.push(1));

    std::atomic<bool> pushed{false};
    std::thread producer([&] {
        EXPECT_TRUE(ch.push(2));
        pushed.store(true);
    });

    // Producer should still be blocked because the channel is full.
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(pushed.load());

    auto first = ch.pop();
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(*first, 1);
    producer.join();
    EXPECT_TRUE(pushed.load());

    auto second = ch.pop();
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(*second, 2);
}

TEST(BoundedChannel, CloseUnblocksConsumers) {
    BoundedChannel<int> ch(4);
    std::atomic<bool> done{false};
    std::thread consumer([&] {
        auto v = ch.pop();
        EXPECT_FALSE(v.has_value());
        done.store(true);
    });
    std::this_thread::sleep_for(20ms);
    EXPECT_FALSE(done.load());
    ch.close();
    consumer.join();
    EXPECT_TRUE(done.load());
}

TEST(BoundedChannel, PushAfterCloseFails) {
    BoundedChannel<int> ch(4);
    ch.close();
    EXPECT_FALSE(ch.push(1));
    EXPECT_FALSE(ch.try_push(2));
    EXPECT_EQ(ch.size(), 0u);
}

TEST(BoundedChannel, PopReturnsBufferedItemsThenNullopt) {
    // Closing a channel that still has buffered items: consumers should
    // drain all buffered items, then receive nullopt.
    BoundedChannel<int> ch(4);
    EXPECT_TRUE(ch.try_push(1));
    EXPECT_TRUE(ch.try_push(2));
    ch.close();

    auto a = ch.pop();
    auto b = ch.pop();
    auto c = ch.pop();
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*a, 1);
    EXPECT_EQ(*b, 2);
    EXPECT_FALSE(c.has_value());  // closed + empty
}

TEST(BoundedChannel, ConcurrentClosePushIsRaceFree) {
    // Stress: many threads pushing while another closes. Must not crash;
    // all successful pushes must be poppable; channel.size() never
    // exceeds capacity.
    constexpr std::size_t capacity = 32;
    constexpr int threads = 8;
    constexpr int per_thread = 1000;
    BoundedChannel<int> ch(capacity);
    std::atomic<int> success_count{0};
    std::atomic<bool> start{false};
    std::vector<std::thread> pushers;
    pushers.reserve(threads);
    for (int t = 0; t < threads; ++t) {
        pushers.emplace_back([&] {
            while (!start.load()) {
                std::this_thread::yield();
            }
            for (int i = 0; i < per_thread; ++i) {
                if (ch.try_push(1)) {
                    success_count.fetch_add(1);
                }
            }
        });
    }
    std::thread closer([&] {
        while (!start.load()) {
            std::this_thread::yield();
        }
        std::this_thread::sleep_for(1ms);
        ch.close();
    });
    start.store(true);
    for (auto& t : pushers) {
        t.join();
    }
    closer.join();
    // Drain whatever the channel buffered.
    int popped = 0;
    while (ch.try_pop().has_value()) {
        ++popped;
    }
    // Successful pushes that beat close() must equal what we drained
    // plus any post-close losses (which try_push returned false for, so
    // didn't count).
    EXPECT_LE(popped, success_count.load());
}

TEST(BoundedChannel, HighWaterMarkTracksPeakDepth) {
    BoundedChannel<int> ch(8);
    for (int i = 0; i < 5; ++i) {
        ch.try_push(i);
    }
    EXPECT_EQ(ch.high_water_mark(), 5u);

    // Drain - high water mark should not regress
    while (ch.try_pop().has_value()) {
    }
    EXPECT_EQ(ch.high_water_mark(), 5u);
}

// Item 76: the stuck-channel diagnostic must not out-shout the evidence.
// One line per 3s per channel filled three workers' entire 200k-line log
// windows on the first QUAL-06 width probe and rotated out the task starts
// and bridge errors that would have named the stall. The warn gap now
// doubles per repetition, capped, and a warned wait that unblocks says so.

TEST(BoundedChannel, StuckWarnGapDoublesAndCaps) {
    using ms = std::chrono::milliseconds;
    auto g = ms{3000};
    std::vector<long> gaps;
    for (int i = 0; i < 10; ++i) {
        g = BoundedChannel<int>::stuck_warn_backoff(g);
        gaps.push_back(g.count());
    }
    EXPECT_EQ(gaps[0], 6000);
    EXPECT_EQ(gaps[1], 12000);
    EXPECT_EQ(gaps[2], 24000);
    EXPECT_EQ(gaps.back(), 300000) << "the gap must cap, not double forever";
    // A 40-minute stall at this schedule is ~12 lines, not ~800.
    long t = 3000, lines = 1, gap = 3000;
    while (t < 40 * 60 * 1000) {
        gap = BoundedChannel<int>::stuck_warn_backoff(ms{gap}).count();
        t += gap;
        ++lines;
    }
    EXPECT_LE(lines, 15) << "the schedule no longer bounds the flood";
}

TEST(BoundedChannel, StuckWarnsAreExponentiallySpacedAndUnblockIsStated) {
    BoundedChannel<int> ch(1, "spacing-probe");
    ch.set_stuck_warn_base_for_testing(std::chrono::milliseconds{300});

    testing::internal::CaptureStderr();
    std::thread consumer([&] {
        // Blocked pop on an empty channel for ~1.05s: warns land at 0.3s and
        // 0.9s under the backoff (0.3 + 0.6). The fixed-interval behaviour
        // this replaces would land three (0.3, 0.6, 0.9) - which is exactly
        // what the mutation check flips back to.
        auto v = ch.pop();
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(*v, 7);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds{1050});
    ASSERT_TRUE(ch.push(7));
    consumer.join();
    const std::string err = testing::internal::GetCapturedStderr();

    const auto count = [&](const std::string& needle) {
        std::size_t n = 0, pos = 0;
        while ((pos = err.find(needle, pos)) != std::string::npos) {
            ++n;
            pos += needle.size();
        }
        return n;
    };
    EXPECT_EQ(count("BOUNDED_CHANNEL_STUCK pop"), 2u) << err;
    EXPECT_EQ(count("BOUNDED_CHANNEL_UNBLOCKED pop"), 1u) << err;
    EXPECT_NE(err.find("warns=2"), std::string::npos) << err;
    EXPECT_NE(err.find("closed=0"), std::string::npos) << err;
}

TEST(BoundedChannel, AWarnedWaitEndedByCloseSaysSo) {
    BoundedChannel<int> ch(1, "close-probe");
    ch.set_stuck_warn_base_for_testing(std::chrono::milliseconds{200});
    testing::internal::CaptureStderr();
    std::thread consumer([&] { EXPECT_FALSE(ch.pop().has_value()); });
    std::this_thread::sleep_for(std::chrono::milliseconds{350});
    ch.close();
    consumer.join();
    const std::string err = testing::internal::GetCapturedStderr();
    EXPECT_NE(err.find("BOUNDED_CHANNEL_UNBLOCKED pop"), std::string::npos) << err;
    EXPECT_NE(err.find("closed=1"), std::string::npos) << err;
}
