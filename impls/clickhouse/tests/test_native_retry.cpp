#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/runtime/runtime_context.hpp"

#include "native/retry.hpp"
#include "test_helpers/sanitizer_slack.hpp"

namespace clink::clickhouse::native {
namespace {

using std::chrono::milliseconds;
using Clock = std::chrono::steady_clock;
using clink::test_support::scale_slack;

// min(cap, initial * 2^n), the ceiling of the nth wait.
std::int64_t rt_ceiling(std::int64_t initial, std::int64_t cap, unsigned n) {
    std::int64_t c = initial;
    for (unsigned i = 0; i < n && c < cap; ++i) {
        c *= 2;
    }
    return std::min(c, cap);
}

// The executor's flag and the external token, both owned by the test.
struct RtCancelFlags {
    std::shared_ptr<std::atomic<bool>> executor = std::make_shared<std::atomic<bool>>(false);
    std::shared_ptr<std::atomic<bool>> external = std::make_shared<std::atomic<bool>>(false);
    [[nodiscard]] CancelSignal signal() const { return CancelSignal{executor, external}; }
};

// Runs a 10 s cancellable_wait, sets `flag` from another thread after 150 ms,
// and returns how long the wait took to notice.
milliseconds rt_time_to_notice(const CancelSignal& cancel,
                               const std::atomic<bool>& stop,
                               std::atomic<bool>& flag,
                               bool& returned) {
    Clock::time_point flipped_at{};
    std::thread flipper([&] {
        std::this_thread::sleep_for(milliseconds{150});
        flipped_at = Clock::now();
        flag.store(true, std::memory_order_release);
    });
    returned = cancellable_wait(milliseconds{10'000}, cancel, stop);
    const auto noticed_at = Clock::now();
    flipper.join();
    return std::chrono::duration_cast<milliseconds>(noticed_at - flipped_at);
}

// --- Backoff -----------------------------------------------------------------

TEST(NativeBackoff, EveryWaitStaysWithinItsFullJitterCeiling) {
    for (std::uint64_t seed = 0; seed < 200; ++seed) {
        Backoff b(milliseconds{100}, milliseconds{10'000}, seed);
        for (unsigned n = 0; n < 20; ++n) {
            const auto wait = b.next().count();
            EXPECT_GE(wait, 0) << "seed " << seed << " n " << n;
            EXPECT_LE(wait, rt_ceiling(100, 10'000, n)) << "seed " << seed << " n " << n;
        }
    }
}

TEST(NativeBackoff, TheFirstWaitIsUniformFromZeroToTheInitialDelay) {
    std::int64_t lowest = 1'000;
    std::int64_t highest = -1;
    double sum = 0;
    constexpr int kSeeds = 4'000;
    for (int seed = 0; seed < kSeeds; ++seed) {
        Backoff b(milliseconds{100}, milliseconds{10'000}, static_cast<std::uint64_t>(seed));
        const auto wait = b.next().count();
        lowest = std::min(lowest, wait);
        highest = std::max(highest, wait);
        sum += static_cast<double>(wait);
    }
    // Full jitter reaches both ends; a half-jitter or fixed delay would not.
    EXPECT_LE(lowest, 2);
    EXPECT_GE(highest, 98);
    EXPECT_LE(highest, 100);
    const double mean = sum / kSeeds;
    EXPECT_GT(mean, 45.0);
    EXPECT_LT(mean, 55.0);
}

TEST(NativeBackoff, TheCeilingDoublesUntilTheCap) {
    // The highest wait seen across seeds tracks min(cap, initial * 2^n).
    constexpr int kSeeds = 2'000;
    std::vector<std::int64_t> highest(10, 0);
    for (int seed = 0; seed < kSeeds; ++seed) {
        Backoff b(milliseconds{100}, milliseconds{10'000}, static_cast<std::uint64_t>(seed));
        for (std::size_t n = 0; n < highest.size(); ++n) {
            highest[n] = std::max(highest[n], b.next().count());
        }
    }
    for (std::size_t n = 0; n < highest.size(); ++n) {
        const auto ceiling = rt_ceiling(100, 10'000, static_cast<unsigned>(n));
        EXPECT_LE(highest[n], ceiling) << n;
        EXPECT_GE(highest[n], ceiling * 95 / 100) << n;
    }
    // 100 * 2^7 = 12800 is past the cap, so waits from the eighth on top out
    // at the cap.
    EXPECT_EQ(rt_ceiling(100, 10'000, 7), 10'000);
}

TEST(NativeBackoff, TheCapHoldsAfterManyAttemptsWithoutOverflow) {
    Backoff b(milliseconds{1}, std::chrono::hours{1}, 7);
    for (int i = 0; i < 200; ++i) {
        const auto wait = b.next();
        EXPECT_GE(wait.count(), 0);
        EXPECT_LE(wait, std::chrono::hours{1});
    }
    EXPECT_EQ(b.attempts(), 200U);
}

TEST(NativeBackoff, AnInitialDelayAboveTheCapIsCapped) {
    for (std::uint64_t seed = 0; seed < 100; ++seed) {
        Backoff b(milliseconds{20'000}, milliseconds{10'000}, seed);
        EXPECT_LE(b.next(), milliseconds{10'000});
    }
}

TEST(NativeBackoff, AZeroInitialDelayNeverWaits) {
    Backoff b(milliseconds{0}, milliseconds{10'000}, 3);
    for (int i = 0; i < 70; ++i) {
        EXPECT_EQ(b.next(), milliseconds{0});
    }
}

TEST(NativeBackoff, ResetStartsTheSequenceAgain) {
    Backoff b(milliseconds{100}, milliseconds{10'000}, 11);
    for (int i = 0; i < 8; ++i) {
        (void)b.next();
    }
    EXPECT_EQ(b.attempts(), 8U);
    b.reset();
    EXPECT_EQ(b.attempts(), 0U);
    for (int i = 0; i < 50; ++i) {
        b.reset();
        EXPECT_LE(b.next(), milliseconds{100});
        EXPECT_EQ(b.attempts(), 1U);
    }
}

TEST(NativeBackoff, TheSameSeedGivesTheSameWaits) {
    Backoff a(milliseconds{100}, milliseconds{10'000}, 42);
    Backoff b(milliseconds{100}, milliseconds{10'000}, 42);
    for (int i = 0; i < 30; ++i) {
        EXPECT_EQ(a.next(), b.next()) << i;
    }
}

TEST(NativeBackoff, TheDefaultsAreAHundredMillisecondsCappedAtTenSeconds) {
    Backoff b;
    EXPECT_EQ(b.attempts(), 0U);
    EXPECT_LE(b.next(), milliseconds{100});
    for (int i = 0; i < 40; ++i) {
        EXPECT_LE(b.next(), milliseconds{10'000});
    }
}

// --- RetryWindow -------------------------------------------------------------

TEST(NativeRetryWindow, StartSetsTheDeadlineOneWindowAhead) {
    RetryWindow w(milliseconds{1'000});
    EXPECT_FALSE(w.started());
    const auto t0 = Clock::now();
    w.start(t0);
    EXPECT_TRUE(w.started());
    EXPECT_EQ(w.deadline(), t0 + milliseconds{1'000});
}

TEST(NativeRetryWindow, AllowsOnlyAWaitThatEndsBeforeTheDeadline) {
    RetryWindow w(milliseconds{1'000});
    const auto t0 = Clock::now();
    w.start(t0);
    EXPECT_TRUE(w.allows(t0, milliseconds{0}));
    EXPECT_TRUE(w.allows(t0, milliseconds{999}));
    EXPECT_FALSE(w.allows(t0, milliseconds{1'000}));  // ends at the deadline, not before
    EXPECT_FALSE(w.allows(t0, milliseconds{5'000}));
    EXPECT_TRUE(w.allows(t0 + milliseconds{500}, milliseconds{499}));
    EXPECT_FALSE(w.allows(t0 + milliseconds{500}, milliseconds{500}));
    EXPECT_FALSE(w.allows(t0 + milliseconds{1'000}, milliseconds{0}));
    EXPECT_FALSE(w.allows(t0 + milliseconds{2'000}, milliseconds{0}));
}

TEST(NativeRetryWindow, ANegativeWaitCountsAsNone) {
    RetryWindow w(milliseconds{1'000});
    const auto t0 = Clock::now();
    w.start(t0);
    EXPECT_TRUE(w.allows(t0 + milliseconds{999}, milliseconds{-5'000}));
    EXPECT_FALSE(w.allows(t0 + milliseconds{1'000}, milliseconds{-5'000}));
}

TEST(NativeRetryWindow, AWindowThatHasNotStartedMeasuresFromNow) {
    const RetryWindow w(milliseconds{1'000});
    const auto now = Clock::now();
    EXPECT_TRUE(w.allows(now, milliseconds{999}));
    EXPECT_FALSE(w.allows(now, milliseconds{1'000}));
    EXPECT_FALSE(w.started());
}

TEST(NativeRetryWindow, StartingAgainKeepsTheFirstDeadline) {
    RetryWindow w(milliseconds{1'000});
    const auto t0 = Clock::now();
    w.start(t0);
    w.start(t0 + milliseconds{800});
    EXPECT_EQ(w.deadline(), t0 + milliseconds{1'000});
    EXPECT_FALSE(w.allows(t0 + milliseconds{800}, milliseconds{500}));
}

TEST(NativeRetryWindow, ACopyKeepsItsParentsDeadline) {
    RetryWindow parent(milliseconds{2'000});
    const auto t0 = Clock::now();
    parent.start(t0);

    // The two halves of a split, each starting at its own first failure.
    RetryWindow left = parent;
    RetryWindow right = parent;
    EXPECT_TRUE(left.started());
    left.start(t0 + milliseconds{1'500});
    right.start(t0 + milliseconds{1'900});
    EXPECT_EQ(left.deadline(), parent.deadline());
    EXPECT_EQ(right.deadline(), parent.deadline());
    EXPECT_FALSE(left.allows(t0 + milliseconds{1'500}, milliseconds{600}));
    EXPECT_TRUE(left.allows(t0 + milliseconds{1'500}, milliseconds{400}));

    // And a half of a half, so one window covers the whole tree.
    RetryWindow quarter = left;
    quarter.start(t0 + milliseconds{1'999});
    EXPECT_EQ(quarter.deadline(), t0 + milliseconds{2'000});
    EXPECT_FALSE(quarter.allows(t0 + milliseconds{2'000}, milliseconds{0}));
}

TEST(NativeRetryWindow, AHugeWindowDoesNotOverflow) {
    RetryWindow w(milliseconds::max());
    const auto t0 = Clock::now();
    w.start(t0);
    EXPECT_EQ(w.deadline(), Clock::time_point::max());
    EXPECT_TRUE(w.allows(t0, std::chrono::hours{24 * 365}));
}

TEST(NativeRetryWindow, AZeroWindowAllowsNothing) {
    RetryWindow w(milliseconds{0});
    const auto t0 = Clock::now();
    w.start(t0);
    EXPECT_EQ(w.deadline(), t0);
    EXPECT_FALSE(w.allows(t0, milliseconds{0}));
}

// --- cancellable_wait --------------------------------------------------------

TEST(NativeCancellableWait, WaitsTheWholeDurationWithoutACancel) {
    const RtCancelFlags flags;
    const std::atomic<bool> stop{false};
    const auto t0 = Clock::now();
    EXPECT_TRUE(cancellable_wait(milliseconds{120}, flags.signal(), stop));
    EXPECT_GE(Clock::now() - t0, milliseconds{120});
}

TEST(NativeCancellableWait, ADefaultSignalNeverCancels) {
    const std::atomic<bool> stop{false};
    EXPECT_TRUE(cancellable_wait(milliseconds{60}, CancelSignal{}, stop));
    EXPECT_TRUE(cancellable_wait(milliseconds{0}, CancelSignal{}, stop));
}

TEST(NativeCancellableWait, AnEarlierCancelOrStopReturnsAtOnce) {
    const std::atomic<bool> no_stop{false};
    const std::atomic<bool> stopped{true};
    RtCancelFlags exec;
    exec.executor->store(true);
    RtCancelFlags ext;
    ext.external->store(true);

    for (const auto& [sig, stop] :
         {std::pair<CancelSignal, const std::atomic<bool>*>{exec.signal(), &no_stop},
          {ext.signal(), &no_stop},
          {CancelSignal{}, &stopped}}) {
        const auto t0 = Clock::now();
        EXPECT_FALSE(cancellable_wait(milliseconds{10'000}, sig, *stop));
        EXPECT_FALSE(cancellable_wait(milliseconds{0}, sig, *stop));
        // Not scaled for a sanitizer build: this is the 50 ms slice a wait
        // that looked only after its first one would take.
        EXPECT_LT(Clock::now() - t0, milliseconds{50});
    }
}

TEST(NativeCancellableWait, ReturnsWithinAHundredMillisecondsOfAnExecutorCancel) {
    const RtCancelFlags flags;
    const std::atomic<bool> stop{false};
    bool returned = true;
    const auto took = rt_time_to_notice(flags.signal(), stop, *flags.executor, returned);
    EXPECT_FALSE(returned);
    EXPECT_LT(took, scale_slack(milliseconds{100}));
}

TEST(NativeCancellableWait, ReturnsWithinAHundredMillisecondsOfAnExternalCancel) {
    const RtCancelFlags flags;
    const std::atomic<bool> stop{false};
    bool returned = true;
    const auto took = rt_time_to_notice(flags.signal(), stop, *flags.external, returned);
    EXPECT_FALSE(returned);
    EXPECT_LT(took, scale_slack(milliseconds{100}));
}

TEST(NativeCancellableWait, ReturnsWithinAHundredMillisecondsOfAStop) {
    const RtCancelFlags flags;
    std::atomic<bool> stop{false};
    bool returned = true;
    const auto took = rt_time_to_notice(flags.signal(), stop, stop, returned);
    EXPECT_FALSE(returned);
    EXPECT_LT(took, scale_slack(milliseconds{100}));
}

TEST(NativeCancellableWait, ACopiedSignalSeesTheCancel) {
    // The writer keeps its own copy of the signal; the flags are shared.
    const RtCancelFlags flags;
    const CancelSignal copy = flags.signal();
    const std::atomic<bool> stop{false};
    bool returned = true;
    const auto took = rt_time_to_notice(copy, stop, *flags.executor, returned);
    EXPECT_FALSE(returned);
    EXPECT_LT(took, scale_slack(milliseconds{100}));
}

}  // namespace
}  // namespace clink::clickhouse::native
