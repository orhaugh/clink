// State TTL: event time, incremental cleanup, metrics, and the semantics
// the retention contract commits to.
//
// The existing test_keyed_state_ttl.cpp covers processing-time v1: stamp
// on write, hide and lazy-purge on read. This covers what that left open,
// and each case here corresponds to a line in the TtlConfig contract:
//
//   * event-time TTL follows the watermark, not the wall clock
//   * nothing expires before the first watermark unless asked
//   * a watermark regression cannot resurrect expired state
//   * cleanup is INCREMENTAL and bounded, and actually releases memory -
//     a lazily-expiring slot that is never read again never shrinks
//   * a snapshot carries the absolute stamp, so a restore does not silently
//     extend every entry's life by the length of the outage
//   * a late record targeting expired state sees nothing
//   * metrics report live/expired/backlog

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

constexpr OperatorId kOp{11};

KeyedState<std::int64_t, std::string> slot(StateBackend& b,
                                           TtlConfig cfg,
                                           const std::string& name = "s") {
    return KeyedState<std::int64_t, std::string>(b, kOp, name, int64_codec(), string_codec(), cfg);
}

TtlConfig event_time(std::chrono::milliseconds ttl) {
    return TtlConfig{.ttl = ttl, .refresh_on_write = true, .domain = TtlTimeDomain::EventTime};
}

// Count entries physically present in the backend for this operator. The
// distinction that matters: get() hiding an entry is not the same as the
// entry being gone, and only the second one gives memory back.
std::size_t resident(StateBackend& b) {
    std::size_t n = 0;
    b.scan(kOp, [&](std::string_view, std::string_view) { ++n; });
    return n;
}

// --- event-time TTL ---------------------------------------------------------

TEST(KeyedStateTtlDepth, EventTimeExpiryFollowsTheWatermarkNotTheWallClock) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1000ms));

    s.advance_watermark(10'000);
    s.put(1, "v");
    ASSERT_TRUE(s.get(1).has_value());

    // Wall clock moves; the watermark does not. A processing-time TTL of
    // 1 s would be at risk here; an event-time one must not be.
    std::this_thread::sleep_for(20ms);
    EXPECT_TRUE(s.get(1).has_value()) << "event-time TTL expired against the wall clock";

    // Watermark to just before the deadline: still live.
    s.advance_watermark(10'999);
    EXPECT_TRUE(s.get(1).has_value());

    // Watermark past the deadline: expired.
    s.advance_watermark(11'000);
    EXPECT_FALSE(s.get(1).has_value()) << "event-time TTL did not expire when the watermark passed";
}

TEST(KeyedStateTtlDepth, NothingExpiresBeforeTheFirstWatermarkByDefault) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1ms));
    // No watermark yet. A naive implementation treats event time as 0,
    // which makes every stamped entry look long expired and wipes the slot
    // the instant the job starts.
    s.put(1, "v");
    EXPECT_TRUE(s.get(1).has_value())
        << "state was expired against a watermark that has not arrived";
    EXPECT_FALSE(s.has_watermark());

    s.advance_watermark(1'000'000);
    EXPECT_FALSE(s.get(1).has_value()) << "once event time exists, the entry must expire";
}

TEST(KeyedStateTtlDepth, ExpireBeforeFirstWatermarkIsOptInAndDoesWhatItSays) {
    InMemoryStateBackend b;
    auto cfg = event_time(1000ms);
    cfg.expire_before_first_watermark = true;
    auto s = slot(b, cfg);

    // With time pinned at 0 and a 1 s TTL, an entry stamped at 0+1000 is
    // NOT yet expired - the opt-in makes time exist, it does not make
    // everything dead.
    s.put(1, "v");
    EXPECT_TRUE(s.get(1).has_value());
    s.advance_watermark(1000);
    EXPECT_FALSE(s.get(1).has_value());
}

TEST(KeyedStateTtlDepth, AWatermarkRegressionCannotResurrectExpiredState) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1000ms));
    s.advance_watermark(10'000);
    s.put(1, "v");
    s.advance_watermark(20'000);
    ASSERT_FALSE(s.get(1).has_value());

    // A regression must be ignored. Honouring it would make already-expired
    // entries readable again, so retention would depend on the order
    // watermarks happened to arrive in.
    s.advance_watermark(5'000);
    EXPECT_FALSE(s.get(1).has_value())
        << "a watermark that went backwards resurrected expired state";
}

TEST(KeyedStateTtlDepth, WritesUnderEventTimeStampAgainstTheWatermark) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(100ms));
    s.advance_watermark(1'000);
    s.put(1, "first");

    // Advance past the first entry's deadline and write again. The second
    // write is stamped against the NEW watermark, so it survives.
    s.advance_watermark(5'000);
    s.put(2, "second");
    EXPECT_FALSE(s.get(1).has_value());
    EXPECT_EQ(s.get(2).value_or(""), "second");
}

// --- incremental cleanup ----------------------------------------------------

TEST(KeyedStateTtlDepth, LazyExpiryAloneNeverReleasesMemory) {
    // The motivating defect for cleanup_batch. An entry written once and
    // never read again is hidden from readers but stays resident, so a TTL
    // that was supposed to bound memory does not.
    InMemoryStateBackend b;
    auto s = slot(b, event_time(10ms));
    s.advance_watermark(1'000);
    for (int i = 0; i < 50; ++i) {
        s.put(i, "v");
    }
    s.advance_watermark(100'000);  // everything is now expired

    EXPECT_EQ(resident(b), 50U) << "nothing has read them, so nothing has purged them";
    EXPECT_FALSE(s.get(0).has_value()) << "but they are correctly invisible";

    const auto removed = s.cleanup_all();
    EXPECT_GT(removed, 0U);
    EXPECT_EQ(resident(b), 0U) << "cleanup did not actually release the expired entries";
}

TEST(KeyedStateTtlDepth, CleanupIsBoundedByItsBudget) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(10ms));
    s.advance_watermark(1'000);
    for (int i = 0; i < 100; ++i) {
        s.put(i, "v");
    }
    s.advance_watermark(100'000);

    // A bounded sweep must not stall the operator thread proportionally to
    // state size, so it visits at most `budget` entries.
    const auto removed = s.cleanup_batch(10);
    EXPECT_LE(removed, 10U) << "cleanup exceeded its budget";
    EXPECT_GT(removed, 0U) << "cleanup made no progress";
    EXPECT_GT(resident(b), 0U) << "a bounded sweep should not have finished the whole slot";
}

TEST(KeyedStateTtlDepth, RepeatedBoundedSweepsEventuallyClearEverything) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(10ms));
    s.advance_watermark(1'000);
    for (int i = 0; i < 100; ++i) {
        s.put(i, "v");
    }
    s.advance_watermark(100'000);

    // The cursor must advance: without it every sweep re-scans the front of
    // the slot and the tail is never reached.
    for (int pass = 0; pass < 40 && resident(b) > 0; ++pass) {
        s.cleanup_batch(10);
    }
    EXPECT_EQ(resident(b), 0U) << "repeated bounded sweeps did not converge; the resume cursor is "
                                  "not advancing and the tail of the slot is never swept";
}

TEST(KeyedStateTtlDepth, CleanupNeverRemovesLiveState) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1000ms));
    s.advance_watermark(10'000);
    for (int i = 0; i < 20; ++i) {
        s.put(i, "old");
    }
    // Move time forward, then refresh half the keys so they are live again.
    s.advance_watermark(10'500);
    for (int i = 0; i < 10; ++i) {
        s.put(i, "refreshed");
    }
    // Past the old deadline, before the refreshed one.
    s.advance_watermark(11'200);

    s.cleanup_all();
    for (int i = 0; i < 10; ++i) {
        EXPECT_EQ(s.get(i).value_or(""), "refreshed") << "cleanup removed a live entry, key " << i;
    }
    for (int i = 10; i < 20; ++i) {
        EXPECT_FALSE(s.get(i).has_value()) << "an expired entry survived cleanup, key " << i;
    }
}

TEST(KeyedStateTtlDepth, CleanupDoesNothingBeforeEventTimeExists) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1ms));
    for (int i = 0; i < 10; ++i) {
        s.put(i, "v");
    }
    // No watermark: nothing can be judged expired, so a sweep must be a
    // no-op rather than wiping the slot.
    EXPECT_EQ(s.cleanup_batch(100), 0U);
    EXPECT_EQ(resident(b), 10U);
}

TEST(KeyedStateTtlDepth, CleanupIgnoresOtherSlotsInTheSameOperator) {
    InMemoryStateBackend b;
    auto a = slot(b, event_time(10ms), "slot_a");
    auto c = slot(b, event_time(1'000'000ms), "slot_b");
    a.advance_watermark(1'000);
    c.advance_watermark(1'000);
    a.put(1, "doomed");
    c.put(1, "safe");
    a.advance_watermark(100'000);
    c.advance_watermark(100'000);

    a.cleanup_all();
    EXPECT_EQ(c.get(1).value_or(""), "safe") << "cleanup crossed a slot boundary";
}

// --- metrics ----------------------------------------------------------------

TEST(KeyedStateTtlDepth, StatsReportExpiryLivenessAndCleanupLag) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1000ms));
    s.advance_watermark(10'000);
    for (int i = 0; i < 30; ++i) {
        s.put(i, "v");
    }
    s.advance_watermark(10'500);
    for (int i = 0; i < 5; ++i) {
        s.put(i, "v");  // refresh a few
    }
    s.advance_watermark(11'200);

    // A read that finds a dead entry counts it.
    EXPECT_FALSE(s.get(29).has_value());
    EXPECT_GE(s.ttl_stats().expired_on_read, 1U);

    s.cleanup_batch(10);
    const auto& st = s.ttl_stats();
    EXPECT_GT(st.scanned_entries, 0U);
    EXPECT_GT(st.estimated_bytes, 0U);
    // Budget-limited sweep leaves a backlog, which IS the cleanup lag: while
    // it is non-zero, expired state is still resident.
    EXPECT_GT(st.unscanned_backlog, 0U);

    s.cleanup_all();
    EXPECT_GT(s.ttl_stats().expired_in_cleanup, 0U);
}

// --- snapshot / restore -----------------------------------------------------

TEST(KeyedStateTtlDepth, RestoreResumesTheSameAbsoluteExpiryNotAFreshOne) {
    InMemoryStateBackend b;
    {
        auto s = slot(b, event_time(1000ms));
        s.advance_watermark(10'000);
        s.put(1, "v");  // expires at 11'000
    }
    const auto snap = b.snapshot(CheckpointId{1});

    // A new backend + slot, as after a restart.
    InMemoryStateBackend restored;
    restored.restore(snap);
    auto s2 = slot(restored, event_time(1000ms));

    // The stamp is absolute. If restore had re-based it on the restart, the
    // entry would live another full TTL from now - which quietly turns a
    // one-hour retention into "one hour after every outage".
    s2.advance_watermark(10'999);
    EXPECT_TRUE(s2.get(1).has_value()) << "the restored entry expired early";
    s2.advance_watermark(11'000);
    EXPECT_FALSE(s2.get(1).has_value())
        << "the restored entry outlived its original deadline; the restore extended its life";
}

TEST(KeyedStateTtlDepth, StateExpiredBeforeASnapshotIsNotResurrectedByRestore) {
    InMemoryStateBackend b;
    {
        auto s = slot(b, event_time(10ms));
        s.advance_watermark(1'000);
        s.put(1, "v");
        s.advance_watermark(100'000);
        s.cleanup_all();  // physically gone before the snapshot
    }
    const auto snap = b.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    auto s2 = slot(restored, event_time(10ms));
    s2.advance_watermark(100'000);
    EXPECT_FALSE(s2.get(1).has_value());
    EXPECT_EQ(resident(restored), 0U) << "a cleaned-up entry came back through the snapshot";
}

// --- late records -----------------------------------------------------------

TEST(KeyedStateTtlDepth, ALateRecordTargetingExpiredStateSeesNothing) {
    InMemoryStateBackend b;
    auto s = slot(b, event_time(1000ms));
    s.advance_watermark(10'000);
    s.put(42, "session");
    s.advance_watermark(20'000);  // key 42 has expired

    // A record with an event time inside the original window arrives after
    // the watermark has moved past it. The documented behaviour is that it
    // sees no state - resurrecting expired state on a late arrival would
    // make retention unbounded again, because any key could be revived.
    EXPECT_FALSE(s.get(42).has_value());

    // It may of course CREATE new state, which then gets its own deadline
    // stamped against the current watermark.
    s.put(42, "recreated");
    EXPECT_EQ(s.get(42).value_or(""), "recreated");
    s.advance_watermark(21'000);
    EXPECT_FALSE(s.get(42).has_value());
}

// --- processing time still behaves ------------------------------------------

TEST(KeyedStateTtlDepth, ProcessingTimeRemainsTheDefaultAndIsUnchanged) {
    InMemoryStateBackend b;
    // Default domain, so a watermark is irrelevant.
    auto s = slot(b, TtlConfig{.ttl = 50ms});
    s.put(1, "v");
    EXPECT_TRUE(s.get(1).has_value());
    std::this_thread::sleep_for(80ms);
    EXPECT_FALSE(s.get(1).has_value());
}

TEST(KeyedStateTtlDepth, ADisabledTtlNeverExpiresAndCleanupIsANoOp) {
    InMemoryStateBackend b;
    auto s = slot(b, TtlConfig{});  // ttl == 0
    for (int i = 0; i < 10; ++i) {
        s.put(i, "v");
    }
    EXPECT_EQ(s.cleanup_batch(100), 0U);
    EXPECT_EQ(resident(b), 10U);
    EXPECT_TRUE(s.get(0).has_value());
}

}  // namespace
