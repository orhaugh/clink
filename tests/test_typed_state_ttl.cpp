// Retention on the collection state types: List, Map, Aggregating,
// Reducing.
//
// typed_state.hpp used to claim these inherited TTL "for free". They did
// not: the constructors never accepted or forwarded a TtlConfig, so a
// caller who read that sentence and expected bounded state got unbounded
// state. These tests exist so the claim cannot become false again.
//
// The semantics under test are the ones a reader is most likely to guess
// wrong, so each has its own case:
//
//   * retention is PER KEY, not per element - the whole collection lives
//     and dies as a unit, because it IS one KeyedState value
//   * any mutation refreshes the whole collection
//   * a read does not refresh
//   * an expired collection reads as EMPTY, and the next add starts fresh
//   * cleanup releases memory rather than merely hiding it

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/typed_state.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

// Distinctly named: identically-named helpers in two anonymous namespaces in
// one binary have collided here before.
std::int64_t g_typed_ttl_fake_now_ms = 2'000'000;

std::int64_t typed_ttl_fake_clock() {
    return g_typed_ttl_fake_now_ms;
}

void typed_advance(std::chrono::milliseconds by) {
    g_typed_ttl_fake_now_ms += by.count();
}

constexpr OperatorId kOp{21};

TtlConfig event_ttl(std::chrono::milliseconds ttl) {
    return TtlConfig{.ttl = ttl, .refresh_on_write = true, .domain = TtlTimeDomain::EventTime};
}

// Entries physically present in the backend. get() hiding a collection is
// not the same as the collection being gone, and only the second gives
// memory back.
std::size_t resident(StateBackend& b) {
    std::size_t n = 0;
    b.scan(kOp, [&](std::string_view, std::string_view) { ++n; });
    return n;
}

ListState<std::int64_t, std::string> list_with(StateBackend& b, TtlConfig ttl) {
    return ListState<std::int64_t, std::string>(b, kOp, "lst", int64_codec(), string_codec(), ttl);
}

MapState<std::int64_t, std::string, std::int64_t> map_with(StateBackend& b, TtlConfig ttl) {
    return MapState<std::int64_t, std::string, std::int64_t>(
        b, kOp, "mp", int64_codec(), string_codec(), int64_codec(), ttl);
}

// --- ListState --------------------------------------------------------------

TEST(TypedStateTtl, ListWithoutTtlRetainsForEver) {
    InMemoryStateBackend b;
    ListState<std::int64_t, std::string> l(b, kOp, "lst", int64_codec(), string_codec());
    l.add(1, "a");
    EXPECT_EQ(l.get(1).size(), 1U);
}

TEST(TypedStateTtl, AnExpiredListReadsAsEmptyAndTheNextAddStartsFresh) {
    InMemoryStateBackend b;
    auto l = list_with(b, event_ttl(1000ms));
    l.advance_watermark(10'000);
    l.add(1, "a");
    l.add(1, "b");
    ASSERT_EQ(l.get(1).size(), 2U);

    l.advance_watermark(20'000);
    // Expired reads as EMPTY, not as an error: the same rule ValueState
    // follows for a late record targeting expired state.
    EXPECT_TRUE(l.get(1).empty());
    EXPECT_TRUE(l.empty(1));

    l.add(1, "c");
    const auto after = l.get(1);
    ASSERT_EQ(after.size(), 1U) << "the expired list was resurrected rather than restarted";
    EXPECT_EQ(after[0], "c");
}

TEST(TypedStateTtl, AppendingRefreshesTheWholeList) {
    InMemoryStateBackend b;
    auto l = list_with(b, event_ttl(1000ms));
    l.advance_watermark(10'000);
    l.add(1, "a");
    // Append just before the deadline: the whole list's clock restarts,
    // which is the intended reading of "keep a key while it is active".
    l.advance_watermark(10'900);
    l.add(1, "b");
    l.advance_watermark(11'500);  // past the ORIGINAL deadline
    EXPECT_EQ(l.get(1).size(), 2U) << "an actively-appended list was expired on its first deadline";
}

TEST(TypedStateTtl, ReadingDoesNotRefreshAList) {
    InMemoryStateBackend b;
    auto l = list_with(b, event_ttl(1000ms));
    l.advance_watermark(10'000);
    l.add(1, "a");
    l.advance_watermark(10'900);
    (void)l.get(1);  // a read, not a write
    l.advance_watermark(11'001);
    EXPECT_TRUE(l.get(1).empty()) << "reading kept the list alive; retention would then depend on "
                                     "read traffic rather than on writes";
}

TEST(TypedStateTtl, ListCleanupReleasesMemoryRatherThanHiding) {
    InMemoryStateBackend b;
    auto l = list_with(b, event_ttl(10ms));
    l.advance_watermark(1'000);
    for (int i = 0; i < 30; ++i) {
        l.add(i, "x");
    }
    l.advance_watermark(100'000);
    EXPECT_EQ(resident(b), 30U) << "nothing has read them, so nothing has purged them";
    EXPECT_TRUE(l.get(0).empty()) << "but they are correctly invisible";

    l.cleanup_batch(1000);
    EXPECT_EQ(resident(b), 0U) << "expired lists were hidden but never released";
}

// --- MapState ---------------------------------------------------------------

TEST(TypedStateTtl, AnExpiredMapReadsAsAbsent) {
    InMemoryStateBackend b;
    auto m = map_with(b, event_ttl(1000ms));
    m.advance_watermark(10'000);
    m.put(1, "a", 10);
    ASSERT_TRUE(m.contains(1, "a"));

    m.advance_watermark(20'000);
    EXPECT_FALSE(m.contains(1, "a"));
    EXPECT_FALSE(m.get(1, "a").has_value());
    EXPECT_TRUE(m.entries(1).empty());
}

TEST(TypedStateTtl, MapRetentionIsPerKeyNotPerEntry) {
    // The semantics most likely to be guessed wrong. The whole map for a
    // key is ONE KeyedState value, so it lives and dies as a unit: touching
    // one entry keeps every other entry for that key alive too.
    //
    // Stated as a test rather than only in a comment because a caller who
    // assumes per-entry expiry would size their state completely wrongly.
    InMemoryStateBackend b;
    auto m = map_with(b, event_ttl(1000ms));
    m.advance_watermark(10'000);
    m.put(1, "cold", 1);
    m.advance_watermark(10'900);
    m.put(1, "hot", 2);  // refreshes the WHOLE key

    m.advance_watermark(11'500);  // past 'cold's own notional deadline
    EXPECT_TRUE(m.contains(1, "cold"))
        << "per-entry expiry is not what this provides; the whole key is refreshed together";
    EXPECT_TRUE(m.contains(1, "hot"));

    // And they expire together too.
    m.advance_watermark(12'000);
    EXPECT_FALSE(m.contains(1, "hot"));
    EXPECT_FALSE(m.contains(1, "cold"));
}

TEST(TypedStateTtl, DifferentKeysExpireIndependently) {
    InMemoryStateBackend b;
    auto m = map_with(b, event_ttl(1000ms));
    m.advance_watermark(10'000);
    m.put(1, "a", 1);
    m.advance_watermark(10'800);
    m.put(2, "a", 2);
    m.advance_watermark(11'200);  // key 1 expired, key 2 not
    EXPECT_FALSE(m.contains(1, "a"));
    EXPECT_TRUE(m.contains(2, "a"));
}

TEST(TypedStateTtl, MapCleanupReleasesMemory) {
    InMemoryStateBackend b;
    auto m = map_with(b, event_ttl(10ms));
    m.advance_watermark(1'000);
    for (int i = 0; i < 25; ++i) {
        m.put(i, "k", i);
    }
    m.advance_watermark(100'000);
    ASSERT_EQ(resident(b), 25U);
    m.cleanup_batch(1000);
    EXPECT_EQ(resident(b), 0U);
}

// --- AggregatingState / ReducingState ---------------------------------------

TEST(TypedStateTtl, AnExpiredAccumulatorRestartsFromTheInitialValue) {
    InMemoryStateBackend b;
    AggregatingState<std::int64_t, std::int64_t, std::int64_t, std::int64_t> agg(
        b,
        kOp,
        "agg",
        int64_codec(),
        int64_codec(),
        [] { return std::int64_t{0}; },
        [](const std::int64_t& acc, const std::int64_t& in) { return acc + in; },
        [](const std::int64_t& acc) { return acc; },
        event_ttl(1000ms));

    agg.advance_watermark(10'000);
    agg.add(1, 5);
    agg.add(1, 5);
    ASSERT_EQ(agg.get(1).value_or(-1), 10);

    agg.advance_watermark(20'000);
    EXPECT_FALSE(agg.get(1).has_value()) << "the expired accumulator was still readable";
    agg.add(1, 3);
    EXPECT_EQ(agg.get(1).value_or(-1), 3)
        << "the accumulator resumed from its expired value instead of restarting";
}

TEST(TypedStateTtl, AnExpiredReductionRestarts) {
    InMemoryStateBackend b;
    ReducingState<std::int64_t, std::int64_t> red(
        b,
        kOp,
        "red",
        int64_codec(),
        int64_codec(),
        [](const std::int64_t& a, const std::int64_t& c) { return a > c ? a : c; },
        event_ttl(1000ms));

    red.advance_watermark(10'000);
    red.add(1, 7);
    ASSERT_EQ(red.get(1).value_or(-1), 7);

    red.advance_watermark(20'000);
    EXPECT_FALSE(red.get(1).has_value());
    red.add(1, 2);
    EXPECT_EQ(red.get(1).value_or(-1), 2) << "the reduction kept its pre-expiry maximum";
}

// --- shared guarantees ------------------------------------------------------

TEST(TypedStateTtl, NothingExpiresBeforeTheFirstWatermark) {
    InMemoryStateBackend b;
    auto l = list_with(b, event_ttl(1ms));
    l.add(1, "a");
    EXPECT_EQ(l.get(1).size(), 1U)
        << "the collection was expired against a watermark that had not arrived";
    EXPECT_EQ(l.cleanup_batch(100), 0U) << "cleanup ran before event time existed";
}

TEST(TypedStateTtl, RestoreResumesTheOriginalDeadline) {
    InMemoryStateBackend b;
    {
        auto l = list_with(b, event_ttl(1000ms));
        l.advance_watermark(10'000);
        l.add(1, "a");  // expires at 11'000
    }
    const auto snap = b.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    auto l2 = list_with(restored, event_ttl(1000ms));

    l2.advance_watermark(10'999);
    EXPECT_EQ(l2.get(1).size(), 1U) << "the restored list expired early";
    l2.advance_watermark(11'000);
    EXPECT_TRUE(l2.get(1).empty())
        << "the restore extended the list's life instead of resuming its deadline";
}

TEST(TypedStateTtl, ProcessingTimeIsAvailableForStreamsWithoutWatermarks) {
    // Processing time is injected rather than slept through: a sleep asserts
    // the eviction AND bets on the scheduler, and the margin is what a loaded
    // runner loses (see the seam's own note in keyed_state.hpp).
    InMemoryStateBackend b;
    ListState<std::int64_t, std::string> l(
        b,
        kOp,
        "lst",
        int64_codec(),
        string_codec(),
        TtlConfig{.ttl = 30ms, .clock_ms = &typed_ttl_fake_clock});
    l.add(1, "a");
    EXPECT_EQ(l.get(1).size(), 1U);
    typed_advance(60ms);
    EXPECT_TRUE(l.get(1).empty());
}

}  // namespace
