// Per-ELEMENT retention for collection state.
//
// The types in typed_state.hpp expire a whole collection as a unit,
// because the collection IS one KeyedState value. These types give each
// element its own backend entry and therefore its own deadline. The tests
// that matter are the ones that distinguish the two, so the choice between
// them stays a real choice rather than a coin flip.

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/expiring_collection_state.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/typed_state.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

constexpr OperatorId kOp{51};

TtlConfig event_ttl(std::chrono::milliseconds ttl) {
    return TtlConfig{.ttl = ttl, .refresh_on_write = true, .domain = TtlTimeDomain::EventTime};
}

ExpiringMapState<std::int64_t, std::string, std::int64_t> exp_map(StateBackend& b, TtlConfig ttl) {
    return ExpiringMapState<std::int64_t, std::string, std::int64_t>(
        b, kOp, "xmap", int64_codec(), string_codec(), int64_codec(), ttl);
}

ExpiringListState<std::int64_t, std::string> exp_list(StateBackend& b, TtlConfig ttl) {
    return ExpiringListState<std::int64_t, std::string>(
        b, kOp, "xlist", int64_codec(), string_codec(), ttl);
}

std::size_t resident(StateBackend& b) {
    std::size_t n = 0;
    b.scan(kOp, [&](std::string_view, std::string_view) { ++n; });
    return n;
}

// --- the distinction from per-key -------------------------------------------

TEST(ExpiringCollectionState, MapEntriesExpireIndividually) {
    // The whole point. MapState refreshes every entry for a key when any
    // one is touched; here a cold entry ages out while a hot one survives.
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(1000ms));
    m.advance_watermark(10'000);
    m.put(1, "cold", 1);
    m.advance_watermark(10'900);
    m.put(1, "hot", 2);  // refreshes ONLY "hot"

    m.advance_watermark(11'200);  // past cold's deadline, inside hot's
    EXPECT_FALSE(m.contains(1, "cold")) << "the cold entry survived; expiry is not per element";
    EXPECT_TRUE(m.contains(1, "hot")) << "the hot entry was expired with the cold one";

    const auto entries = m.entries(1);
    ASSERT_EQ(entries.size(), 1U);
    EXPECT_EQ(entries[0].first, "hot");
}

TEST(ExpiringCollectionState, PerKeyAndPerElementDifferObservably) {
    // Same workload through both types, asserting they genuinely behave
    // differently - so a reader can see which one they want rather than
    // taking the header's word for it.
    InMemoryStateBackend b1;
    MapState<std::int64_t, std::string, std::int64_t> per_key(
        b1, kOp, "mp", int64_codec(), string_codec(), int64_codec(), event_ttl(1000ms));
    per_key.advance_watermark(10'000);
    per_key.put(1, "cold", 1);
    per_key.advance_watermark(10'900);
    per_key.put(1, "hot", 2);
    per_key.advance_watermark(11'200);
    EXPECT_TRUE(per_key.contains(1, "cold")) << "per-key retention should keep the cold entry";

    InMemoryStateBackend b2;
    auto per_elem = exp_map(b2, event_ttl(1000ms));
    per_elem.advance_watermark(10'000);
    per_elem.put(1, "cold", 1);
    per_elem.advance_watermark(10'900);
    per_elem.put(1, "hot", 2);
    per_elem.advance_watermark(11'200);
    EXPECT_FALSE(per_elem.contains(1, "cold")) << "per-element retention should drop it";
}

TEST(ExpiringCollectionState, EachElementIsItsOwnBackendEntry) {
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(1000ms));
    m.advance_watermark(10'000);
    for (int i = 0; i < 10; ++i) {
        m.put(1, "k" + std::to_string(i), i);
    }
    // Ten entries under ONE user key - the representation that makes
    // per-element expiry possible, and the cost that comes with it.
    EXPECT_EQ(resident(b), 10U);
}

// --- map behaviour -----------------------------------------------------------

TEST(ExpiringCollectionState, MapBasicsRoundTrip) {
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(10'000ms));
    m.advance_watermark(1'000);
    m.put(1, "a", 10);
    m.put(1, "b", 20);
    m.put(2, "a", 30);

    EXPECT_EQ(m.get(1, "a").value_or(-1), 10);
    EXPECT_EQ(m.get(1, "b").value_or(-1), 20);
    EXPECT_EQ(m.get(2, "a").value_or(-1), 30);
    EXPECT_FALSE(m.get(1, "missing").has_value());

    auto e1 = m.entries(1);
    EXPECT_EQ(e1.size(), 2U) << "entries() leaked across user keys";
    EXPECT_EQ(m.entries(2).size(), 1U);
}

TEST(ExpiringCollectionState, MapRemoveAndClearAreScopedToTheirKey) {
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(10'000ms));
    m.advance_watermark(1'000);
    m.put(1, "a", 1);
    m.put(1, "b", 2);
    m.put(2, "a", 3);

    m.remove(1, "a");
    EXPECT_FALSE(m.contains(1, "a"));
    EXPECT_TRUE(m.contains(1, "b"));
    EXPECT_TRUE(m.contains(2, "a")) << "remove reached into another key";

    m.clear(1);
    EXPECT_TRUE(m.entries(1).empty());
    EXPECT_EQ(m.entries(2).size(), 1U) << "clear reached into another key";
}

TEST(ExpiringCollectionState, MapPutRefreshesOnlyThatEntry) {
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(1000ms));
    m.advance_watermark(10'000);
    m.put(1, "a", 1);
    m.put(1, "b", 2);
    m.advance_watermark(10'900);
    m.put(1, "a", 99);  // refresh a only

    m.advance_watermark(11'500);
    EXPECT_EQ(m.get(1, "a").value_or(-1), 99);
    EXPECT_FALSE(m.contains(1, "b")) << "refreshing one entry kept another alive";
}

TEST(ExpiringCollectionState, ExpiredMapEntriesAreReleasedByCleanup) {
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(10ms));
    m.advance_watermark(1'000);
    for (int i = 0; i < 20; ++i) {
        m.put(1, "k" + std::to_string(i), i);
    }
    m.advance_watermark(100'000);
    EXPECT_EQ(resident(b), 20U) << "nothing read them, so nothing purged them";
    EXPECT_TRUE(m.entries(1).empty()) << "but they are correctly invisible";
    m.cleanup_batch(1000);
    EXPECT_EQ(resident(b), 0U) << "expired entries were hidden but not released";
}

// --- list behaviour ----------------------------------------------------------

TEST(ExpiringCollectionState, ListPreservesInsertionOrder) {
    InMemoryStateBackend b;
    auto l = exp_list(b, event_ttl(10'000ms));
    l.advance_watermark(1'000);
    l.add(1, "first");
    l.add(1, "second");
    l.add(1, "third");
    EXPECT_EQ(l.get(1), (std::vector<std::string>{"first", "second", "third"}));
}

TEST(ExpiringCollectionState, ListKeepsDuplicateValuesAsDistinctElements) {
    // Each append gets its own sequence, so two equal values are two
    // entries with two deadlines - not one entry that keeps getting
    // refreshed.
    InMemoryStateBackend b;
    auto l = exp_list(b, event_ttl(10'000ms));
    l.advance_watermark(1'000);
    l.add(1, "x");
    l.add(1, "x");
    EXPECT_EQ(l.get(1).size(), 2U);
    EXPECT_EQ(resident(b), 2U);
}

TEST(ExpiringCollectionState, ListElementsExpireIndividuallyAndInOrder) {
    InMemoryStateBackend b;
    auto l = exp_list(b, event_ttl(1000ms));
    l.advance_watermark(10'000);
    l.add(1, "old");
    l.advance_watermark(10'800);
    l.add(1, "new");

    l.advance_watermark(11'200);  // past old's deadline only
    EXPECT_EQ(l.get(1), (std::vector<std::string>{"new"}))
        << "list elements did not expire independently";
}

TEST(ExpiringCollectionState, ListsAreScopedToTheirKey) {
    InMemoryStateBackend b;
    auto l = exp_list(b, event_ttl(10'000ms));
    l.advance_watermark(1'000);
    l.add(1, "a");
    l.add(2, "b");
    EXPECT_EQ(l.get(1), (std::vector<std::string>{"a"}));
    EXPECT_EQ(l.get(2), (std::vector<std::string>{"b"}));
    l.clear(1);
    EXPECT_TRUE(l.empty(1));
    EXPECT_EQ(l.get(2), (std::vector<std::string>{"b"})) << "clear reached into another key";
}

TEST(ExpiringCollectionState, ARestoredListContinuesRatherThanOverwritingItsTail) {
    // The sequence counter is in memory. After a restart it must recover
    // the high-water mark from what is stored, or the first append would
    // reuse seq 0 and overwrite the oldest surviving element.
    InMemoryStateBackend b;
    {
        auto l = exp_list(b, event_ttl(100'000ms));
        l.advance_watermark(1'000);
        l.add(1, "a");
        l.add(1, "b");
    }
    const auto snap = b.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    auto l2 = exp_list(restored, event_ttl(100'000ms));
    l2.advance_watermark(1'000);
    l2.add(1, "c");

    EXPECT_EQ(l2.get(1), (std::vector<std::string>{"a", "b", "c"}))
        << "the restored list overwrote an existing element instead of appending";
}

// --- shared guarantees -------------------------------------------------------

TEST(ExpiringCollectionState, NothingExpiresBeforeTheFirstWatermark) {
    InMemoryStateBackend b;
    auto m = exp_map(b, event_ttl(1ms));
    m.put(1, "a", 1);
    EXPECT_TRUE(m.contains(1, "a"))
        << "an entry was expired against a watermark that had not arrived";
}

TEST(ExpiringCollectionState, RestoreResumesEachElementsOwnDeadline) {
    InMemoryStateBackend b;
    {
        auto m = exp_map(b, event_ttl(1000ms));
        m.advance_watermark(10'000);
        m.put(1, "early", 1);  // expires at 11'000
        m.advance_watermark(10'500);
        m.put(1, "late", 2);  // expires at 11'500
    }
    const auto snap = b.snapshot(CheckpointId{1});

    InMemoryStateBackend restored;
    restored.restore(snap);
    auto m2 = exp_map(restored, event_ttl(1000ms));

    m2.advance_watermark(11'200);
    EXPECT_FALSE(m2.contains(1, "early")) << "a restored element outlived its deadline";
    EXPECT_TRUE(m2.contains(1, "late")) << "a restored element expired early";
}

TEST(ExpiringCollectionState, ProcessingTimeWorksForStreamsWithoutWatermarks) {
    InMemoryStateBackend b;
    auto m = exp_map(b, TtlConfig{.ttl = 30ms});
    m.put(1, "a", 1);
    EXPECT_TRUE(m.contains(1, "a"));
    std::this_thread::sleep_for(60ms);
    EXPECT_FALSE(m.contains(1, "a"));
}

}  // namespace
