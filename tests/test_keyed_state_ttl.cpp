// KeyedState time-to-live: opt-in TtlConfig on construction. Stored
// values get an 8-byte expire-at prefix that get/scan check and
// short-circuit on; expired entries are lazy-purged on the first
// get() that observes them.
//
// Time is INJECTED, not slept through. Every case here drives
// TtlConfig::clock_ms and steps it explicitly, which buys three things a
// sleep cannot:
//
//   * determinism. A 100ms TTL crossed by a 150ms sleep is a race the test
//     usually wins. On a loaded runner or under a sanitizer it does not, and
//     the failure looks like a TTL bug.
//   * exact boundaries. Expiry is `now >= expire_at`, and only a controlled
//     clock can sit ON the boundary to prove which comparison is used.
//     TtlIsInclusiveAtTheExpiryInstant below is the case a sleep cannot
//     write.
//   * speed, which is the least interesting of the three.
//
// The clock is a raw function pointer on TtlConfig, so the fake needs a
// file-scope value for it to read. Named distinctively rather than something
// generic: identically-named helpers in two anonymous namespaces in the same
// binary have collided here before.

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// The fake clock's reading, in ms. Starts well away from zero so a test
// cannot pass by accident on a value that happens to look like an unset
// field.
std::int64_t g_keyed_ttl_fake_now_ms = 1'000'000;

std::int64_t keyed_ttl_fake_clock() {
    return g_keyed_ttl_fake_now_ms;
}

// Each test sets its own starting point: leaking the previous test's reading
// would make the order they run in matter.
void reset_clock() {
    g_keyed_ttl_fake_now_ms = 1'000'000;
}

void advance(std::chrono::milliseconds by) {
    g_keyed_ttl_fake_now_ms += by.count();
}

TtlConfig short_ttl(bool refresh_on_read = false) {
    TtlConfig c;
    c.ttl = 100ms;
    c.refresh_on_write = true;
    c.refresh_on_read = refresh_on_read;
    c.clock_ms = &keyed_ttl_fake_clock;
    return c;
}

}  // namespace

TEST(KeyedStateTtl, NoTtlBehavesLikeBeforeAndKeepsValuesIndefinitely) {
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "no_ttl", string_codec(), int64_codec());
    kv.put("k", 99);
    // A year, to make the point that "indefinitely" is not "longer than the
    // test's patience". No sleep could assert this.
    advance(365 * 24h);
    auto got = kv.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 99);
}

TEST(KeyedStateTtl, EnabledExpiresEntriesAfterTtlElapses) {
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "exp", string_codec(), int64_codec(), short_ttl());

    kv.put("alice", 1);
    kv.put("bob", 2);
    EXPECT_TRUE(kv.get("alice").has_value());
    EXPECT_TRUE(kv.get("bob").has_value());

    advance(150ms);

    EXPECT_FALSE(kv.get("alice").has_value()) << "expired entry must not surface via get()";
    EXPECT_FALSE(kv.get("bob").has_value());
}

TEST(KeyedStateTtl, AnEntryOneMillisecondShortOfExpiryStillReads) {
    // The other side of the boundary, and the reason a controlled clock is
    // worth the plumbing: a TTL that expired everything one tick early would
    // pass every sleep-based case in this file.
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "almost", string_codec(), int64_codec(), short_ttl());

    kv.put("k", 1);
    advance(99ms);
    auto got = kv.get("k");
    ASSERT_TRUE(got.has_value()) << "an entry 1ms short of its 100ms TTL must still be readable";
    EXPECT_EQ(*got, 1);
}

TEST(KeyedStateTtl, TtlIsInclusiveAtTheExpiryInstant) {
    // Pins which comparison expiry uses. keyed_state.hpp checks
    // `now >= expire_at`, so a value written at T with a 100ms TTL is gone at
    // exactly T+100 rather than surviving to T+101. Unpinned, either
    // behaviour passes, and an off-by-one that changed it would surface as a
    // retention bug wherever a TTL is set to an exact window length.
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "boundary", string_codec(), int64_codec(), short_ttl());

    kv.put("k", 1);
    advance(100ms);
    EXPECT_FALSE(kv.get("k").has_value())
        << "expiry is inclusive: at exactly expire_at the entry is gone";
}

TEST(KeyedStateTtl, GetLazyPurgesExpiredEntries) {
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "purge", string_codec(), int64_codec(), short_ttl());

    kv.put("k", 7);
    advance(150ms);
    // First get observes the stale entry and purges it. Subsequent
    // backend scans should not see the slot at all.
    EXPECT_FALSE(kv.get("k").has_value());
    std::size_t seen_in_backend = 0;
    backend.scan(OperatorId{1},
                 [&](StateBackend::KeyView, StateBackend::ValueView) { ++seen_in_backend; });
    EXPECT_EQ(seen_in_backend, 0u) << "lazy purge should remove the entry from the backend";
}

TEST(KeyedStateTtl, RefreshOnWriteResetsExpiry) {
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "refresh", string_codec(), int64_codec(), short_ttl());

    kv.put("k", 1);
    advance(70ms);   // not yet expired
    kv.put("k", 2);  // refresh
    advance(70ms);   // 140ms since the original put, 70ms since the refresh
    auto got = kv.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 2);

    // And the refresh moved the deadline rather than merely delaying it: 31ms
    // more takes this clearly past the refreshed 100ms. Deliberately not
    // exactly 100ms after the refresh - the inclusive-boundary claim belongs
    // to the test named for it, and having two cases fail for one cause makes
    // a future change harder to diagnose.
    advance(31ms);
    EXPECT_FALSE(kv.get("k").has_value())
        << "refresh-on-write should reset the TTL, not extend it indefinitely";
}

TEST(KeyedStateTtl, RefreshOnReadKeepsActiveKeysAlive) {
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(backend,
                                             OperatorId{1},
                                             "read_refresh",
                                             string_codec(),
                                             int64_codec(),
                                             short_ttl(/*refresh_on_read=*/true));

    kv.put("k", 5);
    // Touch the key every 50ms - under the 100ms TTL - so it stays alive far
    // past the original expiry. Twenty iterations rather than four: with the
    // clock injected there is no reason to be shy, and 1000ms of simulated
    // life is a stronger statement than 200ms.
    for (int i = 0; i < 20; ++i) {
        advance(50ms);
        auto got = kv.get("k");
        ASSERT_TRUE(got.has_value()) << "iteration " << i << ": refresh-on-read should keep alive";
        EXPECT_EQ(*got, 5);
    }

    // Stop touching it and it goes, which is what makes the loop above
    // evidence of refreshing rather than of a TTL that never fires.
    advance(150ms);
    EXPECT_FALSE(kv.get("k").has_value())
        << "a key left untouched past its TTL must expire even with refresh_on_read";
}

TEST(KeyedStateTtl, ScanSkipsExpiredEntries) {
    reset_clock();
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "scan_ttl", string_codec(), int64_codec(), short_ttl());

    kv.put("alpha", 1);
    kv.put("beta", 2);
    advance(150ms);
    kv.put("gamma", 3);  // fresh after the others expired

    std::vector<std::pair<std::string, std::int64_t>> seen;
    kv.scan([&](const std::string& k, const std::int64_t& v) { seen.emplace_back(k, v); });
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].first, "gamma");
    EXPECT_EQ(seen[0].second, 3);
}
