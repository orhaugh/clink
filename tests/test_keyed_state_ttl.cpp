// KeyedState time-to-live: opt-in TtlConfig on construction. Stored
// values get an 8-byte expire-at prefix that get/scan check and
// short-circuit on; expired entries are lazy-purged on the first
// get() that observes them.
//
// Tests use sleep_for to cross actual wall-clock boundaries (// TTL is also wall-clock by default).
// The TTL values are deliberately small (100ms) so the suite stays fast; the underlying logic is
// identical at longer horizons.

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

TtlConfig short_ttl(bool refresh_on_read = false) {
    TtlConfig c;
    c.ttl = 100ms;
    c.refresh_on_write = true;
    c.refresh_on_read = refresh_on_read;
    return c;
}

}  // namespace

TEST(KeyedStateTtl, NoTtlBehavesLikeBeforeAndKeepsValuesIndefinitely) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "no_ttl", string_codec(), int64_codec());
    kv.put("k", 99);
    std::this_thread::sleep_for(150ms);
    auto got = kv.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 99);
}

TEST(KeyedStateTtl, EnabledExpiresEntriesAfterTtlElapses) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "exp", string_codec(), int64_codec(), short_ttl());

    kv.put("alice", 1);
    kv.put("bob", 2);
    EXPECT_TRUE(kv.get("alice").has_value());
    EXPECT_TRUE(kv.get("bob").has_value());

    std::this_thread::sleep_for(150ms);

    EXPECT_FALSE(kv.get("alice").has_value()) << "expired entry must not surface via get()";
    EXPECT_FALSE(kv.get("bob").has_value());
}

TEST(KeyedStateTtl, GetLazyPurgesExpiredEntries) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "purge", string_codec(), int64_codec(), short_ttl());

    kv.put("k", 7);
    std::this_thread::sleep_for(150ms);
    // First get observes the stale entry and purges it. Subsequent
    // backend scans should not see the slot at all.
    EXPECT_FALSE(kv.get("k").has_value());
    std::size_t seen_in_backend = 0;
    backend.scan(OperatorId{1},
                 [&](StateBackend::KeyView, StateBackend::ValueView) { ++seen_in_backend; });
    EXPECT_EQ(seen_in_backend, 0u) << "lazy purge should remove the entry from the backend";
}

TEST(KeyedStateTtl, RefreshOnWriteResetsExpiry) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "refresh", string_codec(), int64_codec(), short_ttl());

    kv.put("k", 1);
    std::this_thread::sleep_for(70ms);  // not yet expired
    kv.put("k", 2);                     // refresh
    std::this_thread::sleep_for(70ms);  // 140ms since original put, 70ms since refresh
    auto got = kv.get("k");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 2);
}

TEST(KeyedStateTtl, RefreshOnReadKeepsActiveKeysAlive) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(backend,
                                             OperatorId{1},
                                             "read_refresh",
                                             string_codec(),
                                             int64_codec(),
                                             short_ttl(/*refresh_on_read=*/true));

    kv.put("k", 5);
    // Touch the key every 50ms - under the 100ms TTL - so it stays
    // alive past the original 100ms expiry.
    for (int i = 0; i < 4; ++i) {
        std::this_thread::sleep_for(50ms);
        auto got = kv.get("k");
        ASSERT_TRUE(got.has_value()) << "iteration " << i << ": refresh-on-read should keep alive";
        EXPECT_EQ(*got, 5);
    }
}

TEST(KeyedStateTtl, ScanSkipsExpiredEntries) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "scan_ttl", string_codec(), int64_codec(), short_ttl());

    kv.put("alpha", 1);
    kv.put("beta", 2);
    std::this_thread::sleep_for(150ms);
    kv.put("gamma", 3);  // fresh after the others expired

    std::vector<std::pair<std::string, std::int64_t>> seen;
    kv.scan([&](const std::string& k, const std::int64_t& v) { seen.emplace_back(k, v); });
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].first, "gamma");
    EXPECT_EQ(seen[0].second, 3);
}
