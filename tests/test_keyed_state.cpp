#include <cstdint>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;

TEST(KeyedState, PutGetEraseRoundTrip) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "counts", string_codec(), int64_codec());

    kv.put("a", 5);
    kv.put("b", -7);
    kv.put("a", 50);  // overwrite

    auto a = kv.get("a");
    auto b = kv.get("b");
    auto c = kv.get("c");
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    EXPECT_EQ(*a, 50);
    EXPECT_EQ(*b, -7);
    EXPECT_FALSE(c.has_value());

    kv.erase("a");
    EXPECT_FALSE(kv.get("a").has_value());
    EXPECT_TRUE(kv.get("b").has_value());
}

TEST(KeyedState, ScanVisitsAllPairs) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "totals", string_codec(), int64_codec());
    kv.put("apple", 1);
    kv.put("banana", 2);
    kv.put("cherry", 3);

    std::unordered_map<std::string, std::int64_t> seen;
    kv.scan([&](const std::string& k, const std::int64_t& v) { seen[k] = v; });

    EXPECT_EQ(seen.size(), 3u);
    EXPECT_EQ(seen["apple"], 1);
    EXPECT_EQ(seen["banana"], 2);
    EXPECT_EQ(seen["cherry"], 3);
}

TEST(KeyedState, EraseOfNonExistentKeyIsIdempotent) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "x", string_codec(), int64_codec());

    // Erasing a key that was never put must not throw, must not write
    // anything, and must leave a subsequent put visible.
    kv.erase("never-existed");
    kv.erase("never-existed");
    EXPECT_FALSE(kv.get("never-existed").has_value());

    kv.put("now-here", 7);
    EXPECT_EQ(*kv.get("now-here"), 7);
    kv.erase("now-here");
    kv.erase("now-here");  // double-erase
    EXPECT_FALSE(kv.get("now-here").has_value());
}

TEST(KeyedState, ScanVisitsNothingForEmptySlot) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{1}, "empty", string_codec(), int64_codec());
    int visits = 0;
    kv.scan([&](const std::string&, const std::int64_t&) { ++visits; });
    EXPECT_EQ(visits, 0);
}

TEST(KeyedState, RejectsSlotNameWithReservedDelimiters) {
    // '|' is the stored-key slot/user separator and '\n' is the
    // state-version pack delimiter; a slot name carrying either would make
    // the slot key space non-prefix-free and break slot-aware migration.
    // Reject at construction, matching StateVersionMap::set.
    auto backend = InMemoryStateBackend{};
    EXPECT_THROW((KeyedState<std::string, std::int64_t>(
                     backend, OperatorId{1}, "a|b", string_codec(), int64_codec())),
                 std::invalid_argument);
    EXPECT_THROW((KeyedState<std::string, std::int64_t>(
                     backend, OperatorId{1}, "a\nb", string_codec(), int64_codec())),
                 std::invalid_argument);
    // A plain name still constructs.
    EXPECT_NO_THROW((KeyedState<std::string, std::int64_t>(
        backend, OperatorId{1}, "ok_slot", string_codec(), int64_codec())));
}

TEST(KeyedState, SlotNamespacingPreventsCollisions) {
    auto backend = InMemoryStateBackend{};

    KeyedState<std::string, std::int64_t> a(
        backend, OperatorId{1}, "slot_a", string_codec(), int64_codec());
    KeyedState<std::string, std::int64_t> b(
        backend, OperatorId{1}, "slot_b", string_codec(), int64_codec());

    a.put("k", 100);
    b.put("k", 200);

    EXPECT_EQ(*a.get("k"), 100);
    EXPECT_EQ(*b.get("k"), 200);
}

// Verify the stored-key layout: first byte is the FNV-1a-derived key
// group over the encoded user key bytes (slot bytes are NOT included in
// the hash input). This is the contract that lets restore() filter by
// key-group range without parsing slot names or running user codecs.
TEST(KeyedState, StoredKeyStartsWithKeyGroupPrefixByte) {
    auto backend = InMemoryStateBackend{};
    KeyedState<std::string, std::int64_t> kv(
        backend, OperatorId{42}, "slot", string_codec(), int64_codec());

    kv.put("alice", 1);

    std::string seen_raw_key;
    backend.scan(OperatorId{42},
                 [&](StateBackend::KeyView k, StateBackend::ValueView) { seen_raw_key.assign(k); });
    ASSERT_FALSE(seen_raw_key.empty());

    // The first byte should be key_group_for_key over the user key codec
    // output. string_codec encodes "alice" as [u32 len LE][bytes].
    const auto encoded = string_codec().encode(std::string{"alice"});
    const auto expected_kg =
        key_group_for_key(std::span<const std::byte>{encoded.data(), encoded.size()});
    EXPECT_EQ(static_cast<std::uint8_t>(seen_raw_key.front()),
              static_cast<std::uint8_t>(expected_kg));
}
