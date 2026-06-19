// Codec invariant tests. The codec layer underpins every connector, the
// keyed-state machinery, and the on-disk window-state format, so silent
// regressions here corrupt downstream state without surfacing as crashes.
//
// These tests fix:
//   - Round-trip identity for every built-in codec.
//   - Boundary values (empty, zero, max, signed two's-complement).
//   - Truncation rejection: decode must return nullopt on undersized
//     buffers, never read past the end.
//   - Composition: nested vector<pair<...>> codecs round-trip.

#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"

using namespace clink;

namespace {

template <typename T>
T expect_round_trip(const Codec<T>& c, const T& value) {
    const auto bytes = c.encode(value);
    const auto result = c.decode(bytes);
    EXPECT_TRUE(result.has_value());
    return result.value_or(T{});
}

}  // namespace

// ----- string -----

TEST(Codec, StringRoundTripsAscii) {
    auto c = string_codec();
    EXPECT_EQ(expect_round_trip(c, std::string{"hello"}), "hello");
}

TEST(Codec, StringRoundTripsEmpty) {
    auto c = string_codec();
    auto bytes = c.encode("");
    EXPECT_TRUE(bytes.empty());
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_EQ(*out, "");
}

TEST(Codec, StringRoundTripsBinaryBytes) {
    // String codec is byte-transparent: nulls and high bytes survive.
    std::string s;
    s.push_back('\0');
    s.push_back('\xFF');
    s.push_back('a');
    s.push_back('\0');
    auto c = string_codec();
    auto out = expect_round_trip(c, s);
    EXPECT_EQ(out.size(), s.size());
    EXPECT_EQ(out, s);
}

// ----- uint64 -----

TEST(Codec, Uint64RoundTripsZero) {
    EXPECT_EQ(expect_round_trip(uint64_codec(), std::uint64_t{0}), 0u);
}

TEST(Codec, Uint64RoundTripsMax) {
    EXPECT_EQ(expect_round_trip(uint64_codec(), std::numeric_limits<std::uint64_t>::max()),
              std::numeric_limits<std::uint64_t>::max());
}

TEST(Codec, Uint64IsLittleEndianStable) {
    // Snapshot the on-wire bytes so an accidental endianness flip is
    // caught - keyed-state files must remain readable across hosts.
    auto c = uint64_codec();
    const auto bytes = c.encode(std::uint64_t{0x0102030405060708ULL});
    ASSERT_EQ(bytes.size(), 8u);
    EXPECT_EQ(static_cast<unsigned char>(bytes[0]), 0x08);
    EXPECT_EQ(static_cast<unsigned char>(bytes[7]), 0x01);
}

TEST(Codec, Uint64RejectsTruncatedBuffer) {
    auto c = uint64_codec();
    std::vector<std::byte> short_buf(7);  // one byte short
    EXPECT_FALSE(c.decode(short_buf).has_value());
}

TEST(Codec, Uint64RejectsOversizedBuffer) {
    auto c = uint64_codec();
    std::vector<std::byte> long_buf(9);  // strict size match
    EXPECT_FALSE(c.decode(long_buf).has_value());
}

// ----- uint32 -----

TEST(Codec, Uint32RoundTripsBoundaryValues) {
    auto c = uint32_codec();
    EXPECT_EQ(expect_round_trip(c, std::uint32_t{0}), 0u);
    EXPECT_EQ(expect_round_trip(c, std::numeric_limits<std::uint32_t>::max()),
              std::numeric_limits<std::uint32_t>::max());
}

// ----- int64 -----

TEST(Codec, Int64RoundTripsNegativeAndMin) {
    auto c = int64_codec();
    EXPECT_EQ(expect_round_trip(c, std::int64_t{-1}), -1);
    EXPECT_EQ(expect_round_trip(c, std::numeric_limits<std::int64_t>::min()),
              std::numeric_limits<std::int64_t>::min());
    EXPECT_EQ(expect_round_trip(c, std::numeric_limits<std::int64_t>::max()),
              std::numeric_limits<std::int64_t>::max());
}

// ----- vector -----

TEST(Codec, VectorRoundTripsEmpty) {
    auto c = vector_codec(int64_codec());
    std::vector<std::int64_t> v;
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, VectorRoundTripsValues) {
    auto c = vector_codec(int64_codec());
    std::vector<std::int64_t> v{-100, 0, 1, std::numeric_limits<std::int64_t>::max()};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, VectorOfStringsRoundTripsVariableWidth) {
    auto c = vector_codec(string_codec());
    std::vector<std::string> v{"", "a", "longer string", std::string(1024, 'x')};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, VectorRejectsTruncatedBuffer) {
    auto c = vector_codec(int64_codec());
    const auto bytes = c.encode(std::vector<std::int64_t>{1, 2, 3});
    // Drop the last byte - the inner-element length-prefix now claims
    // more bytes than remain. Decode must reject, not read OOB.
    std::vector<std::byte> truncated(bytes.begin(), bytes.end() - 1);
    EXPECT_FALSE(c.decode(truncated).has_value());
}

TEST(Codec, VectorRejectsHeaderOnlyBuffer) {
    auto c = vector_codec(int64_codec());
    std::vector<std::byte> only_count(4);  // claims 0 elements - valid case
    auto ok = c.decode(only_count);
    ASSERT_TRUE(ok.has_value());
    EXPECT_TRUE(ok->empty());

    std::vector<std::byte> too_short(3);  // can't even read count
    EXPECT_FALSE(c.decode(too_short).has_value());
}

// ----- pair -----

TEST(Codec, PairRoundTripsHomogeneous) {
    auto c = pair_codec(int64_codec(), int64_codec());
    std::pair<std::int64_t, std::int64_t> p{42, -7};
    EXPECT_EQ(expect_round_trip(c, p), p);
}

TEST(Codec, PairRoundTripsHeterogeneous) {
    auto c = pair_codec(string_codec(), int64_codec());
    std::pair<std::string, std::int64_t> p{"window-key", 123456789LL};
    EXPECT_EQ(expect_round_trip(c, p), p);
}

TEST(Codec, PairRejectsTruncatedBuffer) {
    auto c = pair_codec(string_codec(), int64_codec());
    auto bytes = c.encode({"x", 1});
    std::vector<std::byte> truncated(bytes.begin(), bytes.end() - 1);
    EXPECT_FALSE(c.decode(truncated).has_value());
}

TEST(Codec, PairRejectsHeaderOnly) {
    auto c = pair_codec(string_codec(), int64_codec());
    // Header claims first-half length = 0; remaining bytes for B = 0.
    // int64 decoder requires exactly 8 bytes, so the inner decode must
    // fail and the pair decode must propagate nullopt - and it must not
    // crash.
    std::vector<std::byte> only_len(4);
    EXPECT_FALSE(c.decode(only_len).has_value());
}

// ----- composition -----

TEST(Codec, NestedVectorOfPairsRoundTrips) {
    auto inner = pair_codec(string_codec(), int64_codec());
    auto outer = vector_codec(inner);

    std::vector<std::pair<std::string, std::int64_t>> v{
        {"alice", 1},
        {"bob", -2},
        {"", 0},
        {std::string(64, 'k'), std::numeric_limits<std::int64_t>::min()},
    };
    EXPECT_EQ(expect_round_trip(outer, v), v);
}

// ----- Type-instantiation diversity -----
//
// Each unique <T> in vector_codec<T>/pair_codec<T1,T2>/le_uint_codec<T>
// is a separate function in gcov terms. Exercising more combinations
// here lifts function coverage on core/codec.hpp without changing
// behaviour.

TEST(Codec, VectorOfStringsHandlesEmpty) {
    auto c = vector_codec(string_codec());
    std::vector<std::string> v;
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, VectorOfUint64RoundTrips) {
    auto c = vector_codec(uint64_codec());
    std::vector<std::uint64_t> v{0u, 1u, 100u, std::numeric_limits<std::uint64_t>::max()};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, VectorOfUint32RoundTrips) {
    auto c = vector_codec(uint32_codec());
    std::vector<std::uint32_t> v{1u, 2u, 3u, 4u, 5u};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, NestedVectorOfVectorRoundTrips) {
    auto inner = vector_codec(int64_codec());
    auto outer = vector_codec(inner);

    std::vector<std::vector<std::int64_t>> v{
        {1, 2, 3},
        {},
        {-100},
        {0, std::numeric_limits<std::int64_t>::min(), std::numeric_limits<std::int64_t>::max()},
    };
    EXPECT_EQ(expect_round_trip(outer, v), v);
}

TEST(Codec, PairOfPairRoundTrips) {
    auto inner = pair_codec(int64_codec(), string_codec());
    auto outer = pair_codec(string_codec(), inner);

    std::pair<std::string, std::pair<std::int64_t, std::string>> v{"outer", {42, "inner"}};
    EXPECT_EQ(expect_round_trip(outer, v), v);
}

TEST(Codec, PairOfUint32AndUint64RoundTrips) {
    auto c = pair_codec(uint32_codec(), uint64_codec());
    std::pair<std::uint32_t, std::uint64_t> v{std::numeric_limits<std::uint32_t>::max(),
                                              std::numeric_limits<std::uint64_t>::max()};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, PairOfStringsRoundTrips) {
    auto c = pair_codec(string_codec(), string_codec());
    EXPECT_EQ(expect_round_trip(c, std::pair{std::string{""}, std::string{""}}),
              (std::pair{std::string{""}, std::string{""}}));
    EXPECT_EQ(expect_round_trip(c, std::pair{std::string{"key"}, std::string{"value"}}),
              (std::pair{std::string{"key"}, std::string{"value"}}));
}

// ----- trivial (POD memcpy) fallback codec -----

namespace {
struct PodMetric {
    std::int64_t timestamp;
    double value;
    std::uint32_t flags;
    bool operator==(const PodMetric&) const = default;
};
static_assert(std::is_trivially_copyable_v<PodMetric>);
}  // namespace

TEST(Codec, TrivialRoundTripsPodStruct) {
    auto c = clink::trivial_codec<PodMetric>();
    const PodMetric v{1700000000, 3.14159, 0xDEADBEEF};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, TrivialRejectsTruncatedBuffer) {
    auto c = clink::trivial_codec<PodMetric>();
    const PodMetric v{42, 1.5, 7};
    auto bytes = c.encode(v);
    bytes.pop_back();
    EXPECT_FALSE(c.decode(bytes).has_value());
}

TEST(Codec, TrivialRejectsOversizedBuffer) {
    auto c = clink::trivial_codec<PodMetric>();
    const PodMetric v{42, 1.5, 7};
    auto bytes = c.encode(v);
    bytes.push_back(std::byte{0});
    EXPECT_FALSE(c.decode(bytes).has_value());
}

TEST(Codec, TrivialWorksForBuiltinScalar) {
    // trivial_codec is the natural fallback for any POD type; verify
    // it works on a plain int even though int64_codec is the
    // wire-stable preferred choice.
    auto c = clink::trivial_codec<int>();
    EXPECT_EQ(expect_round_trip(c, 0), 0);
    EXPECT_EQ(expect_round_trip(c, -1), -1);
    EXPECT_EQ(expect_round_trip(c, std::numeric_limits<int>::max()),
              std::numeric_limits<int>::max());
}

// ----- optional -----

TEST(Codec, OptionalRoundTripsPresent) {
    auto c = optional_codec(int64_codec());
    std::optional<std::int64_t> v{42};
    EXPECT_EQ(expect_round_trip(c, v), v);
}

TEST(Codec, OptionalRoundTripsAbsent) {
    auto c = optional_codec(int64_codec());
    std::optional<std::int64_t> v{};
    auto bytes = c.encode(v);
    ASSERT_EQ(bytes.size(), 1u);
    EXPECT_EQ(static_cast<unsigned char>(bytes[0]), 0u);
    auto out = c.decode(bytes);
    ASSERT_TRUE(out.has_value());
    EXPECT_FALSE(out->has_value());
}

TEST(Codec, OptionalRejectsEmptyBuffer) {
    auto c = optional_codec(int64_codec());
    std::vector<std::byte> empty;
    EXPECT_FALSE(c.decode(empty).has_value());
}

TEST(Codec, OptionalRejectsInvalidFlag) {
    auto c = optional_codec(int64_codec());
    std::vector<std::byte> bytes{std::byte{2}};  // flag must be 0 or 1
    EXPECT_FALSE(c.decode(bytes).has_value());
}

TEST(Codec, OptionalRejectsTruncatedInner) {
    auto c = optional_codec(int64_codec());
    // Present flag but no int64 bytes follow.
    std::vector<std::byte> bytes{std::byte{1}};
    EXPECT_FALSE(c.decode(bytes).has_value());
}

// ----- set -----

TEST(Codec, SetRoundTripsEmpty) {
    auto c = set_codec(int64_codec());
    std::set<std::int64_t> s;
    EXPECT_EQ(expect_round_trip(c, s), s);
}

TEST(Codec, SetRoundTripsMultipleValues) {
    auto c = set_codec(string_codec());
    std::set<std::string> s{"alice", "bob", "carol"};
    EXPECT_EQ(expect_round_trip(c, s), s);
}

TEST(Codec, SetEncodingIsDeterministic) {
    // std::set iterates in sorted order; encode twice and verify the
    // byte streams match.
    auto c = set_codec(int64_codec());
    std::set<std::int64_t> s{3, 1, 2};
    auto a = c.encode(s);
    auto b = c.encode(s);
    EXPECT_EQ(a, b);
}

TEST(Codec, SetRejectsTruncatedBuffer) {
    auto c = set_codec(int64_codec());
    const auto bytes = c.encode(std::set<std::int64_t>{1, 2, 3});
    std::vector<std::byte> truncated(bytes.begin(), bytes.end() - 1);
    EXPECT_FALSE(c.decode(truncated).has_value());
}

// ----- unordered_set -----

TEST(Codec, UnorderedSetRoundTripsMultipleValues) {
    auto c = unordered_set_codec(string_codec());
    std::unordered_set<std::string> s{"alice", "bob", "carol"};
    EXPECT_EQ(expect_round_trip(c, s), s);
}

// ----- map -----

TEST(Codec, MapRoundTripsEmpty) {
    auto c = map_codec(string_codec(), int64_codec());
    std::map<std::string, std::int64_t> m;
    EXPECT_EQ(expect_round_trip(c, m), m);
}

TEST(Codec, MapRoundTripsMultiple) {
    auto c = map_codec(string_codec(), int64_codec());
    std::map<std::string, std::int64_t> m{
        {"alice", 1},
        {"bob", -2},
        {"", std::numeric_limits<std::int64_t>::max()},
    };
    EXPECT_EQ(expect_round_trip(c, m), m);
}

TEST(Codec, MapEncodingIsDeterministic) {
    auto c = map_codec(int64_codec(), string_codec());
    std::map<std::int64_t, std::string> m{{3, "c"}, {1, "a"}, {2, "b"}};
    auto a = c.encode(m);
    auto b = c.encode(m);
    EXPECT_EQ(a, b);
}

TEST(Codec, MapRejectsTruncatedBuffer) {
    auto c = map_codec(string_codec(), int64_codec());
    const auto bytes = c.encode(std::map<std::string, std::int64_t>{{"k", 1}});
    std::vector<std::byte> truncated(bytes.begin(), bytes.end() - 1);
    EXPECT_FALSE(c.decode(truncated).has_value());
}

// ----- unordered_map -----

TEST(Codec, UnorderedMapRoundTripsMultiple) {
    auto c = unordered_map_codec(string_codec(), int64_codec());
    std::unordered_map<std::string, std::int64_t> m{
        {"alice", 1},
        {"bob", -2},
    };
    EXPECT_EQ(expect_round_trip(c, m), m);
}

// ----- cross-helper composition -----

TEST(Codec, OptionalOfVectorRoundTrips) {
    auto c = optional_codec(vector_codec(int64_codec()));
    std::optional<std::vector<std::int64_t>> present{std::vector<std::int64_t>{1, 2, 3}};
    EXPECT_EQ(expect_round_trip(c, present), present);
    std::optional<std::vector<std::int64_t>> absent;
    EXPECT_EQ(expect_round_trip(c, absent), absent);
}

TEST(Codec, MapOfStringToVectorOfInt64RoundTrips) {
    auto c = map_codec(string_codec(), vector_codec(int64_codec()));
    std::map<std::string, std::vector<std::int64_t>> m{
        {"a", {1, 2, 3}},
        {"b", {}},
        {"c", {std::numeric_limits<std::int64_t>::min()}},
    };
    EXPECT_EQ(expect_round_trip(c, m), m);
}

TEST(Codec, VectorOfOptionalStringRoundTrips) {
    auto c = vector_codec(optional_codec(string_codec()));
    std::vector<std::optional<std::string>> v{
        {std::string{"alice"}},
        std::nullopt,
        std::optional<std::string>{""},
    };
    EXPECT_EQ(expect_round_trip(c, v), v);
}
