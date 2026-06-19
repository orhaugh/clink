// Multi-type KeyedState instantiations to exercise template paths
// gcov counts each as a separate function. The pre-existing
// test_keyed_state.cpp covers <std::string, std::int64_t> only;
// here we add <int, int>, <int, std::string>, and a pair-keyed
// instantiation to lift function coverage on keyed_state.hpp.

#include <cstdint>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/keyed_state.hpp"

using namespace clink;

namespace {

// int <-> int64 codec adapter - same shape used by the window-operator
// tests, copied here to avoid a cross-test dependency.
Codec<int> int_codec() {
    return Codec<int>{.encode = [](const int& v) { return int64_codec().encode(v); },
                      .decode = [](Codec<int>::BytesView b) -> std::optional<int> {
                          auto x = int64_codec().decode(b);
                          if (!x.has_value()) {
                              return std::nullopt;
                          }
                          return static_cast<int>(*x);
                      }};
}

}  // namespace

TEST(KeyedStateMultiType, IntToIntRoundTrip) {
    auto backend = InMemoryStateBackend{};
    KeyedState<int, int> kv(backend, OperatorId{1}, "int_int", int_codec(), int_codec());

    kv.put(1, 100);
    kv.put(2, 200);
    kv.put(3, 300);
    EXPECT_EQ(*kv.get(1), 100);
    EXPECT_EQ(*kv.get(2), 200);
    EXPECT_EQ(*kv.get(3), 300);
    EXPECT_FALSE(kv.get(99).has_value());

    int sum = 0;
    kv.scan([&](const int&, const int& v) { sum += v; });
    EXPECT_EQ(sum, 600);

    kv.erase(2);
    EXPECT_FALSE(kv.get(2).has_value());
}

TEST(KeyedStateMultiType, IntToStringRoundTrip) {
    auto backend = InMemoryStateBackend{};
    KeyedState<int, std::string> kv(backend, OperatorId{1}, "int_str", int_codec(), string_codec());

    kv.put(1, "one");
    kv.put(2, "two");
    EXPECT_EQ(*kv.get(1), "one");
    EXPECT_EQ(*kv.get(2), "two");

    std::string concat;
    kv.scan([&](const int&, const std::string& v) {
        if (!concat.empty()) {
            concat += ',';
        }
        concat += v;
    });
    // unordered_map iteration order is unspecified; just check the
    // contents.
    EXPECT_TRUE(concat == "one,two" || concat == "two,one");
}

TEST(KeyedStateMultiType, PairKeyedRoundTrip) {
    auto backend = InMemoryStateBackend{};
    using PairKey = std::pair<std::int64_t, std::string>;
    KeyedState<PairKey, std::int64_t> kv(
        backend,
        OperatorId{1},
        "pair_keyed",
        pair_codec<std::int64_t, std::string>(int64_codec(), string_codec()),
        int64_codec());

    kv.put({100, "a"}, 1);
    kv.put({200, "a"}, 2);
    kv.put({100, "b"}, 3);

    EXPECT_EQ(*kv.get({100, "a"}), 1);
    EXPECT_EQ(*kv.get({200, "a"}), 2);
    EXPECT_EQ(*kv.get({100, "b"}), 3);

    std::int64_t total = 0;
    kv.scan([&](const PairKey&, const std::int64_t& v) { total += v; });
    EXPECT_EQ(total, 6);
}

TEST(KeyedStateMultiType, OperatorIdAndSlotNameAccessors) {
    auto backend = InMemoryStateBackend{};
    KeyedState<int, int> kv(backend, OperatorId{42}, "named_slot", int_codec(), int_codec());
    EXPECT_EQ(kv.operator_id(), OperatorId{42});
    EXPECT_EQ(kv.slot_name(), "named_slot");
}

TEST(KeyedStateMultiType, EraseOnMissingKeyIsNoOpAcrossTypes) {
    // Each type instantiation has its own `erase` function in gcov;
    // calling them all here lifts coverage for instantiations that
    // existing tests don't otherwise hit.
    auto backend = InMemoryStateBackend{};

    KeyedState<int, int> a(backend, OperatorId{1}, "a", int_codec(), int_codec());
    KeyedState<int, std::string> b(backend, OperatorId{1}, "b", int_codec(), string_codec());
    KeyedState<std::string, int> c(backend, OperatorId{1}, "c", string_codec(), int_codec());

    a.erase(99);
    b.erase(99);
    c.erase("missing");

    EXPECT_FALSE(a.get(99).has_value());
    EXPECT_FALSE(b.get(99).has_value());
    EXPECT_FALSE(c.get("missing").has_value());
}
