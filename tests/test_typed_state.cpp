// Typed keyed-state primitives (FOUND-1): ListState / MapState /
// AggregatingState / ReducingState. Exercises each via the RuntimeContext
// factory, plus the headline property - they are durable for free, surviving
// snapshot/restore like any KeyedState value.

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/core/types.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/typed_state.hpp"

using namespace clink;

namespace {
RuntimeContext ctx_for(InMemoryStateBackend& b, OperatorId op = OperatorId{1}) {
    return RuntimeContext(op, "typed", &b, nullptr);
}
}  // namespace

TEST(TypedState, ListAddGetUpdateClear) {
    InMemoryStateBackend backend;
    auto ctx = ctx_for(backend);
    auto list = ctx.list_state<std::string, std::int64_t>("l", string_codec(), int64_codec());

    EXPECT_TRUE(list.empty("k"));
    list.add("k", 1);
    list.add("k", 2);
    list.add_all("k", {3, 4});
    EXPECT_EQ(list.get("k"), (std::vector<std::int64_t>{1, 2, 3, 4}));
    EXPECT_FALSE(list.empty("k"));

    list.update("k", {9});
    EXPECT_EQ(list.get("k"), (std::vector<std::int64_t>{9}));

    list.clear("k");
    EXPECT_TRUE(list.empty("k"));
    EXPECT_TRUE(list.get("k").empty());
}

TEST(TypedState, ListSurvivesSnapshotRestore) {
    InMemoryStateBackend backend_a;
    {
        auto ctx = ctx_for(backend_a);
        auto list = ctx.list_state<std::string, std::int64_t>("l", string_codec(), int64_codec());
        list.add("a", 10);
        list.add("a", 20);
        list.add("b", 30);
    }
    auto snap = backend_a.snapshot(CheckpointId{1});

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    auto ctx = ctx_for(backend_b);  // same OperatorId
    auto list = ctx.list_state<std::string, std::int64_t>("l", string_codec(), int64_codec());
    EXPECT_EQ(list.get("a"), (std::vector<std::int64_t>{10, 20}));
    EXPECT_EQ(list.get("b"), (std::vector<std::int64_t>{30}));
}

TEST(TypedState, MapPutGetRemoveContainsEntries) {
    InMemoryStateBackend backend;
    auto ctx = ctx_for(backend);
    auto map = ctx.map_state<std::string, std::string, std::int64_t>(
        "m", string_codec(), string_codec(), int64_codec());

    EXPECT_FALSE(map.contains("u", "x"));
    map.put("u", "x", 1);
    map.put("u", "y", 2);
    EXPECT_EQ(map.get("u", "x"), std::optional<std::int64_t>{1});
    map.put("u", "x", 11);  // overwrite, not duplicate
    EXPECT_EQ(map.get("u", "x"), std::optional<std::int64_t>{11});
    EXPECT_EQ(map.entries("u").size(), 2u);
    EXPECT_TRUE(map.contains("u", "y"));

    map.remove("u", "x");
    EXPECT_FALSE(map.contains("u", "x"));
    EXPECT_EQ(map.entries("u").size(), 1u);
    map.remove("u", "y");
    EXPECT_TRUE(map.entries("u").empty());
}

TEST(TypedState, MapSurvivesSnapshotRestore) {
    InMemoryStateBackend backend_a;
    {
        auto ctx = ctx_for(backend_a);
        auto map = ctx.map_state<std::string, std::string, std::int64_t>(
            "m", string_codec(), string_codec(), int64_codec());
        map.put("u", "x", 1);
        map.put("u", "y", 2);
    }
    auto snap = backend_a.snapshot(CheckpointId{1});

    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    auto ctx = ctx_for(backend_b);
    auto map = ctx.map_state<std::string, std::string, std::int64_t>(
        "m", string_codec(), string_codec(), int64_codec());
    EXPECT_EQ(map.get("u", "x"), std::optional<std::int64_t>{1});
    EXPECT_EQ(map.get("u", "y"), std::optional<std::int64_t>{2});
}

TEST(TypedState, AggregatingFoldsAndFinalises) {
    using Acc = std::pair<std::int64_t, std::int64_t>;  // (sum, count)
    InMemoryStateBackend backend_a;
    auto make = [](StateBackend& b) {
        RuntimeContext ctx(OperatorId{1}, "typed", &b, nullptr);
        return ctx.aggregating_state<std::string, std::int64_t, Acc, std::int64_t>(
            "avg",
            string_codec(),
            pair_codec<std::int64_t, std::int64_t>(int64_codec(), int64_codec()),
            []() -> Acc { return {0, 0}; },
            [](const Acc& a, const std::int64_t& v) -> Acc { return {a.first + v, a.second + 1}; },
            [](const Acc& a) -> std::int64_t { return a.second == 0 ? 0 : a.first / a.second; });
    };

    {
        auto agg = make(backend_a);
        EXPECT_FALSE(agg.get("k").has_value());  // no accumulator yet
        agg.add("k", 10);
        agg.add("k", 20);
        agg.add("k", 30);
        EXPECT_EQ(agg.get("k"), std::optional<std::int64_t>{20});  // mean of 10,20,30
    }
    // Survives restore (the accumulator is persisted, continues folding).
    auto snap = backend_a.snapshot(CheckpointId{1});
    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    auto agg = make(backend_b);
    agg.add("k", 40);  // sum 100, count 4
    EXPECT_EQ(agg.get("k"), std::optional<std::int64_t>{25});
}

TEST(TypedState, ReducingReducesAndSurvivesRestore) {
    InMemoryStateBackend backend_a;
    auto make = [](StateBackend& b) {
        RuntimeContext ctx(OperatorId{1}, "typed", &b, nullptr);
        return ctx.reducing_state<std::string, std::int64_t>(
            "max", string_codec(), int64_codec(), [](const std::int64_t& a, const std::int64_t& c) {
                return a > c ? a : c;
            });
    };
    {
        auto red = make(backend_a);
        EXPECT_FALSE(red.get("k").has_value());
        red.add("k", 5);
        red.add("k", 12);
        red.add("k", 3);
        EXPECT_EQ(red.get("k"), std::optional<std::int64_t>{12});
    }
    auto snap = backend_a.snapshot(CheckpointId{1});
    InMemoryStateBackend backend_b;
    backend_b.restore(snap);
    auto red = make(backend_b);
    red.add("k", 20);  // max(12, 20)
    EXPECT_EQ(red.get("k"), std::optional<std::int64_t>{20});
}

TEST(TypedState, FactoriesThrowWithoutBackend) {
    RuntimeContext ctx(OperatorId{1}, "typed", nullptr, nullptr);
    // Lambda so the template-arg comma is not seen as a macro-argument separator.
    auto make = [&] {
        return ctx.list_state<std::string, std::int64_t>("l", string_codec(), int64_codec());
    };
    EXPECT_THROW((void)make(), std::runtime_error);
}
