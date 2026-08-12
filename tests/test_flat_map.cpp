// config::FlatMap: the documented std::map-compatible semantics, pinned.
//
// A sorted, contiguous, string-keyed map that backs config::JsonObject -
// and therefore every SQL Row and every parsed JSON document in the engine.
// It replaced std::map for layout reasons on the row hot path, and the swap
// was safe only because it matches std::map on the four behaviours the
// engine relies on. Those four were stated in a header comment and tested
// only INDIRECTLY, through JSON and SQL suites that would fail for a
// thousand reasons - so a regression in any of them would land far from its
// cause. A coverage audit found no test including this header at all.
//
// Each test below is one clause of that contract, named for the engine
// behaviour that breaks if it regresses.

#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include "clink/config/flat_map.hpp"

using clink::config::FlatMap;

namespace {

std::vector<std::string> keys_in_order(const FlatMap<int>& m) {
    std::vector<std::string> out;
    for (const auto& [k, v] : m) {
        out.push_back(k);
    }
    return out;
}

}  // namespace

TEST(ConfigFlatMap, IterationIsSortedByKeyWhateverTheInsertionOrder) {
    // Serialisation order is stable because iteration is sorted. Canonical
    // JSON equality and SQL DISTINCT both compare serialised rows, so an
    // insertion-ordered container would make two equal rows compare
    // unequal depending on how they were built.
    FlatMap<int> m;
    m.emplace("zulu", 1);
    m.emplace("alpha", 2);
    m.emplace("mike", 3);
    m.emplace("bravo", 4);
    EXPECT_EQ(keys_in_order(m), (std::vector<std::string>{"alpha", "bravo", "mike", "zulu"}));
}

TEST(ConfigFlatMap, EmplaceKeepsTheFirstOccurrenceOfADuplicateKey) {
    // JSON parsing relies on first-duplicate-key-wins. A last-wins
    // container would silently change what a document with a repeated key
    // decodes to - and repeated keys are exactly what a hostile or buggy
    // producer sends.
    FlatMap<int> m;
    const auto first = m.emplace("k", 1);
    EXPECT_TRUE(first.second);
    const auto second = m.emplace("k", 999);
    EXPECT_FALSE(second.second) << "the duplicate reported itself as a fresh insert";
    ASSERT_TRUE(m.contains("k"));
    EXPECT_EQ(m.at("k"), 1) << "a duplicate key overwrote the first occurrence";
    EXPECT_EQ(m.size(), 1U);

    // insert() is emplace() underneath and must agree.
    FlatMap<int> viaInsert;
    viaInsert.insert({std::string{"k"}, 1});
    viaInsert.insert({std::string{"k"}, 999});
    EXPECT_EQ(viaInsert.at("k"), 1);
}

TEST(ConfigFlatMap, InsertOrAssignOverwritesWhereEmplaceWouldNot) {
    // The deliberate counterpart: callers that MEAN to overwrite have a
    // verb for it. If these two ever collapsed into the same behaviour,
    // one of the two call-site intents would be silently wrong.
    FlatMap<int> m;
    m.emplace("k", 1);
    const auto r = m.insert_or_assign("k", 999);
    EXPECT_FALSE(r.second) << "insert_or_assign on an existing key must report 'not inserted'";
    EXPECT_EQ(m.at("k"), 999);
    EXPECT_EQ(m.size(), 1U);
}

TEST(ConfigFlatMap, EraseReturnsTheNextIteratorSoEraseWhileIterateWorks) {
    // The erase-while-iterate idiom appears at call sites that prune rows.
    // With vector semantics a wrong return value does not crash - it SKIPS
    // an entry, which is a silently short row rather than a failure.
    FlatMap<int> m;
    for (const auto* k : {"a", "b", "c", "d"}) {
        m.emplace(k, 1);
    }
    for (auto it = m.begin(); it != m.end();) {
        if (it->first == "b" || it->first == "c") {
            it = m.erase(it);
        } else {
            ++it;
        }
    }
    EXPECT_EQ(keys_in_order(m), (std::vector<std::string>{"a", "d"}));
}

TEST(ConfigFlatMap, SubscriptDefaultConstructsAMissingKeyInSortedPosition) {
    FlatMap<int> m;
    m.emplace("b", 2);
    EXPECT_EQ(m["a"], 0) << "operator[] must default-construct on a missing key";
    m["c"] = 3;
    EXPECT_EQ(keys_in_order(m), (std::vector<std::string>{"a", "b", "c"}))
        << "a subscript-created key landed out of sorted position";
    EXPECT_EQ(m["b"], 2) << "subscript on an EXISTING key must not reset it";
}

TEST(ConfigFlatMap, LookupsAcceptStringViewWithoutBuildingATemporaryKey) {
    // Heterogeneous lookup is the reason the row hot path does not allocate
    // per probe. This pins the API shape (a string_view probe compiles and
    // finds the entry); the absence of the allocation is a performance
    // property the benchmarks own.
    FlatMap<int> m;
    m.emplace("needle", 42);
    const std::string_view probe{"needle"};
    ASSERT_TRUE(m.contains(probe));
    EXPECT_EQ(m.at(probe), 42);
    EXPECT_NE(m.find(probe), m.end());
    EXPECT_EQ(m.count(probe), 1U);
    EXPECT_EQ(m.erase(std::string_view{"absent"}), 0U);
    EXPECT_EQ(m.erase(probe), 1U);
    EXPECT_TRUE(m.empty());
}

TEST(ConfigFlatMap, RetainCompactsInOnePassAndKeepsTheSortedInvariant) {
    FlatMap<int> m;
    for (const auto* k : {"a", "b", "c", "d", "e"}) {
        m.emplace(k, static_cast<int>(k[0]));
    }
    const auto removed = m.retain([](const auto& e) { return e.first != "b" && e.first != "d"; });
    EXPECT_EQ(removed, 2U);
    EXPECT_EQ(keys_in_order(m), (std::vector<std::string>{"a", "c", "e"}))
        << "retain broke the sorted-unique invariant it claims to preserve by construction";
    // The survivors keep their values - a compaction that moved keys and
    // values independently would pass a keys-only check.
    EXPECT_EQ(m.at("a"), static_cast<int>('a'));
    EXPECT_EQ(m.at("e"), static_cast<int>('e'));
}

TEST(ConfigFlatMap, EqualityIsContentBasedAndOrderIndependentByConstruction) {
    // Two maps built in different orders must compare equal, because the
    // sorted invariant makes the backing vectors identical. Row equality
    // depends on this.
    FlatMap<int> a;
    a.emplace("x", 1);
    a.emplace("y", 2);
    FlatMap<int> b;
    b.emplace("y", 2);
    b.emplace("x", 1);
    EXPECT_TRUE(a == b);
    b["z"] = 3;
    EXPECT_FALSE(a == b);
}
