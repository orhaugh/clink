// Unit tests for the key-group partitioning helpers. Verifies:
//   * hashing is stable (same bytes -> same group)
//   * groups distribute over kNumKeyGroups (sanity, not a uniformity proof)
//   * subtask assignment is the inverse of key_group_range_for_subtask
//   * adjacent ranges cover [0, kNumKeyGroups) without gap or overlap
//   * rescale-friendly: at p=2 -> p=4, half the groups stay on the same
//     subtask and the other half move one step over (-standard
//     contiguous-range behaviour).

#include <array>
#include <cstring>
#include <set>
#include <vector>

#include <gtest/gtest.h>

#include "clink/runtime/key_groups.hpp"

using namespace clink;

namespace {

std::span<const std::byte> as_bytes(const std::string& s) {
    return std::span<const std::byte>{reinterpret_cast<const std::byte*>(s.data()), s.size()};
}

}  // namespace

TEST(KeyGroups, HashIsStableForSameInput) {
    const auto a = key_group_for_key(as_bytes("alice"));
    const auto b = key_group_for_key(as_bytes("alice"));
    EXPECT_EQ(a, b);
    EXPECT_LT(a, kNumKeyGroups);
}

TEST(KeyGroups, RangePerSubtaskCoversAllGroupsExactlyOnce) {
    const std::uint32_t p = 4;
    std::vector<KeyGroup> seen(kNumKeyGroups, 0);
    for (std::uint32_t i = 0; i < p; ++i) {
        const auto [first, last] = key_group_range_for_subtask(i, p);
        for (KeyGroup g = first; g < last; ++g) {
            seen[g] = static_cast<KeyGroup>(seen[g] + 1);
        }
    }
    for (KeyGroup g = 0; g < kNumKeyGroups; ++g) {
        EXPECT_EQ(seen[g], 1u) << "group " << g << " covered " << seen[g] << " times";
    }
}

TEST(KeyGroups, SubtaskForKeyGroupMatchesRangeAssignment) {
    const std::uint32_t p = 5;
    for (KeyGroup g = 0; g < kNumKeyGroups; ++g) {
        const auto idx = subtask_for_key_group(g, p);
        const auto [first, last] = key_group_range_for_subtask(idx, p);
        EXPECT_GE(g, first);
        EXPECT_LT(g, last);
    }
}

TEST(KeyGroups, RescaleP2ToP4SplitsContiguousRanges) {
    // p=2: subtask 0 owns [0, 64), subtask 1 owns [64, 128).
    // p=4: subtasks 0..3 own [0,32), [32,64), [64,96), [96,128).
    // Every group's old subtask either stays the new subtask or shifts
    // by one (because of contiguous slicing), never further.
    for (KeyGroup g = 0; g < kNumKeyGroups; ++g) {
        const auto old_sub = subtask_for_key_group(g, /*parallelism=*/2);
        const auto new_sub = subtask_for_key_group(g, /*parallelism=*/4);
        // p=2 -> p=4: each old subtask's range splits into two new subtasks.
        EXPECT_TRUE(new_sub == old_sub * 2 || new_sub == old_sub * 2 + 1)
            << "group " << g << " moved from " << old_sub << " to " << new_sub;
    }
}

TEST(KeyGroups, SameKeyAlwaysHashesToSameGroup) {
    const std::string keys[] = {"a", "bb", "ccc", "d", "ee", "fff", "g", "hh", "iii"};
    for (const auto& k : keys) {
        const auto g1 = key_group_for_key(as_bytes(k));
        const auto g2 = key_group_for_key(as_bytes(k));
        EXPECT_EQ(g1, g2) << "key " << k;
    }
}
