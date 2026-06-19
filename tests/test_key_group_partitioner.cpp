// key_group_partitioner.hpp: a SubtaskEmitter partitioner that routes records
// by key group, consistently with how keyed state / restore / rescale route.
// The load-bearing claims are: same key -> same subtask (determinism), the
// chosen subtask EQUALS the one that owns the key's key group
// (subtask_for_key_group . key_group_for_key), every index is in range, and a
// spread of keys reaches more than one subtask.

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <set>
#include <span>
#include <vector>

#include <gtest/gtest.h>

#include "clink/runtime/key_group_partitioner.hpp"
#include "clink/runtime/key_groups.hpp"

namespace {

using namespace clink;

// A record whose partitioning key is an int64. key_bytes serialises that key
// the same way a Codec<std::int64_t> would, so routing matches state ownership.
struct Rec {
    std::int64_t key;
    int payload;
};

std::vector<std::byte> key_bytes_of_rec(const Rec& r) {
    std::vector<std::byte> bytes(sizeof(r.key));
    std::memcpy(bytes.data(), &r.key, sizeof(r.key));
    return bytes;
}

std::size_t expected_subtask(std::int64_t key, std::uint32_t p) {
    std::vector<std::byte> bytes(sizeof(key));
    std::memcpy(bytes.data(), &key, sizeof(key));
    return subtask_for_key_group(key_group_for_key(std::span<const std::byte>(bytes)), p);
}

TEST(KeyGroupPartitioner, MatchesStateOwnershipFormula) {
    constexpr std::uint32_t kParallelism = 4;
    auto part = make_key_group_partitioner<Rec>(key_bytes_of_rec, kParallelism);
    for (std::int64_t k = 0; k < 500; ++k) {
        EXPECT_EQ(part(Rec{k, 0}), expected_subtask(k, kParallelism))
            << "partitioner must agree with subtask_for_key_group for key " << k;
    }
}

TEST(KeyGroupPartitioner, DeterministicForSameKey) {
    auto part = make_key_group_partitioner<Rec>(key_bytes_of_rec, 8);
    const std::size_t first = part(Rec{12345, 1});
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(part(Rec{12345, i}), first) << "same key must always route to the same subtask";
    }
}

TEST(KeyGroupPartitioner, AllIndicesInRange) {
    constexpr std::uint32_t kParallelism = 6;
    auto part = make_key_group_partitioner<Rec>(key_bytes_of_rec, kParallelism);
    for (std::int64_t k = 0; k < 1000; ++k) {
        EXPECT_LT(part(Rec{k, 0}), kParallelism);
    }
}

TEST(KeyGroupPartitioner, SpreadHitsMultipleSubtasks) {
    constexpr std::uint32_t kParallelism = 4;
    auto part = make_key_group_partitioner<Rec>(key_bytes_of_rec, kParallelism);
    std::set<std::size_t> hit;
    for (std::int64_t k = 0; k < 1000; ++k) {
        hit.insert(part(Rec{k, 0}));
    }
    EXPECT_GT(hit.size(), 1U) << "a spread of keys must reach more than one subtask";
}

TEST(KeyGroupPartitioner, ParallelismOneAlwaysZero) {
    auto part = make_key_group_partitioner<Rec>(key_bytes_of_rec, 1);
    for (std::int64_t k = 0; k < 200; ++k) {
        EXPECT_EQ(part(Rec{k, 0}), 0U);
    }
}

}  // namespace
