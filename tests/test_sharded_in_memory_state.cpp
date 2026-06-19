// ShardedInMemoryStateBackend: key-group-sharded keyed state. The backend is
// a sharding layer over N InMemoryStateBackend shards, so the load-bearing
// claims are: it routes correctly (same key -> same shard), it preserves the
// full keyed/operator-state semantics across shards, and its snapshots are
// byte-compatible with the mono backend (a mono snapshot restores into a
// sharded one and vice versa - the construction-path-symmetry guard).

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/state/in_memory_state_backend.hpp"
#include "clink/state/sharded_in_memory_state_backend.hpp"

namespace {

using namespace clink;

StateBackend::ValueView sv(const std::string& s) {
    return StateBackend::ValueView{s};
}
std::string to_string(const StateBackend::Value& v) {
    return std::string(reinterpret_cast<const char*>(v.data()), v.size());
}
// A key whose leading byte is the key group `kg` (mirrors the keyed-state
// encoder), so we can steer keys onto specific shards / filter ranges.
std::string kg_key(std::uint8_t kg, const std::string& tail) {
    std::string k;
    k.push_back(static_cast<char>(kg));
    k.append(tail);
    return k;
}

TEST(ShardedInMemoryStateBackend, PutGetEraseRoundTrip) {
    ShardedInMemoryStateBackend b;
    const OperatorId op{1};
    // Keys spanning many key groups -> many shards.
    for (std::uint8_t kg :
         {std::uint8_t{1}, std::uint8_t{17}, std::uint8_t{33}, std::uint8_t{120}}) {
        b.put(op, sv(kg_key(kg, "k")), sv("v" + std::to_string(kg)));
    }
    for (std::uint8_t kg :
         {std::uint8_t{1}, std::uint8_t{17}, std::uint8_t{33}, std::uint8_t{120}}) {
        auto v = b.get(op, sv(kg_key(kg, "k")));
        ASSERT_TRUE(v.has_value());
        EXPECT_EQ(to_string(*v), "v" + std::to_string(kg));
    }
    b.erase(op, sv(kg_key(17, "k")));
    EXPECT_FALSE(b.get(op, sv(kg_key(17, "k"))).has_value());
    EXPECT_TRUE(b.get(op, sv(kg_key(1, "k"))).has_value());
}

TEST(ShardedInMemoryStateBackend, ScanVisitsAllPairsAcrossShards) {
    ShardedInMemoryStateBackend b;
    const OperatorId op{2};
    // Key groups chosen to land on distinct shards (kg % kNumShards): 1, 2 and
    // 100 map to shards 1, 2 and 4, so scan() must cross three shards.
    b.put(op, sv(kg_key(1, "a")), sv("A"));    // shard 1
    b.put(op, sv(kg_key(2, "b")), sv("B"));    // shard 2
    b.put(op, sv(kg_key(100, "c")), sv("C"));  // shard 100%16=4
    std::size_t count = 0;
    b.scan(op, [&](StateBackend::KeyView, StateBackend::ValueView) { ++count; });
    EXPECT_EQ(count, 3u) << "scan must visit entries from every shard";
}

TEST(ShardedInMemoryStateBackend, SnapshotRestoreRoundTrip) {
    ShardedInMemoryStateBackend src;
    const OperatorId op{3};
    src.put(op, sv(kg_key(5, "x")), sv("X"));
    src.put(op, sv(kg_key(70, "y")), sv("Y"));
    auto snap = src.snapshot(CheckpointId{1});

    ShardedInMemoryStateBackend dst;
    dst.restore(snap);
    EXPECT_EQ(to_string(*dst.get(op, sv(kg_key(5, "x")))), "X");
    EXPECT_EQ(to_string(*dst.get(op, sv(kg_key(70, "y")))), "Y");
}

// Construction-path symmetry: a snapshot taken by ONE backend must restore
// into the OTHER, both directions, since the sharded snapshot is the same
// canonical Arrow IPC the mono backend emits.
TEST(ShardedInMemoryStateBackend, CrossBackendSnapshotCompatBothWays) {
    const OperatorId op{4};

    // mono -> sharded
    {
        InMemoryStateBackend mono;
        mono.put(op, sv(kg_key(9, "p")), sv("P"));
        mono.put(op, sv(kg_key(99, "q")), sv("Q"));
        ShardedInMemoryStateBackend sharded;
        sharded.restore(mono.snapshot(CheckpointId{1}));
        EXPECT_EQ(to_string(*sharded.get(op, sv(kg_key(9, "p")))), "P");
        EXPECT_EQ(to_string(*sharded.get(op, sv(kg_key(99, "q")))), "Q");
    }
    // sharded -> mono
    {
        ShardedInMemoryStateBackend sharded;
        sharded.put(op, sv(kg_key(9, "p")), sv("P"));
        sharded.put(op, sv(kg_key(99, "q")), sv("Q"));
        InMemoryStateBackend mono;
        mono.restore(sharded.snapshot(CheckpointId{1}));
        EXPECT_EQ(to_string(*mono.get(op, sv(kg_key(9, "p")))), "P");
        EXPECT_EQ(to_string(*mono.get(op, sv(kg_key(99, "q")))), "Q");
    }
}

// The key-group restore filter must work identically to the mono backend
// (reused logic), so a rescaled subtask loads only its assigned range.
TEST(ShardedInMemoryStateBackend, RestoreFiltersByKeyGroupRange) {
    ShardedInMemoryStateBackend src;
    const OperatorId op{5};
    src.put(op, sv(kg_key(5, "alpha")), sv("A"));
    src.put(op, sv(kg_key(30, "beta")), sv("B"));
    src.put(op, sv(kg_key(80, "gamma")), sv("C"));
    src.put(op, sv(kg_key(120, "delta")), sv("D"));
    auto snap = src.snapshot(CheckpointId{1});

    ShardedInMemoryStateBackend lower;
    lower.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});
    EXPECT_TRUE(lower.get(op, sv(kg_key(5, "alpha"))).has_value());
    EXPECT_TRUE(lower.get(op, sv(kg_key(30, "beta"))).has_value());
    EXPECT_FALSE(lower.get(op, sv(kg_key(80, "gamma"))).has_value());

    ShardedInMemoryStateBackend upper;
    upper.restore(snap, KeyGroupRange{KeyGroup{64}, KeyGroup{128}});
    EXPECT_FALSE(upper.get(op, sv(kg_key(5, "alpha"))).has_value());
    EXPECT_TRUE(upper.get(op, sv(kg_key(80, "gamma"))).has_value());
    EXPECT_TRUE(upper.get(op, sv(kg_key(120, "delta"))).has_value());
}

// Operator-state rows (leading byte >= kNumKeyGroups) must survive a
// narrowed restore on every subtask and keep the max-merge on a scale-down
// combine - the sharded backend routes them all to shard 0 but must preserve
// the reused InMemory semantics.
TEST(ShardedInMemoryStateBackend, OperatorStateSurvivesNarrowedRestore) {
    ShardedInMemoryStateBackend src;
    const OperatorId op{6};
    src.put(op, sv(kg_key(95, "keyed")), sv("KEYED95"));
    src.put_operator_state(op, sv("__src_offsets__"), sv("OFFSETS"));
    auto snap = src.snapshot(CheckpointId{1});

    // Restore a range that EXCLUDES key group 95: the keyed row drops, but
    // the operator-state offset row must survive (it has no key group).
    ShardedInMemoryStateBackend dst;
    dst.restore(snap, KeyGroupRange{KeyGroup{0}, KeyGroup{64}});
    EXPECT_FALSE(dst.get(op, sv(kg_key(95, "keyed"))).has_value());
    auto offsets = dst.get_operator_state(op, sv("__src_offsets__"));
    ASSERT_TRUE(offsets.has_value());
    EXPECT_EQ(to_string(*offsets), "OFFSETS");
}

TEST(ShardedInMemoryStateBackend, StateVersionsRoundTrip) {
    ShardedInMemoryStateBackend src;
    StateVersionMap versions;
    versions.set(OperatorId{8}, "counts", 3);
    src.set_state_versions(versions);
    src.put(OperatorId{8}, sv(kg_key(10, "k")), sv("v"));

    ShardedInMemoryStateBackend dst;
    dst.restore(src.snapshot(CheckpointId{1}));
    const auto got = dst.restored_state_versions().get(OperatorId{8}, "counts");
    ASSERT_TRUE(got.has_value());
    EXPECT_EQ(*got, 3u);
}

}  // namespace
