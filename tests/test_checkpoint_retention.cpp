// Unit tests for CheckpointRetention - the pure policy that decides which
// completed checkpoints to purge so checkpoint storage stays bounded.

#include <gtest/gtest.h>

#include "clink/cluster/checkpoint_retention.hpp"

using namespace clink;
using clink::cluster::CheckpointRetention;

TEST(CheckpointRetention, KeepsOnlyLatestWhenNumRetainedIsOne) {
    CheckpointRetention r{1};
    EXPECT_TRUE(r.record_completed(CheckpointId{1}).empty());  // first: nothing to purge
    auto p2 = r.record_completed(CheckpointId{2});
    ASSERT_EQ(p2.size(), 1u);
    EXPECT_EQ(p2[0].value(), 1u);  // 1 subsumed by 2
    auto p3 = r.record_completed(CheckpointId{3});
    ASSERT_EQ(p3.size(), 1u);
    EXPECT_EQ(p3[0].value(), 2u);
    EXPECT_EQ(r.retained_count(), 1u);
}

TEST(CheckpointRetention, KeepsLastNAndPurgesOldest) {
    CheckpointRetention r{3};
    EXPECT_TRUE(r.record_completed(CheckpointId{10}).empty());
    EXPECT_TRUE(r.record_completed(CheckpointId{20}).empty());
    EXPECT_TRUE(r.record_completed(CheckpointId{30}).empty());  // window full, none purged
    auto p = r.record_completed(CheckpointId{40});
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].value(), 10u);  // oldest purged; keep {20,30,40}
    EXPECT_EQ(r.retained_count(), 3u);
}

TEST(CheckpointRetention, ZeroNumRetainedClampsToOne) {
    CheckpointRetention r{0};
    EXPECT_EQ(r.num_retained(), 1u);
    EXPECT_TRUE(r.record_completed(CheckpointId{5}).empty());
    auto p = r.record_completed(CheckpointId{6});
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].value(), 5u);  // the latest is never purged
}

TEST(CheckpointRetention, RepeatedIdIsIdempotent) {
    CheckpointRetention r{2};
    EXPECT_TRUE(r.record_completed(CheckpointId{1}).empty());
    EXPECT_TRUE(r.record_completed(CheckpointId{1}).empty());  // same id again
    EXPECT_EQ(r.retained_count(), 1u);
    EXPECT_TRUE(r.record_completed(CheckpointId{2}).empty());  // {1,2}
    auto p = r.record_completed(CheckpointId{3});              // -> purge 1
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].value(), 1u);
}

TEST(CheckpointRetention, OutOfOrderCompletionPurgesTrueOldest) {
    CheckpointRetention r{2};
    EXPECT_TRUE(r.record_completed(CheckpointId{30}).empty());
    EXPECT_TRUE(r.record_completed(CheckpointId{10}).empty());  // {10,30}
    auto p = r.record_completed(CheckpointId{20});              // {10,20,30} -> purge 10
    ASSERT_EQ(p.size(), 1u);
    EXPECT_EQ(p[0].value(), 10u);
    EXPECT_EQ(r.retained_count(), 2u);  // {20,30}
}

// The sweep contract, QUAL-09 finding 77's residue: record_completed only
// ever returns ids the tracker SAW complete, so a completion broadcast the
// worker missed (partitioned at completion time, or restarted since) left
// its snapshot on disk forever. sweep_cutoff() names the oldest id the
// retained window can legitimately hold; everything on disk below it that
// retained_ids() does not contain is an orphan to sweep.

TEST(CheckpointRetention, SweepCutoffTrailsTheNewestRetainedIdByTheWindow) {
    CheckpointRetention r{3};
    EXPECT_EQ(r.sweep_cutoff().value(), 0u);  // nothing completed: no sweep
    (void)r.record_completed(CheckpointId{12});
    // Window {10,11,12} may legitimately exist even though this tracker
    // only saw 12 (a fresh tracker after a worker restart): 10 and 11 must
    // NOT be sweepable, anything below 10 is.
    EXPECT_EQ(r.sweep_cutoff().value(), 10u);
    (void)r.record_completed(CheckpointId{13});
    EXPECT_EQ(r.sweep_cutoff().value(), 11u);
}

TEST(CheckpointRetention, SweepCutoffNeverUnderflowsEarlyIds) {
    CheckpointRetention r{5};
    (void)r.record_completed(CheckpointId{2});
    EXPECT_EQ(r.sweep_cutoff().value(), 0u);  // 2 - (5-1) would underflow
}

TEST(CheckpointRetention, RetainedIdsProtectGapSurvivorsBelowTheCutoff) {
    // Completion gaps (failed checkpoints between retained ones) put a
    // retained id BELOW the arithmetic cutoff. The cutoff is derived from
    // the newest id so the gap cannot hold it down - the survivor itself
    // is protected by membership, not by the cutoff.
    CheckpointRetention r{3};
    (void)r.record_completed(CheckpointId{5});
    (void)r.record_completed(CheckpointId{8});
    (void)r.record_completed(CheckpointId{9});
    EXPECT_EQ(r.sweep_cutoff().value(), 7u);
    const auto retained = r.retained_ids();
    EXPECT_TRUE(retained.contains(5u));   // below cutoff, still protected
    EXPECT_FALSE(retained.contains(6u));  // a genuine orphan would go
}
