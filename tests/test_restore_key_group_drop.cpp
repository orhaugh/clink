// A restore that discards keyed state must say so.
//
// The silent case is F38 in docs/production-hardening-plan.md, and it cost a
// day to find from the outside. A job restored, reported success, and came
// back with half its keyed state missing. The mechanism:
//
//   * the job ran at parallelism 3, so subtask 1 owned key groups [43, 86);
//   * every record went through subtask 1 regardless of key, because the
//     operator kept keyed state on a stream that was never key-partitioned;
//   * its snapshot therefore held entries for key groups 36 AND 69;
//   * on restore, subtask 1 correctly kept kg 69 and discarded kg 36, and
//     subtask 0 - which owns kg 36 - had an empty snapshot.
//
// The discard is right. Doing it in silence is not: nothing in the logs, no
// metric, no failure. These cases pin the reporting, which is what turns a
// hole in a job's state into something an operator can see.
//
// Deliberately NOT asserting that restore throws. Discarding out-of-range
// entries is correct and routine during a rescale, and a backend cannot tell
// a rescale from a same-parallelism restore - only the caller knows. So the
// backend's job is to count and report, and the counter is what an alert
// watches.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/metrics/state_metrics.hpp"
#include "clink/runtime/key_groups.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace clink;

constexpr OperatorId kOp{7};

std::uint64_t dropped_total() {
    const auto name = clink::metrics::state_metric_name("restore_keys_dropped_total", "in_memory");
    const auto snap = MetricsRegistry::global().snapshot();
    for (const auto& [n, v] : snap.counters) {
        if (n == name) {
            return v;
        }
    }
    return 0;
}

// A keyed entry as the backend stores one: first byte is the key group.
std::string keyed(KeyGroup kg, const std::string& rest) {
    std::string k;
    k.push_back(static_cast<char>(kg));
    k += rest;
    return k;
}

}  // namespace

TEST(RestoreKeyGroupDrop, EntriesOutsideTheAssignedRangeAreCountedNotJustSkipped) {
    InMemoryStateBackend writer;
    // Two entries whose key groups sit either side of the range the reader
    // will be given - the exact shape of F38, where one subtask's snapshot
    // held state belonging to another.
    writer.put(kOp, keyed(KeyGroup{36}, "a"), "v1");
    writer.put(kOp, keyed(KeyGroup{69}, "b"), "v2");
    const auto snap = writer.snapshot(CheckpointId{1});

    const auto before = dropped_total();

    InMemoryStateBackend reader;
    reader.restore(snap, KeyGroupRange{KeyGroup{43}, KeyGroup{86}});

    // The in-range entry is present, the out-of-range one is gone. That part
    // is correct and is asserted so the test cannot pass by restoring
    // nothing at all.
    EXPECT_TRUE(reader.get(kOp, keyed(KeyGroup{69}, "b")).has_value())
        << "an entry INSIDE the assigned range was dropped";
    EXPECT_FALSE(reader.get(kOp, keyed(KeyGroup{36}, "a")).has_value())
        << "an entry outside the assigned range was restored anyway";

    EXPECT_EQ(dropped_total() - before, 1u)
        << "the discarded entry was not counted, so a job that resumed with a hole in its state "
           "has no signal that it did";
}

TEST(RestoreKeyGroupDrop, ARestoreThatDropsNothingDoesNotMoveTheCounter) {
    // Without this the counter could be incremented unconditionally and the
    // case above would still pass, making the metric useless for alerting -
    // it would be non-zero on every healthy restore.
    InMemoryStateBackend writer;
    writer.put(kOp, keyed(KeyGroup{50}, "a"), "v1");
    writer.put(kOp, keyed(KeyGroup{60}, "b"), "v2");
    const auto snap = writer.snapshot(CheckpointId{1});

    const auto before = dropped_total();
    InMemoryStateBackend reader;
    reader.restore(snap, KeyGroupRange{KeyGroup{43}, KeyGroup{86}});

    EXPECT_TRUE(reader.get(kOp, keyed(KeyGroup{50}, "a")).has_value());
    EXPECT_TRUE(reader.get(kOp, keyed(KeyGroup{60}, "b")).has_value());
    EXPECT_EQ(dropped_total() - before, 0u)
        << "a restore that discarded nothing still incremented the drop counter";
}

TEST(RestoreKeyGroupDrop, AnUnfilteredRestoreKeepsEverythingAndCountsNothing) {
    // The default path: no rescale, no filter. Every entry must survive, and
    // a job restoring its own snapshot must never look like it lost state.
    InMemoryStateBackend writer;
    writer.put(kOp, keyed(KeyGroup{1}, "a"), "v1");
    writer.put(kOp, keyed(KeyGroup{127}, "b"), "v2");
    const auto snap = writer.snapshot(CheckpointId{1});

    const auto before = dropped_total();
    InMemoryStateBackend reader;
    reader.restore(snap);

    EXPECT_TRUE(reader.get(kOp, keyed(KeyGroup{1}, "a")).has_value());
    EXPECT_TRUE(reader.get(kOp, keyed(KeyGroup{127}, "b")).has_value());
    EXPECT_EQ(dropped_total() - before, 0u);
}
