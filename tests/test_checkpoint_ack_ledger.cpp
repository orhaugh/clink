// CheckpointAckLedger: an acknowledgement becomes ready only once a checkpoint
// at or above the one that sealed it completes, an aborted checkpoint's are
// released unacknowledged, and no notification - repeated, late, out of order
// or for an id with nothing sealed - moves one sealed above the id it names or
// one not sealed yet. The Redis Streams and Pub/Sub sources acknowledge through
// it; their own tests cover the broker side.

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/connectors/checkpoint_ack_ledger.hpp"

namespace {

using clink::connectors::CheckpointAckLedger;

std::vector<std::string> sorted(std::vector<std::string> v) {
    std::sort(v.begin(), v.end());
    return v;
}

// a, b sealed as checkpoint 1; c as 2; d delivered after the last barrier.
void three_intervals(CheckpointAckLedger<std::string>& ledger) {
    ledger.delivered("a");
    ledger.delivered("b");
    ledger.seal(1);
    ledger.delivered("c");
    ledger.seal(2);
    ledger.delivered("d");
}

TEST(CheckpointAckLedger, NothingIsReadyBeforeItsCheckpointCompletes) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    const auto h = ledger.take();
    EXPECT_TRUE(h.ready.empty());
    EXPECT_TRUE(h.released.empty());
    EXPECT_EQ(ledger.outstanding(), 4u);
    EXPECT_EQ(ledger.sealed_count(), 3u);
    EXPECT_EQ(ledger.open_count(), 1u);
}

TEST(CheckpointAckLedger, ACompletionCoversItsOwnIntervalAndEveryEarlierOneOnly) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    ledger.complete(1);
    EXPECT_EQ(sorted(ledger.take().ready), (std::vector<std::string>{"a", "b"}));
    ledger.complete(2);
    EXPECT_EQ(ledger.take().ready, (std::vector<std::string>{"c"}));
    // d belongs to no checkpoint yet: no completion reaches it, however high.
    ledger.complete(99);
    EXPECT_TRUE(ledger.take().ready.empty());
    EXPECT_EQ(ledger.outstanding(), 1u);
    ledger.seal(100);
    ledger.complete(100);
    EXPECT_EQ(ledger.take().ready, (std::vector<std::string>{"d"}));
    EXPECT_EQ(ledger.outstanding(), 0u);
}

TEST(CheckpointAckLedger, ALaterCompletionSubsumesAnEarlierOneThatNeverArrived) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    ledger.complete(2);  // 1's notification was lost or is still on its way
    EXPECT_EQ(sorted(ledger.take().ready), (std::vector<std::string>{"a", "b", "c"}));
}

TEST(CheckpointAckLedger, RepeatedAndLateNotificationsMoveNothingTwice) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    ledger.complete(2);
    EXPECT_EQ(ledger.take().ready.size(), 3u);
    ledger.complete(2);  // repeated
    ledger.complete(1);  // late, below one already applied
    ledger.abort(1);     // late abort of an interval already acknowledged
    ledger.abort(2);
    const auto h = ledger.take();
    EXPECT_TRUE(h.ready.empty());
    EXPECT_TRUE(h.released.empty());
    EXPECT_EQ(ledger.outstanding(), 1u) << "d stays held";
}

TEST(CheckpointAckLedger, AnAbortReleasesItsIntervalUnacknowledgedAndNoOther) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    ledger.abort(2);
    auto h = ledger.take();
    EXPECT_TRUE(h.ready.empty());
    EXPECT_EQ(h.released, (std::vector<std::string>{"c"}));
    // A completion after the abort does not resurrect c, and covers 1 only.
    ledger.complete(2);
    h = ledger.take();
    EXPECT_EQ(sorted(h.ready), (std::vector<std::string>{"a", "b"}));
    EXPECT_TRUE(h.released.empty());
    EXPECT_EQ(ledger.outstanding(), 1u);
}

TEST(CheckpointAckLedger, AnAbortOfAnIdWithNothingSealedIsANoOp) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    ledger.abort(7);
    ledger.complete(0);
    const auto h = ledger.take();
    EXPECT_TRUE(h.ready.empty());
    EXPECT_TRUE(h.released.empty());
    EXPECT_EQ(ledger.outstanding(), 4u);
}

TEST(CheckpointAckLedger, ABarrierWithNothingDeliveredSealsNothing) {
    CheckpointAckLedger<std::string> ledger;
    ledger.seal(1);
    ledger.delivered("x");
    ledger.seal(2);
    ledger.complete(1);
    EXPECT_TRUE(ledger.take().ready.empty()) << "x was delivered after barrier 1";
    ledger.complete(2);
    EXPECT_EQ(ledger.take().ready, (std::vector<std::string>{"x"}));
}

TEST(CheckpointAckLedger, OutstandingCountsEverythingNotHandedOver) {
    CheckpointAckLedger<std::string> ledger;
    three_intervals(ledger);
    ledger.complete(1);
    EXPECT_EQ(ledger.outstanding(), 4u) << "ready but not yet taken still counts";
    (void)ledger.take();
    EXPECT_EQ(ledger.outstanding(), 2u);
    ledger.abort(2);
    EXPECT_EQ(ledger.outstanding(), 2u);
    (void)ledger.take();
    EXPECT_EQ(ledger.outstanding(), 1u);
}

// Notifications arrive on the commit-dispatch thread while the source thread
// delivers, seals and takes: every acknowledgement is handed over exactly once,
// and only from intervals the completions covered.
TEST(CheckpointAckLedger, NotificationsFromAnotherThreadHandEachAckOverOnce) {
    CheckpointAckLedger<std::string> ledger;
    constexpr int kCheckpoints = 200;
    constexpr int kPerInterval = 50;
    std::atomic<int> sealed_up_to{0};
    std::vector<std::string> taken;
    std::thread notifier([&] {
        int done = 0;
        while (done < kCheckpoints) {
            const int s = sealed_up_to.load();
            if (s > done) {
                done = s;
                ledger.complete(static_cast<std::uint64_t>(done));
            } else {
                std::this_thread::yield();
            }
        }
    });
    for (int c = 1; c <= kCheckpoints; ++c) {
        for (int i = 0; i < kPerInterval; ++i) {
            ledger.delivered(std::to_string(c) + ":" + std::to_string(i));
        }
        ledger.seal(static_cast<std::uint64_t>(c));
        sealed_up_to.store(c);
        auto h = ledger.take();
        taken.insert(taken.end(), h.ready.begin(), h.ready.end());
    }
    notifier.join();
    auto h = ledger.take();
    taken.insert(taken.end(), h.ready.begin(), h.ready.end());
    EXPECT_EQ(taken.size(), static_cast<std::size_t>(kCheckpoints * kPerInterval));
    std::sort(taken.begin(), taken.end());
    EXPECT_EQ(std::adjacent_find(taken.begin(), taken.end()), taken.end()) << "no ack twice";
    EXPECT_EQ(ledger.outstanding(), 0u);
}

}  // namespace
