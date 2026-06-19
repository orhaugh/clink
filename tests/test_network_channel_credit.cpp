// Unit tests for credit-based backpressure on NetworkChannel.
//
// Drives a Sink/Source pair directly (no cluster) and verifies:
//   1. The initial credit grant arrives and unblocks the first push.
//   2. Incremental grants replenish on each pop, so a steady stream
//      doesn't drift the credit counter.
//   3. Exhausting credit makes push() block; replenishment resumes it.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/runtime/network/network_channel.hpp"
#include "clink/runtime/network/wire.hpp"

using namespace clink;
using namespace clink::network;
using namespace std::chrono_literals;

namespace {

// Spin up a (Source, Sink) pair on loopback, returning the pair.
// listen() picks a free port; the sink connects to that port. Both
// halves use int64_codec.
struct ChannelPair {
    std::unique_ptr<NetworkChannelSource<std::int64_t>> source;
    std::unique_ptr<NetworkChannelSink<std::int64_t>> sink;
    std::thread accept_thread;
};

ChannelPair make_pair() {
    ChannelPair p;
    p.source = std::make_unique<NetworkChannelSource<std::int64_t>>(
        /*port=*/0, int64_codec());
    const auto port = p.source->listen();
    p.sink = std::make_unique<NetworkChannelSink<std::int64_t>>("127.0.0.1", port, int64_codec());
    // Sink's connect() blocks waiting for accept; run accept in another
    // thread so connect can complete.
    p.accept_thread = std::thread([&p] { p.source->accept(); });
    p.sink->connect();
    p.accept_thread.join();
    return p;
}

// Wait until the sink has at least `min_credit` available, or timeout.
bool await_credit_at_least(NetworkChannelSink<std::int64_t>& sink,
                           std::uint32_t min_credit,
                           std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (sink.credit_remaining() >= min_credit)
            return true;
        std::this_thread::sleep_for(5ms);
    }
    return false;
}

}  // namespace

// Credit-based backpressure is wire-protocol behavior; the local
// fast path uses BoundedChannel for backpressure instead. Force the
// socket path across this whole test file.
struct NetworkChannelCreditTest : public ::testing::Test {
    clink::network::ScopedDisableLocalDataPlane no_local;
};

TEST_F(NetworkChannelCreditTest, InitialGrantUnblocksFirstPush) {
    auto p = make_pair();
    // After accept, the source's initial CreditUpdate flies and the
    // sink's reader thread processes it. Within a moderate window the
    // sink should see a non-zero credit budget.
    EXPECT_TRUE(await_credit_at_least(*p.sink, 1, 500ms))
        << "initial grant didn't reach sink within 500ms; credit=" << p.sink->credit_remaining();
    EXPECT_GE(p.sink->credit_remaining(), kInitialNetworkCredit / 2);
    p.sink->close_send();
}

TEST_F(NetworkChannelCreditTest, SteadyStreamReplenishesCreditPerBatch) {
    auto p = make_pair();
    ASSERT_TRUE(await_credit_at_least(*p.sink, kInitialNetworkCredit, 500ms));
    const auto before = p.sink->credit_remaining();

    // Push 50 single-record batches; pop each on the receiver side.
    // Every pop sends CreditUpdate(1) back, so the sink's credit
    // should stay flat across the run (modulo the small async grant
    // window).
    constexpr int kCount = 50;
    std::thread consumer([&] {
        for (int i = 0; i < kCount; ++i) {
            auto e = p.source->pop();
            ASSERT_TRUE(e.has_value()) << "pop returned nullopt at i=" << i;
            EXPECT_TRUE(e->is_data());
        }
    });
    for (int i = 0; i < kCount; ++i) {
        Batch<std::int64_t> b;
        b.emplace(static_cast<std::int64_t>(i));
        EXPECT_TRUE(p.sink->push(StreamElement<std::int64_t>::data(std::move(b))));
    }
    consumer.join();

    // Allow the last grant batch to land.
    ASSERT_TRUE(await_credit_at_least(*p.sink, before, 500ms))
        << "credit didn't refill to baseline; before=" << before
        << " after=" << p.sink->credit_remaining();
    EXPECT_EQ(p.sink->blocked_ns_total(), 0u)
        << "should not have blocked at all with batch=1 and ample initial credit";
    p.sink->close_send();
}

TEST_F(NetworkChannelCreditTest, PushBlocksUntilCreditArrives) {
    // Drain the initial budget by pushing kInitialNetworkCredit+1
    // single-record batches; the receiver pops them all but with a
    // ~50ms artificial delay per batch. We expect the sender to BLOCK
    // at some point during the run (blocked_ns_total > 0).
    //
    // Tuning: push batches of kBatchSize records each so the credit
    // budget drains in a small number of pushes (not in 2048 pushes
    // of 1-record batches, which under Arrow IPC framing can stall
    // on TCP send-buffer backpressure before the credit path triggers).
    // kBatches batches × kBatchSize records each ⇒ ~3× the initial
    // credit budget, forcing the sender to block on credit-cv for the
    // tail end while the consumer drains at 10ms/batch.
    auto p = make_pair();
    ASSERT_TRUE(await_credit_at_least(*p.sink, 1, 500ms));

    constexpr int kBatchSize = 64;
    constexpr int kBatches =
        static_cast<int>(kInitialNetworkCredit) / kBatchSize + 20;  // ~52 batches
    std::atomic<bool> consumer_done{false};
    std::thread consumer([&] {
        for (int i = 0; i < kBatches; ++i) {
            auto e = p.source->pop();
            if (!e.has_value())
                break;
            std::this_thread::sleep_for(10ms);
        }
        consumer_done.store(true, std::memory_order_release);
    });
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kBatches; ++i) {
        Batch<std::int64_t> b;
        for (int j = 0; j < kBatchSize; ++j) {
            b.emplace(static_cast<std::int64_t>((i * kBatchSize) + j));
        }
        if (!p.sink->push(StreamElement<std::int64_t>::data(std::move(b)))) {
            FAIL() << "push returned false at batch i=" << i;
        }
    }
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    consumer.join();

    // Initial credit (2048) covers ~32 batches; the remaining ~20
    // batches must wait on credit grants from the (10ms-per-pop)
    // consumer. Blocked time should be on the order of 200ms+.
    EXPECT_GT(p.sink->blocked_ns_total(), 0u)
        << "sender never blocked on credit (slow consumer wasn't slow enough?)";
    EXPECT_GT(elapsed, 100ms) << "sender finished suspiciously fast";
    // Saturation event counter must move in lock-step with the slow path.
    // Any nonzero blocked_ns_total implies at least one event was logged.
    EXPECT_GT(p.sink->saturation_events(), 0u)
        << "saturation_events stayed zero even though push blocked";
    p.sink->close_send();
}

TEST_F(NetworkChannelCreditTest, NoSaturationEventsOnSteadyStream) {
    // Counterpart to PushBlocksUntilCreditArrives: when the consumer
    // is fast enough to keep credits topped up, the sender must never
    // hit the slow path - so both saturation_events and blocked_ns_total
    // stay at zero throughout.
    auto p = make_pair();
    ASSERT_TRUE(await_credit_at_least(*p.sink, kInitialNetworkCredit, 500ms));

    constexpr int kCount = 200;
    std::thread consumer([&] {
        for (int i = 0; i < kCount; ++i) {
            auto e = p.source->pop();
            ASSERT_TRUE(e.has_value());
        }
    });
    for (int i = 0; i < kCount; ++i) {
        Batch<std::int64_t> b;
        b.emplace(static_cast<std::int64_t>(i));
        EXPECT_TRUE(p.sink->push(StreamElement<std::int64_t>::data(std::move(b))));
    }
    consumer.join();

    EXPECT_EQ(p.sink->saturation_events(), 0u)
        << "saturation_events incremented under non-saturating workload";
    EXPECT_EQ(p.sink->blocked_ns_total(), 0u)
        << "blocked_ns_total nonzero under non-saturating workload";
    p.sink->close_send();
}

TEST_F(NetworkChannelCreditTest, GrantsReceivedTracksReverseChannel) {
    // The receiver returns credit on every pop; the sender's reader
    // thread bumps grants_received_ once per CreditUpdate frame. So a
    // steady N-pop workload should yield grants_received >= N (plus
    // the initial grant from listen()/accept()).
    auto p = make_pair();
    ASSERT_TRUE(await_credit_at_least(*p.sink, 1, 500ms));
    const auto baseline = p.sink->grants_received();

    constexpr int kCount = 25;
    std::thread consumer([&] {
        for (int i = 0; i < kCount; ++i) {
            auto e = p.source->pop();
            ASSERT_TRUE(e.has_value());
        }
    });
    for (int i = 0; i < kCount; ++i) {
        Batch<std::int64_t> b;
        b.emplace(static_cast<std::int64_t>(i));
        EXPECT_TRUE(p.sink->push(StreamElement<std::int64_t>::data(std::move(b))));
    }
    consumer.join();

    // Allow the final grant frame to settle.
    const auto deadline = std::chrono::steady_clock::now() + 500ms;
    while (std::chrono::steady_clock::now() < deadline) {
        if (p.sink->grants_received() >= baseline + kCount)
            break;
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_GE(p.sink->grants_received(), baseline + kCount)
        << "grants_received did not advance by the expected number of pops; baseline=" << baseline
        << " now=" << p.sink->grants_received();
    p.sink->close_send();
}

// Backpressure must not cost correctness. The tests above prove the
// credit gate ENGAGES under a slow consumer (blocked_ns_total /
// saturation_events). This proves the data SURVIVES the gate: with the
// consumer slow enough to exhaust the initial credit window and force
// the sender to wait on grants, every pushed record must still arrive
// exactly once and in order over the real socket transport.
TEST_F(NetworkChannelCreditTest, NoRecordLossOrReorderUnderSustainedBackpressure) {
    auto p = make_pair();
    ASSERT_TRUE(await_credit_at_least(*p.sink, 1, 500ms));

    constexpr int kBatchSize = 64;
    // Enough batches that the initial credit window is exhausted and the
    // sender must block on grants returned by the slow consumer.
    constexpr int kBatches = static_cast<int>(kInitialNetworkCredit) / kBatchSize + 30;
    constexpr int kTotal = kBatchSize * kBatches;

    std::vector<std::int64_t> received;
    received.reserve(static_cast<std::size_t>(kTotal));
    std::thread consumer([&] {
        while (static_cast<int>(received.size()) < kTotal) {
            auto e = p.source->pop();
            if (!e.has_value()) {
                break;
            }
            if (e->is_data()) {
                for (const auto& rec : e->as_data()) {
                    received.push_back(rec.value());
                }
            }
            std::this_thread::sleep_for(2ms);  // slow drain forces backpressure
        }
    });

    for (int i = 0; i < kBatches; ++i) {
        Batch<std::int64_t> b;
        b.reserve(kBatchSize);
        for (int j = 0; j < kBatchSize; ++j) {
            b.emplace(static_cast<std::int64_t>((i * kBatchSize) + j));
        }
        ASSERT_TRUE(p.sink->push(StreamElement<std::int64_t>::data(std::move(b))))
            << "push failed at batch " << i;
    }
    p.sink->close_send();
    consumer.join();

    // The credit gate must have engaged...
    EXPECT_GT(p.sink->saturation_events(), 0u)
        << "consumer wasn't slow enough to exercise the credit gate";
    // ...without dropping, duplicating, or reordering any record.
    ASSERT_EQ(received.size(), static_cast<std::size_t>(kTotal))
        << "record count mismatch - loss or duplication under backpressure";
    for (int i = 0; i < kTotal; ++i) {
        ASSERT_EQ(received[static_cast<std::size_t>(i)], static_cast<std::int64_t>(i))
            << "record out of order / wrong value at index " << i;
    }
}
