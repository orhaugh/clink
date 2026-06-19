// Tests for SnapshotWorker: the off-thread durable-write stage of an
// asynchronous checkpoint. The invariants under test are the ones that
// keep async persistence correct:
//   - FIFO: checkpoints persist + ack in barrier order;
//   - ack-after-durable: the ack fires only after persist() returns;
//   - clean drain: a normal shutdown persists + acks the whole backlog;
//   - hard cancel: a torn-down worker drops queued captures WITHOUT
//     acking (an un-ack'd checkpoint is simply never completed);
//   - persist failure: a throwing persist() acks ok=false, never ok=true.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/types.hpp"
#include "clink/runtime/snapshot_worker.hpp"
#include "clink/state/state_backend.hpp"

namespace {

using namespace clink;
using namespace std::chrono_literals;

// A state backend that records the order of persist() calls and lets a
// test hook block or throw inside persist(). The hook runs BEFORE the id
// is recorded, so a throwing hook leaves the id unpersisted (mirroring a
// real failed durable write).
class RecordingBackend final : public StateBackend {
public:
    void put(OperatorId, KeyView, ValueView) override {}
    std::optional<Value> get(OperatorId, KeyView) const override { return std::nullopt; }
    void erase(OperatorId, KeyView) override {}
    void scan(OperatorId, const ScanVisitor&) const override {}
    void restore(const Snapshot&, const KeyGroupRange&) override {}
    std::string description() const override { return "recording"; }

    Snapshot snapshot(CheckpointId id) override { return persist(capture(id)); }
    [[nodiscard]] bool supports_async_persist() const noexcept override { return true; }
    CaptureHandle capture(CheckpointId id) override {
        return CaptureHandle{.checkpoint_id = id, .bytes = {}};
    }
    Snapshot persist(CaptureHandle handle) override {
        if (on_persist_) {
            on_persist_(handle.checkpoint_id);  // may block or throw
        }
        {
            std::lock_guard lock(mu_);
            persisted_.push_back(handle.checkpoint_id.value());
        }
        return Snapshot{.checkpoint_id = handle.checkpoint_id, .bytes = {}};
    }

    std::vector<std::uint64_t> persisted() const {
        std::lock_guard lock(mu_);
        return persisted_;
    }

    std::function<void(CheckpointId)> on_persist_;

private:
    mutable std::mutex mu_;
    std::vector<std::uint64_t> persisted_;
};

struct AckLog {
    std::mutex mu;
    std::vector<std::tuple<std::uint64_t, bool, std::string>> acks;

    SnapshotWorker::Job::ack_fn_t fn() {
        return [this](CheckpointId id, bool ok, std::string err) {
            std::lock_guard lock(mu);
            acks.emplace_back(id.value(), ok, std::move(err));
        };
    }
    std::vector<std::tuple<std::uint64_t, bool, std::string>> snapshot() {
        std::lock_guard lock(mu);
        return acks;
    }
};

SnapshotWorker::Job make_job(std::uint64_t id,
                             StateBackend* be,
                             std::function<void(CheckpointId, bool, std::string)> ack) {
    return SnapshotWorker::Job{
        .handle = CaptureHandle{.checkpoint_id = CheckpointId{id}, .bytes = {}},
        .backend = be,
        .ack = std::move(ack)};
}

TEST(SnapshotWorker, PersistsAndAcksInBarrierOrder) {
    RecordingBackend be;
    AckLog log;
    SnapshotWorker w;
    w.start();
    for (std::uint64_t id : {1u, 2u, 3u, 4u, 5u}) {
        w.enqueue(make_job(id, &be, log.fn()));
    }
    w.drain_and_join();

    EXPECT_EQ(be.persisted(), (std::vector<std::uint64_t>{1, 2, 3, 4, 5}));
    auto acks = log.snapshot();
    ASSERT_EQ(acks.size(), 5u);
    for (std::size_t i = 0; i < acks.size(); ++i) {
        EXPECT_EQ(std::get<0>(acks[i]), i + 1) << "acks must arrive in barrier order";
        EXPECT_TRUE(std::get<1>(acks[i]));
    }
}

TEST(SnapshotWorker, AckFiresOnlyAfterPersistReturns) {
    RecordingBackend be;
    AckLog log;
    std::promise<void> started;
    std::promise<void> release;
    auto started_fut = started.get_future();
    auto release_fut = release.get_future().share();
    be.on_persist_ = [&](CheckpointId) {
        started.set_value();
        release_fut.wait();  // hold persist open
    };

    SnapshotWorker w;
    w.start();
    w.enqueue(make_job(1, &be, log.fn()));
    started_fut.wait();  // persist is now in progress and blocked

    // While persist is blocked, no ack may have fired.
    EXPECT_TRUE(log.snapshot().empty()) << "ack fired before persist returned";

    release.set_value();
    w.drain_and_join();

    auto acks = log.snapshot();
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(std::get<0>(acks[0]), 1u);
    EXPECT_TRUE(std::get<1>(acks[0]));
}

TEST(SnapshotWorker, CancelDropsQueuedCapturesWithoutAcking) {
    RecordingBackend be;
    AckLog log;
    std::promise<void> started;
    std::promise<void> release;
    auto started_fut = started.get_future();
    auto release_fut = release.get_future().share();
    be.on_persist_ = [&](CheckpointId id) {
        if (id.value() == 1) {
            started.set_value();
            release_fut.wait();  // hold checkpoint 1 mid-persist
        }
    };

    SnapshotWorker w;
    w.start();
    w.enqueue(make_job(1, &be, log.fn()));  // worker pops 1, blocks in persist
    started_fut.wait();
    w.enqueue(make_job(2, &be, log.fn()));  // queued behind the in-flight 1

    // Cancel on another thread: it sets the drop flag, closes the queue,
    // then joins (which blocks until persist(1) finishes).
    std::thread canceller([&] { w.cancel_and_join(); });
    // Let cancel set the drop flag before we let persist(1) complete. The
    // single-consumer worker cannot pop checkpoint 2 until persist(1)
    // returns, so by then the drop flag is already set and 2 is skipped.
    std::this_thread::sleep_for(100ms);
    release.set_value();
    canceller.join();

    // Checkpoint 1 was durable, so it acks ok; checkpoint 2 was dropped.
    EXPECT_EQ(be.persisted(), (std::vector<std::uint64_t>{1}));
    auto acks = log.snapshot();
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(std::get<0>(acks[0]), 1u);
    EXPECT_TRUE(std::get<1>(acks[0]));
    for (const auto& a : acks) {
        EXPECT_NE(std::get<0>(a), 2u) << "a dropped capture must never ack";
    }
}

TEST(SnapshotWorker, PersistFailureAcksFalseNeverTrue) {
    RecordingBackend be;
    AckLog log;
    be.on_persist_ = [&](CheckpointId id) {
        if (id.value() == 7) {
            throw std::runtime_error("disk full");
        }
    };

    SnapshotWorker w;
    w.start();
    w.enqueue(make_job(7, &be, log.fn()));
    w.drain_and_join();

    EXPECT_TRUE(be.persisted().empty());
    auto acks = log.snapshot();
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(std::get<0>(acks[0]), 7u);
    EXPECT_FALSE(std::get<1>(acks[0])) << "a failed persist must ack ok=false";
    EXPECT_NE(std::get<2>(acks[0]).find("disk full"), std::string::npos);
}

}  // namespace
