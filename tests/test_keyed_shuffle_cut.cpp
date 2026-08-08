// The checkpoint cut across a KEYED SHUFFLE, which is where F67 lives.
//
// tests/test_checkpoint_cut_consistency.cpp asserts the same invariant over a
// single-input forward path and passes, which exonerates that path. The failing
// distributed job differs in exactly one structural way: its counter is keyed and
// runs at parallelism 4, so records reach it through a partitioner and separate
// channels while the barrier is broadcast to all of them.
//
// The evidence being chased, read off a real failing run's checkpoint tree: at
// checkpoint 21 the source recorded offset 41 (records 0..40 emitted) and eleven of
// twelve keys held exactly the count that implies. Key 5 held one extra, and key 5's
// records are 5, 17, 29 and 41 - so record-41, the 42nd, was counted by an operator
// while the source says it never went out.
//
// Both snapshot points are provably correct: the source writes its offset and emits
// the barrier back to back on its own thread with produce() unable to run between
// them, and the keyed operator captures BEFORE processing the barrier on both the
// synchronous and async-persist paths. So the record is arriving twice, or arriving
// on the wrong side of the barrier, somewhere in the shuffle.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

constexpr int kKeys = 12;

// Per-checkpoint ledger, written by both ends.
struct ShuffleLedger {
    mutable std::mutex mu;
    // What the source had emitted when it snapshotted for this checkpoint.
    std::map<CheckpointId, std::int64_t> source_offset;
    // Per-checkpoint, per-key counts as the keyed operators had them at the barrier.
    std::map<CheckpointId, std::map<int, std::int64_t>> counts_at_barrier;
    // Every record index seen by any keyed subtask, to catch a duplicate directly.
    std::map<int, int> arrivals;
    // Which subtask saw each key, so a key routed to two subtasks is visible.
    std::map<int, std::vector<std::size_t>> key_to_subtasks;
    std::atomic<int> barriers_seen{0};
    std::atomic<int> drains_seen{0};
    std::atomic<int> data_batches{0};
};

// Same shape as the rescale job's replayable source: increment, then emit, so the
// offset means "records emitted", and snapshot it when a barrier is drained.
class ShuffleSource final : public Source<int> {
public:
    ShuffleSource(ShuffleLedger& ledger, int total) : ledger_(ledger), total_(total) {}

    bool produce(Emitter<int>& out) override {
        if (this->cancelled() || counter_ >= total_) {
            return false;
        }
        Batch<int> b;
        b.emplace(counter_);
        ++counter_;
        if (!out.emit_data(std::move(b))) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
        return counter_ < total_;
    }

    void snapshot_offset(StateBackend& /*backend*/,
                         OperatorId /*op_id*/,
                         CheckpointId ckpt) override {
        std::lock_guard lock(ledger_.mu);
        ledger_.source_offset[ckpt] = counter_;
    }

    std::string name() const override { return "shuffle.source"; }

private:
    ShuffleLedger& ledger_;
    int total_;
    std::int64_t counter_{0};
};

// A keyed counter, one instance per subtask. Counts per key and, at each barrier,
// publishes what it held AT THAT MOMENT - the operator runner captures state before
// processing the barrier, so this mirrors what the checkpoint would contain.
class ShuffleCounter final : public Operator<int, int> {
public:
    ShuffleCounter(ShuffleLedger& ledger, std::size_t subtask)
        : ledger_(ledger), subtask_(subtask) {}

    void process(const StreamElement<int>& el, Emitter<int>& out) override {
        if (el.is_data()) {
            for (const auto& rec : el.as_data()) {
                const int v = rec.value();
                const int key = v % kKeys;
                ++counts_[key];
                std::lock_guard lock(ledger_.mu);
                ++ledger_.arrivals[v];
                auto& subs = ledger_.key_to_subtasks[key];
                if (std::find(subs.begin(), subs.end(), subtask_) == subs.end()) {
                    subs.push_back(subtask_);
                }
            }
            ++ledger_.data_batches;
            out.emit_data(el.as_data());
            return;
        }
        if (el.is_watermark()) {
            this->on_watermark(el.as_watermark(), out);
            return;
        }
        if (el.is_drain()) {
            ++ledger_.drains_seen;
            return;
        }
        this->on_barrier(el.as_barrier(), out);
    }

    // The parallel-stage runner routes barriers HERE rather than through process():
    // it calls op->on_barrier(adv.barrier, out) once alignment says forward. A test
    // that watched process() for barriers saw none at all - 86 checkpoints, zero
    // observed - which looks like a broken engine and is really a broken test.
    //
    // Publish what this subtask holds at the barrier, before forwarding: the runner
    // captures state before the barrier is processed, so this is the count the
    // checkpoint would contain.
    void on_barrier(CheckpointBarrier b, Emitter<int>& out) override {
        ++ledger_.barriers_seen;
        {
            std::lock_guard lock(ledger_.mu);
            auto& at = ledger_.counts_at_barrier[b.id()];
            for (const auto& [key, n] : counts_) {
                at[key] += n;
            }
        }
        Operator<int, int>::on_barrier(b, out);
    }

    std::string name() const override { return "shuffle.counter"; }

private:
    ShuffleLedger& ledger_;
    std::size_t subtask_;
    std::map<int, std::int64_t> counts_;
};

class ShuffleSink final : public Sink<int> {
public:
    void on_data(const Batch<int>& /*batch*/) override {}
    std::string name() const override { return "shuffle.sink"; }
};

}  // namespace

// The cut must hold across a keyed shuffle: at every barrier, the sum of the keyed
// operators' counts equals what the source had emitted, and no record arrives twice.
TEST(KeyedShuffleCut, TheCutHoldsAcrossAKeyedShuffleAtParallelismFour) {
    ShuffleLedger ledger;

    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator::Config cfg;
    cfg.interval = 7ms;
    CheckpointCoordinator coord(backend, cfg);

    Dag dag;
    auto src = dag.add_parallel_source<int>(
        [&](std::size_t) { return std::make_shared<ShuffleSource>(ledger, 400); }, 1);
    // The shuffle: parallelism 4, partitioned by key, exactly as the failing job's
    // counter is. The partitioner is the SAME rule the state side would use, so a
    // key can only legitimately land on one subtask.
    auto counted = dag.add_parallel_operator_shuffled<int, int>(
        src,
        [&](std::size_t sub) { return std::make_shared<ShuffleCounter>(ledger, sub); },
        4,
        [](const int& v) { return static_cast<std::size_t>(v % kKeys); });
    dag.add_parallel_sink<int>(
        counted, [](std::size_t) { return std::make_shared<ShuffleSink>(); }, 1);

    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    JobConfig job_cfg;
    job_cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), job_cfg);
    exec.start();
    coord.start_periodic_trigger();

    const auto deadline = std::chrono::steady_clock::now() + 30s;
    while (std::chrono::steady_clock::now() < deadline) {
        bool done = false;
        {
            std::lock_guard lock(ledger.mu);
            done = ledger.source_offset.size() >= 20 && !ledger.source_offset.empty() &&
                   ledger.source_offset.rbegin()->second >= 380;
        }
        if (done) {
            break;
        }
        std::this_thread::sleep_for(5ms);
    }

    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();

    std::lock_guard lock(ledger.mu);
    ASSERT_GE(ledger.source_offset.size(), 10u)
        << "too few checkpoints to say anything - the trigger did not run";

    // 1. No record delivered twice. This is the direct form of the on-disk evidence.
    std::string dup_detail;
    std::size_t dups = 0;
    for (const auto& [v, n] : ledger.arrivals) {
        if (n > 1) {
            ++dups;
            if (dups <= 10) {
                dup_detail += "\n  record " + std::to_string(v) + " arrived " + std::to_string(n) +
                              " times (key " + std::to_string(v % kKeys) + ")";
            }
        }
    }
    EXPECT_EQ(dups, 0u) << dups << " record(s) were delivered more than once." << dup_detail;

    // 2. Every key landed on exactly one subtask. A key seen by two subtasks would
    //    mean the shuffle and the state partitioning disagree, which produces the
    //    same arithmetic without any duplicate delivery.
    std::string split_detail;
    for (const auto& [key, subs] : ledger.key_to_subtasks) {
        if (subs.size() > 1) {
            split_detail += "\n  key " + std::to_string(key) + " was seen by " +
                            std::to_string(subs.size()) + " subtasks";
        }
    }
    EXPECT_TRUE(split_detail.empty())
        << "a key was routed to more than one subtask:" << split_detail;

    // 3. The cut itself: summed counts at each barrier equal the source's offset.
    std::string bad;
    std::size_t checked = 0;
    for (const auto& [id, offset] : ledger.source_offset) {
        const auto it = ledger.counts_at_barrier.find(id);
        if (it == ledger.counts_at_barrier.end()) {
            continue;  // barrier emitted but not yet processed at teardown
        }
        std::int64_t total = 0;
        for (const auto& [key, n] : it->second) {
            total += n;
        }
        ++checked;
        if (total != offset) {
            bad += "\n  checkpoint " + std::to_string(id.value()) + ": source emitted " +
                   std::to_string(offset) + " but the keyed operators held " +
                   std::to_string(total);
        }
    }
    ASSERT_GT(checked, 0u) << "no checkpoint had both halves recorded"
                           << " (source_offset=" << ledger.source_offset.size()
                           << " counts_at_barrier=" << ledger.counts_at_barrier.size()
                           << " barriers_seen=" << ledger.barriers_seen.load()
                           << " drains_seen=" << ledger.drains_seen.load()
                           << " data_batches=" << ledger.data_batches.load()
                           << " arrivals=" << ledger.arrivals.size() << ")";
    EXPECT_TRUE(bad.empty())
        << "the cut does not describe one moment across the shuffle:" << bad
        << "\n\nThis is F67's shape: a checkpoint whose source offset and keyed state "
           "disagree cannot be restored consistently - the source replays from its "
           "offset while the operators already account for records beyond it.";
}
