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
#include "clink/runtime/network/local_data_plane.hpp"
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
    ShuffleSource(ShuffleLedger& ledger, int total, std::string tag)
        : ledger_(ledger), total_(total), tag_(std::move(tag)) {}

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

    std::string name() const override { return "shuffle.source." + tag_; }

private:
    ShuffleLedger& ledger_;
    int total_;
    std::string tag_;
    std::int64_t counter_{0};
};

// A keyed counter, one instance per subtask. Counts per key and, at each barrier,
// publishes what it held AT THAT MOMENT - the operator runner captures state before
// processing the barrier, so this mirrors what the checkpoint would contain.
class ShuffleCounter final : public Operator<int, int> {
public:
    ShuffleCounter(ShuffleLedger& ledger,
                   std::size_t subtask,
                   std::chrono::milliseconds delay,
                   std::string tag)
        : ledger_(ledger), subtask_(subtask), delay_(delay), tag_(std::move(tag)) {}

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
            if (delay_.count() > 0) {
                std::this_thread::sleep_for(delay_);
            }
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

    std::string name() const override { return "shuffle.counter." + tag_; }

private:
    ShuffleLedger& ledger_;
    std::size_t subtask_;
    std::chrono::milliseconds delay_;
    std::string tag_;
    std::map<int, std::int64_t> counts_;
};

class ShuffleSink final : public Sink<int> {
public:
    explicit ShuffleSink(std::string tag) : tag_(std::move(tag)) {}
    void on_data(const Batch<int>& /*batch*/) override {}
    std::string name() const override { return "shuffle.sink." + tag_; }

private:
    std::string tag_;
};

}  // namespace

// The cut must hold across a keyed shuffle: at every barrier, the sum of the keyed
// operators' counts equals what the source had emitted, and no record arrives twice.
namespace {

// Runs the shuffle job and returns the ledger. `channel_capacity` of 1 with a slow
// consumer forces every emit_barrier to BLOCK mid-broadcast: the source pushes the
// barrier to channel 0, blocks because channel 1 is full, and is stalled there while
// the downstream subtasks are still draining pre-barrier records. That window - a
// barrier partially broadcast - is the one condition the unthrottled run never
// reaches, and it is where a record could plausibly land on the wrong side of a cut.
void run_shuffle_job(ShuffleLedger& ledger,
                     std::size_t channel_capacity,
                     std::chrono::milliseconds consumer_delay,
                     const std::string& label) {
    // Operator names must be unique per RUN, not per test.
    //
    // derive_id() hashes the operator name, so two runs sharing names land on the same
    // OperatorIds - and the process-global LocalDataPlane endpoint registry then
    // cross-wires them. Two symptoms of this, both seen here: the first test failed
    // only when the second ran after it, and with a per-TEST tag the backpressure test
    // failed on its second --gtest_repeat iteration, because a repeat reuses the same
    // name. A monotonic counter is deterministic (unlike a random suffix) and unique
    // across every run in the process.
    static std::atomic<unsigned> run_seq{0};
    const std::string tag = label + "." + std::to_string(run_seq.fetch_add(1));
    // Drop the previous run's endpoint registrations. The registry is keyed by
    // host:port and two in-process Dags pick colliding endpoints, so without this the
    // second job's stages resolve to the FIRST job's channels and the two cross-wire
    // (F78). Unique operator names do not help - the collision is on the endpoint key,
    // not the OperatorId, which is why that fix failed.
    clink::network::LocalDataPlane::instance().clear_for_testing();
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator::Config cfg;
    cfg.interval = 7ms;
    CheckpointCoordinator coord(backend, cfg);

    Dag dag;
    dag.set_default_channel_capacity(channel_capacity);
    auto src = dag.add_parallel_source<int>(
        [&](std::size_t) { return std::make_shared<ShuffleSource>(ledger, 400, tag); }, 1);
    auto counted = dag.add_parallel_operator_shuffled<int, int>(
        src,
        [&](std::size_t sub) {
            return std::make_shared<ShuffleCounter>(ledger, sub, consumer_delay, tag);
        },
        4,
        [](const int& v) { return static_cast<std::size_t>(v % kKeys); });
    dag.add_parallel_sink<int>(
        counted, [&](std::size_t) { return std::make_shared<ShuffleSink>(tag); }, 1);

    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    JobConfig job_cfg;
    job_cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), job_cfg);
    exec.start();
    coord.start_periodic_trigger();

    const auto deadline = std::chrono::steady_clock::now() + 40s;
    while (std::chrono::steady_clock::now() < deadline) {
        bool done = false;
        {
            std::lock_guard lock(ledger.mu);
            done = ledger.source_offset.size() >= 15 && !ledger.source_offset.empty() &&
                   ledger.source_offset.rbegin()->second >= 200;
        }
        if (done) {
            break;
        }
        std::this_thread::sleep_for(5ms);
    }

    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();
}

// The three cut assertions, shared by both variants.
void assert_cut_holds(const ShuffleLedger& ledger, const char* what) {
    std::lock_guard lock(ledger.mu);
    ASSERT_GE(ledger.source_offset.size(), 8u) << what << ": too few checkpoints to say anything";

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
    EXPECT_EQ(dups, 0u) << what << ": " << dups << " record(s) delivered more than once."
                        << dup_detail;

    std::string bad;
    std::size_t checked = 0;
    for (const auto& [id, offset] : ledger.source_offset) {
        const auto it = ledger.counts_at_barrier.find(id);
        if (it == ledger.counts_at_barrier.end()) {
            continue;
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
    ASSERT_GT(checked, 0u) << what << ": no checkpoint had both halves recorded";
    EXPECT_TRUE(bad.empty()) << what << ": the cut does not describe one moment:" << bad;
}

}  // namespace

TEST(KeyedShuffleCut, TheCutHoldsAcrossAKeyedShuffleAtParallelismFour) {
    ShuffleLedger ledger;
    run_shuffle_job(ledger, /*channel_capacity=*/1024, /*consumer_delay=*/0ms, "unthrottled");

    {
        std::lock_guard lock(ledger.mu);
        std::string split_detail;
        for (const auto& [key, subs] : ledger.key_to_subtasks) {
            if (subs.size() > 1) {
                split_detail += "\n  key " + std::to_string(key) + " was seen by " +
                                std::to_string(subs.size()) + " subtasks";
            }
        }
        EXPECT_TRUE(split_detail.empty())
            << "a key was routed to more than one subtask:" << split_detail;
    }
    assert_cut_holds(ledger, "unthrottled");
}

// LIMITATION - the engine's, not this test's: one parallel in-process Dag per process.
//
// clear_for_testing() above helps but does NOT make repeats reliable: some
// --gtest_repeat invocations still fail. The cause is a teardown race rather than a
// stale registration, which is why clearing at start cannot fix it - the previous
// job's runners call unregister_endpoint during shutdown, the registry is keyed by
// host:port, and they erase a key the NEXT job has already registered.
//
// So the guarantee is narrow and worth stating exactly: this test is sound run ONCE,
// which is how the suite runs it and how the cluster uses the API (one Dag per worker
// process). It is not safe under --gtest_repeat, and a second parallel-Dag test in
// this binary is not safe either.
//
// A saturated-channel variant is therefore not shipped here. It is the most
// interesting remaining in-process window for F67 - forcing emit_barrier to block
// part-way through its broadcast, with the source offset already recorded and only
// some subtasks holding the barrier - and it needs either per-process test isolation
// or endpoint keys scoped per Dag. See F78.
