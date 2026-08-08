// A checkpoint's cut must describe ONE moment.
//
// The invariant: at the barrier for checkpoint N, whatever offset the source
// records and whatever the downstream operator has processed must agree. The
// source says "I had emitted K records"; the operator must have seen exactly K.
//
// This is not an abstract property. A run of the rescale exactly-once job left a
// completed checkpoint on disk whose source offset was 41 while its keyed counters
// summed to 42 - the operator had counted a record the source did not consider
// emitted. On restore the source replays that record, the operator counts it a
// second time, and the job reports a state mismatch. It surfaced as a rescale
// defect only because a rescale is what forces a restore; the bad cut was written
// by an ordinary periodic checkpoint (F67).
//
// So the check is made directly and in-process, where it is deterministic: no
// rescale, no worker processes, no timing. Each barrier carries its id, both sides
// record what they saw at that id, and the two maps must be equal.
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
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Shared ledger. Both ends write into it keyed by checkpoint id, so a
// disagreement is a difference between two entries rather than something that has
// to be inferred from a snapshot on disk.
struct CutLedger {
    mutable std::mutex mu;
    std::map<CheckpointId, std::int64_t> source_offset;  // what the source recorded
    std::map<CheckpointId, std::int64_t> sink_seen;      // what arrived before the barrier

    void record_source(CheckpointId id, std::int64_t off) {
        std::lock_guard lock(mu);
        source_offset[id] = off;
    }
    void record_sink(CheckpointId id, std::int64_t seen) {
        std::lock_guard lock(mu);
        sink_seen[id] = seen;
    }
};

// The same shape as the rescale job's replayable source: increment, then emit, so
// the offset means "records emitted", and snapshot the offset when a barrier is
// drained. The runner calls snapshot_offset and emit_barrier back to back on this
// thread, which is the property under test.
class CutSource final : public Source<int> {
public:
    CutSource(CutLedger& ledger, std::int64_t total) : ledger_(ledger), total_(total) {}

    bool produce(Emitter<int>& out) override {
        if (this->cancelled() || counter_ >= total_) {
            return false;
        }
        Batch<int> b;
        b.emplace(static_cast<int>(counter_));
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
        ledger_.record_source(ckpt, counter_);
    }

    std::string name() const override { return "cut.source"; }

private:
    CutLedger& ledger_;
    std::int64_t total_;
    std::int64_t counter_{0};
};

// Counts records, and on each barrier records how many it had counted at that
// point. This is the downstream half of the cut.
class CutSink final : public Sink<int> {
public:
    explicit CutSink(CutLedger& ledger) : ledger_(ledger) {}

    void on_data(const Batch<int>& batch) override {
        for (const auto& rec : batch) {
            ++arrivals_[rec.value()];
        }
        seen_ += static_cast<std::int64_t>(batch.size());
    }

    void on_barrier(CheckpointBarrier b) override { ledger_.record_sink(b.id(), seen_); }

    // Any record index that arrived more than once. A duplicate inflates the
    // downstream count without moving the source offset, which is exactly the
    // shape of the on-disk evidence for F67: offset 41, counters summing to 42.
    std::map<int, int> duplicates() const {
        std::map<int, int> dup;
        for (const auto& [v, n] : arrivals_) {
            if (n > 1) {
                dup[v] = n;
            }
        }
        return dup;
    }

private:
    CutLedger& ledger_;
    std::int64_t seen_{0};
    std::map<int, int> arrivals_;
};

}  // namespace

// The direct statement of the invariant, over many checkpoints on a live stream.
//
// The source ticks at 1ms and the trigger at 7ms, so barriers land mid-stream
// rather than at a quiet point - a checkpoint taken while nothing is in flight
// would pass trivially and prove nothing.
TEST(CheckpointCutConsistency, SourceOffsetAndDownstreamCountAgreeAtEveryBarrier) {
    CutLedger ledger;

    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator::Config cfg;
    cfg.interval = 7ms;
    CheckpointCoordinator coord(backend, cfg);

    Dag dag;
    auto src = std::make_shared<CutSource>(ledger, 400);
    auto map = std::make_shared<MapOperator<int, int>>([](int x) { return x; });
    auto sink = std::make_shared<CutSink>(ledger);

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, map);
    dag.add_sink<int>(h1, sink);

    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    // snapshot_offset only runs when a state backend is present, so the cut is
    // only observable on a job configured for checkpointing.
    JobConfig job_cfg;
    job_cfg.state_backend = backend;

    LocalExecutor exec(std::move(dag), job_cfg);
    exec.start();
    coord.start_periodic_trigger();

    // Let the bounded source run out, so the run covers a long stretch of stream
    // rather than a fixed wall-clock slice.
    const auto deadline = std::chrono::steady_clock::now() + 20s;
    while (std::chrono::steady_clock::now() < deadline) {
        bool done = false;
        {
            // Scoped tightly. Holding this across the sleep starves both ends -
            // they take it on every barrier - and the run then observes a stream
            // the test itself throttled rather than the one under test.
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
    src->cancel();
    exec.cancel();
    exec.await_termination();

    std::lock_guard lock(ledger.mu);
    ASSERT_GE(ledger.source_offset.size(), 10u)
        << "too few checkpoints to say anything - the trigger did not run";

    std::vector<std::string> bad;
    for (const auto& [id, offset] : ledger.source_offset) {
        auto it = ledger.sink_seen.find(id);
        if (it == ledger.sink_seen.end()) {
            // The barrier was emitted but had not reached the sink when the run
            // was torn down. Not a cut violation.
            continue;
        }
        if (it->second != offset) {
            bad.push_back("checkpoint " + std::to_string(id.value()) + ": source recorded offset " +
                          std::to_string(offset) + " but downstream had seen " +
                          std::to_string(it->second));
        }
    }

    // The other half of the same invariant. A cut can also be broken by a record
    // arriving twice: the source's offset is right, the downstream count is one
    // too high, and the two disagree without either side having mistimed its
    // snapshot. Checked here because it is the same run and costs nothing.
    const auto dup = sink->duplicates();
    std::string dup_detail;
    for (const auto& [v, n] : dup) {
        dup_detail +=
            "\n  record " + std::to_string(v) + " arrived " + std::to_string(n) + " times";
    }
    EXPECT_TRUE(dup.empty()) << dup.size() << " record(s) were delivered more than once."
                             << dup_detail
                             << "\n\nA duplicated delivery inflates a keyed operator's state "
                                "above what the source's offset accounts for, so the checkpoint "
                                "cannot be restored consistently.";

    std::string detail;
    for (const auto& b : bad) {
        detail += "\n  " + b;
    }
    EXPECT_TRUE(bad.empty())
        << bad.size() << " of " << ledger.source_offset.size()
        << " checkpoints had an inconsistent cut." << detail
        << "\n\nA checkpoint whose source offset and downstream count disagree cannot be "
           "restored correctly: the source replays from its offset and the operator's state "
           "already accounts for records beyond it (or is missing records before it). This is "
           "an exactly-once hole on every recovery path, not only rescale.";
}
