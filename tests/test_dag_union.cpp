#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/multi_input_alignment.hpp"

using namespace clink;
using namespace std::chrono_literals;

// ---------------------------------------------------------------------------
// Unit tests for the alignment helper itself
// ---------------------------------------------------------------------------

TEST(MultiInputAlignment, WatermarkIsMinAcrossInputs) {
    MultiInputAlignment a(3);
    auto w1 = a.on_watermark(0, Watermark{EventTime{100}});
    EXPECT_FALSE(w1.forward);  // other inputs still at min

    auto w2 = a.on_watermark(1, Watermark{EventTime{200}});
    EXPECT_FALSE(w2.forward);

    auto w3 = a.on_watermark(2, Watermark{EventTime{50}});
    ASSERT_TRUE(w3.forward);
    EXPECT_EQ(w3.watermark, Watermark{EventTime{50}});  // min(100,200,50)

    // Lower bound advances when the laggard catches up
    auto w4 = a.on_watermark(2, Watermark{EventTime{150}});
    ASSERT_TRUE(w4.forward);
    EXPECT_EQ(w4.watermark, Watermark{EventTime{100}});  // min(100,200,150)
}

TEST(MultiInputAlignment, BarrierAlignsOnlyWhenAllInputsArrive) {
    MultiInputAlignment a(2);
    auto b1 = a.on_barrier(0, CheckpointBarrier{CheckpointId{7}});
    EXPECT_FALSE(b1.forward);
    EXPECT_TRUE(a.input_paused(0));
    EXPECT_FALSE(a.input_paused(1));

    auto b2 = a.on_barrier(1, CheckpointBarrier{CheckpointId{7}});
    ASSERT_TRUE(b2.forward);
    EXPECT_EQ(b2.barrier.id().value(), 7u);
    EXPECT_FALSE(a.input_paused(0));
    EXPECT_FALSE(a.input_paused(1));
}

TEST(MultiInputAlignment, ClosedInputContributesMaxWatermarkAndUnblocksAlignment) {
    MultiInputAlignment a(2);
    a.on_barrier(0, CheckpointBarrier{CheckpointId{1}});
    EXPECT_FALSE(a.input_paused(1));

    // Input 1 closes without sending barrier 1 - alignment should release.
    auto adv = a.on_input_closed(1);
    EXPECT_TRUE(adv.forward);
    EXPECT_EQ(adv.barrier.id().value(), 1u);

    // Now input 0 sends a watermark; min should reflect input-0's wm only,
    // because input-1 contributes Watermark::max().
    auto w = a.on_watermark(0, Watermark{EventTime{500}});
    ASSERT_TRUE(w.forward);
    EXPECT_EQ(w.watermark, Watermark{EventTime{500}});
}

// ---------------------------------------------------------------------------
// End-to-end: Dag::union_streams<T>
// ---------------------------------------------------------------------------

TEST(DagUnion, MergesDataFromTwoSources) {
    Dag dag;

    std::vector<Record<int>> a_recs;
    std::vector<Record<int>> b_recs;
    for (int i = 1; i <= 4; ++i) {
        a_recs.emplace_back(Record<int>{i});
    }
    for (int i = 100; i <= 103; ++i) {
        b_recs.emplace_back(Record<int>{i});
    }

    auto src_a = std::make_shared<VectorSource<int>>(std::move(a_recs), "src_a");
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_recs), "src_b");
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);
    auto h_u = dag.union_streams<int>(std::vector<StageHandle<int>>{h_a, h_b});
    dag.add_sink<int>(h_u, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto got = sink->collected();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<int>{1, 2, 3, 4, 100, 101, 102, 103}));
    EXPECT_EQ(sink->last_watermark(), Watermark::max());
}

namespace {

// Source that emits a fixed watermark sequence (no data) and then exits.
class WatermarkOnlySource final : public Source<int> {
public:
    explicit WatermarkOnlySource(std::vector<Watermark> sequence, std::string name)
        : seq_(std::move(sequence)), name_(std::move(name)) {}

    bool produce(Emitter<int>& out) override {
        if (i_ >= seq_.size() || this->cancelled()) {
            return false;
        }
        out.emit_watermark(seq_[i_++]);
        return i_ < seq_.size();
    }

    std::string name() const override { return name_; }

private:
    std::vector<Watermark> seq_;
    std::string name_;
    std::size_t i_{0};
};

// Sink that records every Watermark it sees in arrival order.
class WatermarkLog final : public Sink<int> {
public:
    void on_data(const Batch<int>&) override {}
    void on_watermark(Watermark wm) override {
        std::lock_guard lock(mu_);
        log_.push_back(wm);
    }

    std::vector<Watermark> log() const {
        std::lock_guard lock(mu_);
        return log_;
    }

private:
    mutable std::mutex mu_;
    std::vector<Watermark> log_;
};

}  // namespace

TEST(DagUnion, ForwardsMonotonicMinWatermarks) {
    Dag dag;
    auto a = std::make_shared<WatermarkOnlySource>(
        std::vector<Watermark>{Watermark{EventTime{100}}, Watermark{EventTime{300}}}, "a");
    auto b = std::make_shared<WatermarkOnlySource>(
        std::vector<Watermark>{Watermark{EventTime{50}}, Watermark{EventTime{200}}}, "b");
    auto sink = std::make_shared<WatermarkLog>();

    auto h_a = dag.add_source<int>(a);
    auto h_b = dag.add_source<int>(b);
    auto h_u = dag.union_streams<int>(std::vector<StageHandle<int>>{h_a, h_b});
    dag.add_sink<int>(h_u, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto wms = sink->log();
    // The exact prefix of forwarded watermarks depends on interleaving, but
    // the sequence must be monotonically non-decreasing and end at max.
    ASSERT_FALSE(wms.empty());
    for (std::size_t i = 1; i < wms.size(); ++i) {
        EXPECT_LE(wms[i - 1], wms[i]) << "watermarks regressed at index " << i;
    }
    EXPECT_EQ(wms.back(), Watermark::max());
}

namespace {

// Source that emits a single barrier (no data, no watermark) and exits when
// produce() is asked again. Useful for testing barrier alignment with
// minimal noise.
class BarrierOnlySource final : public Source<int> {
public:
    BarrierOnlySource(CheckpointBarrier b, std::string name)
        : barrier_(b), name_(std::move(name)) {}

    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_) {
            out.emit_barrier(barrier_);
            emitted_ = true;
            return true;  // run one more iteration so the source closes cleanly
        }
        return false;
    }

    std::string name() const override { return name_; }

private:
    CheckpointBarrier barrier_;
    std::string name_;
    bool emitted_{false};
};

class BarrierLog final : public Sink<int> {
public:
    void on_data(const Batch<int>&) override {}
    void on_barrier(CheckpointBarrier b) override {
        std::lock_guard lock(mu_);
        log_.push_back(b.id());
    }
    std::vector<CheckpointId> log() const {
        std::lock_guard lock(mu_);
        return log_;
    }

private:
    mutable std::mutex mu_;
    std::vector<CheckpointId> log_;
};

}  // namespace

// ---------------------------------------------------------------------------
// Unaligned-checkpoint state machine + end-to-end
// ---------------------------------------------------------------------------

// Mode is carried on the barrier itself. Tests below stamp
// each barrier with its desired Mode; the aligner reads mode from the
// barrier on first delivery for a given checkpoint id.
TEST(MultiInputAlignment, UnalignedModeForwardsFirstBarrierImmediately) {
    MultiInputAlignment a(3);
    auto first = a.on_barrier(
        1,
        CheckpointBarrier{CheckpointId{9}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned});
    ASSERT_TRUE(first.forward);
    EXPECT_EQ(first.barrier.id().value(), 9u);
    EXPECT_TRUE(first.unaligned_first);
    // No input pauses in unaligned mode - the runner keeps polling
    // everyone, including the input whose barrier just forwarded.
    EXPECT_FALSE(a.input_paused(0));
    EXPECT_FALSE(a.input_paused(1));
    EXPECT_FALSE(a.input_paused(2));
}

TEST(MultiInputAlignment, UnalignedModeAbsorbsLaterBarriersForSameId) {
    MultiInputAlignment a(3);
    const auto unaligned =
        CheckpointBarrier{CheckpointId{4}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned};
    (void)a.on_barrier(0, unaligned);
    auto second = a.on_barrier(1, unaligned);
    EXPECT_FALSE(second.forward) << "subsequent barriers for the same id must not re-forward";
    EXPECT_FALSE(second.unaligned_first);
    auto third = a.on_barrier(2, unaligned);
    EXPECT_FALSE(third.forward);
}

TEST(MultiInputAlignment, AlignedAndUnalignedModesAreIndependentPerBarrier) {
    MultiInputAlignment a1(2);
    auto b1 = a1.on_barrier(0, CheckpointBarrier{CheckpointId{1}});  // default Aligned
    EXPECT_FALSE(b1.forward) << "aligned barrier shouldn't forward until both inputs deliver";

    MultiInputAlignment a2(2);
    auto b2 = a2.on_barrier(
        0,
        CheckpointBarrier{CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned});
    EXPECT_TRUE(b2.forward) << "unaligned barrier forwards on first delivery";
}

TEST(MultiInputAlignment, SameAlignerHandlesAlignedThenUnalignedAcrossCheckpoints) {
    // One aligner can serve checkpoint 1 in aligned mode
    // and checkpoint 2 in unaligned mode; mode is per-checkpoint, not
    // per-aligner. The coordinator decides each checkpoint's mode.
    MultiInputAlignment a(2);

    // Checkpoint 1: aligned. First delivery doesn't forward.
    auto adv1a =
        a.on_barrier(0, CheckpointBarrier{CheckpointId{1}, CheckpointBarrier::Mode::Aligned});
    EXPECT_FALSE(adv1a.forward);
    auto adv1b =
        a.on_barrier(1, CheckpointBarrier{CheckpointId{1}, CheckpointBarrier::Mode::Aligned});
    ASSERT_TRUE(adv1b.forward);
    EXPECT_FALSE(adv1b.unaligned_first);
    EXPECT_EQ(adv1b.barrier.mode(), CheckpointBarrier::Mode::Aligned);

    // Checkpoint 2: unaligned. First delivery forwards immediately.
    auto adv2a =
        a.on_barrier(0, CheckpointBarrier{CheckpointId{2}, CheckpointBarrier::Mode::Unaligned});
    ASSERT_TRUE(adv2a.forward);
    EXPECT_TRUE(adv2a.unaligned_first);
    EXPECT_EQ(adv2a.barrier.mode(), CheckpointBarrier::Mode::Unaligned);
}

TEST(DagUnion, PerOperatorOverrideForcesAlignedDespiteUnalignedJobConfig) {
    // Even when JobConfig.unaligned_checkpoints=true (so
    // source-stamped barriers carry Mode::Unaligned), a per-operator
    // override at the union operator stamps Aligned on the way in,
    // and the aligner pins Aligned for the checkpoint. The slow B
    // source thus blocks the barrier from forwarding until B's own
    // barrier arrives, mirroring aligned semantics.
    Dag dag;
    auto a = std::make_shared<BarrierOnlySource>(CheckpointBarrier{CheckpointId{91}}, "a");

    class SlowBarrierSource final : public Source<int> {
    public:
        bool produce(Emitter<int>& out) override {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(150ms);
            if (!emitted_) {
                out.emit_barrier(CheckpointBarrier{CheckpointId{91}});
                emitted_ = true;
                return true;
            }
            return false;
        }
        std::string name() const override { return "slow_b"; }

    private:
        bool emitted_{false};
    };
    auto b = std::make_shared<SlowBarrierSource>();
    auto sink = std::make_shared<BarrierLog>();

    auto h_a = dag.add_source<int>(a);
    auto h_b = dag.add_source<int>(b);
    auto h_u = dag.union_streams<int>(std::vector<StageHandle<int>>{h_a, h_b});
    dag.add_sink<int>(h_u, sink);

    // Find the union operator's id so we can target the override at it.
    OperatorId union_id{0};
    for (const auto& r : dag.runners()) {
        if (r.name.find("union") != std::string::npos) {
            union_id = r.id;
            break;
        }
    }
    ASSERT_NE(union_id.value(), 0u);

    JobConfig cfg;
    cfg.unaligned_checkpoints = true;  // global flag = Unaligned
    cfg.barrier_mode_overrides_by_operator[union_id] = CheckpointBarrier::Mode::Aligned;

    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();

    // Aligned override means A's barrier must NOT reach the sink
    // until B also delivers (~150ms). Verify that during the first
    // 50ms the sink sees nothing.
    std::this_thread::sleep_for(50ms);
    EXPECT_TRUE(sink->log().empty())
        << "per-operator Aligned override should have suppressed forwarding";

    exec.cancel();
    exec.await_termination();
}

// pending_inputs_for tells stateful multi-input operators
// which input channels still need draining when a barrier goes
// unaligned. Foundation for generic in-flight capture beyond the
// bespoke interval-join logic.
TEST(MultiInputAlignment, PendingInputsForReportsUnseenInputs) {
    MultiInputAlignment a(3);
    const auto unaligned =
        CheckpointBarrier{CheckpointId{50}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned};

    // Before any delivery: every alive input is pending.
    auto initial = a.pending_inputs_for(CheckpointId{50});
    EXPECT_EQ(initial.size(), 3u);

    // Input 1 delivers first; inputs 0 and 2 still pending.
    auto adv = a.on_barrier(1, unaligned);
    ASSERT_TRUE(adv.forward);
    auto after_one = a.pending_inputs_for(CheckpointId{50});
    EXPECT_EQ(after_one.size(), 2u);
    EXPECT_NE(std::find(after_one.begin(), after_one.end(), 0u), after_one.end());
    EXPECT_NE(std::find(after_one.begin(), after_one.end(), 2u), after_one.end());
    EXPECT_EQ(std::find(after_one.begin(), after_one.end(), 1u), after_one.end());
}

TEST(MultiInputAlignment, PendingInputsForExcludesClosedInputs) {
    MultiInputAlignment a(3);
    a.on_input_closed(2);
    // Closed inputs don't appear in pending list - they can't have
    // unsnapshotted in-flight records.
    auto pending = a.pending_inputs_for(CheckpointId{10});
    EXPECT_EQ(pending.size(), 2u);
    EXPECT_EQ(std::find(pending.begin(), pending.end(), 2u), pending.end());
}

TEST(MultiInputAlignment, FirstDeliveryPinsModeForCheckpoint) {
    // If two upstream sources stamp inconsistently for the same
    // checkpoint id (which the coordinator must not do, but a buggy
    // job might), the aligner pins the mode from the first delivery
    // and applies it to every same-id delivery. This keeps the
    // aligner deterministic in the face of upstream mistakes.
    MultiInputAlignment a(2);
    auto adv1 =
        a.on_barrier(0, CheckpointBarrier{CheckpointId{5}, CheckpointBarrier::Mode::Unaligned});
    EXPECT_TRUE(adv1.forward);  // forwarded as unaligned on first delivery

    // Second input delivers the same id stamped Aligned (mismatch).
    // The aligner ignores the mismatched stamp; the checkpoint
    // remains unaligned at this aligner. Practically this means the
    // second delivery is absorbed silently (unaligned mode behaviour).
    auto adv2 =
        a.on_barrier(1, CheckpointBarrier{CheckpointId{5}, CheckpointBarrier::Mode::Aligned});
    EXPECT_FALSE(adv2.forward);
}

TEST(DagUnion, UnalignedBarrierForwardsBeforeAllInputsDeliver) {
    // Source A emits a barrier and exits. Source B is intentionally
    // slow - it sleeps before emitting its own barrier. With aligned
    // semantics the sink wouldn't see the barrier until B catches up;
    // with unaligned semantics A's barrier reaches the sink almost
    // immediately, well before B closes.
    Dag dag;
    auto a = std::make_shared<BarrierOnlySource>(CheckpointBarrier{CheckpointId{77}}, "a");

    class SlowBarrierSource final : public Source<int> {
    public:
        bool produce(Emitter<int>& out) override {
            if (this->cancelled()) {
                return false;
            }
            std::this_thread::sleep_for(200ms);
            if (!emitted_) {
                out.emit_barrier(CheckpointBarrier{CheckpointId{77}});
                emitted_ = true;
                return true;
            }
            return false;
        }
        std::string name() const override { return "slow_b"; }

    private:
        bool emitted_{false};
    };
    auto b = std::make_shared<SlowBarrierSource>();
    auto sink = std::make_shared<BarrierLog>();

    auto h_a = dag.add_source<int>(a);
    auto h_b = dag.add_source<int>(b);
    auto h_u = dag.union_streams<int>(std::vector<StageHandle<int>>{h_a, h_b});
    dag.add_sink<int>(h_u, sink);

    JobConfig cfg;
    cfg.unaligned_checkpoints = true;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();

    // The barrier should appear at the sink well before B's sleep
    // finishes. Poll for up to 100ms (well under B's 200ms sleep).
    const auto deadline = std::chrono::steady_clock::now() + 100ms;
    while (std::chrono::steady_clock::now() < deadline && sink->log().empty()) {
        std::this_thread::sleep_for(5ms);
    }
    EXPECT_FALSE(sink->log().empty())
        << "unaligned mode should forward A's barrier before B's slow emission";
    exec.cancel();
    exec.await_termination();
    // Once everything drains, exactly one barrier should have made
    // it through - the first one A delivered. B's later barrier for
    // the same id is absorbed by the alignment state machine.
    auto final_log = sink->log();
    ASSERT_GE(final_log.size(), 1u);
    EXPECT_EQ(final_log.front().value(), 77u);
}

TEST(DagUnion, BarrierAlignmentReleasesOnlyAfterAllInputsDeliver) {
    Dag dag;
    auto a = std::make_shared<BarrierOnlySource>(CheckpointBarrier{CheckpointId{42}}, "a");
    auto b = std::make_shared<BarrierOnlySource>(CheckpointBarrier{CheckpointId{42}}, "b");
    auto sink = std::make_shared<BarrierLog>();

    auto h_a = dag.add_source<int>(a);
    auto h_b = dag.add_source<int>(b);
    auto h_u = dag.union_streams<int>(std::vector<StageHandle<int>>{h_a, h_b});
    dag.add_sink<int>(h_u, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto seen = sink->log();
    // Exactly one aligned barrier should have been forwarded.
    ASSERT_EQ(seen.size(), 1u);
    EXPECT_EQ(seen[0].value(), 42u);
}

// ---------------------------------------------------------------------------
// Rescale drain-marker forwarding through operator runners
// ---------------------------------------------------------------------------

namespace {

// Emits a batch, then a rescale drain marker mid-stream, then another batch,
// then ends. A forward-edge operator must FORWARD the drain and keep processing
// the post-drain batch. Before the fix the operator runner handed the drain to
// op->process(), whose control-element else branch called as_barrier() and threw
// bad_variant_access, killing the operator thread and dropping the post-drain
// records (and failing the job).
class DrainMidStreamSource final : public Source<int> {
public:
    bool produce(Emitter<int>& out) override {
        if (this->cancelled() || step_ > 2) {
            return false;
        }
        if (step_ == 0) {
            Batch<int> b;
            b.emplace(1);
            b.emplace(2);
            out.emit_data(std::move(b));
        } else if (step_ == 1) {
            out.emit_drain(DrainMarker{.subtask_idx = 0, .target_parallelism = 2});
        } else {
            Batch<int> b;
            b.emplace(3);
            b.emplace(4);
            out.emit_data(std::move(b));
        }
        ++step_;
        return step_ <= 2;
    }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    std::string name() const override { return "drain_mid_stream_src"; }

private:
    int step_ = 0;
};

}  // namespace

TEST(DagDrain, SingleInputOperatorForwardsDrainAndKeepsProcessing) {
    Dag dag;
    auto src = std::make_shared<DrainMidStreamSource>();
    auto map = std::make_shared<MapOperator<int, int>>([](const int& v) { return v * 10; });
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, map);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto got = sink->collected();
    std::sort(got.begin(), got.end());
    // All four records (pre- AND post-drain) survive; the operator forwarded the
    // drain rather than throwing on it.
    EXPECT_EQ(got, (std::vector<int>{10, 20, 30, 40}));
}

TEST(DagDrain, UnionForwardsDrainFromOneBranch) {
    Dag dag;
    auto src_a = std::make_shared<DrainMidStreamSource>();  // 1,2, <drain>, 3,4
    std::vector<Record<int>> b_recs;
    for (int i = 100; i <= 103; ++i) {
        b_recs.emplace_back(Record<int>{i});
    }
    auto src_b = std::make_shared<VectorSource<int>>(std::move(b_recs), "src_b");
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h_a = dag.add_source<int>(src_a);
    auto h_b = dag.add_source<int>(src_b);
    auto h_u = dag.union_streams<int>(std::vector<StageHandle<int>>{h_a, h_b});
    dag.add_sink<int>(h_u, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto got = sink->collected();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<int>{1, 2, 3, 4, 100, 101, 102, 103}));
}
