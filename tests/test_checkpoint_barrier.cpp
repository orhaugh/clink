#include <memory>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;

namespace {

// Source that emits a barrier between two batches. This proves the StreamElement
// envelope carries barriers in-band with data.
class BarrierEmittingSource final : public Source<int> {
public:
    bool produce(Emitter<int>& out) override {
        if (step_ == 0) {
            Batch<int> first;
            first.emplace(1);
            first.emplace(2);
            out.emit_data(std::move(first));
            ++step_;
            return true;
        }
        if (step_ == 1) {
            out.emit_barrier(CheckpointBarrier{CheckpointId{42}});
            ++step_;
            return true;
        }
        if (step_ == 2) {
            Batch<int> second;
            second.emplace(3);
            out.emit_data(std::move(second));
            out.emit_watermark(Watermark::max());
            ++step_;
            return false;
        }
        return false;
    }
    std::string name() const override { return "barrier_source"; }

private:
    int step_{0};
};

}  // namespace

TEST(CheckpointBarrier, PropagatesThroughOperatorsToSink) {
    Dag dag;
    auto src = std::make_shared<BarrierEmittingSource>();
    auto m = std::make_shared<MapOperator<int, int>>([](int x) { return x * 10; });
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, m);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{10, 20, 30}));
    EXPECT_EQ(sink->last_barrier().id().value(), 42u);
}

TEST(CheckpointCoordinator, CompletesWhenAllOperatorsAck) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator coord(backend);

    OperatorId a{1};
    OperatorId b{2};
    coord.register_operator(a);
    coord.register_operator(b);

    auto barrier = coord.trigger();
    EXPECT_FALSE(coord.is_complete(barrier.id()));

    EXPECT_FALSE(coord.acknowledge(barrier.id(), a));
    EXPECT_FALSE(coord.is_complete(barrier.id()));

    EXPECT_TRUE(coord.acknowledge(barrier.id(), b));
    EXPECT_TRUE(coord.is_complete(barrier.id()));
    EXPECT_EQ(coord.last_completed_id(), barrier.id());
}

TEST(CheckpointCoordinator, AbortPreventsCompletion) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator coord(backend);
    coord.register_operator(OperatorId{1});
    auto barrier = coord.trigger();
    coord.abort(barrier.id(), "test abort");
    EXPECT_FALSE(coord.acknowledge(barrier.id(), OperatorId{1}));
    EXPECT_FALSE(coord.is_complete(barrier.id()));
}

// ---------------------------------------------------------------------------
// Barrier alignment mode
// ---------------------------------------------------------------------------

TEST(CheckpointBarrier, DefaultModeIsAligned) {
    CheckpointBarrier b{CheckpointId{1}};
    EXPECT_EQ(b.mode(), CheckpointBarrier::Mode::Aligned);
}

TEST(CheckpointBarrier, ExplicitModeRoundTripsThroughCtor) {
    CheckpointBarrier b{CheckpointId{1}, /*terminal=*/false, CheckpointBarrier::Mode::Unaligned};
    EXPECT_EQ(b.mode(), CheckpointBarrier::Mode::Unaligned);
    EXPECT_FALSE(b.is_terminal());

    CheckpointBarrier terminal{
        CheckpointId{2}, /*terminal=*/true, CheckpointBarrier::Mode::Unaligned};
    EXPECT_TRUE(terminal.is_terminal());
    EXPECT_EQ(terminal.mode(), CheckpointBarrier::Mode::Unaligned);
}

TEST(CheckpointBarrier, EqualityComparesMode) {
    CheckpointBarrier aligned{CheckpointId{1}, CheckpointBarrier::Mode::Aligned};
    CheckpointBarrier unaligned{CheckpointId{1}, CheckpointBarrier::Mode::Unaligned};
    EXPECT_FALSE(aligned == unaligned);
    CheckpointBarrier aligned_copy{CheckpointId{1}, CheckpointBarrier::Mode::Aligned};
    EXPECT_TRUE(aligned == aligned_copy);
}

TEST(CheckpointCoordinator, TriggerStampsDefaultModeFromConfig) {
    auto backend = std::make_shared<InMemoryStateBackend>();

    // Default config -> Aligned.
    CheckpointCoordinator aligned(backend);
    EXPECT_EQ(aligned.trigger().mode(), CheckpointBarrier::Mode::Aligned);

    // Unaligned config -> Unaligned.
    CheckpointCoordinator::Config cfg;
    cfg.default_mode = CheckpointBarrier::Mode::Unaligned;
    CheckpointCoordinator unaligned(backend, cfg);
    EXPECT_EQ(unaligned.trigger().mode(), CheckpointBarrier::Mode::Unaligned);
}

TEST(CheckpointCoordinator, TriggerWithOverrideWinsOverConfig) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator::Config cfg;
    cfg.default_mode = CheckpointBarrier::Mode::Aligned;
    CheckpointCoordinator coord(backend, cfg);

    // Per-trigger override flips this single checkpoint to unaligned;
    // the coordinator's config is unchanged for subsequent triggers.
    auto a = coord.trigger();
    EXPECT_EQ(a.mode(), CheckpointBarrier::Mode::Aligned);

    auto b = coord.trigger(CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(b.mode(), CheckpointBarrier::Mode::Unaligned);

    auto c = coord.trigger();
    EXPECT_EQ(c.mode(), CheckpointBarrier::Mode::Aligned);
}

// Adaptive mode resolver chooses per-checkpoint mode from
// an external signal. We simulate the signal here; production wiring
// will feed it from NetworkChannelSink::saturation_events totals.
TEST(CheckpointCoordinator, ModeResolverGetsCalledOnEachTrigger) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator coord(backend);

    int call_count = 0;
    coord.set_mode_resolver([&](CheckpointId id, CheckpointBarrier::Mode default_mode) {
        ++call_count;
        EXPECT_EQ(default_mode, CheckpointBarrier::Mode::Aligned);
        // Alternate mode based on id parity (a stand-in for "signal
        // says we saturated this cycle").
        return id.value() % 2 == 0 ? CheckpointBarrier::Mode::Unaligned
                                   : CheckpointBarrier::Mode::Aligned;
    });

    auto first = coord.trigger();   // id=1 -> Aligned
    auto second = coord.trigger();  // id=2 -> Unaligned
    auto third = coord.trigger();   // id=3 -> Aligned

    EXPECT_EQ(call_count, 3);
    EXPECT_EQ(first.mode(), CheckpointBarrier::Mode::Aligned);
    EXPECT_EQ(second.mode(), CheckpointBarrier::Mode::Unaligned);
    EXPECT_EQ(third.mode(), CheckpointBarrier::Mode::Aligned);
}

TEST(CheckpointCoordinator, ExplicitOverrideBypassesResolver) {
    auto backend = std::make_shared<InMemoryStateBackend>();
    CheckpointCoordinator coord(backend);

    bool resolver_called = false;
    coord.set_mode_resolver([&](CheckpointId, CheckpointBarrier::Mode) {
        resolver_called = true;
        return CheckpointBarrier::Mode::Unaligned;
    });

    auto b = coord.trigger(CheckpointBarrier::Mode::Aligned);
    EXPECT_EQ(b.mode(), CheckpointBarrier::Mode::Aligned);
    EXPECT_FALSE(resolver_called)
        << "explicit per-trigger override should skip the adaptive resolver";
}
