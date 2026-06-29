// Iterative dataflow (DataStream.iterate() / closeWith()
// analogue). A feedback channel routes records from a downstream
// stage back into an upstream head; the head's merged output is what
// the body operators consume. Termination is by channel-close
// cascade - when the body's loop branch runs out of records its
// downstream feedback channel closes, the head sees both inputs done,
// and the loop unwinds.
//
// Test covers the canonical convergence pattern: integers count
// down by 1 each pass; values that reach 0 exit via the "done"
// branch while still-positive values loop back. Input {3, 4, 5}
// should yield exactly 3 zeros at the sink.

#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/codec.hpp"
#include "clink/operators/map_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

#include "test_helpers/sanitizer_slack.hpp"

using namespace clink;

namespace {

// Inline codec for int - 4 bytes little-endian. Just enough for the
// cycle-checkpoint tests; the production sources use proper int64
// codecs from core/codec.hpp.
Codec<int> int_codec() {
    return Codec<int>{.encode =
                          [](const int& v) {
                              std::vector<std::byte> out(4);
                              const auto u = static_cast<std::uint32_t>(v);
                              for (int i = 0; i < 4; ++i) {
                                  out[i] = static_cast<std::byte>((u >> (i * 8)) & 0xFF);
                              }
                              return out;
                          },
                      .decode = [](std::span<const std::byte> b) -> std::optional<int> {
                          if (b.size() < 4) {
                              return std::nullopt;
                          }
                          std::uint32_t u = 0;
                          for (int i = 0; i < 4; ++i) {
                              u |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[i]))
                                   << (i * 8);
                          }
                          return static_cast<int>(u);
                      }};
}

}  // namespace

TEST(DagIteration, CountDownConvergesToZero) {
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{3}, Record<int>{4}, Record<int>{5}});
    auto h_src = dag.add_source<int>(src);

    auto iter = dag.iterate_stream<int>(h_src);
    // Body: subtract 1.
    auto h_body = dag.add_operator<int, int>(
        iter.head_output(), std::make_shared<MapOperator<int, int>>([](int v) { return v - 1; }));
    // Split on body output: still-positive (branch 0) loops back,
    // zero/negative (branch 1) flows to the sink.
    auto branches =
        dag.add_split<int>(h_body, [](const int& v) -> int { return v > 0 ? 0 : 1; }, 2);
    iter.close_with(branches[0]);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[1], sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto out = sink->collected();
    EXPECT_EQ(out.size(), 3u);
    for (int v : out) {
        EXPECT_EQ(v, 0) << "every input should converge to 0";
    }
}

TEST(DagIteration, SumOfIterationsMatchesArithmeticSeries) {
    // Same shape, different observation: route the loop branch into
    // a SECOND sink so we can count how many times the body ran. For
    // input N, the body runs N times (emits N-1, N-2, ..., 0). Input
    // {1, 2, 3, 4} should produce 1+2+3+4 = 10 body emissions on the
    // loop branch + 4 final zeros on the output branch.
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{1}, Record<int>{2}, Record<int>{3}, Record<int>{4}});
    auto h_src = dag.add_source<int>(src);

    auto iter = dag.iterate_stream<int>(h_src);
    auto h_body = dag.add_operator<int, int>(
        iter.head_output(), std::make_shared<MapOperator<int, int>>([](int v) { return v - 1; }));
    // Tee the body output BEFORE the split so we can count total
    // iterations independently of the loop/output routing.
    auto tee = dag.fork<int>(h_body, 2);
    auto branches =
        dag.add_split<int>(tee[0], [](const int& v) -> int { return v > 0 ? 0 : 1; }, 2);
    iter.close_with(branches[0]);

    auto out_sink = std::make_shared<CollectingSink<int>>();
    auto count_sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[1], out_sink);
    dag.add_sink<int>(tee[1], count_sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(out_sink->collected().size(), 4u);
    // For inputs {1,2,3,4} the body produces 1+2+3+4 = 10 records.
    EXPECT_EQ(count_sink->collected().size(), 10u);
}

// ---------------------------------------------------------------------------
// Cycle checkpointing
// ---------------------------------------------------------------------------

// A source that emits one batch of records, then a barrier, then idles
// long enough for the iteration to take a snapshot before exiting.
// Used to test that records still circulating in the loop at the
// barrier moment get captured into the checkpoint.
class DataThenBarrierSource final : public Source<int> {
public:
    DataThenBarrierSource(std::vector<Record<int>> records, CheckpointBarrier b)
        : records_(std::move(records)), barrier_(b) {}

    bool produce(Emitter<int>& out) override {
        using namespace std::chrono_literals;
        if (this->cancelled()) {
            return false;
        }
        if (!emitted_data_) {
            out.emit_data(Batch<int>{records_});
            emitted_data_ = true;
            // Give the loop a moment to spin before the barrier
            // arrives so the feedback channel has records in it.
            std::this_thread::sleep_for(20ms);
            return true;
        }
        if (!emitted_barrier_) {
            out.emit_barrier(barrier_);
            emitted_barrier_ = true;
            // Hold a moment after the barrier so the head's drain
            // completes before the source exits and the loop tears
            // down.
            std::this_thread::sleep_for(30ms);
            return false;
        }
        return false;
    }

    std::string name() const override { return "data_then_barrier"; }

private:
    std::vector<Record<int>> records_;
    CheckpointBarrier barrier_;
    bool emitted_data_{false};
    bool emitted_barrier_{false};
};

TEST(DagIterationCheckpoint, BarrierCapturesInflightFeedbackRecords) {
    // Build a DAG with codec-aware iterate_stream + a state backend.
    // Source emits a few records, then a barrier. The body's
    // operations take longer than the head's poll cycle, so at the
    // moment the head sees the barrier some records are still
    // circulating in feedback. Those records should land in the
    // state backend under the head's inflight slot.
    auto backend = std::make_shared<InMemoryStateBackend>();

    Dag dag;
    auto src = std::make_shared<DataThenBarrierSource>(
        std::vector<Record<int>>{Record<int>{5}, Record<int>{6}, Record<int>{7}},
        CheckpointBarrier{CheckpointId{42}});
    auto h_src = dag.add_source<int>(src);

    IterationConfig iter_cfg;
    iter_cfg.idle_threshold = clink::test_support::kIterationIdleThreshold;
    auto iter = dag.iterate_stream<int>(h_src, int_codec(), iter_cfg);
    // Body: subtract 1 with a small sleep so records linger in the
    // loop. The sleep makes it likely that some records are in the
    // feedback channel when the barrier arrives.
    auto h_body = dag.add_operator<int, int>(iter.head_output(),
                                             std::make_shared<MapOperator<int, int>>([](int v) {
                                                 using namespace std::chrono_literals;
                                                 std::this_thread::sleep_for(2ms);
                                                 return v - 1;
                                             }));
    auto branches =
        dag.add_split<int>(h_body, [](const int& v) -> int { return v > 0 ? 0 : 1; }, 2);
    iter.close_with(branches[0]);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[1], sink);

    // Find the iterate_head's operator id so we can scan its state slot.
    OperatorId head_id{0};
    for (const auto& r : dag.runners()) {
        if (r.name == "iterate_head") {
            head_id = r.id;
            break;
        }
    }
    ASSERT_NE(head_id.value(), 0u);

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // The state backend should hold the captured in-flight buffer
    // under the head's __iterate_inflight__ slot. The slot may have
    // been erased at end-of-run if the head's restore-on-startup ran
    // (it doesn't on a fresh job), so we look at the snapshot taken
    // *during* the run by reaching into the backend's scan.
    //
    // Note: the head's snapshot is its WRITE to the backend during
    // the barrier-forwarding step. We then run to completion (which
    // never restores in this run, so the slot isn't erased on this
    // path). For this test we just verify the bytes were ever
    // written.
    bool slot_seen = false;
    backend->scan(head_id, [&](StateBackend::KeyView k, StateBackend::ValueView) {
        if (k == "__iterate_inflight__") {
            slot_seen = true;
        }
    });
    EXPECT_TRUE(slot_seen)
        << "cycle-checkpointing should have persisted at least one in-flight buffer";
    // Sink delivery is intentionally NOT asserted here. The barrier's
    // drain-feedback step pulls every still-circulating record into
    // the state backend, so under the timing this test forces (the
    // 20ms post-data sleep keeps records in feedback when the barrier
    // arrives) they never re-flow through the body and never reach
    // the sink branch - they only resurface on a subsequent restore.
    // That resurfacing path is covered by
    // DagIterationCheckpoint.RestoreReplaysCapturedInflightRecords.
    (void)sink;
}

TEST(DagIterationCheckpoint, RestoreReplaysCapturedInflightRecords) {
    // Stage 1: synthesize a state-backend entry that pretends a
    // previous run snapshotted some in-flight records. We don't go
    // through a real barrier here - the goal is to verify that the
    // head's RESTORE path picks up the persisted records and pushes
    // them into the feedback channel at startup.
    auto backend = std::make_shared<InMemoryStateBackend>();

    // We need to know the head's operator id. Build a dummy DAG to
    // derive the id, then a fresh DAG for the actual run.
    OperatorId head_id{0};
    {
        Dag probe;
        auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{});
        auto h_src = probe.add_source<int>(src);
        auto iter = probe.iterate_stream<int>(h_src, int_codec());
        for (const auto& r : probe.runners()) {
            if (r.name == "iterate_head") {
                head_id = r.id;
                break;
            }
        }
    }
    ASSERT_NE(head_id.value(), 0u);

    // Stash three records under the head's inflight slot. They should
    // be replayed into feedback at startup, the body decrements each,
    // and the split routes them to output once they hit zero.
    {
        std::vector<Record<int>> persist{Record<int>{1}, Record<int>{2}, Record<int>{3}};
        auto bytes = Dag::serialize_records_(persist, int_codec());
        backend->put(
            head_id,
            StateBackend::KeyView{"__iterate_inflight__"},
            StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()), bytes.size()});
    }

    // Stage 2: build the live DAG. External input is empty (the
    // previous run is what fed the iteration). The body subtracts 1
    // until zero. Captured records should reach the output sink.
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{});
    auto h_src = dag.add_source<int>(src);
    auto iter = dag.iterate_stream<int>(h_src, int_codec());
    auto h_body = dag.add_operator<int, int>(
        iter.head_output(), std::make_shared<MapOperator<int, int>>([](int v) { return v - 1; }));
    auto branches =
        dag.add_split<int>(h_body, [](const int& v) -> int { return v > 0 ? 0 : 1; }, 2);
    iter.close_with(branches[0]);
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[1], sink);

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    // The three replayed records should each have converged to 0.
    EXPECT_EQ(sink->collected().size(), 3u);
    for (int v : sink->collected()) {
        EXPECT_EQ(v, 0);
    }
}

TEST(DagIterationMaxRecords, CapTerminatesNonConvergentLoops) {
    // A body that increments instead of decrements - every record
    // grows by 1 each pass, so the split routing (v > 0 → loop)
    // sends every record back into feedback forever. Without a cap
    // this would never terminate. With IterationConfig::max_records
    // set to 100, the head closes after pushing 100 records through.
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{1}, Record<int>{2}, Record<int>{3}});
    auto h_src = dag.add_source<int>(src);

    IterationConfig cfg;
    cfg.max_records = 100;
    auto iter = dag.iterate_stream<int>(h_src, std::nullopt, cfg);
    auto h_body = dag.add_operator<int, int>(
        iter.head_output(), std::make_shared<MapOperator<int, int>>([](int v) { return v + 1; }));
    // Route everything back into the loop - never exits via output
    // branch on its own. Only the max-records cap can break this.
    auto branches = dag.add_split<int>(h_body, [](const int&) -> int { return 0; }, 2);
    iter.close_with(branches[0]);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[1], sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    // The output sink should be empty (no records ever routed there),
    // but the test's success criterion is simply that run() returned.
    // Without the cap it would block forever.
    EXPECT_TRUE(sink->collected().empty());
}

TEST(DagIteration, EmptyInputProducesEmptyOutput) {
    // Edge case: with no input records, the head's external input
    // closes immediately and the feedback channel has nothing to
    // process either. The head should terminate without spinning.
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{});
    auto h_src = dag.add_source<int>(src);

    auto iter = dag.iterate_stream<int>(h_src);
    auto h_body = dag.add_operator<int, int>(
        iter.head_output(), std::make_shared<MapOperator<int, int>>([](int v) { return v - 1; }));
    auto branches =
        dag.add_split<int>(h_body, [](const int& v) -> int { return v > 0 ? 0 : 1; }, 2);
    iter.close_with(branches[0]);

    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(branches[1], sink);

    LocalExecutor exec(std::move(dag));
    exec.run();
    EXPECT_TRUE(sink->collected().empty());
}
