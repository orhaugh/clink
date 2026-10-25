// Event-time order in the CEP operator.
//
// By default a pattern sees each key's records in the order they arrive.
// order_by_event_time() holds each record until the watermark passes it and
// feeds the key's held records to the pattern in event-time order, so a record
// that arrives out of order but ahead of the watermark takes its place in time.
// These tests pin that order, the tie-break among equal times, the late bound,
// the end of the input, and that held records and the late bound both survive
// a checkpoint.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "clink/cep/cep_operator.hpp"
#include "clink/cep/pattern.hpp"
#include "clink/core/codec.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"

namespace {

using namespace clink;
using namespace clink::cep;

struct Tx {
    std::int64_t key{0};
    int kind{0};  // 1 = credit, 2 = debit
    int payload{0};
};

constexpr OperatorId kOp{41};

// A credit directly followed by a debit.
Pattern<Tx> credit_then_debit() {
    return Pattern<Tx>::begin("c")
        .where([](const Tx& t) { return t.kind == 1; })
        .next("d")
        .where([](const Tx& t) { return t.kind == 2; });
}

// Names a match by its credit's payload and its debit's, so a test can see
// which records matched.
int describe(const PatternMatch<Tx>& m) {
    return m.at("c").front().payload * 100 + m.at("d").front().payload;
}

std::shared_ptr<CepOperator<Tx, int>> make_op(bool event_time_order) {
    auto op = std::make_shared<CepOperator<Tx, int>>(
        credit_then_debit(),
        trivial_codec<Tx>(),
        [](const Tx& t) { return t.key; },
        describe,
        "cep_event_time_order_test");
    op->order_by_event_time(event_time_order);
    return op;
}

// Drives the operator directly, so watermarks, the end of the input and
// checkpoints fall exactly where a test puts them.
class Probe {
public:
    explicit Probe(bool event_time_order, std::optional<Snapshot> restore_from = std::nullopt)
        : backend_(std::make_shared<InMemoryStateBackend>()), op_(make_op(event_time_order)) {
        op_->set_id(kOp);
        if (restore_from.has_value()) {
            backend_->restore(*restore_from);
        }
        rctx_ = std::make_unique<RuntimeContext>(kOp, "cep", backend_.get(), /*metrics=*/nullptr);
        op_->attach_runtime(rctx_.get());
        // As the runner restores: timers and operator state first, then open().
        op_->restore_timers(*backend_, kOp);
        op_->open();
    }

    void feed(std::int64_t key, int kind, int payload, std::int64_t ts) {
        Batch<Tx> b;
        b.push(Record<Tx>{Tx{key, kind, payload}, EventTime::from_millis(ts)});
        op_->process(StreamElement<Tx>::data(std::move(b)), emitter_);
    }

    void watermark(std::int64_t ms) {
        op_->process(StreamElement<Tx>::watermark(Watermark{EventTime::from_millis(ms)}), emitter_);
    }

    void end() { op_->flush(emitter_); }

    // A checkpoint as the runner takes one: the operator's timers and late
    // bound into operator state, then the backend.
    Snapshot checkpoint() {
        op_->snapshot_timers(*backend_, kOp);
        return backend_->snapshot(CheckpointId{1});
    }

    // The matches emitted since the last call.
    std::vector<int> take() { return std::exchange(emitted_, {}); }

private:
    std::shared_ptr<InMemoryStateBackend> backend_;
    std::shared_ptr<CepOperator<Tx, int>> op_;
    std::unique_ptr<RuntimeContext> rctx_;
    std::vector<int> emitted_;
    Emitter<int> emitter_{Emitter<int>::Forward{[this](StreamElement<int> e) {
        if (e.is_data()) {
            for (const auto& r : e.as_data().records()) {
                emitted_.push_back(r.value());
            }
        }
        return true;
    }}};
};

}  // namespace

TEST(CepEventTimeOrder, ARecordAheadOfTheWatermarkTakesItsPlaceInEventTime) {
    // The debit arrives first and the credit it follows second, both ahead of
    // the watermark.
    Probe arrival(false);
    arrival.feed(1, 2, 2, 20);
    arrival.feed(1, 1, 1, 10);
    arrival.watermark(20);
    EXPECT_TRUE(arrival.take().empty()) << "in arrival order the debit comes before its credit";

    Probe event_time(true);
    event_time.feed(1, 2, 2, 20);
    event_time.feed(1, 1, 1, 10);
    EXPECT_TRUE(event_time.take().empty()) << "matched before the watermark passed the records";
    event_time.watermark(15);
    EXPECT_TRUE(event_time.take().empty()) << "the debit at 20 is ahead of a watermark at 15";
    event_time.watermark(20);
    EXPECT_EQ(event_time.take(), std::vector<int>{102});
}

TEST(CepEventTimeOrder, RecordsAtTheSameTimeKeepTheOrderTheyArrivedIn) {
    Probe credit_first(true);
    credit_first.feed(1, 1, 1, 10);
    credit_first.feed(1, 2, 2, 10);
    credit_first.watermark(10);
    EXPECT_EQ(credit_first.take(), std::vector<int>{102});

    Probe debit_first(true);
    debit_first.feed(1, 2, 2, 10);
    debit_first.feed(1, 1, 1, 10);
    debit_first.watermark(10);
    EXPECT_TRUE(debit_first.take().empty());
}

TEST(CepEventTimeOrder, EachKeyIsReleasedOnlyAsFarAsTheWatermark) {
    Probe p(true);
    p.feed(1, 2, 2, 20);
    p.feed(2, 1, 3, 5);
    p.feed(1, 1, 1, 10);
    p.feed(2, 2, 4, 30);
    p.watermark(25);
    EXPECT_EQ(p.take(), std::vector<int>{102}) << "key 2's debit at 30 is still held";
    p.watermark(30);
    EXPECT_EQ(p.take(), std::vector<int>{304});
}

TEST(CepEventTimeOrder, ARecordAtOrBehindTheWatermarkIsDropped) {
    Probe p(true);
    p.feed(1, 1, 1, 10);
    p.watermark(10);
    p.feed(1, 2, 2, 10);  // at the watermark
    p.feed(1, 2, 3, 5);   // behind it
    p.feed(1, 2, 4, 15);  // ahead of it
    p.watermark(20);
    EXPECT_EQ(p.take(), std::vector<int>{104});
}

TEST(CepEventTimeOrder, TheEndOfTheInputReleasesWhatIsStillHeld) {
    Probe p(true);
    p.feed(1, 2, 2, 20);
    p.feed(1, 1, 1, 10);
    p.end();
    EXPECT_EQ(p.take(), std::vector<int>{102});
}

TEST(CepEventTimeOrder, HeldRecordsAndTheLateBoundSurviveACheckpoint) {
    Snapshot snap;
    {
        Probe before(true);
        before.feed(1, 1, 1, 10);
        before.watermark(10);      // the credit reaches the pattern
        before.feed(1, 2, 2, 20);  // the debit is held
        snap = before.checkpoint();
        EXPECT_TRUE(before.take().empty());
    }
    Probe after(true, std::move(snap));
    // Behind the watermark the first run had passed, so dropped. Matched, it
    // would complete the credit at 10 with a debit from before it.
    after.feed(1, 2, 3, 5);
    after.watermark(20);
    EXPECT_EQ(after.take(), std::vector<int>{102});
}

TEST(CepEventTimeOrder, AJobMatchesItsRecordsInEventTimeOrder) {
    // Through a Dag: the source's end-of-input watermark releases the records,
    // which it delivered out of order.
    Dag dag;
    auto src = std::make_shared<VectorSource<Tx>>(std::vector<Record<Tx>>{
        Record<Tx>{Tx{1, 2, 2}, EventTime::from_millis(20)},
        Record<Tx>{Tx{1, 1, 1}, EventTime::from_millis(10)},
    });
    auto h_src = dag.add_source<Tx>(src);
    auto h_op = dag.add_operator<Tx, int>(h_src, make_op(true));
    auto sink = std::make_shared<CollectingSink<int>>();
    dag.add_sink<int>(h_op, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.run();

    EXPECT_EQ(sink->collected(), std::vector<int>{102});
}
