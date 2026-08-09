// Downstream rebind (hot rescale, design record 008, increment 4).
//
// The task fed by a rescaled operator keeps running across the cutover; its
// input SET changes. Old inputs end with barrier C then close - the existing
// closed-and-drained rule retires them. New inputs join mid-run: the worker
// splices a channel into the union's UnionRebindSlot, the runner admits it
// through MultiInputAlignment::add_input, and from then on barriers align
// across the grown membership and the newcomer's records flow.
//
// The aligner-level tests pin the admission rules exactly (a join is
// refused while a barrier is in flight, admitted between barriers, and a
// post-join barrier is not forwarded until the newcomer delivers it). The
// runner-level test proves membership end to end: data spliced in flows,
// the next barrier counts the newcomer, and the union survives the old
// inputs closing and exits when the grown set closes.
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_barrier.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/multi_input_alignment.hpp"
#include "clink/runtime/union_rebind.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

struct RebindStepGate {
    std::mutex mu;
    std::condition_variable cv;
    bool open{false};
    void signal() {
        {
            std::lock_guard lock(mu);
            open = true;
        }
        cv.notify_all();
    }
    [[nodiscard]] bool await(std::chrono::seconds bound = std::chrono::seconds{30}) {
        std::unique_lock lock(mu);
        return cv.wait_for(lock, bound, [&] { return open; });
    }
};

// A source driven entirely by an external channel: the test pushes
// elements, the source relays them, and a closed feed ends the stream.
// This makes union inputs fully scriptable without timing.
class RebindFedSource final : public Source<int> {
public:
    explicit RebindFedSource(std::string name) : name_(std::move(name)) {
        feed_ = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    }

    std::shared_ptr<BoundedChannel<StreamElement<int>>> feed() { return feed_; }

    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        auto e = feed_->pop();
        if (!e.has_value()) {
            return false;
        }
        if (e->is_data()) {
            out.emit_data(std::move(e->as_data()));
        } else if (e->is_watermark()) {
            out.emit_watermark(e->as_watermark());
        } else if (e->is_barrier()) {
            out.emit_barrier(e->as_barrier());
        }
        return true;
    }

    void cancel() override {
        Source<int>::cancel();
        feed_->close();
    }

    std::string name() const override { return name_; }

private:
    std::shared_ptr<BoundedChannel<StreamElement<int>>> feed_;
    std::string name_;
};

// Ordered capture with a per-kind arrival gate, so the test can wait for
// "barrier N arrived" instead of sleeping.
class RebindCaptureSink final : public Sink<int> {
public:
    struct Event {
        enum class Kind { Data, Barrier } kind;
        std::int64_t value{0};
    };

    void on_data(const Batch<int>& batch) override {
        std::lock_guard lock(mu_);
        for (const auto& rec : batch) {
            events_.push_back({Event::Kind::Data, rec.value()});
            if (rec.value() == awaited_value_.load()) {
                arrived_.signal();
            }
        }
    }
    void on_barrier(CheckpointBarrier b) override {
        std::lock_guard lock(mu_);
        events_.push_back({Event::Kind::Barrier, static_cast<std::int64_t>(b.id().value())});
        if (static_cast<std::int64_t>(b.id().value()) == awaited_barrier_.load()) {
            arrived_.signal();
        }
    }
    void on_watermark(Watermark) override {}

    void await_value(std::int64_t v) { awaited_value_.store(v); }
    void await_barrier(std::int64_t id) { awaited_barrier_.store(id); }
    [[nodiscard]] bool arrived() { return arrived_.await(); }
    void rearm() {
        std::lock_guard lock(mu_);
        std::lock_guard gate_lock(arrived_.mu);
        arrived_.open = false;
        awaited_value_.store(-1);
        awaited_barrier_.store(-1);
    }

    [[nodiscard]] std::vector<Event> events() const {
        std::lock_guard lock(mu_);
        return events_;
    }

    std::string name() const override { return "rebind.capture"; }

private:
    mutable std::mutex mu_;
    std::vector<Event> events_;
    std::atomic<std::int64_t> awaited_value_{-1};
    std::atomic<std::int64_t> awaited_barrier_{-1};
    RebindStepGate arrived_;
};

Batch<int> one(int v) {
    Batch<int> b;
    b.emplace(v);
    return b;
}

std::string rebind_tag() {
    static std::atomic<unsigned> seq{0};
    return "rb" + std::to_string(seq.fetch_add(1));
}

}  // namespace

TEST(UnionRebind, AlignmentAdmitsBetweenBarriersAndCountsTheNewcomerAfterwards) {
    MultiInputAlignment align(2);

    // Barrier 1 completes over the original membership.
    EXPECT_FALSE(align.on_barrier(0, CheckpointBarrier{CheckpointId{1}}).forward);
    EXPECT_TRUE(align.on_barrier(1, CheckpointBarrier{CheckpointId{1}}).forward);

    // Between barriers: the join is admitted and gets the next index.
    const auto idx = align.add_input();
    ASSERT_TRUE(idx.has_value());
    EXPECT_EQ(*idx, 2u);
    EXPECT_EQ(align.input_count(), 3u);

    // Barrier 2 must now wait for all THREE inputs - the newcomer is a
    // full member, not a bystander.
    EXPECT_FALSE(align.on_barrier(0, CheckpointBarrier{CheckpointId{2}}).forward);
    EXPECT_FALSE(align.on_barrier(1, CheckpointBarrier{CheckpointId{2}}).forward)
        << "barrier 2 aligned over the pre-join membership: the newcomer was never counted "
           "and a snapshot taken now would miss its in-flight records";
    EXPECT_TRUE(align.on_barrier(2, CheckpointBarrier{CheckpointId{2}}).forward);
}

TEST(UnionRebind, AlignmentRefusesAJoinWhileABarrierIsInFlight) {
    MultiInputAlignment align(2);

    // Barrier 5 is pending: input 0 delivered, input 1 has not.
    EXPECT_FALSE(align.on_barrier(0, CheckpointBarrier{CheckpointId{5}}).forward);
    EXPECT_FALSE(align.add_input().has_value())
        << "a join was admitted mid-alignment; barrier 5's delivery bitmap was sized "
           "without the newcomer and its completion no longer means what it claims";

    // Completion clears the way.
    EXPECT_TRUE(align.on_barrier(1, CheckpointBarrier{CheckpointId{5}}).forward);
    EXPECT_TRUE(align.add_input().has_value());
}

TEST(UnionRebind, ALingeringCompletedUnalignedEntryDoesNotWedgeTheJoin) {
    // Single input, unaligned barrier: the first delivery forwards and the
    // bookkeeping entry lingers (its GC runs on SUBSEQUENT deliveries,
    // which never come at input_count == 1). add_input must sweep it
    // rather than read it as in-flight.
    MultiInputAlignment align(1);
    const auto adv = align.on_barrier(
        0, CheckpointBarrier{CheckpointId{7}, false, CheckpointBarrier::Mode::Unaligned});
    EXPECT_TRUE(adv.forward);
    EXPECT_TRUE(adv.unaligned_first);
    EXPECT_TRUE(align.add_input().has_value())
        << "a completed unaligned barrier's lingering bookkeeping blocked the join forever";
}

TEST(UnionRebind, TheUnionAdmitsASplicedChannelAndItsBarriersCountIt) {
    const auto tag = rebind_tag();
    auto s0 = std::make_shared<RebindFedSource>("rebind.s0." + tag);
    auto s1 = std::make_shared<RebindFedSource>("rebind.s1." + tag);
    auto sink = std::make_shared<RebindCaptureSink>();
    auto slot = std::make_shared<UnionRebindSlot<int>>();

    Dag dag;
    auto h0 = dag.add_source<int>(s0);
    auto h1 = dag.add_source<int>(s1);
    auto merged = dag.union_streams<int>({h0, h1}, slot);
    dag.add_sink<int>(merged, sink);

    JobConfig cfg;
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    // Barrier 1 over the original membership.
    s0->feed()->push(StreamElement<int>::data(one(10)));
    s1->feed()->push(StreamElement<int>::data(one(11)));
    sink->await_barrier(1);
    s0->feed()->push(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{1}}));
    s1->feed()->push(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{1}}));
    ASSERT_TRUE(sink->arrived()) << "barrier 1 never aligned over the original inputs";

    // Splice a third input mid-run and prove membership by data flow.
    auto spliced = std::make_shared<BoundedChannel<StreamElement<int>>>(64);
    slot->splice(spliced);
    sink->rearm();
    sink->await_value(42);
    spliced->push(StreamElement<int>::data(one(42)));
    ASSERT_TRUE(sink->arrived())
        << "a record pushed into the spliced channel never reached the union's output - the "
           "slot was never drained or the admitted channel is not being polled";

    // The next barrier counts the newcomer: deliver it on the two original
    // inputs plus the spliced one; it must forward exactly once.
    sink->rearm();
    sink->await_barrier(2);
    s0->feed()->push(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{2}}));
    s1->feed()->push(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{2}}));
    spliced->push(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{2}}));
    ASSERT_TRUE(sink->arrived()) << "barrier 2 never aligned over the grown membership";

    // Scale-down shape: the original inputs close (their upstreams ended at
    // the cutover); the union keeps running on the spliced input alone.
    s0->feed()->close();
    s1->feed()->close();
    sink->rearm();
    sink->await_value(43);
    spliced->push(StreamElement<int>::data(one(43)));
    ASSERT_TRUE(sink->arrived())
        << "the union stopped serving the spliced input once the original inputs closed";

    // And the union exits when the GROWN set has fully closed - if the
    // aligner never grew, all_closed() fired at the originals' close and
    // record 43 above would already have been lost.
    spliced->close();
    exec.await_termination();

    const auto events = sink->events();
    std::size_t b1 = 0;
    std::size_t b2 = 0;
    bool saw_42 = false;
    bool saw_43 = false;
    for (const auto& e : events) {
        if (e.kind == RebindCaptureSink::Event::Kind::Barrier) {
            b1 += e.value == 1 ? 1u : 0u;
            b2 += e.value == 2 ? 1u : 0u;
        } else {
            saw_42 |= e.value == 42;
            saw_43 |= e.value == 43;
        }
    }
    EXPECT_EQ(b1, 1u) << "barrier 1 forwarded other than exactly once";
    EXPECT_EQ(b2, 1u) << "barrier 2 forwarded other than exactly once";
    EXPECT_TRUE(saw_42);
    EXPECT_TRUE(saw_43);
}
