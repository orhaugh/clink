// The armed drain-at-checkpoint contract (hot rescale, design record 008).
//
// Arming a source with checkpoint id C means: barrier C is your last. The
// runner emits barrier C exactly as usual - the offset snapshot captures the
// cut, the barrier flows downstream, the ack fires - and then the source
// emits nothing more. No record after C, no end-of-input tail (no max
// watermark firing event-time windows, no terminal commit): a cutover is a
// handoff to the post-C subtasks, not an ending.
//
// Everything here is scripted with latches; a step happens when its gate
// opens, never after a sleep. The barrier ids are chosen by the test, so
// "an EARLIER barrier does not stop the source" and "the armed barrier does"
// are both pinned in one flow. Each of the two source runners (single and
// parallel) carries its own copy of the armed check, so each gets its own
// test - F81 taught that a guard tested in only one of two twin paths can be
// deleted from the other without anything going red.
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <map>
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
#include "clink/runtime/network/local_data_plane.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

struct ArmedDrainGate {
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

// Scripted source. Five records, a parked step for the first barrier, one
// more record to prove that barrier stopped nothing, a parked step for the
// armed barrier, and then - reachable only if the arm failed - two further
// records and a clean end-of-input.
class ArmedDrainScriptedSource final : public Source<int> {
public:
    ArmedDrainScriptedSource(ArmedDrainGate& pre_done,
                             ArmedDrainGate& gate1,
                             ArmedDrainGate& mid_done,
                             ArmedDrainGate& gate2)
        : pre_done_(pre_done), gate1_(gate1), mid_done_(mid_done), gate2_(gate2) {}

    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        const int call = ++calls_;
        if (call <= 5) {
            emit_one_(out);
            if (call == 5) {
                pre_done_.signal();
            }
            return true;
        }
        if (call == 6) {
            // The test injects barrier 1 before opening this gate; returning
            // without emitting lets the runner's drain emit it next.
            (void)gate1_.await();
            return true;
        }
        if (call == 7) {
            emit_one_(out);  // r5: follows barrier 1, so barrier 1 stopped nothing
            mid_done_.signal();
            return true;
        }
        if (call == 8) {
            // The test injects the ARMED barrier before opening this gate.
            (void)gate2_.await();
            return true;
        }
        // Reachable only if the armed drain failed to stop the loop.
        if (call <= 10) {
            emit_one_(out);
            return true;
        }
        return false;
    }

    void snapshot_offset(StateBackend& /*backend*/, OperatorId /*op*/, CheckpointId ckpt) override {
        std::lock_guard lock(mu_);
        offset_at_snapshot_[ckpt.value()] = emitted_;
    }

    // Bounded, so the UNARMED control run exercises the full end-of-input
    // tail (max watermark + terminal commit) that the armed run must skip.
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }

    [[nodiscard]] int produce_calls() const noexcept { return calls_.load(); }
    [[nodiscard]] int offset_at(std::uint64_t ckpt) const {
        std::lock_guard lock(mu_);
        auto it = offset_at_snapshot_.find(ckpt);
        return it == offset_at_snapshot_.end() ? -1 : it->second;
    }

    std::string name() const override { return name_; }
    void set_name(std::string n) { name_ = std::move(n); }

private:
    void emit_one_(Emitter<int>& out) {
        Batch<int> b;
        b.emplace(emitted_++);
        out.emit_data(std::move(b));
    }

    ArmedDrainGate& pre_done_;
    ArmedDrainGate& gate1_;
    ArmedDrainGate& mid_done_;
    ArmedDrainGate& gate2_;
    std::atomic<int> calls_{0};
    int emitted_{0};
    mutable std::mutex mu_;
    std::map<std::uint64_t, int> offset_at_snapshot_;
    std::string name_{"armed_drain.source"};
};

// Ordered capture of everything the sink sees. The drain marker is
// deliberately invisible here - the sink runner no-ops it - so the armed
// contract is asserted through what must and must not follow barrier C.
class ArmedDrainCaptureSink final : public Sink<int> {
public:
    struct Event {
        enum class Kind { Data, Barrier, TerminalBarrier, MaxWatermark, OtherWatermark } kind;
        std::int64_t value{0};  // record value or barrier id
    };

    void on_data(const Batch<int>& batch) override {
        std::lock_guard lock(mu_);
        for (const auto& rec : batch) {
            events_.push_back({Event::Kind::Data, rec.value()});
        }
    }
    void on_barrier(CheckpointBarrier b) override {
        {
            std::lock_guard lock(mu_);
            events_.push_back(
                {b.is_terminal() ? Event::Kind::TerminalBarrier : Event::Kind::Barrier,
                 static_cast<std::int64_t>(b.id().value())});
        }
        if (!b.is_terminal() && b.id().value() == awaited_barrier_.load()) {
            barrier_seen_.signal();
        }
    }
    void on_watermark(Watermark wm) override {
        std::lock_guard lock(mu_);
        events_.push_back(
            {wm == Watermark::max() ? Event::Kind::MaxWatermark : Event::Kind::OtherWatermark,
             wm.timestamp().millis()});
    }

    void await_barrier(std::uint64_t id) { awaited_barrier_.store(id); }
    [[nodiscard]] bool barrier_arrived() { return barrier_seen_.await(); }

    [[nodiscard]] std::vector<Event> events() const {
        std::lock_guard lock(mu_);
        return events_;
    }

    std::string name() const override { return name_; }
    void set_name(std::string n) { name_ = std::move(n); }

private:
    mutable std::mutex mu_;
    std::vector<Event> events_;
    std::atomic<std::uint64_t> awaited_barrier_{0};
    ArmedDrainGate barrier_seen_;
    std::string name_{"armed_drain.sink"};
};

using Kind = ArmedDrainCaptureSink::Event::Kind;

// Unique operator names per run: derive_id hashes the name and the process-
// global LocalDataPlane registry cross-wires same-named stages across tests
// (the F78 lesson, inherited from the shuffle-cut suite).
std::string unique_tag() {
    static std::atomic<unsigned> seq{0};
    return "run" + std::to_string(seq.fetch_add(1));
}

}  // namespace

TEST(SourceArmedDrain, ArmedSourceStopsExactlyAtTheCutoverBarrier) {
    network::LocalDataPlane::instance().clear_for_testing();
    ArmedDrainGate pre_done;
    ArmedDrainGate gate1;
    ArmedDrainGate mid_done;
    ArmedDrainGate gate2;
    const auto tag = unique_tag();

    auto src = std::make_shared<ArmedDrainScriptedSource>(pre_done, gate1, mid_done, gate2);
    src->set_name("armed_drain.source." + tag);
    auto sink = std::make_shared<ArmedDrainCaptureSink>();
    sink->set_name("armed_drain.sink." + tag);

    Dag dag;
    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, sink);
    ASSERT_EQ(dag.source_injectors().size(), 1u);
    auto injectors = dag.source_injectors();

    std::mutex ack_mu;
    std::vector<std::uint64_t> acked;

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    cfg.drain_at_checkpoint =
        std::make_shared<std::atomic<std::uint64_t>>(2);  // armed BEFORE start
    cfg.on_checkpoint_ack = [&](CheckpointId id, bool ok, std::string /*error*/) {
        std::lock_guard lock(ack_mu);
        if (ok) {
            acked.push_back(id.value());
        }
    };
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    // Five records out, source parked.
    ASSERT_TRUE(pre_done.await()) << "the source never finished its pre-barrier records";

    // Barrier 1 is NOT the armed id. It must flow - and stop nothing.
    sink->await_barrier(1);
    injectors.front()(CheckpointBarrier{CheckpointId{1}});
    gate1.signal();
    ASSERT_TRUE(sink->barrier_arrived()) << "barrier 1 never reached the sink";
    ASSERT_TRUE(mid_done.await())
        << "the source never emitted past barrier 1 - an unarmed barrier stopped it";

    // Barrier 2 IS the armed id. It must flow, and be the last thing that does.
    sink->await_barrier(2);
    injectors.front()(CheckpointBarrier{CheckpointId{2}});
    gate2.signal();
    ASSERT_TRUE(sink->barrier_arrived()) << "the armed barrier never reached the sink";

    // The runner must exit on its own: no cancel. A failed arm leaves the
    // scripted source running to its end-of-input instead, and the capture
    // assertions below name exactly what leaked.
    exec.await_termination();

    const auto events = sink->events();
    // Expected, in order: r0..r4, B1, r5, B2 - and nothing after B2.
    ASSERT_GE(events.size(), 8u);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(events[static_cast<std::size_t>(i)].kind, Kind::Data);
        EXPECT_EQ(events[static_cast<std::size_t>(i)].value, i);
    }
    EXPECT_EQ(events[5].kind, Kind::Barrier);
    EXPECT_EQ(events[5].value, 1);
    EXPECT_EQ(events[6].kind, Kind::Data);
    EXPECT_EQ(events[6].value, 5);
    EXPECT_EQ(events[7].kind, Kind::Barrier);
    EXPECT_EQ(events[7].value, 2);
    EXPECT_EQ(events.size(), 8u)
        << "something followed the armed barrier. A record here means the produce loop ran "
           "past the cutover cut; a max watermark or terminal barrier means the end-of-input "
           "tail ran for what is a handoff, firing windows and committing a tail that the "
           "post-cutover subtasks will produce again - counted twice after the restore.";

    // The cut the snapshot captured is the cut the stream shows: six records
    // (r0..r5) preceded barrier 2, and the offset snapshot at 2 says so.
    EXPECT_EQ(src->offset_at(2), 6)
        << "the offset snapshot at the armed barrier disagrees with the emitted stream";
    // Produce was never called again after the armed barrier's drain.
    EXPECT_LE(src->produce_calls(), 8);

    // The arm must not swallow C's ack - it is what the coordinator
    // completes the cutover checkpoint on. Every runner with a backend acks
    // each barrier in-process (source and sink here), so assert membership
    // and absence, not a single-acker sequence.
    {
        std::lock_guard lock(ack_mu);
        EXPECT_GE(std::count(acked.begin(), acked.end(), std::uint64_t{2}), 1)
            << "no ok-ack for the armed checkpoint: the drain pre-empted the ack, so the "
               "coordinator could never complete the cutover checkpoint";
        for (const auto id : acked) {
            EXPECT_TRUE(id == 1 || id == 2) << "an ack fired for a checkpoint never injected";
        }
    }
}

TEST(SourceArmedDrain, AnUnarmedSourceRunsItsFullEndOfInputTail) {
    // The control: identical pipeline, gates pre-opened, nothing armed. The
    // bounded source runs to genuine end-of-input, so the tail the armed run
    // must skip is all here: every scripted record, then the max watermark,
    // then the terminal barrier. Delete the arm-vs-tail distinction in the
    // runner - for instance by making any drain skip the tail - and this
    // test names it.
    network::LocalDataPlane::instance().clear_for_testing();
    ArmedDrainGate pre_done;
    ArmedDrainGate gate1;
    ArmedDrainGate mid_done;
    ArmedDrainGate gate2;
    gate1.signal();
    gate2.signal();
    const auto tag = unique_tag();

    auto src = std::make_shared<ArmedDrainScriptedSource>(pre_done, gate1, mid_done, gate2);
    src->set_name("armed_drain.source." + tag);
    auto sink = std::make_shared<ArmedDrainCaptureSink>();
    sink->set_name("armed_drain.sink." + tag);

    Dag dag;
    auto h0 = dag.add_source<int>(src);
    dag.add_sink<int>(h0, sink);

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    // No drain_at_checkpoint: unarmed.
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();
    exec.await_termination();

    const auto events = sink->events();
    std::size_t data_count = 0;
    bool saw_max_watermark = false;
    bool saw_terminal = false;
    for (const auto& e : events) {
        data_count += e.kind == Kind::Data ? 1u : 0u;
        saw_max_watermark |= e.kind == Kind::MaxWatermark;
        saw_terminal |= e.kind == Kind::TerminalBarrier;
    }
    EXPECT_EQ(data_count, 8u) << "the unarmed source did not deliver its full script";
    EXPECT_TRUE(saw_max_watermark)
        << "no max watermark at end-of-input: the tail this suite proves the ARMED run skips "
           "did not run for the unarmed one, so the armed assertions above are vacuous";
    EXPECT_TRUE(saw_terminal) << "no terminal barrier at end-of-input (in-process tail commit)";
}

TEST(SourceArmedDrain, TheParallelSourceRunnerHonoursTheSameArm) {
    // The parallel-source runner carries its own copy of the armed check.
    // Same script, driven through add_parallel_source + fan-in sink.
    network::LocalDataPlane::instance().clear_for_testing();
    ArmedDrainGate pre_done;
    ArmedDrainGate gate1;
    ArmedDrainGate mid_done;
    ArmedDrainGate gate2;
    const auto tag = unique_tag();

    auto src = std::make_shared<ArmedDrainScriptedSource>(pre_done, gate1, mid_done, gate2);
    src->set_name("armed_drain.psource." + tag);
    auto sink = std::make_shared<ArmedDrainCaptureSink>();
    sink->set_name("armed_drain.psink." + tag);

    Dag dag;
    auto h0 = dag.add_parallel_source<int>([&](std::size_t) { return src; }, 1);
    dag.add_parallel_sink<int>(h0, [&](std::size_t) { return sink; }, 1);
    ASSERT_EQ(dag.source_injectors().size(), 1u);
    auto injectors = dag.source_injectors();

    JobConfig cfg;
    cfg.state_backend = std::make_shared<InMemoryStateBackend>();
    cfg.drain_at_checkpoint = std::make_shared<std::atomic<std::uint64_t>>(2);
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    ASSERT_TRUE(pre_done.await());
    sink->await_barrier(1);
    injectors.front()(CheckpointBarrier{CheckpointId{1}});
    gate1.signal();
    ASSERT_TRUE(sink->barrier_arrived()) << "barrier 1 never reached the fan-in sink";
    ASSERT_TRUE(mid_done.await()) << "an unarmed barrier stopped the parallel source runner";

    sink->await_barrier(2);
    injectors.front()(CheckpointBarrier{CheckpointId{2}});
    gate2.signal();
    ASSERT_TRUE(sink->barrier_arrived()) << "the armed barrier never reached the fan-in sink";

    exec.await_termination();

    const auto events = sink->events();
    std::size_t after_armed_barrier = 0;
    bool past_armed = false;
    bool saw_max_watermark = false;
    for (const auto& e : events) {
        if (past_armed) {
            ++after_armed_barrier;
        }
        past_armed |= e.kind == Kind::Barrier && e.value == 2;
        saw_max_watermark |= e.kind == Kind::MaxWatermark;
    }
    EXPECT_EQ(after_armed_barrier, 0u)
        << "the parallel source runner emitted past the armed barrier - its copy of the "
           "armed check is gone or wrong while the single-subtask runner's still passes";
    EXPECT_FALSE(saw_max_watermark)
        << "the parallel runner ran the end-of-input tail on an armed drain";
    EXPECT_LE(src->produce_calls(), 8);
}
