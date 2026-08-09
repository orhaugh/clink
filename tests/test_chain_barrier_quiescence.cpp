// A chain's checkpoint owner captures a backend other chain members are still
// writing.
//
// In the cluster, one subtask's in-process dag is a CHAIN of runners - for the
// rescale exactly-once job's counter subtask: bridge-source -> counter ->
// bridge-sink. Three runners, three THREADS, one shared state backend, and the
// single-writer scheme makes the most-downstream element (the sink) the
// checkpoint owner: only the owner calls backend->snapshot()/capture(), when the
// barrier reaches IT.
//
// Capture-before-process protects the OWNER's stream position only. A non-owner
// upstream of it processes the barrier, forwards it, and carries straight on -
// nothing stops it popping the next record and writing to the shared backend
// while the barrier is still queued between it and the owner. Those post-barrier
// writes land inside the owner's capture, so the persisted checkpoint holds MORE
// than the cut.
//
// This produced the F67 artefact exactly: a completed checkpoint whose source
// offset said 41 records while a counter's state included record-41. The source
// side is immune by asymmetry - its backend row is written only at its barrier
// drain, never per record - which is why the offset stayed exact while the counts
// skewed. In production the skew needed the owner's thread to lag at the precise
// barrier, roughly one failure in thirty runs; here the lag is FORCED with a
// gate, so the window is deterministic on every run.
//
// The fix under test is the ChainBarrierEpoch rendezvous (dag.hpp): after
// forwarding barrier N, a non-owner may not process further elements until the
// owner's capture of N completes. Before that rendezvous existed, this test
// failed every run with "contains 6 ... where the cut is 5".
//
// No sleeps as synchronisation anywhere: every step waits on a condition.
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/job_config.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/state/in_memory_state_backend.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

constexpr int kPreBarrierRecords = 5;
const OperatorId kCountOp{42};
constexpr const char* kCountKey = "count";

// One latch per named step. A deadline exists only as a failure bound.
struct StepGate {
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

std::int64_t decode_count(const std::optional<std::vector<std::byte>>& v) {
    if (!v.has_value() || v->size() != sizeof(std::int64_t)) {
        return 0;
    }
    std::int64_t out = 0;
    std::memcpy(&out, v->data(), sizeof(out));
    return out;
}

std::vector<std::byte> encode_count(std::int64_t n) {
    std::vector<std::byte> out(sizeof(n));
    std::memcpy(out.data(), &n, sizeof(n));
    return out;
}

// Records what the shared count WAS at the exact moment the owner captured.
// Observing the capture itself, rather than the file it produces, keeps the
// assertion at the cut point instead of re-deriving it from bytes.
//
// Delegation rather than inheritance because InMemoryStateBackend is final; only
// the seven pure virtuals are forwarded, which is all this chain touches.
class SnapshotProbeBackend final : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { inner_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_.scan(op, visit); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg_filter = {}) override {
        inner_.restore(snap, kg_filter);
    }
    std::string description() const override { return "snapshot-probe over in-memory"; }

    Snapshot snapshot(CheckpointId id) override {
        const auto count = decode_count(
            inner_.get(kCountOp, StateBackend::KeyView{kCountKey, std::strlen(kCountKey)}));
        {
            std::lock_guard lock(mu_);
            count_at_capture_[id.value()] = count;
        }
        captured_.signal();
        return inner_.snapshot(id);
    }

    [[nodiscard]] bool await_first_capture() { return captured_.await(); }

    [[nodiscard]] std::optional<std::int64_t> count_at(std::uint64_t id) const {
        std::lock_guard lock(mu_);
        auto it = count_at_capture_.find(id);
        if (it == count_at_capture_.end()) {
            return std::nullopt;
        }
        return it->second;
    }

private:
    InMemoryStateBackend inner_;
    mutable std::mutex mu_;
    std::map<std::uint64_t, std::int64_t> count_at_capture_;
    StepGate captured_;
};

// Scripted source. Emits kPreBarrierRecords, parks until the test has injected
// the barrier, then emits exactly ONE more record and ends. The barrier is
// drained by the runner BETWEEN produce() calls, so the wait pass below pins the
// barrier's stream position between record N-1 and record N.
class ScriptedSource final : public Source<int> {
public:
    ScriptedSource(StepGate& pre_done, StepGate& resume, StepGate& post_emitted)
        : pre_done_(pre_done), resume_(resume), post_emitted_(post_emitted) {}

    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        if (next_ < kPreBarrierRecords) {
            Batch<int> b;
            b.emplace(next_++);
            out.emit_data(std::move(b));
            return true;
        }
        if (!waited_) {
            waited_ = true;
            pre_done_.signal();
            // Park until the test has called coord.trigger(). Returning true
            // WITHOUT emitting lets the runner's drain emit the queued barrier
            // before the next produce() call.
            (void)resume_.await();
            return true;
        }
        if (!post_sent_) {
            post_sent_ = true;
            Batch<int> b;
            b.emplace(next_++);  // the single post-barrier record
            out.emit_data(std::move(b));
            post_emitted_.signal();
            return true;
        }
        return false;
    }

    // The runner's drain calls this per barrier BEFORE emitting it, so it is a
    // direct probe for "the drain saw barrier N" without touching runner code.
    void snapshot_offset(StateBackend& /*backend*/, OperatorId /*op*/, CheckpointId ckpt) override {
        last_drained_.store(static_cast<std::int64_t>(ckpt.value()));
        drained_.signal();
    }

    [[nodiscard]] bool await_drained() { return drained_.await(std::chrono::seconds{10}); }
    [[nodiscard]] std::int64_t last_drained() const { return last_drained_.load(); }

    std::string name() const override { return "quiescence.source"; }

private:
    StepGate& pre_done_;
    StepGate& resume_;
    StepGate& post_emitted_;
    int next_{0};
    bool waited_{false};
    bool post_sent_{false};
    StepGate drained_;
    std::atomic<std::int64_t> last_drained_{-1};
};

// The non-owner chain member: writes one row in the SHARED backend per record,
// exactly as a keyed counter does, and signals once the post-barrier record has
// been applied.
class SharedCountOperator final : public Operator<int, int> {
public:
    SharedCountOperator(StateBackend& backend, StepGate& post_processed, StepGate& saw_barrier)
        : backend_(backend), post_processed_(post_processed), saw_barrier_(saw_barrier) {}

    void process(const StreamElement<int>& el, Emitter<int>& out) override {
        if (el.is_data()) {
            for (const auto& rec : el.as_data()) {
                const auto key = StateBackend::KeyView{kCountKey, std::strlen(kCountKey)};
                const auto next = decode_count(backend_.get(kCountOp, key)) + 1;
                const auto bytes = encode_count(next);
                backend_.put(kCountOp,
                             key,
                             StateBackend::ValueView{reinterpret_cast<const char*>(bytes.data()),
                                                     bytes.size()});
                if (rec.value() == kPreBarrierRecords) {
                    post_processed_.signal();
                }
            }
            out.emit_data(el.as_data());
            return;
        }
        if (el.is_watermark()) {
            this->on_watermark(el.as_watermark(), out);
            return;
        }
        first_barrier_terminal_.store(el.as_barrier().is_terminal() ? 1 : 0);
        saw_barrier_.signal();
        this->on_barrier(el.as_barrier(), out);
    }

    std::string name() const override { return "quiescence.counter"; }

private:
    StateBackend& backend_;
    StepGate& post_processed_;
    StepGate& saw_barrier_;

public:
    std::atomic<int> first_barrier_terminal_{-1};
};

// The chain OWNER. Its on_barrier runs on the sink runner's thread immediately
// before the owner capture, so parking here holds the capture open while the
// upstream counter keeps processing - the production lag, made deterministic.
class GatedOwnerSink final : public Sink<int> {
public:
    GatedOwnerSink(StepGate& entered, StepGate& at_barrier, StepGate& release)
        : entered_(entered), at_barrier_(at_barrier), release_(release) {}

    void on_data(const Batch<int>& /*batch*/) override {}

    void on_barrier(CheckpointBarrier b) override {
        entered_.signal();  // unconditional: distinguishes "runner never called" from early-return
        if (b.is_terminal() || parked_once_) {
            return;  // only the first periodic barrier is held open
        }
        parked_once_ = true;
        at_barrier_.signal();
        (void)release_.await();
    }

    std::string name() const override { return "quiescence.sink"; }

private:
    StepGate& entered_;
    StepGate& at_barrier_;
    StepGate& release_;
    bool parked_once_{false};
};

}  // namespace

TEST(ChainBarrierQuiescence, OwnerCaptureExcludesRecordsProcessedUpstreamAfterTheBarrier) {
    StepGate pre_done;
    StepGate resume;
    StepGate post_emitted;
    StepGate post_processed;
    StepGate counter_saw_barrier;
    StepGate sink_entered;
    StepGate sink_at_barrier;
    StepGate release_capture;

    auto backend = std::make_shared<SnapshotProbeBackend>();
    CheckpointCoordinator::Config qcfg;
    CheckpointCoordinator coord(backend, qcfg);

    Dag dag;
    auto src = std::make_shared<ScriptedSource>(pre_done, resume, post_emitted);
    auto op = std::make_shared<SharedCountOperator>(*backend, post_processed, counter_saw_barrier);
    auto sink = std::make_shared<GatedOwnerSink>(sink_entered, sink_at_barrier, release_capture);

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    ASSERT_EQ(dag.source_injectors().size(), 1u)
        << "no barrier injector for the source: trigger() would be a silent no-op and only "
           "the EOS terminal barrier would ever flow";
    // trigger() ISSUES a barrier and returns it; delivery is the caller's job (the
    // periodic thread is what normally injects). A manual trigger() alone is a
    // silent no-op at the source - the first cut of this test lost half an hour to
    // exactly that, with only the EOS terminal barrier ever flowing.
    auto injectors = dag.source_injectors();
    coord.set_source_injectors(dag.source_injectors());

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), cfg);
    exec.start();

    // 1. All pre-barrier records are emitted; the source is parked.
    ASSERT_TRUE(pre_done.await()) << "the source never finished its pre-barrier records";

    // 2. Inject the barrier while the source is parked, then let it resume. The
    //    runner drains the barrier between produce() calls, so the barrier's
    //    stream position is pinned: after record N-1, before record N.
    injectors.front()(coord.trigger());
    resume.signal();

    // 2a. The source runner's drain picked the barrier up and emitted it.
    ASSERT_TRUE(src->await_drained())
        << "the source runner's drain never saw the injected barrier: injection reached a "
           "different object, or the drain never ran";
    ASSERT_EQ(src->last_drained(), 1) << "the drain saw a different barrier id";

    // 3a. Bisection probe: did the barrier reach the counter at all?
    ASSERT_TRUE(counter_saw_barrier.await(std::chrono::seconds{10}))
        << "the barrier never reached the counter, so the fault is between the source "
           "runner's drain and the operator runner";
    ASSERT_EQ(op->first_barrier_terminal_.load(), 0)
        << "the FIRST barrier through the counter was terminal - the triggered barrier "
           "never flowed";

    ASSERT_TRUE(sink_entered.await(std::chrono::seconds{10}))
        << "sink->on_barrier was never invoked although the counter forwarded the barrier";

    // 3. The barrier has travelled counter -> sink; the sink is parked in
    //    on_barrier, which on the owner path runs immediately before capture.
    ASSERT_TRUE(sink_at_barrier.await()) << "the barrier never reached the chain owner";

    // 4. The source is NOT held (it writes its backend row only at its own drain),
    //    so it emits the post-barrier record into the counter's queue. The counter,
    //    however, is held at the rendezvous: it must not process that record while
    //    the owner has yet to capture. That is the contract under test, and it is
    //    proven by ordering rather than by a timing window - see steps 5 and 6.
    ASSERT_TRUE(post_emitted.await()) << "the source never emitted the post-barrier record";

    // 5. Release the owner. Its capture must contain only the pre-barrier records:
    //    if the counter had slipped past the rendezvous, the count would be 6 and
    //    the assertion below names the defect.
    release_capture.signal();
    ASSERT_TRUE(backend->await_first_capture()) << "the owner never captured";

    // 6. Only AFTER the capture may the counter process the post-barrier record.
    //    This await also proves the rendezvous releases - a held member that never
    //    woke would hang here, well inside the gate's failure bound.
    ASSERT_TRUE(post_processed.await())
        << "the counter never resumed after the owner's capture - the rendezvous "
           "held it but failed to release it";

    exec.cancel();
    exec.await_termination();

    const auto captured = backend->count_at(1);
    ASSERT_TRUE(captured.has_value()) << "no capture recorded for checkpoint 1";
    EXPECT_EQ(*captured, kPreBarrierRecords)
        << "the chain owner's capture for checkpoint 1 contains " << *captured
        << " counted records where the barrier's cut is " << kPreBarrierRecords
        << ". A non-owner chain member wrote the shared backend after forwarding the "
           "barrier and before the owner - a separate runner thread - captured. A "
           "restore from this checkpoint replays the post-barrier record against state "
           "that already includes it: counted twice, exactly the F67 artefact. The "
           "ChainBarrierEpoch rendezvous in dag.hpp exists to make this impossible.";
}
