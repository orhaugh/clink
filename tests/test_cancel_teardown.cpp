// What a CANCEL does to an operator's lifecycle hooks.
//
// Cancellation was audited at the process level - the job reports cancelled, the
// submitter exits non-zero, the wall time is bounded - and not at the operator
// level. Nothing asserted that close() ran, and nothing asserted that flush() did
// NOT.
//
// That second one is the contract. `clink stop` is the graceful path: it drains,
// takes a final checkpoint and commits the tail. `clink cancel` is abrupt, and its
// whole value is being abrupt - GracefulStopTest exists to keep the two
// distinguishable. An operator that flushes buffered window or join state on
// cancel is quietly draining, which makes the two paths differ by degree rather
// than in kind.
//
// It was doing exactly that, on some operators and not others: the single-input
// runners gate flush() on a clean exit, and the multi-input ones did not. So
// whether a cancelled job drained depended on how many inputs an operator happened
// to have.

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
#include "clink/core/record.hpp"
#include "clink/operators/map_operator.hpp"
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

// Counts its lifecycle hooks. Named for this file so it cannot ODR-collide with
// a same-shaped helper in another test TU - that has bitten this repo before.
class CancelTeardownCountingSink final : public Sink<int> {
public:
    void open() override { opens.fetch_add(1, std::memory_order_relaxed); }
    void on_data(const Batch<int>& batch) override {
        writes.fetch_add(static_cast<int>(batch.size()), std::memory_order_relaxed);
    }
    void flush() override { flushes.fetch_add(1, std::memory_order_relaxed); }
    void close() override { closes.fetch_add(1, std::memory_order_relaxed); }
    std::string name() const override { return "cancel_teardown.sink"; }

    std::atomic<int> opens{0};
    std::atomic<int> writes{0};
    std::atomic<int> flushes{0};
    std::atomic<int> closes{0};
};

// Produces until cancelled. cancelled() is what Source::cancel() sets, so a
// source that never returns false unless cancelled proves the cancel hook
// reached it: if it did not, the test would hang rather than fail.
class CancelTeardownEndlessSource final : public Source<int> {
public:
    bool produce(Emitter<int>& out) override {
        if (this->cancelled()) {
            return false;
        }
        Batch<int> b;
        b.emplace(next_++);
        if (!out.emit_data(std::move(b))) {
            return false;
        }
        emitted.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(2ms);
        return true;
    }
    std::string name() const override { return "cancel_teardown.source"; }

    std::atomic<int> emitted{0};

private:
    int next_{0};
};

bool await(const std::function<bool()>& pred, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

}  // namespace

TEST(CancelTeardown, ASingleInputSinkClosesWithoutFlushingOnCancel) {
    Dag dag;
    auto src = std::make_shared<CancelTeardownEndlessSource>();
    auto sink = std::make_shared<CancelTeardownCountingSink>();
    auto h = dag.add_source<int>(src);
    dag.add_sink<int>(h, sink);

    LocalExecutor exec(std::move(dag));
    exec.start();
    ASSERT_TRUE(await([&] { return sink->writes.load() > 0; }, 5s))
        << "the job never produced, so cancelling it proves nothing";

    exec.cancel();
    exec.await_termination();

    EXPECT_EQ(sink->opens.load(), 1);
    EXPECT_EQ(sink->closes.load(), 1) << "a cancelled sink was not closed, so whatever it holds - "
                                         "a file handle, a broker connection - is released only "
                                         "by process exit";
    EXPECT_EQ(sink->flushes.load(), 0)
        << "a cancelled sink flushed. Cancel is the abrupt path; draining is what `clink stop` "
           "is for, and if cancel drains too the two differ only by degree";
}

TEST(CancelTeardown, AFanInSinkBehavesTheSameWayAsASingleInputOne) {
    // The inconsistency this test was written for. A sink at parallelism 1 fed by
    // an upstream at parallelism 2 runs on the MULTI-INPUT runner, which called
    // flush() unconditionally after its loop. So a cancelled job drained that
    // sink's buffered state and discarded a single-input sink's - same job, same
    // cancel, different durability, decided by the shape of the graph.
    //
    // It has to be add_parallel_sink with a fan-in, not union_streams: a union
    // feeds a SINGLE-input sink, so the first version of this test exercised the
    // runner that was already correct and passed with the fix reverted.
    Dag dag;
    auto sink = std::make_shared<CancelTeardownCountingSink>();
    auto src_handle = dag.add_parallel_source<int>(
        [](std::size_t) -> std::shared_ptr<Source<int>> {
            return std::make_shared<CancelTeardownEndlessSource>();
        },
        /*parallelism*/ 2);
    dag.add_parallel_sink<int>(
        src_handle,
        [sink](std::size_t) -> std::shared_ptr<Sink<int>> { return sink; },
        /*parallelism*/ 1);

    LocalExecutor exec(std::move(dag));
    exec.start();
    ASSERT_TRUE(await([&] { return sink->writes.load() > 0; }, 5s))
        << "the fan-in job never produced, so cancelling it proves nothing";

    exec.cancel();
    exec.await_termination();

    EXPECT_EQ(sink->closes.load(), 1);
    EXPECT_EQ(sink->flushes.load(), 0)
        << "a cancelled FAN-IN sink flushed where a single-input one does not. Whether a cancel "
           "drains must not depend on how many inputs an operator happens to have";
}

TEST(CancelTeardown, ACleanCompletionStillFlushes) {
    // The other half, and the one a too-eager gate breaks. Suppressing flush() on
    // cancel must not suppress it at end of input, or every bounded job silently
    // loses its buffered tail - which would be a far worse defect than the one
    // being fixed.
    Dag dag;
    std::vector<Record<int>> recs;
    for (int i = 0; i < 8; ++i) {
        recs.emplace_back(Record<int>{i});
    }
    auto src = std::make_shared<VectorSource<int>>(std::move(recs), "bounded");
    auto sink = std::make_shared<CancelTeardownCountingSink>();
    auto h = dag.add_source<int>(src);
    dag.add_sink<int>(h, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->writes.load(), 8);
    EXPECT_EQ(sink->flushes.load(), 1)
        << "a job that reached end of input did not flush; its buffered tail is lost";
    EXPECT_EQ(sink->closes.load(), 1);
}

TEST(CancelTeardown, ACleanCompletionOfAFanInSinkAlsoFlushes) {
    // Suppressing flush on cancel must not suppress it at end of input for the
    // fan-in runner either, or every bounded job with a fan-in silently loses its
    // buffered tail - a worse defect than the one being fixed.
    Dag dag;
    auto sink = std::make_shared<CancelTeardownCountingSink>();
    auto src_handle = dag.add_parallel_source<int>(
        [](std::size_t subtask) -> std::shared_ptr<Source<int>> {
            std::vector<Record<int>> recs{Record<int>{static_cast<int>(subtask) * 10},
                                          Record<int>{static_cast<int>(subtask) * 10 + 1}};
            return std::make_shared<VectorSource<int>>(std::move(recs), "bounded");
        },
        /*parallelism*/ 2);
    dag.add_parallel_sink<int>(
        src_handle,
        [sink](std::size_t) -> std::shared_ptr<Sink<int>> { return sink; },
        /*parallelism*/ 1);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->writes.load(), 4);
    EXPECT_EQ(sink->flushes.load(), 1)
        << "gating flush on a clean exit also suppressed it at end of input for the fan-in "
           "runner, which loses the tail of every bounded job that fans in";
}

// --- A 2PC sink's handle must be inside the checkpoint it names ------
//
// The defect behind follow-up item 31, pinned at the level it actually lives:
// the ORDER of the sink's barrier hook against the state snapshot.
//
// CommittingSink::on_barrier prepares the transaction for checkpoint N and
// records its handle in operator state. recover_all_() re-commits that handle
// after a restart. So the handle has to be inside snapshot N - if the runner
// snapshots first, the handle for N lands in snapshot N+1, a job restored from N
// finds no handle for N, never commits N's staged transaction, and resumes past
// the records it covered. They are gone.
//
// Rare in the field because the coordinator's CommitCheckpoint normally arrives
// and commits N while the job is alive. The window only bites on a restart whose
// restore point is exactly N. Reproduced under load at iteration 3 of 40, and
// 40/40 clean after the fix.
//
// This test does not need a cluster: it asserts the ordering directly, which is
// the property the integration test can only observe by luck.

namespace {

// Records the order of the two events that matter.
class OrderRecordingSink final : public Sink<int> {
public:
    void on_data(const Batch<int>&) override {}
    void on_barrier(CheckpointBarrier b) override {
        events.push_back("barrier-" + std::to_string(b.id().value()));
    }
    std::string name() const override { return "order_recording.sink"; }

    std::vector<std::string> events;
};

// Notes when it is snapshotted, into the same log the sink writes to, so the two
// events are ordered against each other. Delegates everything else -
// InMemoryStateBackend is final, so this wraps rather than derives.
class OrderRecordingBackend final : public StateBackend {
public:
    explicit OrderRecordingBackend(std::vector<std::string>* log) : log_(log) {}

    void put(OperatorId op, KeyView key, ValueView value) override { inner_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_.scan(op, visit); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        inner_.restore(snap, kg);
    }
    std::string description() const override { return "order_recording"; }

    Snapshot snapshot(CheckpointId id) override {
        log_->push_back("snapshot-" + std::to_string(id.value()));
        return inner_.snapshot(id);
    }

private:
    InMemoryStateBackend inner_;
    std::vector<std::string>* log_;
};

}  // namespace

TEST(CancelTeardown, ASinkPreparesItsTransactionBeforeTheSnapshotThatMustContainIt) {
    auto sink = std::make_shared<OrderRecordingSink>();
    auto backend = std::make_shared<OrderRecordingBackend>(&sink->events);

    Dag dag;
    auto src = std::make_shared<CancelTeardownEndlessSource>();
    auto h = dag.add_source<int>(src);
    dag.add_sink<int>(h, sink);

    // Drive real barriers through a CheckpointCoordinator, the same way
    // test_periodic_checkpoint.cpp does - the ordering under test only happens
    // on the barrier path.
    CheckpointCoordinator::Config ccfg;
    ccfg.interval = std::chrono::milliseconds{20};
    CheckpointCoordinator coord(backend, ccfg);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();
    coord.start_periodic_trigger();
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();

    // Find the first checkpoint that produced both events and assert the order.
    // Asserting on a specific id would make the test depend on how many
    // checkpoints the interval happened to fire.
    bool checked = false;
    for (std::size_t i = 0; i + 1 < sink->events.size(); ++i) {
        if (!sink->events[i].starts_with("barrier-")) {
            continue;
        }
        const auto id = sink->events[i].substr(std::string("barrier-").size());
        const auto want_snapshot = "snapshot-" + id;
        // The snapshot for this id must come AFTER the barrier hook for it.
        const auto snap_at = std::find(sink->events.begin(), sink->events.end(), want_snapshot);
        if (snap_at == sink->events.end()) {
            continue;
        }
        const auto snap_idx = static_cast<std::size_t>(snap_at - sink->events.begin());
        EXPECT_GT(snap_idx, i)
            << "the state snapshot for checkpoint " << id
            << " ran BEFORE the sink's barrier hook, so a CommittingSink's handle for that "
               "checkpoint lands in the NEXT snapshot. A job restored from it finds no handle, "
               "never commits the staged transaction, and loses the records it covered";
        checked = true;
        break;
    }
    EXPECT_TRUE(checked) << "no checkpoint produced both a barrier hook and a snapshot, so the "
                            "ordering was never observed: "
                         << sink->events.size() << " events recorded";
}

// --- The same ordering, when the sink is CHAINED behind an operator ---------
//
// Follow-up item 36: the other half of item 31. That fix ordered the sink's
// barrier hook before the snapshot INSIDE the sink runner, which is the whole
// story only when the sink is the (sub)task's checkpoint owner - a plain
// source -> sink subtask, which is what the test above builds.
//
// Put an operator in front and the ownership moves. `add_operator` makes the
// most-downstream OPERATOR the chain's checkpoint owner and demotes everything
// before it (dag.hpp, chain_checkpoint_owner_); a sink added afterwards never
// becomes the owner, so `sink_owns_checkpoint` is false and the sink does not
// snapshot at all. The owner's runner snapshots the shared backend and only THEN
// forwards the barrier downstream, where the sink's on_barrier finally runs and a
// CommittingSink stages its handle - into the backend, after the snapshot that
// was supposed to contain it.
//
// So the handle for checkpoint N lands in snapshot N+1 on every chained sink,
// deterministically, not as a race. A job restored from N finds no handle for N
// and never commits N's staged transaction.
//
// The log has to be mutex-guarded here, unlike the test above: the snapshot runs
// on the owner operator's runner thread and the barrier hook on the sink's, so
// two threads append to it.

namespace {

class ChainOrderLog {
public:
    void record(std::string what) {
        std::lock_guard lock(mu_);
        events_.push_back(std::move(what));
    }
    std::vector<std::string> snapshot_events() const {
        std::lock_guard lock(mu_);
        return events_;
    }

private:
    mutable std::mutex mu_;
    std::vector<std::string> events_;
};

class ChainedOrderRecordingSink final : public Sink<int> {
public:
    explicit ChainedOrderRecordingSink(ChainOrderLog* log) : log_(log) {}
    void on_data(const Batch<int>&) override {}
    void on_barrier(CheckpointBarrier b) override {
        log_->record("barrier-" + std::to_string(b.id().value()));
    }
    std::string name() const override { return "chained_order_recording.sink"; }

private:
    ChainOrderLog* log_;
};

class ChainedOrderRecordingBackend final : public StateBackend {
public:
    explicit ChainedOrderRecordingBackend(ChainOrderLog* log) : log_(log) {}

    void put(OperatorId op, KeyView key, ValueView value) override { inner_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_.scan(op, visit); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        inner_.restore(snap, kg);
    }
    std::string description() const override { return "chained_order_recording"; }

    Snapshot snapshot(CheckpointId id) override {
        log_->record("snapshot-" + std::to_string(id.value()));
        return inner_.snapshot(id);
    }

private:
    InMemoryStateBackend inner_;
    ChainOrderLog* log_;
};

}  // namespace

TEST(CancelTeardown, AChainedSinkPreparesItsTransactionBeforeTheSnapshotThatMustContainIt) {
    ChainOrderLog log;
    auto sink = std::make_shared<ChainedOrderRecordingSink>(&log);
    auto backend = std::make_shared<ChainedOrderRecordingBackend>(&log);

    Dag dag;
    auto src = std::make_shared<CancelTeardownEndlessSource>();
    auto h = dag.add_source<int>(src);
    // The operator is what moves checkpoint ownership off the sink.
    // A plain identity map. Its only job is to exist: an operator in the chain is
    // what moves checkpoint ownership off the sink.
    auto op_h = dag.add_operator<int, int>(
        h,
        std::make_shared<MapOperator<int, int>>([](const int& v) { return v; }, "chain_identity"));
    dag.add_sink<int>(op_h, sink);

    CheckpointCoordinator::Config ccfg;
    ccfg.interval = std::chrono::milliseconds{20};
    CheckpointCoordinator coord(backend, ccfg);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();
    coord.start_periodic_trigger();
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();

    const auto events = log.snapshot_events();
    bool checked = false;
    for (std::size_t i = 0; i + 1 < events.size(); ++i) {
        if (!events[i].starts_with("barrier-")) {
            continue;
        }
        const auto id = events[i].substr(std::string("barrier-").size());
        const auto snap_at = std::find(events.begin(), events.end(), "snapshot-" + id);
        if (snap_at == events.end()) {
            continue;
        }
        const auto snap_idx = static_cast<std::size_t>(snap_at - events.begin());
        EXPECT_GT(snap_idx, i)
            << "the state snapshot for checkpoint " << id
            << " ran BEFORE the CHAINED sink's barrier hook. The chain's owner operator "
               "snapshots the shared backend and only then forwards the barrier, so a "
               "CommittingSink's handle for that checkpoint is staged into the NEXT snapshot. A "
               "job restored from this checkpoint finds no handle and never commits the staged "
               "transaction.";
        checked = true;
        break;
    }
    EXPECT_TRUE(checked) << "no checkpoint produced both a barrier hook and a snapshot, so the "
                            "ordering was never observed: "
                         << events.size() << " events recorded";
}

// --- Two-input operators sit OUTSIDE the single-writer ownership scheme ------
//
// Found while fixing item 36, and recorded because it is a violation of an
// invariant this file states rather than a style point.
//
// `add_operator` maintains chain_checkpoint_owner_ so that exactly ONE element in
// a fused chain snapshots the shared backend per barrier. The sink barrier path
// gives the reason: "a delta-commit backend (RemoteReadBackend) must be
// single-writer per barrier".
//
// `add_co_operator` never touches that flag, and its runner snapshots
// unconditionally whenever a state backend is present. So a subtask holding a
// two-input operator AND a sink has two elements snapshotting the same backend
// for the same checkpoint id. This predates the item-36 ownership move - it was
// already true when the most-downstream OPERATOR owned the checkpoint, because
// the co-operator ignored the flag then too.
//
// This test asserts the invariant, so it FAILS while that is the case. It is
// written to be precise about what it measures: how many times snapshot() is
// called for one checkpoint id, not whether the job works. Full-state backends
// tolerate the second write, which is why nothing noticed.

namespace {

class SnapshotCountingBackend final : public StateBackend {
public:
    void put(OperatorId op, KeyView key, ValueView value) override { inner_.put(op, key, value); }
    std::optional<Value> get(OperatorId op, KeyView key) const override {
        return inner_.get(op, key);
    }
    void erase(OperatorId op, KeyView key) override { inner_.erase(op, key); }
    void scan(OperatorId op, const ScanVisitor& visit) const override { inner_.scan(op, visit); }
    void restore(const Snapshot& snap, const KeyGroupRange& kg = {}) override {
        inner_.restore(snap, kg);
    }
    std::string description() const override { return "snapshot_counting"; }

    Snapshot snapshot(CheckpointId id) override {
        {
            std::lock_guard lock(mu_);
            ++counts_[id.value()];
        }
        return inner_.snapshot(id);
    }

    std::map<std::uint64_t, int> counts() const {
        std::lock_guard lock(mu_);
        return counts_;
    }

private:
    InMemoryStateBackend inner_;
    mutable std::mutex mu_;
    std::map<std::uint64_t, int> counts_;
};

class TeardownPassThroughCoOp final : public CoOperator<int, int, int> {
public:
    void process_element1(const StreamElement<int>& el, Emitter<int>& out) override {
        if (el.is_data()) {
            Batch<int> copy = el.as_data();
            (void)out.emit_data(std::move(copy));
        }
    }
    void process_element2(const StreamElement<int>& el, Emitter<int>& out) override {
        if (el.is_data()) {
            Batch<int> copy = el.as_data();
            (void)out.emit_data(std::move(copy));
        }
    }
    std::string name() const override { return "teardown_passthrough.coop"; }
};

}  // namespace

TEST(CancelTeardown, OneCheckpointProducesExactlyOneSnapshotOfASharedBackend) {
    auto backend = std::make_shared<SnapshotCountingBackend>();

    Dag dag;
    auto left = dag.add_source<int>(std::make_shared<CancelTeardownEndlessSource>());
    auto right = dag.add_source<int>(std::make_shared<CancelTeardownEndlessSource>());
    auto joined = dag.add_co_operator<int, int, int>(
        left, right, std::make_shared<TeardownPassThroughCoOp>());
    // The sink's identity is irrelevant here; only the snapshot count is. It gets
    // its own log so nothing is shared with the ordering tests above.
    static ChainOrderLog unused_log;
    dag.add_sink<int>(joined, std::make_shared<ChainedOrderRecordingSink>(&unused_log));

    CheckpointCoordinator::Config ccfg;
    ccfg.interval = std::chrono::milliseconds{25};
    CheckpointCoordinator coord(backend, ccfg);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();
    coord.start_periodic_trigger();
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();

    const auto counts = backend->counts();
    ASSERT_FALSE(counts.empty()) << "no checkpoint snapshotted at all, so nothing was measured";
    for (const auto& [id, n] : counts) {
        EXPECT_LE(n, 1) << "checkpoint " << id << " snapshotted the shared backend " << n
                        << " times. add_co_operator does not participate in the chain's "
                           "checkpoint-ownership scheme (chain_checkpoint_owner_), so a subtask "
                           "with a two-input operator and a sink has two writers per barrier. A "
                           "delta-commit backend such as RemoteReadBackend requires exactly one.";
    }
}

// A chain that fans out to TWO sinks keeps the pre-existing arrangement.
//
// Neither sink can be ordered correctly: whichever one snapshots would have to do
// so after BOTH have staged their handles, and they run on separate runner threads
// with no rendezvous between them (follow-up 42). So the item-36 ownership move
// applies only to a single-sink chain, and a second sink re-promotes the operator
// the first one demoted.
//
// This is pinned by a test because getting it wrong is not cosmetic and was not
// hypothetical. Leaving the FIRST sink as owner made it the only element that
// acked, so the second sink never acked and
// CommitGroupAtomicityTest.NoSinkPublishesACheckpointTheOtherAborted saw one sink
// publish a checkpoint the other had not - "sink A committed checkpoints {1}, sink
// B {1,2}". A later cut then revoked both without re-promoting anything, which
// leaves NOBODY snapshotting. The assertion below is exactly one writer per
// checkpoint, which both of those violate in opposite directions.
TEST(CancelTeardown, ATwoSinkChainStillSnapshotsExactlyOncePerCheckpoint) {
    auto backend = std::make_shared<SnapshotCountingBackend>();
    static ChainOrderLog log_a;
    static ChainOrderLog log_b;

    Dag dag;
    auto h = dag.add_source<int>(std::make_shared<CancelTeardownEndlessSource>());
    auto op_h = dag.add_operator<int, int>(
        h, std::make_shared<MapOperator<int, int>>([](const int& v) { return v; }, "fanout_id"));
    dag.add_sink<int>(op_h, std::make_shared<ChainedOrderRecordingSink>(&log_a));
    dag.add_sink<int>(op_h, std::make_shared<ChainedOrderRecordingSink>(&log_b));

    CheckpointCoordinator::Config ccfg;
    ccfg.interval = std::chrono::milliseconds{25};
    CheckpointCoordinator coord(backend, ccfg);
    for (const auto& r : dag.runners()) {
        coord.register_operator(r.id);
    }
    coord.set_source_injectors(dag.source_injectors());

    JobConfig cfg;
    cfg.state_backend = backend;
    LocalExecutor exec(std::move(dag), std::move(cfg));
    exec.start();
    coord.start_periodic_trigger();
    std::this_thread::sleep_for(std::chrono::milliseconds{300});
    coord.stop_periodic_trigger();
    exec.cancel();
    exec.await_termination();

    const auto counts = backend->counts();
    ASSERT_FALSE(counts.empty()) << "no checkpoint snapshotted at all, so nothing was measured: "
                                    "with two sinks and an operator, the operator must own it";
    for (const auto& [id, n] : counts) {
        EXPECT_EQ(n, 1) << "checkpoint " << id << " snapshotted the shared backend " << n
                        << " times; a chain must have exactly one writer per barrier. 0 means "
                           "ownership was revoked from both sinks with nothing re-promoted; 2 "
                           "means two elements own it.";
    }
}
