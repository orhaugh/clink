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
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/checkpoint/checkpoint_coordinator.hpp"
#include "clink/core/record.hpp"
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
