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

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "clink/core/record.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"

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
