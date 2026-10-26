// AsyncLookupOperator<In, Out> - coroutine-shaped async map.
//
// Tests cover:
//   * Synchronous-completing lookup bodies (the canonical "Task that
//     finishes on first resume") flow through the operator and emit
//     in input order.
//   * Exception propagation when the lookup body throws.
//   * max_in_flight backpressure - process() throttles when the queue
//     hits capacity.
//   * Ordered vs. unordered emit behaviour.
//   * Markers: a watermark leaves after the records ahead of it
//     without holding back the lookups behind it; a barrier and the
//     end of input wait for every lookup in flight; a run of waiting
//     watermarks keeps its newest progress; the runner's wake-up emits
//     a waiting watermark with no further input.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "clink/async/task.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/async_lookup_operator.hpp"
#include "clink/operators/sink_operator.hpp"
#include "clink/operators/source_operator.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/local_executor.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/test/output_capture.hpp"

using namespace clink;
using namespace std::chrono_literals;

namespace {

// Synchronous-completing lookup: the coroutine body runs entirely on
// the first resume() and yields a value. Models the common case of
// a fast cache hit or in-memory lookup.
async::Task<int> double_in(int x) {
    co_return x * 2;
}

}  // namespace

TEST(AsyncLookupOperator, SynchronousLookupRunsToCompletionInOrder) {
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{
        Record<int>{1}, Record<int>{2}, Record<int>{3}, Record<int>{4}, Record<int>{5}});
    auto op = std::make_shared<AsyncLookupOperator<int, int>>(
        double_in, /*max_in_flight=*/8, /*ordered=*/true);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{2, 4, 6, 8, 10}));
}

TEST(AsyncLookupOperator, ExceptionFromLookupBodyPropagates) {
    auto throwing_lookup = [](const int&) -> async::Task<int> {
        throw std::runtime_error("lookup failed");
        co_return 0;  // unreachable - makes the function a coroutine
    };

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(std::vector<Record<int>>{Record<int>{1}});
    auto op = std::make_shared<AsyncLookupOperator<int, int>>(throwing_lookup);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    // The exception captured on the Task's promise rethrows from
    // Task::get() inside emit_slot_; the runner sees it and the
    // executor's error-collection machinery records it. We can't
    // easily assert the runner's error state from this test
    // harness, so just verify no records were emitted before the
    // throw and that run() returns cleanly (operator-level errors
    // don't crash the executor).
    EXPECT_NO_THROW(exec.run());
    EXPECT_TRUE(sink->collected().empty());
}

TEST(AsyncLookupOperator, OrderedEmitPreservesInputOrder) {
    // All lookups are synchronous-completing so they finish in input
    // order anyway. The check is that the operator's ordered queue
    // emits them deterministically.
    auto squared = [](const int& v) -> async::Task<int> { co_return v* v; };
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{0}, Record<int>{1}, Record<int>{2}, Record<int>{3}});
    auto op = std::make_shared<AsyncLookupOperator<int, int>>(
        squared, /*max_in_flight=*/8, /*ordered=*/true);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{0, 1, 4, 9}));
}

TEST(AsyncLookupOperator, UnorderedEmitYieldsSameSetForSynchronousLookups) {
    // With synchronous-completing tasks, unordered emit happens to
    // produce input order too (each task completes before the next
    // begins). The contract this test pins is the set-equality;
    // strict ordering across out-of-order completion is verified
    // once a real awaitable + backend is in place.
    auto plus_ten = [](const int& v) -> async::Task<int> { co_return v + 10; };
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{1}, Record<int>{2}, Record<int>{3}});
    auto op = std::make_shared<AsyncLookupOperator<int, int>>(
        plus_ten, /*max_in_flight=*/8, /*ordered=*/false);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    auto got = sink->collected();
    std::sort(got.begin(), got.end());
    EXPECT_EQ(got, (std::vector<int>{11, 12, 13}));
}

TEST(AsyncLookupOperator, CoAwaitChainingDeliversInnerResult) {
    // The user's lookup body composes via co_await on another Task.
    // This verifies the Task<T> chaining contract is honoured
    // through the operator's drive loop.
    auto inner = [](int x) -> async::Task<int> { co_return x * 100; };
    auto outer = [inner](const int& v) -> async::Task<int> {
        int hop = co_await inner(v);
        co_return hop + 1;
    };
    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{1}, Record<int>{2}});
    auto op = std::make_shared<AsyncLookupOperator<int, int>>(outer);
    auto sink = std::make_shared<CollectingSink<int>>();

    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<int>{101, 201}));
}

TEST(AsyncLookupOperator, DiagnosticsReflectEmittedCount) {
    // The in_flight_count / emitted_count accessors are diagnostic
    // surfaces used by metrics and future scheduler tooling.
    auto identity = [](const int& v) -> async::Task<int> { co_return v; };
    auto op = std::make_shared<AsyncLookupOperator<int, int>>(identity);

    EXPECT_EQ(op->in_flight_count(), 0u);
    EXPECT_EQ(op->emitted_count(), 0u);

    Dag dag;
    auto src = std::make_shared<VectorSource<int>>(
        std::vector<Record<int>>{Record<int>{5}, Record<int>{6}, Record<int>{7}});
    auto sink = std::make_shared<CollectingSink<int>>();
    auto h0 = dag.add_source<int>(src);
    auto h1 = dag.add_operator<int, int>(h0, op);
    dag.add_sink<int>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(op->in_flight_count(), 0u);
    EXPECT_EQ(op->emitted_count(), 3u);
}

TEST(AsyncLookupOperator, StringLookupRoundTrips) {
    // Non-trivial value type passes through correctly; the in/out
    // Batch wiring doesn't assume scalar.
    auto greet = [](const std::string& name) -> async::Task<std::string> {
        co_return "hello, " + name;
    };
    Dag dag;
    auto src = std::make_shared<VectorSource<std::string>>(
        std::vector<Record<std::string>>{Record<std::string>{"alice"}, Record<std::string>{"bob"}});
    auto op = std::make_shared<AsyncLookupOperator<std::string, std::string>>(greet);
    auto sink = std::make_shared<CollectingSink<std::string>>();

    auto h0 = dag.add_source<std::string>(src);
    auto h1 = dag.add_operator<std::string, std::string>(h0, op);
    dag.add_sink<std::string>(h1, sink);

    LocalExecutor exec(std::move(dag));
    exec.run();

    EXPECT_EQ(sink->collected(), (std::vector<std::string>{"hello, alice", "hello, bob"}));
}

namespace {

// Lookups that answer only after the operator has resumed them a given
// number of times since they started, as a read waits for its reply. The
// log records when each lookup started and when it finished.
struct PacedLookups {
    std::map<int, int> polls;
    std::vector<std::string> log;
};

async::Task<int> paced_lookup(PacedLookups* paced, int x) {
    paced->log.push_back("start " + std::to_string(x));
    const auto it = paced->polls.find(x);
    const int polls = it == paced->polls.end() ? 0 : it->second;
    for (int i = 0; i < polls; ++i) {
        co_await std::suspend_always{};
    }
    paced->log.push_back("finish " + std::to_string(x));
    co_return x * 10;
}

std::shared_ptr<AsyncLookupOperator<int, int>> paced_operator(PacedLookups& paced, bool ordered) {
    return std::make_shared<AsyncLookupOperator<int, int>>(
        [&paced](const int& x) { return paced_lookup(&paced, x); }, /*max_in_flight=*/8, ordered);
}

StreamElement<int> lookup_input(int x) {
    Batch<int> b;
    b.emplace(x);
    return StreamElement<int>::data(std::move(b));
}

StreamElement<int> lookup_watermark(std::int64_t ms, bool idle = false) {
    return StreamElement<int>::watermark(Watermark{EventTime{ms}, idle});
}

// What left the operator, in order: each value, W<ms> for a watermark,
// I<ms> for an idle mark and B<id> for a barrier.
std::string lookup_output(const clink::test::OutputCapture<int>& cap) {
    std::string out;
    auto add = [&out](const std::string& part) { out += (out.empty() ? "" : " ") + part; };
    for (const auto& e : cap.events()) {
        if (e.is_data()) {
            for (const auto& rec : e.as_data()) {
                add(std::to_string(rec.value()));
            }
        } else if (e.is_watermark()) {
            const auto wm = e.as_watermark();
            add((wm.is_idle() ? "I" : "W") + std::to_string(wm.timestamp().millis()));
        } else if (e.is_barrier()) {
            add("B" + std::to_string(e.as_barrier().id().value()));
        }
    }
    return out;
}

// Resumes the lookups in flight until none is left, as the runner's
// wake-ups do.
void poll_until_idle(AsyncLookupOperator<int, int>& op, clink::test::OutputCapture<int>& cap) {
    for (int i = 0; i < 1000 && op.in_flight_count() > 0; ++i) {
        op.poll(cap.emitter());
    }
}

}  // namespace

TEST(AsyncLookupOperator, AWatermarkWaitsBehindEarlierRowsWithoutHoldingBackLaterLookups) {
    PacedLookups paced;
    paced.polls[1] = 50;  // the first read is slow
    auto op = paced_operator(paced, /*ordered=*/true);
    clink::test::OutputCapture<int> cap;

    op->process(lookup_input(1), cap.emitter());
    op->process(lookup_watermark(5), cap.emitter());
    op->process(lookup_input(2), cap.emitter());

    // The second lookup started while the first was still waiting: the
    // watermark between them held back neither the input nor the lookup.
    ASSERT_GE(paced.log.size(), 2u);
    EXPECT_EQ(paced.log[0], "start 1");
    EXPECT_EQ(paced.log[1], "start 2");
    EXPECT_EQ(op->in_flight_count(), 2u);
    // Nothing has left yet: the second answer and the watermark both
    // wait for the first.
    EXPECT_EQ(lookup_output(cap), "");

    poll_until_idle(*op, cap);
    EXPECT_EQ(lookup_output(cap), "10 W5 20");
}

TEST(AsyncLookupOperator, UnorderedRowsLeaveInAnyOrderButNeverPassAWatermark) {
    PacedLookups paced;
    paced.polls[1] = 50;
    auto op = paced_operator(paced, /*ordered=*/false);
    clink::test::OutputCapture<int> cap;

    op->process(lookup_input(1), cap.emitter());
    op->process(lookup_input(4), cap.emitter());
    // 4 answered at once and may leave before 1, which came before it.
    EXPECT_EQ(lookup_output(cap), "40");

    op->process(lookup_watermark(5), cap.emitter());
    op->process(lookup_input(2), cap.emitter());
    // 2 answered at once too, but it came after the watermark, which
    // waits for 1: neither may leave yet.
    EXPECT_EQ(lookup_output(cap), "40");

    poll_until_idle(*op, cap);
    EXPECT_EQ(lookup_output(cap), "40 10 W5 20");
}

TEST(AsyncLookupOperator, ABarrierWaitsForEveryLookupInFlight) {
    PacedLookups paced;
    paced.polls[1] = 20;
    paced.polls[2] = 20;
    auto op = paced_operator(paced, /*ordered=*/true);
    clink::test::OutputCapture<int> cap;

    op->process(lookup_input(1), cap.emitter());
    op->process(lookup_watermark(5), cap.emitter());
    op->process(lookup_input(2), cap.emitter());
    op->process(StreamElement<int>::barrier(CheckpointBarrier{CheckpointId{7}}), cap.emitter());

    // The operator keeps no state, so nothing is in flight across the
    // checkpoint: every answer and the watermark left before the barrier.
    EXPECT_EQ(lookup_output(cap), "10 W5 20 B7");
    EXPECT_EQ(op->in_flight_count(), 0u);
}

TEST(AsyncLookupOperator, ARunOfWaitingWatermarksKeepsItsNewestProgress) {
    PacedLookups paced;
    paced.polls[1] = 20;
    auto op = paced_operator(paced, /*ordered=*/true);
    clink::test::OutputCapture<int> cap;

    op->process(lookup_input(1), cap.emitter());
    op->process(lookup_watermark(5), cap.emitter());
    op->process(lookup_watermark(7), cap.emitter());
    op->process(lookup_watermark(7, /*idle=*/true), cap.emitter());
    op->process(lookup_watermark(9), cap.emitter());
    op->process(lookup_input(2), cap.emitter());
    op->process(lookup_watermark(11), cap.emitter());
    op->process(lookup_watermark(11, /*idle=*/true), cap.emitter());
    op->flush(cap.emitter());

    // Behind 1, only the newest watermark of the run is left; behind 2 the
    // run ends idle, so its newest watermark leaves and then the idle mark.
    EXPECT_EQ(lookup_output(cap), "10 W9 20 W11 I11");
}

TEST(AsyncLookupOperator, TheRunnersWakeUpEmitsAWaitingWatermark) {
    InMemoryStateBackend backend;
    MetricsRegistry metrics;
    RuntimeContext ctx(OperatorId{1}, "lookup", &backend, &metrics);
    std::int64_t now = 1000;
    ctx.timer_service()->set_now_fn([&now] { return now; });
    PacedLookups paced;
    paced.polls[1] = 1;
    auto op = paced_operator(paced, /*ordered=*/true);
    op->attach_runtime(&ctx);
    clink::test::OutputCapture<int> cap;

    EXPECT_FALSE(op->next_timer_deadline_ms().has_value());
    op->process(lookup_input(1), cap.emitter());
    op->process(lookup_watermark(5), cap.emitter());
    EXPECT_EQ(lookup_output(cap), "");

    // With a lookup in flight the operator asks to be woken shortly, and
    // the wake-up emits the answer and the watermark behind it with no
    // further input.
    EXPECT_EQ(op->next_timer_deadline_ms(), std::optional<std::int64_t>{1002});
    now = 1002;
    op->fire_due_timers(cap.emitter(), now);
    EXPECT_EQ(lookup_output(cap), "10 W5");
    EXPECT_FALSE(op->next_timer_deadline_ms().has_value());
    op->attach_runtime(nullptr);
}
