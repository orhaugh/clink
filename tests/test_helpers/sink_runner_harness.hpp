#pragma once

// SinkRunnerHarness - one sink driven through its own Dag runner, with commits
// and aborts dispatched to it the way a worker dispatches them.
//
// A worker delivers CommitCheckpoint and AbortCheckpoint to a sink from a
// thread other than the sink's runner, through the task's CommitDispatchGate,
// and the runner retires that gate as the sink's teardown begins. Whether a
// commit that arrives after the sink's input has ended runs or is refused is
// decided between the two: the runner's end-of-stream wait for staged commits
// (Sink::staged_commits_outstanding) holds the teardown, and with it the gate,
// open only while the sink reports a staged commit. This harness reproduces
// that arrangement without a coordinator or a worker, so a connector's tests
// can put a commit after the end of input deterministically and see which way
// the runner decided.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <ostream>
#include <thread>
#include <utility>

#include "clink/cluster/commit_dispatch_gate.hpp"
#include "clink/core/stream_element.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/bounded_channel.hpp"
#include "clink/runtime/dag.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/state_backend.hpp"

namespace clink::test_support {

// Where the sink's runner stood once its whole input, end included, was taken.
enum class AfterInput {
    // Still holding the sink open, polling in the end-of-stream wait.
    Waiting,
    // Its teardown had begun, or it had returned: a commit now is refused.
    TornDown,
    // Neither within the bound.
    Timeout,
};

// So a failed expectation names the outcome rather than printing its bytes.
inline void PrintTo(AfterInput a, std::ostream* os) {
    switch (a) {
        case AfterInput::Waiting:
            *os << "Waiting";
            return;
        case AfterInput::TornDown:
            *os << "TornDown";
            return;
        case AfterInput::Timeout:
            *os << "Timeout";
            return;
    }
}

template <typename In>
class SinkRunnerHarness {
public:
    using Channel = BoundedChannel<StreamElement<In>>;

    // `backend` is the sink's state backend and must outlive the harness.
    SinkRunnerHarness(std::shared_ptr<Sink<In>> sink, StateBackend* backend)
        : sink_(std::move(sink)), in_(std::make_shared<Channel>(64)) {
        const auto stage = dag_.add_sink<In>(StageHandle<In>{in_, 0}, sink_);
        runner_index_ = stage.runner_index;
        // What the worker's sink registration installs.
        sink_->set_commit_dispatch_fence([gate = gate_] { gate->retire_and_drain(); });
        commit_ = cluster::gated_dispatch(
            gate_, [sink = sink_](std::uint64_t ckpt) { sink->on_commit(ckpt); });
        abort_ = cluster::gated_dispatch(
            gate_, [sink = sink_](std::uint64_t ckpt) { sink->on_abort(ckpt); });
        const auto& runner = dag_.runners()[runner_index_];
        ctx_ = std::make_unique<RuntimeContext>(
            runner.id, runner.name, backend, &MetricsRegistry::global());
    }

    ~SinkRunnerHarness() {
        // A runner still waiting is released the way a cancel releases it.
        stop_.store(true, std::memory_order_release);
        dag_.runners()[runner_index_].cancel();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    SinkRunnerHarness(const SinkRunnerHarness&) = delete;
    SinkRunnerHarness& operator=(const SinkRunnerHarness&) = delete;
    SinkRunnerHarness(SinkRunnerHarness&&) = delete;
    SinkRunnerHarness& operator=(SinkRunnerHarness&&) = delete;

    void push(StreamElement<In> element) { in_->push(std::move(element)); }
    // The clean end of input: what a finished upstream leaves.
    void end_input() { in_->close(ChannelCloseReason::Finished); }

    void start() {
        thread_ = std::thread([this] {
            try {
                dag_.runners()[runner_index_].run(*ctx_, [this] {
                    polls_.fetch_add(1, std::memory_order_acq_rel);
                    return stop_.load(std::memory_order_acquire);
                });
            } catch (...) {
                failure_ = std::current_exception();
            }
            finished_.store(true, std::memory_order_release);
        });
    }

    // Waits for the runner to take its whole input and then reports where it
    // stood. The witness for the end-of-stream wait is the runner's stop flag:
    // past its last element the runner reads it a fixed handful of times on its
    // way to teardown, and only the wait, which re-reads it every 20 ms, reads
    // it again and again. kWaitPolls is well above that handful.
    AfterInput await_end_of_input(std::chrono::milliseconds bound) {
        constexpr std::size_t kWaitPolls = 10;
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (in_->size() > 0) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return AfterInput::Timeout;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        const auto base = polls_.load(std::memory_order_acquire);
        while (true) {
            if (sink_->teardown_begun() || finished_.load(std::memory_order_acquire)) {
                return AfterInput::TornDown;
            }
            if (polls_.load(std::memory_order_acquire) >= base + kWaitPolls) {
                return AfterInput::Waiting;
            }
            if (std::chrono::steady_clock::now() >= deadline) {
                return AfterInput::Timeout;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
    }

    // A CommitCheckpoint or AbortCheckpoint, dispatched as the worker does it.
    // Throws cluster::CommitDispatchRefused once the sink's teardown has begun.
    void commit(std::uint64_t checkpoint_id) { commit_(checkpoint_id); }
    void abort(std::uint64_t checkpoint_id) { abort_(checkpoint_id); }

    // True once the runner returned within `bound`.
    bool join(std::chrono::milliseconds bound) {
        const auto deadline = std::chrono::steady_clock::now() + bound;
        while (!finished_.load(std::memory_order_acquire)) {
            if (std::chrono::steady_clock::now() >= deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        thread_.join();
        return true;
    }

    // What the runner threw, once join() has returned true.
    [[nodiscard]] std::exception_ptr failure() const { return failure_; }
    [[nodiscard]] bool dispatch_retired() const { return gate_->retired(); }

private:
    std::shared_ptr<Sink<In>> sink_;
    std::shared_ptr<Channel> in_;
    Dag dag_;
    std::size_t runner_index_{0};
    std::shared_ptr<cluster::CommitDispatchGate> gate_ =
        std::make_shared<cluster::CommitDispatchGate>();
    std::function<void(std::uint64_t)> commit_;
    std::function<void(std::uint64_t)> abort_;
    std::unique_ptr<RuntimeContext> ctx_;
    std::atomic<std::size_t> polls_{0};
    std::atomic<bool> stop_{false};
    std::atomic<bool> finished_{false};
    std::exception_ptr failure_;
    std::thread thread_;
};

}  // namespace clink::test_support
