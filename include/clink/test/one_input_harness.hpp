#pragma once

// clink::test::OneInputOperatorHarness - the in-process harness for
// testing a single-input Operator<In, Out> deterministically: no
// threads, no channels, no wall clock, milliseconds per test.
//
//   auto h = clink::test::OneInputOperatorHarness<int, int>::create(my_op);
//   h.open();
//   h.process_element(1);
//   h.process_element(2, /*event_time_ms=*/1000);
//   h.process_watermark(2000);          // fires due event-time timers
//   h.advance_processing_time_to(5000); // fires due processing-time timers
//   EXPECT_EQ(h.output().values(), expected);
//
// Fidelity by construction: the harness composes the PRODUCTION pieces
// an operator runner would - a real RuntimeContext over an in-memory
// state backend, the operator's own TimerService with an injected
// manual clock, and the engine's Emitter - and drives the operator
// through its real hooks. Event-time timer firing is the operator's
// own on_watermark path (the base fires due timers then forwards);
// processing-time firing replicates the runner's between-pops poll_due.
// Timer ties at one timestamp fire in lexicographic key order (the
// TimerService set order) - deterministic and documented.
//
// Lifecycle is enforced: processing before open() or after close()
// throws std::logic_error with a clear message; the destructor closes
// an open operator automatically (best-effort, exceptions swallowed -
// assert explicitly via close() when teardown behaviour matters).
//
// Part of the public clink testing API (docs/internals/testing-framework.md).

#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include "clink/core/stream_element.hpp"
#include "clink/metrics/metrics_registry.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/test/output_capture.hpp"

namespace clink::test {

template <typename In, typename Out>
class OneInputOperatorHarness {
public:
    struct Options {
        std::string operator_name{"operator-under-test"};
        std::uint64_t operator_id{1};
        // The manual clock's starting point (processing time, ms).
        std::int64_t initial_processing_time_ms{0};
        // Override the state backend (defaults to a fresh in-memory one).
        std::shared_ptr<StateBackend> state_backend;
    };

    // Create from a shared operator instance.
    static OneInputOperatorHarness create(std::shared_ptr<Operator<In, Out>> op,
                                          Options options = {}) {
        return OneInputOperatorHarness(std::move(op), std::move(options));
    }

    // Create from an operator VALUE (moved into shared ownership) - the
    // concise form for test-local operator types.
    template <typename Op>
        requires std::derived_from<Op, Operator<In, Out>>
    static OneInputOperatorHarness create(Op op, Options options = {}) {
        return OneInputOperatorHarness(std::make_shared<Op>(std::move(op)), std::move(options));
    }

    OneInputOperatorHarness(OneInputOperatorHarness&&) noexcept = default;
    OneInputOperatorHarness& operator=(OneInputOperatorHarness&&) noexcept = default;
    OneInputOperatorHarness(const OneInputOperatorHarness&) = delete;
    OneInputOperatorHarness& operator=(const OneInputOperatorHarness&) = delete;

    ~OneInputOperatorHarness() {
        if (core_ && core_->state == Lifecycle::Open) {
            try {
                close();
            } catch (...) {
                // Destructors must not throw; assert teardown behaviour
                // explicitly via close() where it matters.
            }
        }
    }

    // ---- Lifecycle ----

    void open() {
        require_state_(Lifecycle::Created, "open()");
        core_->op->attach_runtime(&core_->ctx);
        core_->op->open();
        core_->state = Lifecycle::Open;
    }

    void close() {
        require_state_(Lifecycle::Open, "close()");
        core_->op->close();
        core_->op->attach_runtime(nullptr);
        core_->state = Lifecycle::Closed;
    }

    // ---- Input ----

    void process_element(In value) {
        Batch<In> b;
        b.emplace(std::move(value));
        process_batch(std::move(b));
    }

    void process_element(In value, std::int64_t event_time_ms) {
        Batch<In> b;
        b.emplace(std::move(value), EventTime{event_time_ms});
        process_batch(std::move(b));
    }

    void process_batch(Batch<In> batch) {
        require_state_(Lifecycle::Open, "process_batch()");
        core_->op->process(StreamElement<In>::data(std::move(batch)), capture_.emitter());
    }

    // Full generality: hand the operator any stream element.
    void process(const StreamElement<In>& element) {
        require_state_(Lifecycle::Open, "process()");
        core_->op->process(element, capture_.emitter());
    }

    // The operator's real watermark path: the base implementation fires
    // due event-time timers, then forwards the watermark; overriding
    // operators (windows, the ProcessFunction adapter) run their own
    // production logic.
    void process_watermark(std::int64_t timestamp_ms) {
        process_watermark(Watermark{EventTime{timestamp_ms}});
    }

    void process_watermark(Watermark wm) {
        require_state_(Lifecycle::Open, "process_watermark()");
        core_->op->on_watermark(wm, capture_.emitter());
        if (!wm.is_idle()) {
            core_->last_watermark_ms = wm.timestamp().millis();
        }
    }

    void process_barrier(CheckpointBarrier barrier) {
        require_state_(Lifecycle::Open, "process_barrier()");
        core_->op->on_barrier(barrier, capture_.emitter());
    }

    // End-of-input residuals (windows, sorts, joins): the operator's
    // flush hook, exactly as the runner calls it after the input drains.
    void flush() {
        require_state_(Lifecycle::Open, "flush()");
        core_->op->flush(capture_.emitter());
    }

    // ---- Processing time (manual clock; never the wall clock) ----

    std::int64_t processing_time_ms() const noexcept { return *core_->now_ms; }

    // Move the clock WITHOUT firing timers (setup positioning).
    void set_processing_time(std::int64_t now_ms) {
        require_monotonic_(now_ms);
        *core_->now_ms = now_ms;
    }

    // Move the clock and fire every due processing-time timer through the
    // operator's on_processing_time_timer hook - the runner's between-pops
    // poll, made explicit. Timers registered DURING a fire are not fired
    // in the same call (production poll_due semantics; they fire on the
    // next advance).
    void advance_processing_time_to(std::int64_t now_ms) {
        require_state_(Lifecycle::Open, "advance_processing_time_to()");
        require_monotonic_(now_ms);
        *core_->now_ms = now_ms;
        auto& out = capture_.emitter();
        core_->ctx.timer_service()->poll_due(now_ms,
                                             [this, &out](std::int64_t ts, const std::string& key) {
                                                 core_->op->on_processing_time_timer(ts, key, out);
                                             });
    }

    void advance_processing_time_by(std::int64_t delta_ms) {
        advance_processing_time_to(*core_->now_ms + delta_ms);
    }

    // ---- Inspection ----

    OutputCapture<Out>& output() noexcept { return capture_; }
    const OutputCapture<Out>& output() const noexcept { return capture_; }

    // Convenience projections (the full model lives on output()).
    std::vector<Out> output_values() const { return capture_.values(); }
    std::vector<Watermark> output_watermarks() const { return capture_.watermarks(); }

    // The last non-idle watermark processed (nullopt before the first).
    std::optional<std::int64_t> current_watermark_ms() const noexcept {
        return core_->last_watermark_ms;
    }

    // Registered timers, ordered exactly as they will fire.
    const TimerService::TimerSet& processing_time_timers() const {
        return core_->ctx.timer_service()->processing_time_timers();
    }
    const TimerService::TimerSet& event_time_timers() const {
        return core_->ctx.timer_service()->event_time_timers();
    }

    // Diagnostics escape hatches: the real runtime pieces, for the cases
    // the typed API does not cover. State inspection through the backend
    // is read-only unless you call its mutators.
    RuntimeContext& runtime() noexcept { return core_->ctx; }
    StateBackend& state_backend() noexcept { return *core_->backend; }
    MetricsRegistry& metrics() noexcept { return core_->metrics; }
    Operator<In, Out>& op() noexcept { return *core_->op; }

private:
    enum class Lifecycle { Created, Open, Closed };

    struct Core {
        Core(std::shared_ptr<Operator<In, Out>> op_in, Options opts)
            : op(std::move(op_in)),
              backend(opts.state_backend ? std::move(opts.state_backend)
                                         : std::make_shared<InMemoryStateBackend>()),
              now_ms(std::make_shared<std::int64_t>(opts.initial_processing_time_ms)),
              ctx(OperatorId{opts.operator_id},
                  std::move(opts.operator_name),
                  backend.get(),
                  &metrics) {
            // Deterministic time: the operator's TimerService reads the
            // harness clock, so now_ms() never touches the wall clock.
            ctx.timer_service()->set_now_fn([now = now_ms] { return *now; });
        }

        std::shared_ptr<Operator<In, Out>> op;
        std::shared_ptr<StateBackend> backend;
        std::shared_ptr<std::int64_t> now_ms;
        MetricsRegistry metrics;
        RuntimeContext ctx;
        Lifecycle state{Lifecycle::Created};
        std::optional<std::int64_t> last_watermark_ms;
    };

    OneInputOperatorHarness(std::shared_ptr<Operator<In, Out>> op, Options opts)
        : core_(std::make_unique<Core>(std::move(op), std::move(opts))) {}

    void require_state_(Lifecycle expected, const char* what) const {
        if (core_->state != expected) {
            throw std::logic_error(std::string{"OneInputOperatorHarness: "} + what +
                                   " called in lifecycle state " + state_name_(core_->state) +
                                   " (expected " + state_name_(expected) +
                                   "); call open() first and do not reuse a closed harness");
        }
    }

    void require_monotonic_(std::int64_t now_ms) const {
        if (now_ms < *core_->now_ms) {
            throw std::logic_error(
                "OneInputOperatorHarness: processing time may not move backwards (current " +
                std::to_string(*core_->now_ms) + "ms, requested " + std::to_string(now_ms) + "ms)");
        }
    }

    static const char* state_name_(Lifecycle s) noexcept {
        switch (s) {
            case Lifecycle::Created:
                return "Created";
            case Lifecycle::Open:
                return "Open";
            case Lifecycle::Closed:
                return "Closed";
        }
        return "?";
    }

    std::unique_ptr<Core> core_;
    OutputCapture<Out> capture_;
};

}  // namespace clink::test
