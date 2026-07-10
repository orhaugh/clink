#pragma once

// clink::test::TwoInputOperatorHarness - the in-process harness for a
// CoOperator<In1, In2, Out> (co-process functions, joins, connected
// streams). Same determinism and lifecycle contract as the one-input
// harness, plus the engine's REAL two-input watermark semantics: each
// input's watermark feeds the production MultiInputAlignment, and the
// operator only sees the combined watermark (the running minimum, with
// idleness handled exactly as the runner handles it) when the aligner
// says it advanced.
//
//   auto h = clink::test::TwoInputOperatorHarness<L, R, Out>::create(my_co_op);
//   h.open();
//   h.process_left(l, 1000);
//   h.process_right(r, 900);
//   h.process_left_watermark(2000);   // combined wm still Watermark::min()
//   h.process_right_watermark(1500);  // combined advances to 1500 -> operator fires
//   h.mark_left_idle();               // left stops constraining the minimum
//
// KeyedTwoInputOperatorHarness adds the same typed keyed-state
// inspection the keyed one-input harness has.
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
#include "clink/runtime/multi_input_alignment.hpp"
#include "clink/runtime/runtime_context.hpp"
#include "clink/state/in_memory_state_backend.hpp"
#include "clink/test/keyed_harness.hpp"
#include "clink/test/output_capture.hpp"

namespace clink::test {

template <typename In1, typename In2, typename Out>
class TwoInputOperatorHarness {
public:
    struct Options {
        std::string operator_name{"co-operator-under-test"};
        std::uint64_t operator_id{1};
        std::int64_t initial_processing_time_ms{0};
        std::shared_ptr<StateBackend> state_backend;
    };

    static TwoInputOperatorHarness create(std::shared_ptr<CoOperator<In1, In2, Out>> op,
                                          Options options = {}) {
        return TwoInputOperatorHarness(std::move(op), std::move(options));
    }

    template <typename Op>
        requires std::derived_from<Op, CoOperator<In1, In2, Out>>
    static TwoInputOperatorHarness create(Op op, Options options = {}) {
        return TwoInputOperatorHarness(std::make_shared<Op>(std::move(op)), std::move(options));
    }

    TwoInputOperatorHarness(TwoInputOperatorHarness&&) noexcept = default;
    TwoInputOperatorHarness& operator=(TwoInputOperatorHarness&&) noexcept = default;
    TwoInputOperatorHarness(const TwoInputOperatorHarness&) = delete;
    TwoInputOperatorHarness& operator=(const TwoInputOperatorHarness&) = delete;

    ~TwoInputOperatorHarness() {
        if (core_ && core_->state == Lifecycle::Open) {
            try {
                close();
            } catch (...) {
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

    // ---- Input (left = input 1, right = input 2) ----

    void process_left(In1 value) {
        Batch<In1> b;
        b.emplace(std::move(value));
        process_left_batch(std::move(b));
    }
    void process_left(In1 value, std::int64_t event_time_ms) {
        Batch<In1> b;
        b.emplace(std::move(value), EventTime{event_time_ms});
        process_left_batch(std::move(b));
    }
    void process_left_batch(Batch<In1> batch) {
        require_state_(Lifecycle::Open, "process_left_batch()");
        core_->op->process_element1(StreamElement<In1>::data(std::move(batch)), capture_.emitter());
    }

    void process_right(In2 value) {
        Batch<In2> b;
        b.emplace(std::move(value));
        process_right_batch(std::move(b));
    }
    void process_right(In2 value, std::int64_t event_time_ms) {
        Batch<In2> b;
        b.emplace(std::move(value), EventTime{event_time_ms});
        process_right_batch(std::move(b));
    }
    void process_right_batch(Batch<In2> batch) {
        require_state_(Lifecycle::Open, "process_right_batch()");
        core_->op->process_element2(StreamElement<In2>::data(std::move(batch)), capture_.emitter());
    }

    // ---- Watermarks: the engine's real two-input combination ----
    //
    // Each per-input watermark feeds the production MultiInputAlignment;
    // the operator's on_watermark runs only when the COMBINED watermark
    // (the running minimum over both inputs, idleness-aware) advances -
    // exactly the runner's behaviour. Returns the combined watermark
    // that was delivered, or nullopt when the minimum did not move.

    std::optional<Watermark> process_left_watermark(std::int64_t ts) {
        return input_watermark_(0, Watermark{EventTime{ts}});
    }
    std::optional<Watermark> process_right_watermark(std::int64_t ts) {
        return input_watermark_(1, Watermark{EventTime{ts}});
    }

    // Idleness: an idle input stops constraining the combined minimum.
    std::optional<Watermark> mark_left_idle() {
        return input_watermark_(0, Watermark{EventTime{0}, /*idle=*/true});
    }
    std::optional<Watermark> mark_right_idle() {
        return input_watermark_(1, Watermark{EventTime{0}, /*idle=*/true});
    }

    // ---- Processing time (manual clock) ----

    std::int64_t processing_time_ms() const noexcept { return *core_->now_ms; }

    void advance_processing_time_to(std::int64_t now_ms) {
        require_state_(Lifecycle::Open, "advance_processing_time_to()");
        if (now_ms < *core_->now_ms) {
            throw std::logic_error(
                "TwoInputOperatorHarness: processing time may not move "
                "backwards");
        }
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

    // ---- End of input ----

    void flush() {
        require_state_(Lifecycle::Open, "flush()");
        core_->op->flush(capture_.emitter());
    }

    // ---- Inspection ----

    OutputCapture<Out>& output() noexcept { return capture_; }
    std::vector<Out> output_values() const { return capture_.values(); }

    // The combined watermark last delivered to the operator (nullopt
    // until the running minimum first advances).
    std::optional<std::int64_t> current_watermark_ms() const noexcept {
        return core_->combined_wm_ms;
    }

    const TimerService::TimerSet& processing_time_timers() const {
        return core_->ctx.timer_service()->processing_time_timers();
    }
    const TimerService::TimerSet& event_time_timers() const {
        return core_->ctx.timer_service()->event_time_timers();
    }

    RuntimeContext& runtime() noexcept { return core_->ctx; }
    StateBackend& state_backend() noexcept { return *core_->backend; }
    CoOperator<In1, In2, Out>& op() noexcept { return *core_->op; }

private:
    enum class Lifecycle { Created, Open, Closed };

    struct Core {
        Core(std::shared_ptr<CoOperator<In1, In2, Out>> op_in, Options opts)
            : op(std::move(op_in)),
              backend(opts.state_backend ? std::move(opts.state_backend)
                                         : std::make_shared<InMemoryStateBackend>()),
              now_ms(std::make_shared<std::int64_t>(opts.initial_processing_time_ms)),
              ctx(OperatorId{opts.operator_id},
                  std::move(opts.operator_name),
                  backend.get(),
                  &metrics),
              alignment(2) {
            ctx.timer_service()->set_now_fn([now = now_ms] { return *now; });
        }

        std::shared_ptr<CoOperator<In1, In2, Out>> op;
        std::shared_ptr<StateBackend> backend;
        std::shared_ptr<std::int64_t> now_ms;
        MetricsRegistry metrics;
        RuntimeContext ctx;
        MultiInputAlignment alignment;
        Lifecycle state{Lifecycle::Created};
        std::optional<std::int64_t> combined_wm_ms;
    };

    TwoInputOperatorHarness(std::shared_ptr<CoOperator<In1, In2, Out>> op, Options opts)
        : core_(std::make_unique<Core>(std::move(op), std::move(opts))) {}

    std::optional<Watermark> input_watermark_(std::size_t input, Watermark wm) {
        require_state_(Lifecycle::Open, "process_*_watermark()");
        const auto adv = core_->alignment.on_watermark(input, wm);
        if (!adv.forward) {
            return std::nullopt;
        }
        core_->op->on_watermark(adv.watermark, capture_.emitter());
        if (!adv.watermark.is_idle()) {
            core_->combined_wm_ms = adv.watermark.timestamp().millis();
        }
        return adv.watermark;
    }

    void require_state_(Lifecycle expected, const char* what) const {
        if (core_->state != expected) {
            throw std::logic_error(std::string{"TwoInputOperatorHarness: "} + what +
                                   " called in the wrong lifecycle state; open() first and do "
                                   "not reuse a closed harness");
        }
    }

    std::unique_ptr<Core> core_;
    OutputCapture<Out> capture_;
};

// Keyed two-input harness: the two-input harness plus the keyed-state
// inspection surface (production read/write paths, default codecs).
template <typename In1, typename In2, typename Out, typename K>
class KeyedTwoInputOperatorHarness : public TwoInputOperatorHarness<In1, In2, Out> {
public:
    using Base = TwoInputOperatorHarness<In1, In2, Out>;
    using Options = typename Base::Options;

    static KeyedTwoInputOperatorHarness create(std::shared_ptr<CoOperator<In1, In2, Out>> op,
                                               Options options = {}) {
        return KeyedTwoInputOperatorHarness(Base::create(std::move(op), std::move(options)));
    }

    template <typename Op>
        requires std::derived_from<Op, CoOperator<In1, In2, Out>>
    static KeyedTwoInputOperatorHarness create(Op op, Options options = {}) {
        return KeyedTwoInputOperatorHarness(Base::create(std::move(op), std::move(options)));
    }

    template <typename V>
    std::optional<V> state_value(const K& key, const std::string& slot, Codec<K> kc, Codec<V> vc) {
        return this->runtime()
            .template keyed_state<K, V>(slot, std::move(kc), std::move(vc))
            .get(key);
    }
    template <typename V>
        requires HasDefaultCodec<K> && HasDefaultCodec<V>
    std::optional<V> state_value(const K& key, const std::string& slot) {
        return state_value<V>(key, slot, default_codec<K>::get(), default_codec<V>::get());
    }

    template <typename V>
    void seed_state(
        const K& key, const std::string& slot, const V& value, Codec<K> kc, Codec<V> vc) {
        this->runtime()
            .template keyed_state<K, V>(slot, std::move(kc), std::move(vc))
            .put(key, value);
    }
    template <typename V>
        requires HasDefaultCodec<K> && HasDefaultCodec<V>
    void seed_state(const K& key, const std::string& slot, const V& value) {
        seed_state<V>(key, slot, value, default_codec<K>::get(), default_codec<V>::get());
    }

private:
    explicit KeyedTwoInputOperatorHarness(Base base) : Base(std::move(base)) {}
};

}  // namespace clink::test
