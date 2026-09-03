// Stable tier: the virtual hooks a consumer overrides.
// Compile-only; frozen (see README.md). Additions only.
//
// Every hook is overridden with `override`, so a changed virtual signature in
// a base class fails this translation unit instead of silently turning the
// consumer's override into an unrelated overload.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "clink/async/task.hpp"
#include "clink/connectors/committing_sink.hpp"
#include "clink/operators/async_co_process_function.hpp"
#include "clink/operators/async_process_function.hpp"
#include "clink/operators/operator_base.hpp"
#include "clink/operators/process_function.hpp"
#include "clink/operators/window_evictor.hpp"
#include "clink/operators/window_trigger.hpp"
#include "clink/runtime/dead_letter.hpp"
#include "clink/state/state_backend.hpp"
#include "clink/time/watermark_strategy.hpp"

namespace {

// --- Operator<In, Out> -------------------------------------------------------

class EveryOperatorHook final : public clink::Operator<std::int64_t, std::string> {
public:
    void open() override {}
    void close() override {}
    void process(const clink::StreamElement<std::int64_t>& element,
                 clink::Emitter<std::string>& out) override {
        if (element.is_data()) {
            clink::Batch<std::string> batch;
            for (const auto& r : element.as_data()) {
                batch.push(clink::Record<std::string>{std::to_string(r.value()),
                                                      r.event_time().value_or(clink::EventTime{})});
            }
            out.emit_data(std::move(batch));
        }
    }
    void on_watermark(clink::Watermark wm, clink::Emitter<std::string>& out) override {
        out.emit_watermark(wm);
    }
    void on_watermark_observed(clink::Watermark) override {}
    void on_barrier(clink::CheckpointBarrier barrier, clink::Emitter<std::string>& out) override {
        out.emit_barrier(barrier);
    }
    void on_processing_time_timer(std::int64_t timestamp_ms,
                                  const std::string& key,
                                  clink::Emitter<std::string>& out) override {
        (void)timestamp_ms;
        (void)key;
        (void)out;
    }
    void on_event_time_timer(std::int64_t timestamp_ms,
                             const std::string& key,
                             clink::Emitter<std::string>& out) override {
        (void)timestamp_ms;
        (void)key;
        (void)out;
    }
    void flush(clink::Emitter<std::string>&) override {}
    std::string name() const override { return "every-hook"; }
    [[nodiscard]] bool fires_state_touching_timers() const noexcept override { return false; }
    [[nodiscard]] bool fires_state_touching_processing_time_timers() const noexcept override {
        return false;
    }
};

// --- Source<Out> -------------------------------------------------------------

class EverySourceHook final : public clink::Source<std::int64_t> {
public:
    void open() override {}
    void close() override {}
    bool produce(clink::Emitter<std::int64_t>& out) override {
        out.emit_watermark(clink::Watermark::max());
        return false;
    }
    void cancel() override { clink::Source<std::int64_t>::cancel(); }
    [[nodiscard]] bool is_bounded() const noexcept override { return true; }
    [[nodiscard]] std::size_t split_count() const noexcept override { return 1; }
    void flush(clink::Emitter<std::int64_t>&) override {}
    std::string name() const override { return "every-source-hook"; }
    void snapshot_offset(clink::StateBackend& backend,
                         clink::OperatorId op_id,
                         clink::CheckpointId ckpt_id) override {
        (void)backend;
        (void)op_id;
        (void)ckpt_id;
    }
    bool restore_offset(clink::StateBackend& backend, clink::OperatorId op_id) override {
        (void)backend;
        (void)op_id;
        return false;
    }
    void notify_checkpoint_complete(clink::CheckpointId) override {}
    void notify_checkpoint_aborted(clink::CheckpointId) override {}
};

// --- Sink<In> ----------------------------------------------------------------

class EverySinkHook final : public clink::Sink<std::string> {
public:
    void open() override {}
    void close() override {}
    void close_cancelled() override { close(); }
    void on_data(const clink::Batch<std::string>& batch) override { (void)batch.size(); }
    void on_watermark(clink::Watermark) override {}
    void on_barrier(clink::CheckpointBarrier) override {}
    [[nodiscard]] bool stages_state_at_barrier() const noexcept override { return false; }
    void on_commit(std::uint64_t) override {}
    void on_abort(std::uint64_t) override {}
    void flush() override {}
    std::string name() const override { return "every-sink-hook"; }
};

// --- CoOperator<In1, In2, Out> -----------------------------------------------

class EveryCoOperatorHook final : public clink::CoOperator<std::int64_t, std::string, std::string> {
public:
    void open() override {}
    void close() override {}
    void process_element1(const clink::StreamElement<std::int64_t>&,
                          clink::Emitter<std::string>&) override {}
    void process_element2(const clink::StreamElement<std::string>&,
                          clink::Emitter<std::string>&) override {}
    void on_watermark(clink::Watermark wm, clink::Emitter<std::string>& out) override {
        out.emit_watermark(wm);
    }
    void on_barrier(clink::CheckpointBarrier b, clink::Emitter<std::string>& out) override {
        out.emit_barrier(b);
    }
    void on_processing_time_timer(std::int64_t,
                                  const std::string&,
                                  clink::Emitter<std::string>&) override {}
    void on_event_time_timer(std::int64_t,
                             const std::string&,
                             clink::Emitter<std::string>&) override {}
    void flush(clink::Emitter<std::string>&) override {}
    std::string name() const override { return "every-co-operator-hook"; }
};

// --- CommittingSink<In, Committable> -----------------------------------------

class EveryCommittingSinkVerb final : public clink::CommittingSink<std::string, std::string> {
public:
    using clink::CommittingSink<std::string, std::string>::CommittingSink;
    void on_open() override {}
    void write(const clink::Batch<std::string>& batch) override { (void)batch.size(); }
    std::optional<std::string> prepare_commit(std::uint64_t checkpoint_id) override {
        return std::to_string(checkpoint_id);
    }
    bool commit(const std::string&) override { return true; }
    void abort(const std::string&) override {}
    void recover(const std::string& committable) override { commit(committable); }
    std::string serialize(const std::string& committable) const override { return committable; }
    std::string deserialize(std::string_view bytes) const override { return std::string{bytes}; }
};

// --- ProcessFunction family --------------------------------------------------

class EveryProcessFunctionHook final : public clink::ProcessFunction<std::int64_t, std::string> {
public:
    void open(clink::RuntimeContext&) override {}
    void close() override {}
    void process_element(const std::int64_t& value,
                         clink::ProcessFunctionContext<std::string>& ctx,
                         clink::Collector<std::string>& out) override {
        (void)ctx.timestamp();
        (void)ctx.current_watermark();
        (void)ctx.timer_service();
        out.collect(std::to_string(value));
        out.collect_with_timestamp(std::to_string(value), clink::EventTime{1});
    }
    void on_timer(std::int64_t timestamp_ms,
                  clink::OnTimerContext<std::string>& ctx,
                  clink::Collector<std::string>& out) override {
        (void)timestamp_ms;
        (void)ctx.time_domain();
        (void)out;
    }
    void flush(clink::Collector<std::string>&) override {}
    std::string name() const override { return "every-process-function-hook"; }
};

class EveryKeyedProcessFunctionHook final
    : public clink::KeyedProcessFunction<std::string, std::int64_t, std::string> {
public:
    void process_element(const std::int64_t&,
                         clink::ProcessFunctionContext<std::string>&,
                         clink::Collector<std::string>& out) override {
        out.collect(current_key());
    }
};

class EveryCoProcessFunctionHook final
    : public clink::CoProcessFunction<std::int64_t, std::string, std::string> {
public:
    void open(clink::RuntimeContext&) override {}
    void close() override {}
    void process_element1(const std::int64_t&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
    void process_element2(const std::string&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
    void on_timer(std::int64_t,
                  clink::OnTimerContext<std::string>&,
                  clink::Collector<std::string>&) override {}
    void flush(clink::Collector<std::string>&) override {}
    std::string name() const override { return "every-co-process-function-hook"; }
};

class EveryKeyedCoProcessFunctionHook final
    : public clink::KeyedCoProcessFunction<std::string, std::int64_t, std::string, std::string> {
public:
    void process_element1(const std::int64_t&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
    void process_element2(const std::string&,
                          clink::ProcessFunctionContext<std::string>&,
                          clink::Collector<std::string>&) override {}
};

class EveryBroadcastProcessFunctionHook final
    : public clink::BroadcastProcessFunction<std::int64_t, std::string, std::string, std::string> {
public:
    void open(clink::RuntimeContext&) override {}
    void close() override {}
    void process_element(const std::int64_t&,
                         const clink::BroadcastState<std::string>&,
                         clink::Collector<std::string>&) override {}
    void process_broadcast_element(const std::string&,
                                   clink::BroadcastState<std::string>&,
                                   clink::Collector<std::string>&) override {}
    std::string name() const override { return "every-broadcast-process-function-hook"; }
};

class EveryProcessWindowFunctionHook final
    : public clink::ProcessWindowFunction<std::string, std::int64_t, std::string> {
public:
    void open(clink::RuntimeContext&) override {}
    void close() override {}
    void process(const std::string& key,
                 const clink::WindowContext& ctx,
                 const std::vector<std::int64_t>& elements,
                 clink::Collector<std::string>& out) override {
        (void)ctx.window_start;
        (void)ctx.window_end;
        (void)ctx.window_max_event_time;
        out.collect(key + std::to_string(elements.size()));
    }
    std::string name() const override { return "every-process-window-function-hook"; }
};

// --- Async process functions -------------------------------------------------

class EveryAsyncKeyedProcessFunctionHook final
    : public clink::AsyncKeyedProcessFunction<std::string, std::int64_t, std::string> {
public:
    void open(clink::RuntimeContext&) override {}
    void close() override {}
    clink::async::Task<void> process_element(const std::string& key,
                                             const std::int64_t& value,
                                             clink::AsyncKeyedProcessContext<std::string>& ctx,
                                             clink::Collector<std::string>& out) override {
        (void)ctx.timestamp();
        (void)ctx.current_watermark();
        out.collect(key + std::to_string(value));
        co_return;
    }
    void on_timer(const std::string&,
                  std::int64_t,
                  clink::AsyncKeyedOnTimerContext<std::string>&,
                  clink::Collector<std::string>&) override {}
    void flush(clink::Collector<std::string>&) override {}
    std::string name() const override { return "every-async-keyed-process-function-hook"; }
};

class EveryAsyncKeyedCoProcessFunctionHook final
    : public clink::
          AsyncKeyedCoProcessFunction<std::string, std::int64_t, std::string, std::string> {
public:
    clink::async::Task<void> process_element1(const std::string&,
                                              const std::int64_t&,
                                              clink::AsyncKeyedProcessContext<std::string>&,
                                              clink::Collector<std::string>&) override {
        co_return;
    }
    clink::async::Task<void> process_element2(const std::string&,
                                              const std::string&,
                                              clink::AsyncKeyedProcessContext<std::string>&,
                                              clink::Collector<std::string>&) override {
        co_return;
    }
    void on_timer(const std::string&,
                  std::int64_t,
                  clink::AsyncKeyedOnTimerContext<std::string>&,
                  clink::Collector<std::string>&) override {}
    void flush(clink::Collector<std::string>&) override {}
    std::string name() const override { return "every-async-keyed-co-process-function-hook"; }
};

// --- Watermark strategy, trigger, evictor ------------------------------------

class EveryWatermarkStrategyHook final : public clink::WatermarkStrategy<std::int64_t> {
public:
    void on_record(const clink::Record<std::int64_t>&) override {}
    std::optional<clink::Watermark> current_watermark() override { return std::nullopt; }
    void set_idle_partitions(const std::vector<std::int32_t>&) override {}
};

class EveryTriggerHook final : public clink::Trigger<std::int64_t, clink::TimeWindow> {
public:
    clink::TriggerResult on_element(const std::int64_t&,
                                    std::int64_t,
                                    const clink::TimeWindow& window,
                                    clink::TriggerContext<clink::TimeWindow>& ctx) override {
        (void)window.start;
        (void)window.end;
        (void)ctx.current_watermark();
        (void)ctx.current_processing_time();
        return clink::TriggerResult::Continue;
    }
    clink::TriggerResult on_event_time(std::int64_t,
                                       const clink::TimeWindow&,
                                       clink::TriggerContext<clink::TimeWindow>&) override {
        return clink::TriggerResult::Fire;
    }
    clink::TriggerResult on_processing_time(std::int64_t,
                                            const clink::TimeWindow&,
                                            clink::TriggerContext<clink::TimeWindow>&) override {
        return clink::TriggerResult::Continue;
    }
    void clear(const clink::TimeWindow&, clink::TriggerContext<clink::TimeWindow>&) override {}
    [[nodiscard]] bool is_stateful() const noexcept override { return false; }
    [[nodiscard]] std::string snapshot_state() const override { return {}; }
    void restore_state(std::string_view) override {}
};

class EveryEvictorHook final : public clink::Evictor<std::int64_t, clink::TimeWindow> {
public:
    void evict_before(std::vector<clink::Record<std::int64_t>>&,
                      std::int64_t,
                      const clink::TimeWindow&,
                      clink::TriggerContext<clink::TimeWindow>&) override {}
    void evict_after(std::vector<clink::Record<std::int64_t>>&,
                     std::int64_t,
                     const clink::TimeWindow&,
                     clink::TriggerContext<clink::TimeWindow>&) override {}
};

// --- Dead-letter queue -------------------------------------------------------

class EveryDeadLetterQueueHook final : public clink::DeadLetterQueue {
public:
    void report(const clink::BadRecord& rec) override {
        (void)rec.payload;
        (void)rec.error;
        (void)rec.connector;
        (void)rec.direction;
        (void)rec.location;
    }
};

// --- StateBackend: the key-value core a custom backend implements ------------
// (abstract on purpose: the remaining virtuals have defaults or are internal
// to the engine's own backends; a consumer backend overrides these three.)

class KeyValueBackendCore : public clink::StateBackend {
public:
    void put(clink::OperatorId, KeyView, ValueView) override {}
    std::optional<Value> get(clink::OperatorId, KeyView) const override { return std::nullopt; }
    void erase(clink::OperatorId, KeyView) override {}
};

[[maybe_unused]] void instantiate() {
    (void)std::make_shared<EveryOperatorHook>();
    (void)std::make_shared<EverySourceHook>();
    (void)std::make_shared<EverySinkHook>();
    (void)std::make_shared<EveryCoOperatorHook>();
    (void)std::make_shared<EveryCommittingSinkVerb>(0U);
    (void)std::make_shared<EveryProcessFunctionHook>();
    (void)std::make_shared<EveryKeyedProcessFunctionHook>();
    (void)std::make_shared<EveryCoProcessFunctionHook>();
    (void)std::make_shared<EveryKeyedCoProcessFunctionHook>();
    (void)std::make_shared<EveryBroadcastProcessFunctionHook>();
    (void)std::make_shared<EveryProcessWindowFunctionHook>();
    (void)std::make_shared<EveryAsyncKeyedProcessFunctionHook>();
    (void)std::make_shared<EveryAsyncKeyedCoProcessFunctionHook>();
    (void)std::make_unique<EveryWatermarkStrategyHook>();
    (void)std::make_unique<EveryTriggerHook>();
    (void)std::make_unique<EveryEvictorHook>();
    (void)std::make_shared<EveryDeadLetterQueueHook>();
}

}  // namespace
